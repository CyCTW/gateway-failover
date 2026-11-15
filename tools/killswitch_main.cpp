// Kill switch CLI (spec §12.4 manual trigger).
//
//   killswitch --account ACC1 [--scope account|all|strategy:CODE] [--flag PATH] [--demo]
//
// v1 has no real broker adapter, so only --demo (in-process mock broker with a few
// working orders) actually runs. A real deployment links a broker adapter built with the
// kill switch's own credentials and passes it to gw::KillSwitch.
#include <chrono>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#include "gw/kill_switch.hpp"
#include "gw/mock_broker.hpp"

namespace {

struct StderrAlerts : gw::AlertSink {
  void alert(gw::AlertLevel lvl, const std::string& code, const std::string& msg) override {
    const char* l = lvl == gw::AlertLevel::Critical ? "CRIT" : lvl == gw::AlertLevel::Warn ? "WARN" : "INFO";
    std::fprintf(stderr, "[%s] %s: %s\n", l, code.c_str(), msg.c_str());
  }
};

int usage() {
  std::fprintf(stderr, "usage: killswitch --account ACC [--scope account|all|strategy:CODE] [--flag PATH] [--demo]\n");
  return 2;
}

}  // namespace

int main(int argc, char** argv) {
  gw::KillSwitchConfig cfg;
  std::string flag_path;
  bool demo = false;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto next = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
    if (a == "--account") {
      cfg.accounts.push_back(next());
    } else if (a == "--scope") {
      const std::string s = next();
      if (s == "account") cfg.scope = gw::KillSwitchConfig::Scope::Account;
      else if (s == "all") cfg.scope = gw::KillSwitchConfig::Scope::All;
      else if (s.rfind("strategy:", 0) == 0) {
        cfg.scope = gw::KillSwitchConfig::Scope::Strategy;
        cfg.strategy_code = s.substr(9);
      } else return usage();
    } else if (a == "--flag") {
      flag_path = next();
    } else if (a == "--demo") {
      demo = true;
    } else {
      return usage();
    }
  }
  if (cfg.accounts.empty()) return usage();

  StderrAlerts alerts;
  auto isolate = [&] {
    if (!flag_path.empty()) {
      gw::FileKillFlag(flag_path).trip("killswitch CLI");
      std::fprintf(stderr, "shared kill flag set: %s\n", flag_path.c_str());
    }
  };

  if (!demo) {
    std::fprintf(stderr, "no broker adapter linked in this build; use --demo\n");
    isolate();  // still stop the gateway at the source
    return 3;
  }

  gw::SimClock clock;
  gw::BrokerCapabilities caps;
  caps.cross_session_cancel = true;
  gw::MockExchange ex(clock, caps);
  auto trader = ex.open_session("gateway", cfg.accounts[0], false);
  trader->connect("primary");
  for (int i = 0; i < 4; ++i) {
    gw::NewOrderReq r;
    r.cl_ord_id = "01MMK000000" + std::to_string(i + 1);
    r.account = cfg.accounts[0];
    r.symbol = "2330";
    r.price = 100 + i;
    r.qty = 10;
    trader->send_new(r);
  }
  auto ks_session = ex.open_session("ops", cfg.accounts[0], false);
  gw::KillSwitch ks(cfg, *ks_session, alerts);
  auto rep = ks.run(isolate, [&](gw::TimeMs ms) {
    clock.advance(ms);
    ex.step();
  });
  std::cout << rep.summary() << "\n";
  return rep.success ? 0 : 1;
}
