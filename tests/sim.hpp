// Deterministic simulation harness: sim clock + mock broker + gateway.
#pragma once

#include <cstdio>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "gw/gateway.hpp"
#include "gw/kill_switch.hpp"
#include "gw/mock_broker.hpp"
#include "testing.hpp"

#ifndef GW_TEST_DIR
#define GW_TEST_DIR "."
#endif

namespace sim {

using namespace gw;

struct RecordingAlerts : AlertSink {
  struct A {
    AlertLevel level;
    std::string code, msg;
  };
  std::vector<A> all;
  bool verbose = false;
  void alert(AlertLevel l, const std::string& code, const std::string& msg) override {
    all.push_back({l, code, msg});
    if (verbose) std::fprintf(stderr, "      alert %s: %s\n", code.c_str(), msg.c_str());
  }
  int count(const std::string& code) const {
    int n = 0;
    for (const auto& a : all) n += a.code == code;
    return n;
  }
  bool has(const std::string& code) const { return count(code) > 0; }
};

struct FakeMarket : MarketData {
  std::map<std::string, Price> px;
  std::optional<Price> last_price(const std::string& s) const override {
    auto it = px.find(s);
    if (it == px.end()) return std::nullopt;
    return it->second;
  }
};

inline std::string test_path(const std::string& name) {
  std::filesystem::create_directories(GW_TEST_DIR "/wal");
  return std::string(GW_TEST_DIR "/wal/") + name;
}

struct Sim {
  SimClock clock;
  MockExchange ex{clock};
  RecordingAlerts alerts;
  InProcessStrategyChannel strat;
  MemoryKillFlag kill;
  MemoryHeartbeat hb;
  MarketData* market = nullptr;
  GatewayConfig cfg;
  std::string wal_path;
  std::unique_ptr<MockSession> session;
  std::unique_ptr<Gateway> gw;

  explicit Sim(const std::string& name) {
    wal_path = test_path(name + ".wal");
    std::remove(wal_path.c_str());
    cfg.gateway_id = 1;
    cfg.account = "ACC1";
    cfg.endpoints = {"primary", "backup-line", "backup-host"};
    StrategyConfig mm;
    mm.name = "market_maker";
    mm.code = "MMK";
    StrategyConfig tr;
    tr.name = "trend";
    tr.code = "TRD";
    cfg.strategies = {mm, tr};
  }

  StrategyConfig& strategy_cfg(const std::string& name) {
    for (auto& s : cfg.strategies)
      if (s.name == name) return s;
    throw std::runtime_error("no strategy " + name);
  }

  void boot() {
    session = ex.open_session("gateway", cfg.account, true);
    GatewayDeps d;
    d.clock = &clock;
    d.broker = session.get();
    d.channel = &strat;
    d.alerts = &alerts;
    d.kill_flag = &kill;
    d.heartbeat = &hb;
    d.market = market;
    gw = std::make_unique<Gateway>(cfg, wal_path, d);
    gw->start();
  }

  // Process dies: no clean shutdown, WAL stays as written (mmap'ed), link drops.
  void crash() {
    gw.reset();
    session.reset();
  }

  void step(TimeMs dt = 10) {
    clock.advance(dt);
    ex.step();
    if (gw) gw->poll();
  }

  template <class Pred>
  bool run_until(Pred pred, TimeMs max = 60'000, TimeMs dt = 10) {
    for (TimeMs t = 0; t < max; t += dt) {
      if (pred()) return true;
      step(dt);
    }
    return pred();
  }

  void run_for(TimeMs ms, TimeMs dt = 10) {
    for (TimeMs t = 0; t < ms; t += dt) step(dt);
  }

  bool recovered() const { return gw && !gw->recovering(); }
  bool active() const { return recovered() && gw->state() == GwState::Active; }

  void boot_and_recover() {
    boot();
    REQUIRE(run_until([&] { return active(); }));
    run_until([&] { return gw->trading_enabled("market_maker"); }, 5'000);
  }

  std::string order(const std::string& strategy, const std::string& sym, Side side, Price px, Qty qty) {
    auto r = gw->submit_new(strategy, OrderParams{sym, side, px, qty});
    if (!r.ok) throw t::Failure("submit failed: " + r.error);
    return r.cl_ord_id;
  }

  size_t exchange_open() const { return ex.open_orders(cfg.account).size(); }

  Qty pos(const std::string& owner, const std::string& sym) const {
    auto all = gw->book().positions();
    return all.count(owner) && all[owner].count(sym) ? all[owner][sym] : 0;
  }
};

}  // namespace sim
