// Kill switch (spec §12) and watchdog (§13).
//
// The kill switch is deliberately stateless: it never reads gateway memory or the WAL.
// Everything comes from broker queries over its own session (ideally second credentials,
// another host, another line). It only cancels; flattening positions is a separate,
// manual step. Running it twice is safe.
#pragma once

#include <functional>
#include <string>
#include <vector>

#include "gw/broker.hpp"
#include "gw/safety.hpp"

namespace gw {

struct KillSwitchConfig {
  enum class Scope { Strategy, Account, All };
  Scope scope = Scope::Account;
  std::vector<std::string> accounts;  // Account scope uses the first; All uses every one
  int gateway_id = 1;                 // Strategy scope: ClOrdID prefix to match
  std::string strategy_code;          // Strategy scope
  std::string channel = "API";
  bool include_other_channels = false;  // also cancel orders entered via WEB/PHONE etc.
  std::vector<std::string> endpoints = {"primary"};
  int max_attempts_per_order = 3;  // cancel rejects are recorded, not retried forever
  TimeMs verify_interval = 500;
  TimeMs throttle_wait = 50;
  int max_rounds = 40;
};

struct KillSwitchReport {
  bool connected = false;
  bool success = false;  // two consecutive empty open-order queries
  int rounds = 0;
  int cancels_sent = 0;
  std::vector<std::string> cancel_rejects;
  std::vector<OpenOrderInfo> residual;  // still working at the end
  PositionMap positions;                // for the operator; never flattened automatically
  std::string summary() const;
};

class KillSwitch {
 public:
  KillSwitch(KillSwitchConfig cfg, BrokerAdapter& session, AlertSink& alerts, MarketData* market = nullptr);

  // isolate: stop the source (kill the gateway process or trip the shared kill flag).
  // sleep:   wait between rounds (real time in production, sim time in tests).
  KillSwitchReport run(const std::function<void()>& isolate, const std::function<void(TimeMs)>& sleep);

 private:
  bool in_scope(const OpenOrderInfo& o) const;
  std::vector<OpenOrderInfo> query_targets(bool* ok);

  KillSwitchConfig cfg_;
  BrokerAdapter& s_;
  AlertSink& alerts_;
  MarketData* market_;
  uint64_t next_req_ = 1;
};

struct WatchdogConfig {
  TimeMs heartbeat_timeout = 3'000;
  bool kill_switch_on_loss = false;  // set for high-risk strategies (spec §13)
};

class Watchdog {
 public:
  Watchdog(WatchdogConfig cfg, const Heartbeat& hb, const Clock& clock, AlertSink& alerts,
           std::function<void()> fire_kill_switch, std::function<void()> restart_gateway);
  void poll();
  bool fired() const { return fired_; }

 private:
  WatchdogConfig cfg_;
  const Heartbeat& hb_;
  const Clock& clock_;
  AlertSink& alerts_;
  std::function<void()> kill_;
  std::function<void()> restart_;
  bool fired_ = false;
  TimeMs armed_at_;
};

}  // namespace gw
