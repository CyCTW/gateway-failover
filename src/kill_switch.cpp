#include "gw/kill_switch.hpp"

#include <algorithm>
#include <cmath>
#include <map>

#include "gw/clordid.hpp"

namespace gw {

std::string KillSwitchReport::summary() const {
  std::string s = success ? "kill switch OK" : "kill switch INCOMPLETE";
  s += ": rounds=" + std::to_string(rounds) + " cancels=" + std::to_string(cancels_sent) +
       " rejects=" + std::to_string(cancel_rejects.size()) + " residual=" + std::to_string(residual.size());
  for (const auto& [sym, q] : positions) s += " pos[" + sym + "]=" + std::to_string(q);
  return s;
}

KillSwitch::KillSwitch(KillSwitchConfig cfg, BrokerAdapter& session, AlertSink& alerts, MarketData* market)
    : cfg_(std::move(cfg)), s_(session), alerts_(alerts), market_(market) {}

bool KillSwitch::in_scope(const OpenOrderInfo& o) const {
  if (!cfg_.include_other_channels && !o.channel.empty() && o.channel != cfg_.channel) return false;
  if (cfg_.scope != KillSwitchConfig::Scope::Strategy) return true;
  auto p = ClOrdIdCodec::parse(o.cl_ord_id);
  return p && p->gateway_id == cfg_.gateway_id && p->code == cfg_.strategy_code;
}

std::vector<OpenOrderInfo> KillSwitch::query_targets(bool* ok) {
  *ok = true;
  std::vector<OpenOrderInfo> out;
  std::vector<std::string> accounts = cfg_.accounts;
  if (cfg_.scope != KillSwitchConfig::Scope::All && !accounts.empty()) accounts.resize(1);
  for (const auto& acc : accounts) {
    auto q = s_.query_open_orders(acc);
    if (!q) {
      *ok = false;
      continue;
    }
    for (const auto& o : *q)
      if (in_scope(o)) out.push_back(o);
  }
  return out;
}

KillSwitchReport KillSwitch::run(const std::function<void()>& isolate, const std::function<void(TimeMs)>& sleep) {
  KillSwitchReport rep;
  alerts_.alert(AlertLevel::Critical, "kill_switch_start", "isolating source and cancelling");
  if (isolate) isolate();  // step 1: stop new orders at the source

  if (!s_.connected()) {
    for (const auto& ep : cfg_.endpoints)
      if (s_.connect(ep) == LoginResult::Ok) break;
  }
  rep.connected = s_.connected();
  if (!rep.connected) {
    alerts_.alert(AlertLevel::Critical, "kill_switch_no_link",
                  "cannot reach broker: use broker risk desk / phone procedure");
    return rep;
  }

  std::map<std::string, int> attempts;
  int empty_streak = 0;
  bool batch_done = false;
  std::vector<OpenOrderInfo> last;
  while (rep.rounds < cfg_.max_rounds) {
    ++rep.rounds;
    bool ok = false;
    last = query_targets(&ok);  // step 2
    if (!ok) {
      empty_streak = 0;
      sleep(cfg_.verify_interval);
      continue;
    }
    if (last.empty()) {
      if (++empty_streak >= 2) {  // step 4: two consecutive empty queries
        rep.success = true;
        break;
      }
      sleep(cfg_.verify_interval);
      continue;
    }
    empty_streak = 0;

    // step 3: batch when available and safe for the scope, else one by one by risk.
    const bool can_batch = s_.capabilities().batch_cancel && cfg_.scope != KillSwitchConfig::Scope::Strategy &&
                           !cfg_.include_other_channels;
    if (can_batch && !batch_done) {
      std::vector<std::string> accounts = cfg_.accounts;
      if (cfg_.scope != KillSwitchConfig::Scope::All && !accounts.empty()) accounts.resize(1);
      for (const auto& acc : accounts) s_.mass_cancel(acc);
      batch_done = true;
    } else {
      auto risk = [&](const OpenOrderInfo& o) {
        double dist = 0;
        if (market_)
          if (auto p = market_->last_price(o.symbol); p && *p > 0) dist = std::fabs(double(o.price - *p)) / double(*p);
        return dist;
      };
      std::sort(last.begin(), last.end(), [&](const OpenOrderInfo& a, const OpenOrderInfo& b) {
        const double da = risk(a), db = risk(b);
        if (da != db) return da < db;
        return a.leaves_qty * a.price > b.leaves_qty * b.price;
      });
      bool progressed = false;
      for (const auto& o : last) {
        if (attempts[o.cl_ord_id] >= cfg_.max_attempts_per_order) continue;
        int waits = 0;
        while (!s_.can_send() && waits++ < 100) sleep(cfg_.throttle_wait);  // respect rate limits
        if (!s_.can_send()) break;
        CancelReq r{"KS" + std::to_string(next_req_++), o.cl_ord_id, o.broker_order_id};
        if (s_.send_cancel(r)) {
          ++attempts[o.cl_ord_id];
          ++rep.cancels_sent;
          progressed = true;
        }
      }
      if (!progressed) {
        alerts_.alert(AlertLevel::Critical, "kill_switch_stuck", "orders survive repeated cancels");
        break;
      }
    }
    for (const BrokerEvent& ev : s_.poll())
      if (ev.type == BrokerEventType::Report && ev.report.kind == ReportKind::CancelReject)
        rep.cancel_rejects.push_back(ev.report.cl_ord_id + ": " + ev.report.text);
    sleep(cfg_.verify_interval);
  }

  for (const BrokerEvent& ev : s_.poll())
    if (ev.type == BrokerEventType::Report && ev.report.kind == ReportKind::CancelReject)
      rep.cancel_rejects.push_back(ev.report.cl_ord_id + ": " + ev.report.text);
  if (!rep.success) rep.residual = last;
  // step 5: report residual orders and positions.
  for (const auto& acc : cfg_.accounts)
    if (auto p = s_.query_positions(acc))
      for (const auto& [sym, q] : *p) rep.positions[sym] += q;
  alerts_.alert(rep.success ? AlertLevel::Warn : AlertLevel::Critical, "kill_switch_done", rep.summary());
  return rep;
}

// ---------------------------------------------------------------- Watchdog

Watchdog::Watchdog(WatchdogConfig cfg, const Heartbeat& hb, const Clock& clock, AlertSink& alerts,
                   std::function<void()> fire_kill_switch, std::function<void()> restart_gateway)
    : cfg_(cfg), hb_(hb), clock_(clock), alerts_(alerts), kill_(std::move(fire_kill_switch)),
      restart_(std::move(restart_gateway)), armed_at_(clock.now()) {}

void Watchdog::poll() {
  const TimeMs now = clock_.now();
  const auto last = hb_.last_beat();
  const TimeMs ref = last ? *last : armed_at_;
  if (now - ref <= cfg_.heartbeat_timeout) {
    fired_ = false;  // re-arm once the gateway beats again
    return;
  }
  if (fired_) return;
  fired_ = true;
  alerts_.alert(AlertLevel::Critical, "gateway_heartbeat_lost", "last beat " + std::to_string(now - ref) + "ms ago");
  if (cfg_.kill_switch_on_loss && kill_) kill_();
  if (restart_) restart_();
}

}  // namespace gw
