#include "gw/connection.hpp"

#include <algorithm>

namespace gw {

const char* to_string(ConnPhase p) {
  switch (p) {
    case ConnPhase::Connected: return "Connected";
    case ConnPhase::Fast: return "Fast";
    case ConnPhase::Alert: return "Alert";
    case ConnPhase::Incident: return "Incident";
    case ConnPhase::Stopped: return "Stopped";
  }
  return "?";
}

const char* to_string(RecoveryAction a) {
  switch (a) {
    case RecoveryAction::Resume: return "resume";
    case RecoveryAction::CancelThenResume: return "cancel_then_resume";
    case RecoveryAction::Halt: return "halt";
  }
  return "?";
}

ConnectionManager::ConnectionManager(ReconnectPolicy policy, std::vector<std::string> endpoints)
    : policy_(std::move(policy)), endpoints_(std::move(endpoints)), rng_(policy_.seed) {
  if (endpoints_.empty()) endpoints_.push_back("primary");
}

void ConnectionManager::on_disconnected(TimeMs now, bool count_flap) {
  if (!connected_ && outage_start_ != 0) return;  // already in an outage
  connected_ = false;
  outage_start_ = now;
  next_attempt_ = now;  // first retry is immediate
  attempts_ = 0;
  rotate_ = 0;
  if (count_flap) disconnects_.push_back(now);
}

void ConnectionManager::on_connected(TimeMs now) {
  connected_ = true;
  last_outage_ = now - outage_start_;
}

void ConnectionManager::on_login_rejected() { stopped_ = true; }

ConnPhase ConnectionManager::phase(TimeMs now) const {
  if (stopped_) return ConnPhase::Stopped;
  if (connected_) return ConnPhase::Connected;
  const TimeMs o = now - outage_start_;
  if (o < policy_.fast_phase_end) return ConnPhase::Fast;
  if (o < policy_.alert_phase_end) return ConnPhase::Alert;
  return ConnPhase::Incident;
}

TimeMs ConnectionManager::jittered(TimeMs d) {
  if (d <= 0 || policy_.jitter <= 0) return d;
  std::uniform_real_distribution<double> u(-policy_.jitter, policy_.jitter);
  return std::max<TimeMs>(0, static_cast<TimeMs>(static_cast<double>(d) * (1.0 + u(rng_))));
}

std::optional<std::string> ConnectionManager::due_attempt(TimeMs now) {
  if (connected_ || stopped_ || now < next_attempt_) return std::nullopt;
  const ConnPhase ph = phase(now);
  if (ph == ConnPhase::Fast) return endpoints_.front();
  return endpoints_[rotate_++ % endpoints_.size()];  // primary, backup line, backup host ...
}

void ConnectionManager::attempt_failed(TimeMs now) {
  ++attempts_;
  const ConnPhase ph = phase(now);
  TimeMs delay;
  if (ph == ConnPhase::Fast) {
    const size_t i = std::min<size_t>(attempts_, policy_.backoff.size() - 1);
    delay = policy_.backoff[i];
  } else if (ph == ConnPhase::Alert) {
    delay = policy_.alert_retry;
  } else {
    delay = policy_.incident_retry;
  }
  next_attempt_ = now + jittered(delay);
}

bool ConnectionManager::flapping(TimeMs now) const {
  int n = 0;
  for (TimeMs t : disconnects_)
    if (now - t <= policy_.flap_window) ++n;
  return n >= policy_.max_flaps;
}

RecoveryAction decide_recovery(const RecoveryPolicy& p, const RecoveryFacts& f) {
  auto worst = [](RecoveryAction a, RecoveryAction b) { return static_cast<int>(a) >= static_cast<int>(b) ? a : b; };
  RecoveryAction a = p.default_action;
  if (p.require_clean_reconcile && !f.reconcile_clean) a = RecoveryAction::Halt;
  if (f.flapping) a = RecoveryAction::Halt;
  if (!f.market_open) a = RecoveryAction::Halt;
  if (f.outage >= p.incident_outage) a = RecoveryAction::Halt;
  if (f.outage > p.max_outage_for_resume) a = worst(a, p.escalate_action);
  if (f.price_check_enabled && (!f.price_move || *f.price_move > p.max_price_move)) a = worst(a, p.escalate_action);
  if (!f.strategy_continuous) a = worst(a, p.escalate_action);
  return a;
}

}  // namespace gw
