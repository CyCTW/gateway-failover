// Reconnect schedule, flap detection (spec §10.1) and recovery-mode decision (§10.4).
#pragma once

#include <deque>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "gw/types.hpp"

namespace gw {

struct ReconnectPolicy {
  TimeMs fast_phase_end = 10'000;   // 0-10s: primary endpoint, exponential backoff
  TimeMs alert_phase_end = 60'000;  // 10-60s: rotate endpoints, alert
  std::vector<TimeMs> backoff = {0, 1'000, 2'000, 4'000};
  TimeMs alert_retry = 2'000;
  TimeMs incident_retry = 30'000;   // >60s: incident, retry every 30s until close
  double jitter = 0.2;              // +/- fraction applied to each delay
  int max_flaps = 3;                // disconnects within flap_window => flapping
  TimeMs flap_window = 600'000;
  uint32_t seed = 42;
};

enum class ConnPhase { Connected, Fast, Alert, Incident, Stopped };
const char* to_string(ConnPhase p);

class ConnectionManager {
 public:
  ConnectionManager(ReconnectPolicy policy, std::vector<std::string> endpoints);

  // `count_flap` is false for the initial connect after a process start.
  void on_disconnected(TimeMs now, bool count_flap = true);
  void on_connected(TimeMs now);
  void on_login_rejected();  // stop retrying: avoid locking the account
  void attempt_failed(TimeMs now);

  // Endpoint to try now, if an attempt is due.
  std::optional<std::string> due_attempt(TimeMs now);

  ConnPhase phase(TimeMs now) const;
  bool flapping(TimeMs now) const;
  TimeMs outage(TimeMs now) const { return connected_ ? 0 : now - outage_start_; }
  TimeMs outage_start() const { return outage_start_; }
  TimeMs last_outage() const { return last_outage_; }
  int attempts() const { return attempts_; }
  const ReconnectPolicy& policy() const { return policy_; }

 private:
  TimeMs jittered(TimeMs d);

  ReconnectPolicy policy_;
  std::vector<std::string> endpoints_;
  std::mt19937 rng_;
  bool connected_ = false;
  bool stopped_ = false;
  TimeMs outage_start_ = 0;
  TimeMs last_outage_ = 0;
  TimeMs next_attempt_ = 0;
  int attempts_ = 0;
  size_t rotate_ = 0;
  std::deque<TimeMs> disconnects_;
};

enum class RecoveryAction : uint8_t { Resume = 0, CancelThenResume = 1, Halt = 2 };
const char* to_string(RecoveryAction a);

struct RecoveryPolicy {
  RecoveryAction default_action = RecoveryAction::Resume;
  TimeMs max_outage_for_resume = 5'000;
  double max_price_move = 0.005;  // 0.5%
  bool require_clean_reconcile = true;
  RecoveryAction escalate_action = RecoveryAction::CancelThenResume;
  TimeMs incident_outage = 60'000;  // §10.3: beyond this, treat as full restart -> manual release
};

struct RecoveryFacts {
  TimeMs outage = 0;
  bool reconcile_clean = true;
  bool flapping = false;
  bool price_check_enabled = false;
  std::optional<double> price_move;  // nullopt with check enabled = unknown => escalate
  bool strategy_continuous = true;
  bool market_open = true;
};

// Conditions are combined by taking the most conservative outcome.
RecoveryAction decide_recovery(const RecoveryPolicy& p, const RecoveryFacts& f);

}  // namespace gw
