// Gateway: connection, order entry, WAL, state machine, reconciliation, recovery and
// mass cancel (spec §4, §8-§11). Single-threaded and poll-driven: the owner calls
// poll() in its event loop. All time comes from Clock, so tests are deterministic.
#pragma once

#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "gw/broker.hpp"
#include "gw/clordid.hpp"
#include "gw/connection.hpp"
#include "gw/order_book.hpp"
#include "gw/reconciler.hpp"
#include "gw/safety.hpp"
#include "gw/strategy_channel.hpp"
#include "gw/wal.hpp"

namespace gw {

struct StrategyConfig {
  std::string name;
  std::string code;  // 3-char ClOrdID short code
  RecoveryPolicy recovery;
  int64_t max_notional = INT64_MAX;  // risk limit on reserved notional (price * leaves)
  TimeMs heartbeat_timeout = 0;      // 0 = strategy liveness not monitored
  bool cancel_on_strategy_loss = true;
  bool high_risk = false;            // watchdog fires the kill switch before restarting
};

struct GatewayConfig {
  int gateway_id = 1;
  std::string account;
  std::string channel = "API";  // our order-entry channel as the broker labels it
  std::vector<std::string> endpoints = {"primary"};
  std::vector<StrategyConfig> strategies;
  ReconnectPolicy reconnect;
  TimeMs replay_silence_timeout = 2'000;
  int replay_max_retries = 3;
  TimeMs replay_retry_delay = 500;
  TimeMs recheck_delay = 1'000;  // "wait one more round" before re-replaying (3a/3b/5)
  TimeMs cancel_verify_interval = 500;
  int max_cancel_attempts = 3;
  PositionMap opening_positions;  // start-of-day positions for the §9.4 account check
  int reject_burst = 20;          // broker degradation: this many rejects ...
  TimeMs reject_window = 1'000;   // ... within this window halts new orders
  std::function<bool(TimeMs)> market_open;  // optional market-session predicate
};

struct MassCancelScope {
  enum class Kind { All, Strategy, Orders };
  Kind kind = Kind::All;
  std::string code;              // Strategy
  std::vector<std::string> ids;  // Orders
  std::string id() const;
  static MassCancelScope parse(const std::string& id);
  static MassCancelScope all() { return {}; }
  static MassCancelScope strategy(const std::string& code) { return {Kind::Strategy, code, {}}; }
  static MassCancelScope orders(std::vector<std::string> ids) { return {Kind::Orders, "", std::move(ids)}; }
};

enum class GwState { Init, Recovering, Active, Halted };
const char* to_string(GwState s);

struct SubmitResult {
  bool ok = false;
  std::string cl_ord_id;
  std::string error;
};

struct OrderParams {
  std::string symbol;
  Side side = Side::Buy;
  Price price = 0;
  Qty qty = 0;
  std::string tif = "DAY";
  std::string parent_id, algo_tag, risk_tag;
};

struct GatewayDeps {
  Clock* clock = nullptr;
  BrokerAdapter* broker = nullptr;
  StrategyChannel* channel = nullptr;
  AlertSink* alerts = nullptr;
  KillFlag* kill_flag = nullptr;      // optional
  Heartbeat* heartbeat = nullptr;     // optional
  MarketData* market = nullptr;       // optional: enables the price-move recovery condition
};

class Gateway {
 public:
  Gateway(GatewayConfig cfg, const std::string& wal_path, GatewayDeps deps);

  // Replays the WAL without side effects, then starts recovery (spec §8).
  void start();
  void poll();

  SubmitResult submit_new(const std::string& strategy, const OrderParams& p);
  SubmitResult submit_cancel(const std::string& strategy, const std::string& cl_ord_id);
  SubmitResult submit_replace(const std::string& strategy, const std::string& cl_ord_id, Price px, Qty qty);
  void strategy_heartbeat(const std::string& strategy);

  void request_mass_cancel(const MassCancelScope& scope);
  bool operator_release();  // clears an account halt after manual review
  bool operator_release_strategy(const std::string& strategy);

  GwState state() const { return state_; }
  bool recovering() const { return recovering_; }
  bool link_up() const { return link_up_; }
  bool account_halted() const { return account_halted_; }
  const std::string& halt_reason() const { return halt_reason_; }
  bool trading_enabled(const std::string& strategy) const;
  bool mass_cancel_active() const;
  const OrderBook& book() const { return book_; }
  const ClOrdIdCodec& codec() const { return codec_; }
  const ReconcileResult& last_reconcile() const { return last_reconcile_; }
  int replay_rounds() const { return total_replay_rounds_; }
  uint64_t next_clordid_seq() const { return next_seq_; }
  size_t wal_truncated_bytes() const { return wal_.truncated_bytes(); }

 private:
  struct StrategyRt {
    StrategyConfig cfg;
    bool enabled = false;  // last state announced to the strategy
    bool awaiting_release = false;
    RecoveryAction pre_action = RecoveryAction::Resume;
    bool alive = true;
    bool lost_during_outage = false;
    TimeMs last_hb = 0;
    std::vector<Fill> recovered_fills;
  };

  enum class RecPhase { Idle, WaitConnect, Replaying, WaitRecheck, WaitRetry };
  struct Recovery {
    RecPhase phase = RecPhase::Idle;
    bool restart = false;
    int round = 0;
    int failures = 0;
    std::vector<ExecReport> replay;
    std::vector<ExecReport> buffer;  // live reports received while replaying
    TimeMs last_activity = 0;
    TimeMs recheck_at = 0;
    bool got_end = false;
    int64_t expected = -1;
    TimeMs outage = 0;
    std::map<std::string, Price> prices_at_disconnect;
  };

  struct CancelJob {
    MassCancelScope scope;
    enum class Phase { Round1, Round2, Verify, Done } phase = Phase::Round1;
    std::deque<CancelReq> outbox;
    std::map<std::string, int> attempts;
    std::set<std::string> gave_up;
    int empty_streak = 0;
    TimeMs next_verify = 0;
  };

  // WAL
  uint64_t wal_append(RecordType t, const std::string& payload);
  void replay_wal();
  void note_id(const std::string& id);
  void write_correction(const Correction& c);
  void write_control(ControlKind k, const std::string& scope, const std::string& text = {});

  // events
  void handle_event(const BrokerEvent& ev);
  void process_live_report(const ExecReport& r);
  void classify_broker_order(const std::string& cl_ord_id);
  void after_apply(const std::string& cl_ord_id, const ApplyResult& res);
  void note_reject();

  // connection / recovery
  void on_link_down(const std::string& reason);
  void on_link_up();
  void try_connect();
  void begin_recovery(bool restart);
  void start_replay_round(bool new_round);
  void progress_recovery();
  void replay_failed(const std::string& why);
  void finish_replay_round();
  void import_facts(const OrderBook& shadow, const OrderDiff& d);
  void fallback_to_queries();
  void complete_recovery(bool clean);
  RecoveryFacts make_facts(const StrategyRt& s, bool clean) const;
  void halt_account(const std::string& reason);

  // strategies
  StrategyRt* strategy(const std::string& name);
  bool may_trade(const StrategyRt& s) const;
  void refresh_strategy_states();
  void push_fill(StrategyRt& s, const Fill& f, bool recovered);
  void send_snapshot(StrategyRt& s);
  void check_strategy_heartbeats();

  // mass cancel
  void start_cancel_job(const MassCancelScope& scope, bool resumed);
  bool job_covers(const CancelJob& j, const StrategyRt& s) const;
  bool scope_matches(const MassCancelScope& sc, const std::string& cl_ord_id, const std::string& channel) const;
  void enqueue_cancel(CancelJob& j, const std::string& cl_ord_id, const std::string& broker_order_id);
  void progress_cancel_jobs();
  void drain(CancelJob& j);
  std::string new_request_id(const std::string& cl_ord_id);

  void alert(AlertLevel lvl, const std::string& code, const std::string& msg);
  TimeMs now() const { return clock_.now(); }

  GatewayConfig cfg_;
  Clock& clock_;
  BrokerAdapter& broker_;
  StrategyChannel& channel_;
  AlertSink& alerts_;
  KillFlag* kill_flag_;
  Heartbeat* heartbeat_;
  MarketData* market_;

  Wal wal_;
  OrderBook book_;
  ClOrdIdCodec codec_;
  ConnectionManager conn_;
  ConnPhase last_phase_ = ConnPhase::Connected;
  std::vector<StrategyRt> strategies_;

  GwState state_ = GwState::Init;
  bool recovering_ = false;
  bool link_up_ = false;
  bool account_halted_ = false;
  std::string halt_reason_;
  uint64_t next_seq_ = 1;
  TimeMs last_wal_ts_ = 0;
  uint64_t event_seq_ = 0;
  std::set<std::string> pushed_fills_;
  std::set<std::string> wal_open_jobs_;  // mass cancels started but not finished before a crash
  Recovery rec_;
  ReconcileResult last_reconcile_;
  int total_replay_rounds_ = 0;
  std::vector<CancelJob> jobs_;
  std::deque<TimeMs> reject_times_;
  std::map<std::string, Price> last_prices_;
};

}  // namespace gw
