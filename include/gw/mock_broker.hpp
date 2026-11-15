// In-process mock broker with fault injection (spec §14). MockExchange owns the
// authoritative order store and the day's report log; MockSession is one login.
#pragma once

#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "gw/broker.hpp"

namespace gw {

struct MockFaults {
  // live report delivery
  int drop_live = 0;            // drop the next N live reports
  bool duplicate_live = false;  // deliver every live report twice
  TimeMs live_delay = 0;        // delay live delivery
  bool omit_exec_id = false;    // fills carry no exec id

  // order entry
  bool drop_next_new = false;              // next new order is lost in transit
  bool disconnect_after_next_new = false;  // broker accepts the order, link drops before the ack
  int cancel_reject_next = 0;              // next N cancels rejected ("too late")
  int rate_limit_per_tick = 0;             // messages per session per step(); 0 = unlimited
  bool login_reject = false;

  // replay
  int replay_fail = 0;          // next N replay requests fail outright
  bool replay_shuffle = false;  // shuffled order
  uint32_t replay_seed = 7;
  bool replay_merge = false;    // drop intermediate Ack/Status/Replaced reports
  int replay_drop_tail = 0;     // incomplete replay (ReplayEnd still reports the full count)
  bool replay_no_end = false;   // no ReplayEnd marker at all
  std::function<bool(const ExecReport&)> replay_filter;  // false = omit from replay
  std::function<void(ExecReport&)> replay_mutate;        // tamper with replayed fields
};

class MockSession;

class MockExchange {
 public:
  explicit MockExchange(Clock& clock, BrokerCapabilities caps = {});
  ~MockExchange();

  std::unique_ptr<MockSession> open_session(const std::string& user, const std::string& account, bool subscribe_reports,
                                            const std::string& channel = "API");

  MockFaults& faults() { return faults_; }
  BrokerCapabilities& caps() { return caps_; }
  void set_endpoint_up(const std::string& endpoint, bool up);

  // Advance: deliver due reports, reset rate-limit counters.
  void step();

  // Market-side actions driven by tests.
  bool fill(const std::string& cl_ord_id, Qty qty, Price px);
  bool exchange_cancel(const std::string& cl_ord_id);
  // Order that reaches the account through another channel (spec §9.3 4c).
  std::string inject_external(const std::string& account, const std::string& symbol, Side side, Price px, Qty qty,
                              const std::string& channel = "WEB");
  // Order on OUR channel whose ClOrdID cannot be decoded (spec §9.3 4b).
  std::string inject_orphan(const std::string& account, const std::string& cl_ord_id, const std::string& symbol,
                            Side side, Price px, Qty qty);

  struct MOrder {
    std::string cl_ord_id, broker_order_id, account, symbol, channel, user;
    uint64_t session_id = 0;
    Side side = Side::Buy;
    Price price = 0;
    Qty qty = 0, cum = 0;
    OrdStatus status = OrdStatus::New;
    uint64_t mod_seq = 0;
  };
  const MOrder* order(const std::string& cl_ord_id) const;
  std::vector<const MOrder*> open_orders(const std::string& account) const;
  PositionMap positions(const std::string& account) const;
  const std::vector<ExecReport>& day_log() const { return day_log_; }
  size_t orders_received() const { return orders_.size(); }

 private:
  friend class MockSession;
  void attach(MockSession* s);
  void detach(MockSession* s);
  void on_link_drop(MockSession* s);

  bool handle_new(MockSession& s, const NewOrderReq& r);
  bool handle_cancel(MockSession& s, const CancelReq& r);
  bool handle_replace(MockSession& s, const ReplaceReq& r);
  bool handle_mass_cancel(MockSession& s, const std::string& account);
  bool handle_replay(MockSession& s);
  void cancel_order(MOrder& o, const std::string& request_id, MockSession* requester);

  ExecReport make_report(const MOrder& o, ReportKind kind) const;
  void emit(const ExecReport& r, MockSession* requester, TimeMs extra_delay = 0);

  Clock& clock_;
  BrokerCapabilities caps_;
  MockFaults faults_;
  std::map<std::string, MOrder> orders_;
  std::vector<ExecReport> day_log_;
  std::set<MockSession*> sessions_;
  std::set<std::string> down_endpoints_;
  uint64_t next_session_id_ = 1;
  uint64_t next_broker_id_ = 1;
  uint64_t next_exec_id_ = 1;
  uint64_t next_external_ = 1;
};

class MockSession : public BrokerAdapter {
 public:
  ~MockSession() override;

  BrokerCapabilities capabilities() const override { return ex_.caps_; }
  LoginResult connect(const std::string& endpoint) override;
  void disconnect() override;
  bool connected() const override { return connected_; }
  bool can_send() const override;
  bool send_new(const NewOrderReq& r) override;
  bool send_cancel(const CancelReq& r) override;
  bool send_replace(const ReplaceReq& r) override;
  bool mass_cancel(const std::string& account) override;
  bool request_replay() override;
  std::optional<std::vector<OpenOrderInfo>> query_open_orders(const std::string& account) override;
  std::optional<OrderQueryResult> query_order(const std::string& cl_ord_id) override;
  std::optional<PositionMap> query_positions(const std::string& account) override;
  std::vector<BrokerEvent> poll() override;

  // Test hooks.
  void drop_link();  // network failure: queued reports are lost
  uint64_t id() const { return id_; }
  const std::string& user() const { return user_; }
  const std::string& last_endpoint() const { return endpoint_; }
  int connect_attempts() const { return connect_attempts_; }
  int sent_messages() const { return sent_total_; }

 private:
  friend class MockExchange;
  MockSession(MockExchange& ex, uint64_t id, std::string user, std::string account, bool subscribe, std::string channel);
  bool consume_budget();

  struct Pending {
    TimeMs at;
    BrokerEvent ev;
  };
  MockExchange& ex_;
  uint64_t id_;
  std::string user_, account_, channel_;
  bool subscribe_;
  bool connected_ = false;
  std::string endpoint_;
  int sends_this_tick_ = 0;
  int sent_total_ = 0;
  int connect_attempts_ = 0;
  std::deque<Pending> queue_;
};

}  // namespace gw
