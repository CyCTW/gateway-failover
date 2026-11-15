#include "gw/mock_broker.hpp"

#include <algorithm>
#include <random>

namespace gw {

// ================================================================ MockExchange

MockExchange::MockExchange(Clock& clock, BrokerCapabilities caps) : clock_(clock), caps_(caps) {}

MockExchange::~MockExchange() = default;

std::unique_ptr<MockSession> MockExchange::open_session(const std::string& user, const std::string& account,
                                                        bool subscribe_reports, const std::string& channel) {
  std::unique_ptr<MockSession> s(new MockSession(*this, next_session_id_++, user, account, subscribe_reports, channel));
  attach(s.get());
  return s;
}

void MockExchange::attach(MockSession* s) { sessions_.insert(s); }
void MockExchange::detach(MockSession* s) { sessions_.erase(s); }

void MockExchange::set_endpoint_up(const std::string& endpoint, bool up) {
  if (up)
    down_endpoints_.erase(endpoint);
  else
    down_endpoints_.insert(endpoint);
}

void MockExchange::step() {
  for (MockSession* s : sessions_) s->sends_this_tick_ = 0;
}

ExecReport MockExchange::make_report(const MOrder& o, ReportKind kind) const {
  ExecReport r;
  r.kind = kind;
  r.cl_ord_id = o.cl_ord_id;
  r.broker_order_id = o.broker_order_id;
  r.account = o.account;
  r.symbol = o.symbol;
  r.side = o.side;
  r.price = o.price;
  r.order_qty = o.qty;
  r.cum_qty = o.cum;
  r.transact_time = clock_.now();
  r.mod_seq = o.mod_seq;
  r.channel = o.channel;
  return r;
}

void MockExchange::emit(const ExecReport& r, MockSession* requester, TimeMs extra_delay) {
  const bool is_request_answer = r.kind == ReportKind::CancelReject || r.kind == ReportKind::ReplaceReject;
  if (!is_request_answer) day_log_.push_back(r);  // the broker's replayable record of the day

  for (MockSession* s : sessions_) {
    if (!s->connected_) continue;
    const bool wants = (s->subscribe_ && s->account_ == r.account && !is_request_answer) || s == requester;
    if (!wants) continue;
    if (faults_.drop_live > 0) {
      --faults_.drop_live;
      continue;
    }
    BrokerEvent ev;
    ev.type = BrokerEventType::Report;
    ev.source = ReportSource::Live;
    ev.report = r;
    const TimeMs at = clock_.now() + faults_.live_delay + extra_delay;
    s->queue_.push_back({at, ev});
    if (faults_.duplicate_live) s->queue_.push_back({at, ev});
  }
}

bool MockExchange::handle_new(MockSession& s, const NewOrderReq& r) {
  if (faults_.drop_next_new) {
    faults_.drop_next_new = false;
    return true;  // the transport "accepted" it; the broker never sees it
  }
  if (orders_.count(r.cl_ord_id)) {
    MOrder dup;
    dup.cl_ord_id = r.cl_ord_id;
    dup.account = r.account;
    ExecReport rej = make_report(dup, ReportKind::ReplaceReject);
    rej.text = "duplicate ClOrdID";
    emit(rej, &s);
    return true;
  }
  MOrder o;
  o.cl_ord_id = r.cl_ord_id;
  o.broker_order_id = "B" + std::to_string(next_broker_id_++);
  o.account = r.account;
  o.symbol = r.symbol;
  o.channel = s.channel_;
  o.user = s.user_;
  o.session_id = s.id_;
  o.side = r.side;
  o.price = r.price;
  o.qty = r.qty;
  o.status = OrdStatus::New;
  auto& stored = orders_[o.cl_ord_id] = o;
  emit(make_report(stored, ReportKind::Ack), &s);
  if (faults_.disconnect_after_next_new) {
    faults_.disconnect_after_next_new = false;
    s.drop_link();
  }
  return true;
}

void MockExchange::cancel_order(MOrder& o, const std::string& request_id, MockSession* requester) {
  o.status = OrdStatus::Canceled;
  ExecReport r = make_report(o, ReportKind::Canceled);
  r.request_id = request_id;
  emit(r, requester);
}

bool MockExchange::handle_cancel(MockSession& s, const CancelReq& r) {
  auto it = orders_.find(r.cl_ord_id);
  auto reject = [&](const std::string& why) {
    MOrder tmp;
    tmp.cl_ord_id = r.cl_ord_id;
    tmp.account = s.account_;
    if (it != orders_.end()) tmp = it->second;
    ExecReport rej = make_report(tmp, ReportKind::CancelReject);
    rej.request_id = r.request_id;
    rej.text = why;
    emit(rej, &s);
    return true;
  };
  if (it == orders_.end()) return reject("unknown order");
  MOrder& o = it->second;
  if (is_terminal(o.status)) return reject("too late: order is " + std::string(to_string(o.status)));
  if (faults_.cancel_reject_next > 0) {
    --faults_.cancel_reject_next;
    return reject("too late to cancel");
  }
  if (o.user != s.user_ && !caps_.cross_session_cancel) return reject("order belongs to another session");
  cancel_order(o, r.request_id, &s);
  return true;
}

bool MockExchange::handle_replace(MockSession& s, const ReplaceReq& r) {
  auto it = orders_.find(r.cl_ord_id);
  if (it == orders_.end() || is_terminal(it->second.status) || r.new_qty < it->second.cum) {
    MOrder tmp;
    tmp.cl_ord_id = r.cl_ord_id;
    tmp.account = s.account_;
    ExecReport rej = make_report(it == orders_.end() ? tmp : it->second, ReportKind::ReplaceReject);
    rej.request_id = r.request_id;
    emit(rej, &s);
    return true;
  }
  MOrder& o = it->second;
  o.price = r.new_price;
  o.qty = r.new_qty;
  ++o.mod_seq;
  ExecReport rep = make_report(o, ReportKind::Replaced);
  rep.request_id = r.request_id;
  emit(rep, &s);
  if (o.cum >= o.qty) o.status = OrdStatus::Filled;
  return true;
}

bool MockExchange::handle_mass_cancel(MockSession& s, const std::string& account) {
  if (!caps_.batch_cancel) return false;
  for (auto& [id, o] : orders_)
    if (o.account == account && o.channel == s.channel_ && !is_terminal(o.status)) cancel_order(o, "", &s);
  return true;
}

bool MockExchange::handle_replay(MockSession& s) {
  if (faults_.replay_fail > 0) {
    --faults_.replay_fail;
    return false;
  }
  std::vector<ExecReport> out;
  for (const ExecReport& r : day_log_)
    if (r.account == s.account_) out.push_back(r);
  if (faults_.replay_merge) {
    // Keep only the final Replaced per order and drop Ack/Status if the order progressed.
    std::map<std::string, size_t> last_replace;
    std::set<std::string> progressed;
    for (size_t i = 0; i < out.size(); ++i) {
      if (out[i].kind == ReportKind::Replaced) last_replace[out[i].cl_ord_id] = i;
      if (out[i].kind == ReportKind::Fill || out[i].kind == ReportKind::Canceled) progressed.insert(out[i].cl_ord_id);
    }
    std::vector<ExecReport> merged;
    for (size_t i = 0; i < out.size(); ++i) {
      const auto& r = out[i];
      if ((r.kind == ReportKind::Ack || r.kind == ReportKind::Status) && progressed.count(r.cl_ord_id)) continue;
      if (r.kind == ReportKind::Replaced && last_replace[r.cl_ord_id] != i) continue;
      merged.push_back(r);
    }
    out.swap(merged);
  }
  if (faults_.replay_filter) {
    std::vector<ExecReport> kept;
    for (auto& r : out)
      if (faults_.replay_filter(r)) kept.push_back(r);
    out.swap(kept);
  }
  if (faults_.replay_mutate)
    for (auto& r : out) faults_.replay_mutate(r);
  if (faults_.replay_shuffle) {
    std::mt19937 rng(faults_.replay_seed);
    std::shuffle(out.begin(), out.end(), rng);
  }
  const int64_t intended = static_cast<int64_t>(out.size());  // what the broker claims it sent
  if (faults_.replay_drop_tail > 0)
    out.resize(out.size() > size_t(faults_.replay_drop_tail) ? out.size() - faults_.replay_drop_tail : 0);

  const TimeMs now = clock_.now();
  for (auto& r : out) {
    BrokerEvent ev;
    ev.type = BrokerEventType::Report;
    ev.source = ReportSource::Replay;
    ev.report = r;
    s.queue_.push_back({now, ev});
  }
  if (!faults_.replay_no_end && caps_.replay_end_marker) {
    BrokerEvent end;
    end.type = BrokerEventType::ReplayEnd;
    end.source = ReportSource::Replay;
    end.expected_count = intended;
    s.queue_.push_back({now, end});
  }
  return true;
}

bool MockExchange::fill(const std::string& cl_ord_id, Qty qty, Price px) {
  auto it = orders_.find(cl_ord_id);
  if (it == orders_.end() || is_terminal(it->second.status)) return false;
  MOrder& o = it->second;
  qty = std::min(qty, o.qty - o.cum);
  if (qty <= 0) return false;
  o.cum += qty;
  o.status = o.cum >= o.qty ? OrdStatus::Filled : OrdStatus::PartiallyFilled;
  ExecReport r = make_report(o, ReportKind::Fill);
  r.last_qty = qty;
  r.last_px = px;
  if (!faults_.omit_exec_id) r.exec_id = "E" + std::to_string(next_exec_id_++);
  emit(r, nullptr);
  return true;
}

bool MockExchange::exchange_cancel(const std::string& cl_ord_id) {
  auto it = orders_.find(cl_ord_id);
  if (it == orders_.end() || is_terminal(it->second.status)) return false;
  cancel_order(it->second, "", nullptr);
  return true;
}

std::string MockExchange::inject_external(const std::string& account, const std::string& symbol, Side side, Price px,
                                          Qty qty, const std::string& channel) {
  MOrder o;
  o.cl_ord_id = channel + "-" + std::to_string(next_external_++);
  o.broker_order_id = "B" + std::to_string(next_broker_id_++);
  o.account = account;
  o.symbol = symbol;
  o.channel = channel;
  o.user = "external";
  o.side = side;
  o.price = px;
  o.qty = qty;
  auto& stored = orders_[o.cl_ord_id] = o;
  emit(make_report(stored, ReportKind::Ack), nullptr);
  return stored.cl_ord_id;
}

std::string MockExchange::inject_orphan(const std::string& account, const std::string& cl_ord_id,
                                        const std::string& symbol, Side side, Price px, Qty qty) {
  MOrder o;
  o.cl_ord_id = cl_ord_id;
  o.broker_order_id = "B" + std::to_string(next_broker_id_++);
  o.account = account;
  o.symbol = symbol;
  o.channel = "API";
  o.user = "gateway";
  o.side = side;
  o.price = px;
  o.qty = qty;
  auto& stored = orders_[o.cl_ord_id] = o;
  emit(make_report(stored, ReportKind::Ack), nullptr);
  return cl_ord_id;
}

const MockExchange::MOrder* MockExchange::order(const std::string& cl_ord_id) const {
  auto it = orders_.find(cl_ord_id);
  return it == orders_.end() ? nullptr : &it->second;
}

std::vector<const MockExchange::MOrder*> MockExchange::open_orders(const std::string& account) const {
  std::vector<const MOrder*> v;
  for (const auto& [id, o] : orders_)
    if (o.account == account && !is_terminal(o.status)) v.push_back(&o);
  return v;
}

PositionMap MockExchange::positions(const std::string& account) const {
  PositionMap p;
  for (const auto& [id, o] : orders_)
    if (o.account == account && o.cum) p[o.symbol] += o.side == Side::Buy ? o.cum : -o.cum;
  return p;
}

void MockExchange::on_link_drop(MockSession* s) {
  if (!caps_.cancel_on_disconnect) return;
  for (auto& [id, o] : orders_)
    if (o.session_id == s->id_ && !is_terminal(o.status)) cancel_order(o, "", nullptr);
}

// ================================================================ MockSession

MockSession::MockSession(MockExchange& ex, uint64_t id, std::string user, std::string account, bool subscribe,
                         std::string channel)
    : ex_(ex), id_(id), user_(std::move(user)), account_(std::move(account)), channel_(std::move(channel)),
      subscribe_(subscribe) {}

MockSession::~MockSession() {
  if (connected_) ex_.on_link_drop(this);
  ex_.detach(this);
}

LoginResult MockSession::connect(const std::string& endpoint) {
  ++connect_attempts_;
  endpoint_ = endpoint;
  if (ex_.down_endpoints_.count(endpoint)) return LoginResult::Unreachable;
  if (ex_.faults_.login_reject) return LoginResult::Rejected;
  connected_ = true;
  queue_.clear();
  return LoginResult::Ok;
}

void MockSession::disconnect() {
  if (!connected_) return;
  connected_ = false;
  queue_.clear();
  ex_.on_link_drop(this);
}

void MockSession::drop_link() {
  if (!connected_) return;
  connected_ = false;
  queue_.clear();
  BrokerEvent ev;
  ev.type = BrokerEventType::Disconnected;
  ev.reason = "link dropped";
  queue_.push_back({0, ev});
  ex_.on_link_drop(this);
}

bool MockSession::can_send() const {
  return connected_ && (ex_.faults_.rate_limit_per_tick == 0 || sends_this_tick_ < ex_.faults_.rate_limit_per_tick);
}

bool MockSession::consume_budget() {
  if (!can_send()) return false;
  ++sends_this_tick_;
  ++sent_total_;
  return true;
}

bool MockSession::send_new(const NewOrderReq& r) { return consume_budget() && ex_.handle_new(*this, r); }
bool MockSession::send_cancel(const CancelReq& r) { return consume_budget() && ex_.handle_cancel(*this, r); }
bool MockSession::send_replace(const ReplaceReq& r) { return consume_budget() && ex_.handle_replace(*this, r); }
bool MockSession::mass_cancel(const std::string& account) {
  return consume_budget() && ex_.handle_mass_cancel(*this, account);
}
bool MockSession::request_replay() { return connected_ && ex_.handle_replay(*this); }

std::optional<std::vector<OpenOrderInfo>> MockSession::query_open_orders(const std::string& account) {
  if (!connected_) return std::nullopt;
  std::vector<OpenOrderInfo> v;
  for (const auto* o : ex_.open_orders(account)) {
    OpenOrderInfo i;
    i.cl_ord_id = o->cl_ord_id;
    i.broker_order_id = o->broker_order_id;
    i.account = o->account;
    i.symbol = o->symbol;
    i.channel = o->channel;
    i.side = o->side;
    i.price = o->price;
    i.order_qty = o->qty;
    i.cum_qty = o->cum;
    i.leaves_qty = o->qty - o->cum;
    v.push_back(i);
  }
  return v;
}

std::optional<OrderQueryResult> MockSession::query_order(const std::string& cl_ord_id) {
  if (!connected_) return std::nullopt;
  OrderQueryResult res;
  const auto* o = ex_.order(cl_ord_id);
  if (!o) return res;
  res.found = true;
  res.status = o->status;
  res.info.cl_ord_id = o->cl_ord_id;
  res.info.broker_order_id = o->broker_order_id;
  res.info.account = o->account;
  res.info.symbol = o->symbol;
  res.info.channel = o->channel;
  res.info.side = o->side;
  res.info.price = o->price;
  res.info.order_qty = o->qty;
  res.info.cum_qty = o->cum;
  res.info.leaves_qty = is_terminal(o->status) ? 0 : o->qty - o->cum;
  return res;
}

std::optional<PositionMap> MockSession::query_positions(const std::string& account) {
  if (!connected_) return std::nullopt;
  return ex_.positions(account);
}

std::vector<BrokerEvent> MockSession::poll() {
  std::vector<BrokerEvent> out;
  const TimeMs now = ex_.clock_.now();
  // Deliver in arrival order; a delayed head blocks later ones (TCP-like ordering).
  while (!queue_.empty() && queue_.front().at <= now) {
    out.push_back(std::move(queue_.front().ev));
    queue_.pop_front();
  }
  return out;
}

}  // namespace gw
