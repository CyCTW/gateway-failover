#include "gw/gateway.hpp"

#include <algorithm>
#include <cmath>
#include <sstream>

namespace gw {

const char* to_string(GwState s) {
  switch (s) {
    case GwState::Init: return "Init";
    case GwState::Recovering: return "Recovering";
    case GwState::Active: return "Active";
    case GwState::Halted: return "Halted";
  }
  return "?";
}

std::string MassCancelScope::id() const {
  switch (kind) {
    case Kind::All: return "ALL";
    case Kind::Strategy: return "S:" + code;
    case Kind::Orders: {
      std::string s = "O:";
      for (size_t i = 0; i < ids.size(); ++i) s += (i ? "," : "") + ids[i];
      return s;
    }
  }
  return "ALL";
}

MassCancelScope MassCancelScope::parse(const std::string& id) {
  if (id.rfind("S:", 0) == 0) return strategy(id.substr(2));
  if (id.rfind("O:", 0) == 0) {
    std::vector<std::string> ids;
    std::stringstream ss(id.substr(2));
    std::string item;
    while (std::getline(ss, item, ',')) ids.push_back(item);
    return orders(ids);
  }
  return all();
}

namespace {
const std::string kGatewayCode = "GW0";  // request ids issued by the gateway itself
}

Gateway::Gateway(GatewayConfig cfg, const std::string& wal_path, GatewayDeps deps)
    : cfg_(std::move(cfg)),
      clock_(*deps.clock),
      broker_(*deps.broker),
      channel_(*deps.channel),
      alerts_(*deps.alerts),
      kill_flag_(deps.kill_flag),
      heartbeat_(deps.heartbeat),
      market_(deps.market),
      wal_(wal_path),
      codec_(cfg_.gateway_id),
      conn_(cfg_.reconnect, cfg_.endpoints) {
  for (const auto& s : cfg_.strategies) {
    codec_.register_strategy(s.name, s.code);
    StrategyRt rt;
    rt.cfg = s;
    strategies_.push_back(rt);
  }
}

// ====================================================================== WAL

uint64_t Gateway::wal_append(RecordType t, const std::string& payload) { return wal_.append(t, now(), payload); }

void Gateway::write_correction(const Correction& c) {
  wal_append(RecordType::Correction, encode(c));
  book_.apply_correction(c);
}

void Gateway::write_control(ControlKind k, const std::string& scope, const std::string& text) {
  wal_append(RecordType::Control, encode(ControlRecord{k, scope, text}));
}

void Gateway::note_id(const std::string& id) {
  auto p = ClOrdIdCodec::parse(id);
  if (p && p->gateway_id == cfg_.gateway_id) next_seq_ = std::max(next_seq_, p->seq + 1);
}

// Spec §8 steps 1-2: rebuild the local view with no side effects.
void Gateway::replay_wal() {
  for (const WalRecord& rec : wal_.records()) {
    last_wal_ts_ = std::max(last_wal_ts_, rec.ts);
    switch (rec.type) {
      case RecordType::NewOrder: {
        NewOrderReq r = decode_new(rec.payload);
        note_id(r.cl_ord_id);
        book_.apply_new(r);
        break;
      }
      case RecordType::CancelOrder: {
        CancelReq r = decode_cancel(rec.payload);
        note_id(r.request_id);
        book_.apply_cancel_req(r);
        break;
      }
      case RecordType::ReplaceOrder: {
        ReplaceReq r = decode_replace(rec.payload);
        note_id(r.request_id);
        book_.apply_replace_req(r);
        break;
      }
      case RecordType::Report: {
        ReportSource src;
        ExecReport r = decode_report(rec.payload, &src, nullptr);
        book_.apply_report(r, src == ReportSource::Live ? "live" : "recovery");
        note_id(r.cl_ord_id);
        break;
      }
      case RecordType::Correction:
        book_.apply_correction(decode_correction(rec.payload));
        break;
      case RecordType::Control: {
        ControlRecord c = decode_control(rec.payload);
        if (c.kind == ControlKind::MassCancelStart) wal_open_jobs_.insert(c.scope);
        if (c.kind == ControlKind::MassCancelDone) wal_open_jobs_.erase(c.scope);
        if (c.kind == ControlKind::AccountHalt) {
          account_halted_ = true;
          halt_reason_ = c.text;
        }
        if (c.kind == ControlKind::OperatorRelease) account_halted_ = false;
        break;
      }
      case RecordType::Checkpoint:
        break;  // reserved
    }
  }
  // Fills already in the WAL are treated as delivered to strategies (spec §8 step 8).
  for (const auto& [id, o] : book_.orders())
    for (const auto& [k, f] : o.fills) pushed_fills_.insert(k);
}

void Gateway::start() {
  replay_wal();
  const auto unknown = book_.unknown_orders();
  alert(AlertLevel::Info, "wal_replayed",
        std::to_string(wal_.records().size()) + " records, " + std::to_string(unknown.size()) + " unknown-state orders, " +
            std::to_string(wal_.truncated_bytes()) + " torn bytes truncated");
  for (auto& s : strategies_) s.last_hb = now();
  conn_.on_disconnected(now(), /*count_flap=*/false);
  last_phase_ = ConnPhase::Fast;
  begin_recovery(/*restart=*/true);
}

// ====================================================================== poll loop

void Gateway::poll() {
  if (heartbeat_) heartbeat_->beat(now());
  if (link_up_ && !broker_.connected()) on_link_down("transport closed");
  if (link_up_ && market_) {
    // Prices as of the last poll with a healthy link: the reference for the
    // price-move condition when the link turns out to be down (spec §10.4).
    for (const Order* o : book_.active_orders())
      if (auto p = market_->last_price(o->symbol)) last_prices_[o->symbol] = *p;
  }
  for (const BrokerEvent& ev : broker_.poll()) handle_event(ev);
  if (!link_up_) try_connect();
  progress_recovery();
  progress_cancel_jobs();
  check_strategy_heartbeats();
  refresh_strategy_states();
}

void Gateway::handle_event(const BrokerEvent& ev) {
  switch (ev.type) {
    case BrokerEventType::Disconnected:
      on_link_down(ev.reason.empty() ? "disconnected" : ev.reason);
      return;
    case BrokerEventType::ReplayEnd:
      if (recovering_ && rec_.phase == RecPhase::Replaying) {
        rec_.got_end = true;
        rec_.expected = ev.expected_count;
        finish_replay_round();
      }
      return;
    case BrokerEventType::Report:
      if (ev.source == ReportSource::Replay) {
        if (recovering_ && rec_.phase == RecPhase::Replaying) {
          rec_.replay.push_back(ev.report);
          rec_.last_activity = now();
        }
        return;
      }
      if (recovering_) {
        // Spec §9.1 / §11: live reports during recovery go to the buffer, never straight
        // into the local book.
        rec_.buffer.push_back(ev.report);
        if (ev.report.kind == ReportKind::CancelReject)
          alert(AlertLevel::Info, "cancel_rejected", ev.report.cl_ord_id + ": " + ev.report.text);
        return;
      }
      process_live_report(ev.report);
      return;
  }
}

void Gateway::process_live_report(const ExecReport& r) {
  wal_append(RecordType::Report, encode(r, ReportSource::Live, now()));
  const bool known = book_.find(r.cl_ord_id) != nullptr;
  if (!known && (r.kind == ReportKind::CancelReject || r.kind == ReportKind::ReplaceReject)) {
    alert(AlertLevel::Warn, "reject_for_unknown_order", r.cl_ord_id + ": " + r.text);
    return;
  }
  ApplyResult res = book_.apply_report(r, "live");
  if (!known) classify_broker_order(r.cl_ord_id);
  if (r.kind == ReportKind::CancelReject)  // expected outcome: log only, never touch positions
    alert(AlertLevel::Info, "cancel_rejected", r.cl_ord_id + ": " + r.text);
  if (r.kind == ReportKind::Rejected) note_reject();
  after_apply(r.cl_ord_id, res);
}

void Gateway::classify_broker_order(const std::string& id) {
  const Order* o = book_.find(id);
  if (!o) return;
  Correction c;
  c.cl_ord_id = id;
  if (!o->channel.empty() && o->channel != cfg_.channel) {
    c.kind = CorrectionKind::External;
    c.note = "order from channel " + o->channel;
    write_correction(c);
    alert(AlertLevel::Info, "external_order", id + " (" + o->channel + ") counted in account positions, not touched");
  } else if (auto s = codec_.resolve_strategy(id)) {
    c.kind = CorrectionKind::AdoptOrder;
    c.strategy = *s;
    c.note = "attributed via ClOrdID";
    write_correction(c);
    note_id(id);
    alert(AlertLevel::Warn, "order_adopted", id + " -> " + *s);
  } else {
    c.kind = CorrectionKind::Unallocated;
    c.note = "unattributable order on our channel";
    write_correction(c);
    alert(AlertLevel::Critical, "unallocated_order", id + " moved to unallocated bucket; cancelling");
    if (book_.find(id)->active()) start_cancel_job(MassCancelScope::orders({id}), false);
  }
}

void Gateway::after_apply(const std::string& id, const ApplyResult& res) {
  if (res.invariant_violation) halt_account("invariant violated: " + res.detail);
  if (res.conflict) halt_account("conflicting report: " + res.detail);
  const Order* o = book_.find(id);
  if (!o || o->owner != Ownership::Strategy) return;
  StrategyRt* s = strategy(o->strategy);
  if (!s) return;
  for (const Fill& f : res.new_fills) push_fill(*s, f, false);
  if (res.status_changed) channel_.on_order_update(s->cfg.name, make_view(*o), ++event_seq_);
}

void Gateway::note_reject() {
  const TimeMs t = now();
  reject_times_.push_back(t);
  while (!reject_times_.empty() && t - reject_times_.front() > cfg_.reject_window) reject_times_.pop_front();
  if (static_cast<int>(reject_times_.size()) >= cfg_.reject_burst) halt_account("broker degraded: reject burst");
}

// ====================================================================== connection

void Gateway::on_link_down(const std::string& reason) {
  if (!link_up_) return;
  link_up_ = false;
  conn_.on_disconnected(now());
  last_phase_ = ConnPhase::Fast;
  alert(AlertLevel::Warn, "link_down", reason);
  if (!recovering_) {
    begin_recovery(false);
  } else {
    rec_.phase = RecPhase::WaitConnect;
    rec_.replay.clear();
    rec_.buffer.clear();
  }
  // Remember prices so the reconnect decision can measure the move (spec §10.4).
  rec_.prices_at_disconnect = last_prices_;
  for (auto& s : strategies_)
    if (!s.alive) s.lost_during_outage = true;
}

void Gateway::try_connect() {
  const ConnPhase ph = conn_.phase(now());
  if (ph != last_phase_) {
    if (ph == ConnPhase::Alert)
      alert(AlertLevel::Warn, "reconnect_alert_phase", "still disconnected; rotating endpoints");
    if (ph == ConnPhase::Incident)
      alert(AlertLevel::Critical, "connection_incident", "disconnected beyond alert phase; use kill switch path");
    last_phase_ = ph;
  }
  auto ep = conn_.due_attempt(now());
  if (!ep) return;
  switch (broker_.connect(*ep)) {
    case LoginResult::Ok:
      link_up_ = true;
      conn_.on_connected(now());
      alert(AlertLevel::Info, "link_up", *ep);
      on_link_up();
      break;
    case LoginResult::Rejected:
      conn_.on_login_rejected();
      alert(AlertLevel::Critical, "login_rejected", "stopped reconnecting to avoid account lockout");
      halt_account("login rejected");
      break;
    case LoginResult::Unreachable:
      conn_.attempt_failed(now());
      break;
  }
}

void Gateway::begin_recovery(bool restart) {
  recovering_ = true;
  state_ = GwState::Recovering;
  const auto prices = rec_.prices_at_disconnect;
  rec_ = Recovery{};
  rec_.prices_at_disconnect = prices;
  rec_.phase = RecPhase::WaitConnect;
  rec_.restart = restart;
}

void Gateway::on_link_up() {
  rec_.outage = rec_.restart ? (last_wal_ts_ ? now() - last_wal_ts_ : 0) : conn_.last_outage();
  rec_.round = 0;
  rec_.failures = 0;
  rec_.buffer.clear();
  // Spec §8 step 4 / §11: if policy already demands a cancel, do not wait for replay.
  for (auto& s : strategies_) {
    s.pre_action = account_halted_ ? RecoveryAction::Halt : decide_recovery(s.cfg.recovery, make_facts(s, true));
    if (s.pre_action != RecoveryAction::Resume) start_cancel_job(MassCancelScope::strategy(s.cfg.code), false);
  }
  for (const std::string& scope : wal_open_jobs_) start_cancel_job(MassCancelScope::parse(scope), true);
  wal_open_jobs_.clear();
  start_replay_round(true);
}

// ====================================================================== recovery

void Gateway::start_replay_round(bool new_round) {
  if (new_round) ++rec_.round;
  ++total_replay_rounds_;
  rec_.replay.clear();
  rec_.got_end = false;
  rec_.expected = -1;
  rec_.phase = RecPhase::Replaying;
  rec_.last_activity = now();
  if (!broker_.request_replay()) replay_failed("replay request failed");
}

void Gateway::progress_recovery() {
  if (!recovering_ || !link_up_) return;
  if (rec_.phase == RecPhase::Replaying && now() - rec_.last_activity >= cfg_.replay_silence_timeout) {
    if (broker_.capabilities().replay_end_marker)
      replay_failed("replay went silent without end marker");
    else
      finish_replay_round();  // broker has no end marker: silence means done
  }
  if (rec_.phase == RecPhase::WaitRecheck && now() >= rec_.recheck_at) start_replay_round(true);
  if (rec_.phase == RecPhase::WaitRetry && now() >= rec_.recheck_at) start_replay_round(false);
}

void Gateway::replay_failed(const std::string& why) {
  ++rec_.failures;
  alert(AlertLevel::Warn, "replay_failed", why + " (attempt " + std::to_string(rec_.failures) + ")");
  if (rec_.failures <= cfg_.replay_max_retries) {
    rec_.phase = RecPhase::WaitRetry;
    rec_.recheck_at = now() + cfg_.replay_retry_delay * rec_.failures;
    return;
  }
  fallback_to_queries();
}

// Spec §9.3 #7: replay unusable; use query APIs as the basis and stay halted.
void Gateway::fallback_to_queries() {
  alert(AlertLevel::Critical, "replay_unavailable", "falling back to open-order query; trading stays halted");
  if (auto open = broker_.query_open_orders(cfg_.account)) {
    for (const OpenOrderInfo& i : *open) {
      if (book_.find(i.cl_ord_id) && book_.find(i.cl_ord_id)->acked) continue;
      ExecReport r;
      r.kind = ReportKind::Ack;
      r.cl_ord_id = i.cl_ord_id;
      r.broker_order_id = i.broker_order_id;
      r.account = i.account;
      r.symbol = i.symbol;
      r.side = i.side;
      r.price = i.price;
      r.order_qty = i.order_qty;
      r.channel = i.channel;
      const bool known = book_.find(i.cl_ord_id) != nullptr;
      wal_append(RecordType::Report, encode(r, ReportSource::Recovery, now()));
      book_.apply_report(r, "recovery");
      if (!known) classify_broker_order(i.cl_ord_id);
    }
  }
  halt_account("replay unavailable");
  complete_recovery(false);
}

void Gateway::finish_replay_round() {
  if (rec_.got_end && rec_.expected >= 0 && static_cast<int64_t>(rec_.replay.size()) != rec_.expected) {
    replay_failed("incomplete replay: got " + std::to_string(rec_.replay.size()) + " of " +
                  std::to_string(rec_.expected));
    return;
  }
  // Spec §9.1: shadow book from replay + buffered live reports; local untouched until diff.
  OrderBook shadow;
  for (const ExecReport& r : rec_.replay) shadow.apply_report(r, "replay");
  for (const ExecReport& r : rec_.buffer) shadow.apply_report(r, "live");

  ReconcileContext ctx{&codec_, cfg_.channel};
  last_reconcile_ = reconcile(book_, shadow, ctx);
  const bool final_round = rec_.round >= 2;
  bool need_recheck = false;
  bool clean = true;

  for (const OrderDiff& d : last_reconcile_.diffs) {
    switch (d.kind) {
      case DiffKind::Match:
        break;
      case DiffKind::BrokerNewer:
      case DiffKind::BrokerOnlyOurs:
      case DiffKind::BrokerOnlyUnknown:
      case DiffKind::BrokerOnlyExternal:
        import_facts(shadow, d);
        break;
      case DiffKind::LocalPendingMissing: {
        auto q = broker_.query_order(d.cl_ord_id);
        const bool broker_knows = q && q->found;
        if (!final_round) {
          need_recheck = true;
        } else if (broker_knows) {
          clean = false;
          halt_account("order " + d.cl_ord_id + " visible in query but not in replay");
        } else {
          Correction c{CorrectionKind::MarkFailed, d.cl_ord_id, d.strategy, "", 0, "never reached broker"};
          write_correction(c);
          alert(AlertLevel::Warn, "order_failed", d.cl_ord_id + " never reached the broker; risk released");
        }
        break;
      }
      case DiffKind::LocalAckedMissing:
        if (!final_round) {
          need_recheck = true;
        } else {
          clean = false;
          halt_account("confirmed order " + d.cl_ord_id + " missing from broker replay");
        }
        break;
      case DiffKind::LocalNewer:
        if (d.broker_has_more) import_facts(shadow, d);  // union only; never roll back local fills
        if (!final_round) {
          need_recheck = true;
        } else {
          clean = false;
          halt_account("local state ahead of broker for " + d.cl_ord_id);
        }
        break;
      case DiffKind::FieldConflict:
        clean = false;
        halt_account("field conflict on " + d.cl_ord_id + ": " + d.detail);
        break;
    }
  }

  if (need_recheck && !final_round) {
    alert(AlertLevel::Info, "reconcile_recheck", "differences need a second replay round");
    rec_.phase = RecPhase::WaitRecheck;
    rec_.recheck_at = now() + cfg_.recheck_delay;
    return;
  }

  // Spec §9.4: account-level position check.
  if (auto broker_pos = broker_.query_positions(cfg_.account)) {
    PositionMap local = book_.account_positions();
    for (const auto& [sym, q] : cfg_.opening_positions) local[sym] += q;
    for (const PositionDiff& pd : check_positions(local, *broker_pos)) {
      Correction c{CorrectionKind::UnallocatedPosition, "", "", pd.symbol, pd.broker - pd.local,
                   "account position mismatch"};
      write_correction(c);
      alert(AlertLevel::Critical, "position_mismatch",
            pd.symbol + " local=" + std::to_string(pd.local) + " broker=" + std::to_string(pd.broker));
      clean = false;
      halt_account("account position mismatch on " + pd.symbol);
    }
  } else {
    clean = false;
    alert(AlertLevel::Warn, "position_query_failed", "account check skipped; treating reconcile as unclean");
  }
  complete_recovery(clean);
}

void Gateway::import_facts(const OrderBook& shadow, const OrderDiff& d) {
  const Order* s = shadow.find(d.cl_ord_id);
  if (!s) return;
  const bool existed = book_.find(d.cl_ord_id) != nullptr;
  for (const ExecReport& r : missing_facts(book_.find(d.cl_ord_id), *s)) {
    wal_append(RecordType::Report, encode(r, ReportSource::Recovery, now()));
    ApplyResult res = book_.apply_report(r, "recovery");
    if (res.invariant_violation) halt_account("invariant violated after import: " + res.detail);
  }
  if (!existed) classify_broker_order(d.cl_ord_id);
}

void Gateway::complete_recovery(bool clean) {
  recovering_ = false;
  rec_.phase = RecPhase::Idle;
  for (const auto& [id, o] : book_.orders()) note_id(id);

  // Spec §8 step 8: push only what strategies have not seen, then a snapshot.
  for (auto& s : strategies_) {
    s.recovered_fills.clear();
    for (const auto& [id, o] : book_.orders())
      if (o.owner == Ownership::Strategy && o.strategy == s.cfg.name)
        for (const auto& [k, f] : o.fills) push_fill(s, f, true);
    send_snapshot(s);
  }

  for (auto& s : strategies_) {
    RecoveryAction a = decide_recovery(s.cfg.recovery, make_facts(s, clean));
    if (static_cast<int>(s.pre_action) > static_cast<int>(a)) a = s.pre_action;
    if (account_halted_) a = RecoveryAction::Halt;
    if (a != RecoveryAction::Resume) start_cancel_job(MassCancelScope::strategy(s.cfg.code), false);
    if (a == RecoveryAction::Halt) s.awaiting_release = true;
    s.lost_during_outage = false;
    alert(AlertLevel::Info, "recovery_action", s.cfg.name + ": " + to_string(a));
  }
  state_ = account_halted_ ? GwState::Halted : GwState::Active;
  alert(clean ? AlertLevel::Info : AlertLevel::Warn, "recovery_complete",
        std::string(clean ? "clean" : "NOT clean") + ", rounds=" + std::to_string(rec_.round));
}

RecoveryFacts Gateway::make_facts(const StrategyRt& s, bool clean) const {
  RecoveryFacts f;
  f.outage = rec_.outage;
  f.reconcile_clean = clean;
  f.flapping = conn_.flapping(now());
  f.market_open = cfg_.market_open ? cfg_.market_open(now()) : true;
  f.strategy_continuous = s.alive && !s.lost_during_outage;
  f.price_check_enabled = market_ != nullptr;
  if (market_) {
    double worst = 0;
    bool unknown = false;
    for (const Order* o : book_.active_orders()) {
      if (o->owner != Ownership::Strategy || o->strategy != s.cfg.name) continue;
      auto before = rec_.prices_at_disconnect.find(o->symbol);
      auto after = market_->last_price(o->symbol);
      if (before == rec_.prices_at_disconnect.end() || !after || before->second == 0) {
        unknown = true;
        continue;
      }
      worst = std::max(worst, std::fabs(double(*after - before->second)) / double(before->second));
    }
    if (!unknown) f.price_move = worst;
  }
  return f;
}

void Gateway::halt_account(const std::string& reason) {
  if (!account_halted_) {
    account_halted_ = true;
    halt_reason_ = reason;
    write_control(ControlKind::AccountHalt, "", reason);
    alert(AlertLevel::Critical, "account_halted", reason);
  }
  if (!recovering_) state_ = GwState::Halted;
}

bool Gateway::operator_release() {
  if (recovering_) return false;
  account_halted_ = false;
  halt_reason_.clear();
  write_control(ControlKind::OperatorRelease, "", "manual release");
  for (auto& s : strategies_) s.awaiting_release = false;
  state_ = GwState::Active;
  alert(AlertLevel::Info, "operator_release", "account released");
  return true;
}

bool Gateway::operator_release_strategy(const std::string& name) {
  StrategyRt* s = strategy(name);
  if (!s || recovering_) return false;
  s->awaiting_release = false;
  return true;
}

// ====================================================================== strategies

Gateway::StrategyRt* Gateway::strategy(const std::string& name) {
  for (auto& s : strategies_)
    if (s.cfg.name == name) return &s;
  return nullptr;
}

bool Gateway::may_trade(const StrategyRt& s) const {
  if (recovering_ || !link_up_ || account_halted_ || s.awaiting_release || !s.alive) return false;
  for (const CancelJob& j : jobs_)
    if (j.phase != CancelJob::Phase::Done && job_covers(j, s)) return false;
  return true;
}

bool Gateway::trading_enabled(const std::string& name) const {
  for (const auto& s : strategies_)
    if (s.cfg.name == name) return may_trade(s);
  return false;
}

void Gateway::refresh_strategy_states() {
  for (auto& s : strategies_) {
    const bool should = may_trade(s);
    if (should == s.enabled) continue;
    s.enabled = should;
    std::string reason = should ? "trading enabled" : "trading disabled";
    if (!should) {
      if (recovering_) reason = "recovering";
      if (!link_up_) reason = "broker link down";
      if (account_halted_) reason = "account halted: " + halt_reason_;
      if (s.awaiting_release) reason = "halted: awaiting manual release";
    }
    channel_.on_trading_status(s.cfg.name, should, reason);
  }
}

void Gateway::push_fill(StrategyRt& s, const Fill& f, bool recovered) {
  if (!pushed_fills_.insert(f.key).second) return;
  if (recovered) s.recovered_fills.push_back(f);
  channel_.on_fill(s.cfg.name, f, recovered, ++event_seq_);
}

void Gateway::send_snapshot(StrategyRt& s) {
  StrategySnapshot snap;
  snap.strategy = s.cfg.name;
  for (const Order* o : book_.active_orders())
    if (o->owner == Ownership::Strategy && o->strategy == s.cfg.name) snap.open_orders.push_back(make_view(*o));
  snap.recovered_fills = s.recovered_fills;
  auto pos = book_.positions();
  if (pos.count(s.cfg.name)) snap.positions = pos[s.cfg.name];
  snap.seq_cut = event_seq_;
  channel_.on_snapshot(snap);
}

void Gateway::strategy_heartbeat(const std::string& name) {
  StrategyRt* s = strategy(name);
  if (!s) return;
  s->last_hb = now();
  if (!s->alive) {
    s->alive = true;
    alert(AlertLevel::Info, "strategy_back", name);
    if (!recovering_) send_snapshot(*s);
  }
}

void Gateway::check_strategy_heartbeats() {
  for (auto& s : strategies_) {
    if (s.cfg.heartbeat_timeout <= 0 || !s.alive) continue;
    if (now() - s.last_hb <= s.cfg.heartbeat_timeout) continue;
    s.alive = false;
    if (recovering_) s.lost_during_outage = true;
    alert(AlertLevel::Warn, "strategy_lost", s.cfg.name);
    if (s.cfg.cancel_on_strategy_loss) start_cancel_job(MassCancelScope::strategy(s.cfg.code), false);
  }
}

// ====================================================================== order entry

SubmitResult Gateway::submit_new(const std::string& name, const OrderParams& p) {
  StrategyRt* s = strategy(name);
  if (!s) return {false, "", "unknown strategy"};
  if (!may_trade(*s)) return {false, "", recovering_ ? "recovering: new orders rejected" : "trading disabled"};
  if (kill_flag_ && kill_flag_->tripped()) return {false, "", "kill switch engaged"};
  if (p.qty <= 0 || p.price <= 0) return {false, "", "invalid price/qty"};
  if (!broker_.can_send()) return {false, "", "throttled"};
  if (book_.reserved_notional(name) + p.price * p.qty > s->cfg.max_notional) return {false, "", "risk limit"};

  NewOrderReq r;
  r.cl_ord_id = codec_.make(s->cfg.code, next_seq_++);
  r.strategy = name;
  r.account = cfg_.account;
  r.symbol = p.symbol;
  r.side = p.side;
  r.price = p.price;
  r.qty = p.qty;
  r.tif = p.tif;
  r.parent_id = p.parent_id;
  r.algo_tag = p.algo_tag;
  r.risk_tag = p.risk_tag;
  wal_append(RecordType::NewOrder, encode(r));  // write-ahead: before the wire
  book_.apply_new(r);
  if (!broker_.send_new(r))
    alert(AlertLevel::Warn, "send_failed", r.cl_ord_id + " state unknown; risk stays reserved");
  return {true, r.cl_ord_id, ""};
}

SubmitResult Gateway::submit_cancel(const std::string& name, const std::string& id) {
  const Order* o = book_.find(id);
  if (!o || o->owner != Ownership::Strategy || o->strategy != name) return {false, id, "not your order"};
  if (!o->active()) return {false, id, "order not active"};
  if (o->broker_order_id.empty()) return {false, id, "order unconfirmed: no broker order id yet"};
  if (!link_up_) return {false, id, "broker link down"};
  if (!broker_.can_send()) return {false, id, "throttled"};
  CancelReq r{new_request_id(id), id, o->broker_order_id};
  wal_append(RecordType::CancelOrder, encode(r));
  book_.apply_cancel_req(r);
  broker_.send_cancel(r);
  return {true, id, ""};
}

SubmitResult Gateway::submit_replace(const std::string& name, const std::string& id, Price px, Qty qty) {
  StrategyRt* s = strategy(name);
  const Order* o = book_.find(id);
  if (!s || !o || o->owner != Ownership::Strategy || o->strategy != name) return {false, id, "not your order"};
  if (!may_trade(*s)) return {false, id, "trading disabled"};
  if (kill_flag_ && kill_flag_->tripped()) return {false, id, "kill switch engaged"};
  if (!o->active() || o->broker_order_id.empty()) return {false, id, "order not amendable"};
  if (!broker_.can_send()) return {false, id, "throttled"};
  const int64_t delta = px * qty - o->price * o->leaves_qty();
  if (book_.reserved_notional(name) + std::max<int64_t>(0, delta) > s->cfg.max_notional) return {false, id, "risk limit"};
  ReplaceReq r{new_request_id(id), id, o->broker_order_id, px, qty};
  wal_append(RecordType::ReplaceOrder, encode(r));
  book_.apply_replace_req(r);
  broker_.send_replace(r);
  return {true, id, ""};
}

std::string Gateway::new_request_id(const std::string& cl_ord_id) {
  auto p = ClOrdIdCodec::parse(cl_ord_id);
  const std::string code = p && p->gateway_id == cfg_.gateway_id ? p->code : kGatewayCode;
  return codec_.make(code, next_seq_++);
}

// ====================================================================== mass cancel (spec §11)

void Gateway::request_mass_cancel(const MassCancelScope& scope) { start_cancel_job(scope, false); }

bool Gateway::mass_cancel_active() const {
  for (const auto& j : jobs_)
    if (j.phase != CancelJob::Phase::Done) return true;
  return false;
}

void Gateway::start_cancel_job(const MassCancelScope& scope, bool resumed) {
  const std::string id = scope.id();
  for (const auto& j : jobs_)
    if (j.phase != CancelJob::Phase::Done && j.scope.id() == id) return;
  if (!resumed) write_control(ControlKind::MassCancelStart, id);
  CancelJob j;
  j.scope = scope;
  jobs_.push_back(j);
  alert(AlertLevel::Warn, "mass_cancel_start", id + (resumed ? " (resumed after restart)" : ""));
  refresh_strategy_states();  // block new orders immediately
}

bool Gateway::job_covers(const CancelJob& j, const StrategyRt& s) const {
  switch (j.scope.kind) {
    case MassCancelScope::Kind::All: return true;
    case MassCancelScope::Kind::Strategy: return j.scope.code == s.cfg.code;
    case MassCancelScope::Kind::Orders: return false;
  }
  return false;
}

bool Gateway::scope_matches(const MassCancelScope& sc, const std::string& id, const std::string& channel) const {
  if (!channel.empty() && channel != cfg_.channel) return false;  // never touch external orders
  if (const Order* o = book_.find(id); o && o->owner == Ownership::External) return false;
  switch (sc.kind) {
    case MassCancelScope::Kind::All: return true;
    case MassCancelScope::Kind::Strategy: {
      auto p = ClOrdIdCodec::parse(id);
      return p && p->gateway_id == cfg_.gateway_id && p->code == sc.code;
    }
    case MassCancelScope::Kind::Orders: return std::find(sc.ids.begin(), sc.ids.end(), id) != sc.ids.end();
  }
  return false;
}

void Gateway::enqueue_cancel(CancelJob& j, const std::string& id, const std::string& bid) {
  for (const auto& r : j.outbox)
    if (r.cl_ord_id == id) return;
  j.outbox.push_back(CancelReq{"", id, bid});
}

void Gateway::drain(CancelJob& j) {
  while (!j.outbox.empty() && link_up_ && broker_.can_send()) {
    CancelReq r = j.outbox.front();
    j.outbox.pop_front();
    r.request_id = new_request_id(r.cl_ord_id);
    wal_append(RecordType::CancelOrder, encode(r));  // cancel requests are write-ahead too
    book_.apply_cancel_req(r);
    broker_.send_cancel(r);
    ++j.attempts[r.cl_ord_id];
  }
}

void Gateway::progress_cancel_jobs() {
  if (!link_up_) return;
  for (CancelJob& j : jobs_) {
    switch (j.phase) {
      case CancelJob::Phase::Done:
        break;
      case CancelJob::Phase::Round1: {
        // Round 1 uses what we already know; do not wait for replay.
        if (j.scope.kind == MassCancelScope::Kind::All && broker_.capabilities().batch_cancel) {
          if (!broker_.can_send()) break;
          write_control(ControlKind::BatchCancelSent, j.scope.id());
          broker_.mass_cancel(cfg_.account);
        } else {
          struct Target {
            const Order* o;
            double dist;
          };
          std::vector<Target> targets;
          for (const Order* o : book_.active_orders()) {
            if (o->broker_order_id.empty() || !scope_matches(j.scope, o->cl_ord_id, o->channel)) continue;
            double dist = 0;
            if (market_)
              if (auto p = market_->last_price(o->symbol); p && *p > 0) dist = std::fabs(double(o->price - *p)) / double(*p);
            targets.push_back({o, dist});
          }
          // Near the market and large first.
          std::sort(targets.begin(), targets.end(), [](const Target& a, const Target& b) {
            if (a.dist != b.dist) return a.dist < b.dist;
            return a.o->leaves_qty() * a.o->price > b.o->leaves_qty() * b.o->price;
          });
          for (const auto& t : targets) enqueue_cancel(j, t.o->cl_ord_id, t.o->broker_order_id);
        }
        j.phase = CancelJob::Phase::Round2;
        drain(j);
        break;
      }
      case CancelJob::Phase::Round2: {
        drain(j);
        if (!j.outbox.empty()) break;
        // Round 2: orders the broker has that we did not cancel (orphans, unconfirmed).
        auto open = broker_.query_open_orders(cfg_.account);
        if (!open) break;
        for (const OpenOrderInfo& i : *open)
          if (scope_matches(j.scope, i.cl_ord_id, i.channel) && !j.attempts.count(i.cl_ord_id))
            enqueue_cancel(j, i.cl_ord_id, i.broker_order_id);
        drain(j);
        j.phase = CancelJob::Phase::Verify;
        j.next_verify = now() + cfg_.cancel_verify_interval;
        break;
      }
      case CancelJob::Phase::Verify: {
        drain(j);
        if (!j.outbox.empty() || now() < j.next_verify) break;
        auto open = broker_.query_open_orders(cfg_.account);
        if (!open) break;
        std::vector<OpenOrderInfo> remaining;
        for (const OpenOrderInfo& i : *open)
          if (scope_matches(j.scope, i.cl_ord_id, i.channel)) remaining.push_back(i);
        j.next_verify = now() + cfg_.cancel_verify_interval;
        if (remaining.empty()) {
          // Done only after two consecutive empty queries.
          if (++j.empty_streak >= 2) {
            j.phase = CancelJob::Phase::Done;
            write_control(ControlKind::MassCancelDone, j.scope.id());
            alert(AlertLevel::Info, "mass_cancel_done", j.scope.id());
          }
          break;
        }
        j.empty_streak = 0;
        for (const OpenOrderInfo& i : remaining) {
          if (j.attempts[i.cl_ord_id] < cfg_.max_cancel_attempts) {
            enqueue_cancel(j, i.cl_ord_id, i.broker_order_id);
          } else if (j.gave_up.insert(i.cl_ord_id).second) {
            alert(AlertLevel::Critical, "cancel_stuck",
                  i.cl_ord_id + " still working after " + std::to_string(cfg_.max_cancel_attempts) +
                      " cancels; manual action required");
          }
        }
        drain(j);
        break;
      }
    }
  }
}

void Gateway::alert(AlertLevel lvl, const std::string& code, const std::string& msg) { alerts_.alert(lvl, code, msg); }

}  // namespace gw
