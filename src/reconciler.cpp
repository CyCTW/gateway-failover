#include "gw/reconciler.hpp"

#include <set>

namespace gw {

const char* to_string(DiffKind k) {
  switch (k) {
    case DiffKind::Match: return "Match";
    case DiffKind::BrokerNewer: return "BrokerNewer(2)";
    case DiffKind::LocalPendingMissing: return "LocalPendingMissing(3a)";
    case DiffKind::LocalAckedMissing: return "LocalAckedMissing(3b)";
    case DiffKind::BrokerOnlyOurs: return "BrokerOnlyOurs(4a)";
    case DiffKind::BrokerOnlyUnknown: return "BrokerOnlyUnknown(4b)";
    case DiffKind::BrokerOnlyExternal: return "BrokerOnlyExternal(4c)";
    case DiffKind::LocalNewer: return "LocalNewer(5)";
    case DiffKind::FieldConflict: return "FieldConflict(6)";
  }
  return "?";
}

size_t ReconcileResult::count(DiffKind k) const {
  size_t n = 0;
  for (const auto& d : diffs) n += d.kind == k;
  return n;
}

std::vector<const OrderDiff*> ReconcileResult::of(DiffKind k) const {
  std::vector<const OrderDiff*> v;
  for (const auto& d : diffs)
    if (d.kind == k) v.push_back(&d);
  return v;
}

namespace {

// Does `a` hold broker facts that `b` lacks?
bool has_more(const Order& a, const Order& b) {
  for (const auto& [k, f] : a.fills)
    if (!b.fills.count(k)) return true;
  return (a.acked && !b.acked) || (a.canceled_seen && !b.canceled_seen) || (a.rejected_seen && !b.rejected_seen) ||
         a.canceled_qty > b.canceled_qty || a.mod_seq > b.mod_seq;
}

std::string conflict_of(const Order& l, const Order& s) {
  if (l.failed) return "order was marked failed locally but broker has it";
  if (!l.symbol.empty() && !s.symbol.empty() && (l.symbol != s.symbol || l.side != s.side)) return "symbol/side differ";
  if (!l.account.empty() && !s.account.empty() && l.account != s.account) return "account differs";
  if (!l.broker_order_id.empty() && !s.broker_order_id.empty() && l.broker_order_id != s.broker_order_id)
    return "broker order id differs";
  for (const auto& [k, f] : l.fills) {
    auto it = s.fills.find(k);
    if (it != s.fills.end() && (it->second.qty != f.qty || it->second.px != f.px)) return "fill " + k + " differs";
  }
  if (l.mod_seq == s.mod_seq && l.mod_seq > 0 && (l.price != s.price || l.order_qty != s.order_qty))
    return "same amend seq, different price/qty";
  if (l.mod_seq == 0 && s.mod_seq == 0 && l.has_report && (l.price != s.price || l.order_qty != s.order_qty))
    return "price/qty differ without amend";
  return {};
}

}  // namespace

ReconcileResult reconcile(const OrderBook& local, const OrderBook& shadow, const ReconcileContext& ctx) {
  ReconcileResult res;
  std::set<std::string> seen;

  for (const auto& [id, l] : local.orders()) {
    seen.insert(id);
    OrderDiff d;
    d.cl_ord_id = id;
    d.strategy = l.strategy;
    const Order* s = shadow.find(id);
    if (!s) {
      if (l.failed || l.owner == Ownership::External)
        d.kind = DiffKind::Match;  // already resolved / not ours to judge
      else if (!l.has_report)
        d.kind = DiffKind::LocalPendingMissing;
      else
        d.kind = DiffKind::LocalAckedMissing;
      res.diffs.push_back(d);
      continue;
    }
    const std::string conflict = conflict_of(l, *s);
    d.broker_has_more = has_more(*s, l);
    if (!conflict.empty()) {
      d.kind = DiffKind::FieldConflict;
      d.detail = conflict;
    } else if (has_more(l, *s)) {
      d.kind = DiffKind::LocalNewer;
    } else if (d.broker_has_more) {
      d.kind = DiffKind::BrokerNewer;
    } else {
      d.kind = DiffKind::Match;
    }
    res.diffs.push_back(d);
  }

  for (const auto& [id, s] : shadow.orders()) {
    if (seen.count(id)) continue;
    OrderDiff d;
    d.cl_ord_id = id;
    d.broker_has_more = true;
    if (!s.channel.empty() && s.channel != ctx.our_channel) {
      d.kind = DiffKind::BrokerOnlyExternal;
    } else if (auto strat = ctx.codec ? ctx.codec->resolve_strategy(id) : std::nullopt) {
      d.kind = DiffKind::BrokerOnlyOurs;
      d.strategy = *strat;
    } else {
      d.kind = DiffKind::BrokerOnlyUnknown;
    }
    res.diffs.push_back(d);
  }
  return res;
}

std::vector<PositionDiff> check_positions(const PositionMap& local_total, const PositionMap& broker) {
  std::vector<PositionDiff> out;
  std::set<std::string> syms;
  for (const auto& [s, q] : local_total) syms.insert(s);
  for (const auto& [s, q] : broker) syms.insert(s);
  for (const auto& s : syms) {
    const Qty l = local_total.count(s) ? local_total.at(s) : 0;
    const Qty b = broker.count(s) ? broker.at(s) : 0;
    if (l != b) out.push_back({s, l, b});
  }
  return out;
}

std::vector<ExecReport> missing_facts(const Order* local, const Order& s) {
  std::vector<ExecReport> out;
  auto base = [&](ReportKind k) {
    ExecReport r;
    r.kind = k;
    r.cl_ord_id = s.cl_ord_id;
    r.broker_order_id = s.broker_order_id;
    r.account = s.account;
    r.symbol = s.symbol;
    r.side = s.side;
    r.price = s.price;
    r.order_qty = s.order_qty;
    r.channel = s.channel;
    return r;
  };
  if (s.acked && (!local || !local->acked)) out.push_back(base(ReportKind::Ack));
  if (s.mod_seq > 0 && (!local || s.mod_seq > local->mod_seq)) {
    ExecReport r = base(ReportKind::Replaced);
    r.mod_seq = s.mod_seq;
    out.push_back(r);
  }
  for (const auto& [k, f] : s.fills) {
    if (local && local->fills.count(k)) continue;
    ExecReport r = base(ReportKind::Fill);
    r.exec_id = k;
    r.last_qty = f.qty;
    r.last_px = f.px;
    r.transact_time = f.time;
    out.push_back(r);
  }
  if (s.canceled_seen && (!local || !local->canceled_seen || local->canceled_qty < s.canceled_qty)) {
    ExecReport r = base(ReportKind::Canceled);
    r.cum_qty = s.order_qty - s.canceled_qty;
    out.push_back(r);
  }
  if (s.rejected_seen && (!local || !local->rejected_seen)) out.push_back(base(ReportKind::Rejected));
  return out;
}

}  // namespace gw
