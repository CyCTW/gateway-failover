#include "gw/order_book.hpp"

#include <algorithm>

namespace gw {

const char* to_string(OrdStatus s) {
  switch (s) {
    case OrdStatus::PendingNew: return "PendingNew";
    case OrdStatus::New: return "New";
    case OrdStatus::PartiallyFilled: return "PartiallyFilled";
    case OrdStatus::Filled: return "Filled";
    case OrdStatus::Canceled: return "Canceled";
    case OrdStatus::Rejected: return "Rejected";
  }
  return "?";
}

const char* to_string(ReportKind k) {
  switch (k) {
    case ReportKind::Ack: return "Ack";
    case ReportKind::Fill: return "Fill";
    case ReportKind::Canceled: return "Canceled";
    case ReportKind::Rejected: return "Rejected";
    case ReportKind::Replaced: return "Replaced";
    case ReportKind::CancelReject: return "CancelReject";
    case ReportKind::ReplaceReject: return "ReplaceReject";
    case ReportKind::Status: return "Status";
  }
  return "?";
}

// ---------------------------------------------------------------- Order

Qty Order::cum_qty() const {
  Qty sum = 0;
  for (const auto& [k, f] : fills) sum += f.qty;
  return sum;
}

Qty Order::leaves_qty() const {
  if (failed || rejected_seen || canceled_seen) return 0;
  return std::max<Qty>(0, order_qty - cum_qty() - canceled_qty);
}

// Status is a pure function of the accumulated facts, which is what makes report
// application order-independent. Terminal states can never regress.
OrdStatus Order::status() const {
  if (failed) return OrdStatus::Rejected;
  const Qty cum = cum_qty();
  if (order_qty > 0 && cum >= order_qty) return OrdStatus::Filled;
  if (canceled_seen) return OrdStatus::Canceled;
  if (rejected_seen) return OrdStatus::Rejected;
  if (cum > 0) return OrdStatus::PartiallyFilled;
  if (acked) return OrdStatus::New;
  return OrdStatus::PendingNew;
}

std::string Order::owner_key() const {
  switch (owner) {
    case Ownership::Strategy: return strategy;
    case Ownership::Unallocated: return kUnallocatedOwner;
    case Ownership::External: return kExternalOwner;
  }
  return kUnallocatedOwner;
}

// ---------------------------------------------------------------- OrderBook

std::string synth_fill_base(const ExecReport& r) {
  const std::string& oid = r.broker_order_id.empty() ? r.cl_ord_id : r.broker_order_id;
  return "S|" + oid + "|" + std::to_string(r.last_px) + "|" + std::to_string(r.last_qty) + "|" +
         std::to_string(r.transact_time);
}

Order& OrderBook::get_or_create(const std::string& id, bool* created) {
  auto it = orders_.find(id);
  if (it != orders_.end()) {
    if (created) *created = false;
    return it->second;
  }
  Order& o = orders_[id];
  o.cl_ord_id = id;
  if (created) *created = true;
  return o;
}

void OrderBook::apply_new(const NewOrderReq& r) {
  Order& o = get_or_create(r.cl_ord_id, nullptr);
  o.has_request = true;
  o.strategy = r.strategy;
  o.owner = Ownership::Strategy;
  if (o.account.empty()) o.account = r.account;
  if (o.symbol.empty()) o.symbol = r.symbol;
  if (o.order_qty == 0) {
    o.side = r.side;
    o.price = r.price;
    o.order_qty = r.qty;
  }
  o.tif = r.tif;
  o.parent_id = r.parent_id;
  o.algo_tag = r.algo_tag;
  o.risk_tag = r.risk_tag;
}

void OrderBook::apply_cancel_req(const CancelReq& r) {
  request_to_order_[r.request_id] = r.cl_ord_id;
  if (Order* o = find_mut(r.cl_ord_id)) {
    if (o->active()) o->pending_requests.insert(r.request_id);
  }
}

void OrderBook::apply_replace_req(const ReplaceReq& r) {
  request_to_order_[r.request_id] = r.cl_ord_id;
  if (Order* o = find_mut(r.cl_ord_id)) {
    if (o->active()) o->pending_requests.insert(r.request_id);
  }
}

ApplyResult OrderBook::apply_report(const ExecReport& r, const std::string& stream) {
  ApplyResult res;
  Order& o = get_or_create(r.cl_ord_id, &res.created);
  const OrdStatus before = o.status();
  o.has_report = true;

  // Static fields: first writer wins; disagreement is a conflict (spec §9.3 #6).
  if (o.account.empty()) o.account = r.account;
  if (o.symbol.empty()) {
    o.symbol = r.symbol;
    o.side = r.side;
  } else if (!r.symbol.empty() && (o.symbol != r.symbol || o.side != r.side)) {
    res.conflict = true;
    res.detail = "symbol/side mismatch";
  }
  if (o.channel.empty()) o.channel = r.channel;
  if (!r.broker_order_id.empty()) {
    if (o.broker_order_id.empty()) {
      o.broker_order_id = r.broker_order_id;
    } else if (o.broker_order_id != r.broker_order_id) {
      res.conflict = true;
      res.detail = "broker order id mismatch";
    }
  }
  if (o.order_qty == 0 && r.order_qty > 0 && r.kind != ReportKind::Replaced) {
    o.order_qty = r.order_qty;
    o.price = r.price;
  }

  switch (r.kind) {
    case ReportKind::Ack:
    case ReportKind::Status:
      o.acked = true;
      break;
    case ReportKind::Fill: {
      o.acked = true;
      std::string key = r.exec_id;
      if (key.empty()) {
        const std::string base = synth_fill_base(r);
        const int n = ++synth_counts_[stream + "\x1f" + base];
        key = base + "#" + std::to_string(n);
      }
      auto it = o.fills.find(key);
      if (it == o.fills.end()) {
        Fill f{key, r.cl_ord_id, r.last_qty, r.last_px, r.transact_time};
        o.fills.emplace(key, f);
        res.new_fills.push_back(f);
      } else if (it->second.qty != r.last_qty || it->second.px != r.last_px) {
        res.conflict = true;
        res.detail = "fill " + key + " differs";
      }
      break;
    }
    case ReportKind::Canceled: {
      o.acked = true;
      o.canceled_seen = true;
      const Qty oq = r.order_qty > 0 ? r.order_qty : o.order_qty;
      o.canceled_qty = std::max(o.canceled_qty, std::max<Qty>(0, oq - r.cum_qty));
      break;
    }
    case ReportKind::Rejected:
      o.rejected_seen = true;
      break;
    case ReportKind::Replaced: {
      o.acked = true;
      const uint64_t seq = r.mod_seq ? r.mod_seq : static_cast<uint64_t>(std::max<TimeMs>(0, r.transact_time));
      if (seq == 0) {
        o.amend_uncertain = true;
      } else if (seq > o.mod_seq) {
        o.mod_seq = seq;
        o.price = r.price;
        o.order_qty = r.order_qty;
      } else if (seq == o.mod_seq && (o.price != r.price || o.order_qty != r.order_qty)) {
        res.conflict = true;
        res.detail = "amend seq collision";
      }
      break;
    }
    case ReportKind::CancelReject:
    case ReportKind::ReplaceReject:
      res.request_rejected = true;
      break;
  }

  if (!r.request_id.empty()) {
    if (r.kind == ReportKind::Canceled || r.kind == ReportKind::Replaced || r.kind == ReportKind::CancelReject ||
        r.kind == ReportKind::ReplaceReject)
      o.pending_requests.erase(r.request_id);
  }
  if (!o.active()) o.pending_requests.clear();

  // Invariant: filled + canceled <= order qty (spec §7).
  if (o.order_qty > 0 && o.cum_qty() + o.canceled_qty > o.order_qty) {
    res.invariant_violation = true;
    res.detail = "cum+canceled exceeds order qty on " + o.cl_ord_id;
  }
  res.status_changed = before != o.status();
  return res;
}

void OrderBook::apply_correction(const Correction& c) {
  if (c.kind == CorrectionKind::UnallocatedPosition) {
    unallocated_adjust_[c.symbol] += c.qty;
    return;
  }
  Order& o = get_or_create(c.cl_ord_id, nullptr);
  switch (c.kind) {
    case CorrectionKind::MarkFailed: o.failed = true; break;
    case CorrectionKind::AdoptOrder:
      o.owner = Ownership::Strategy;
      o.strategy = c.strategy;
      break;
    case CorrectionKind::Unallocated: o.owner = Ownership::Unallocated; break;
    case CorrectionKind::External: o.owner = Ownership::External; break;
    case CorrectionKind::UnallocatedPosition: break;
  }
}

const Order* OrderBook::find(const std::string& id) const {
  auto it = orders_.find(id);
  return it == orders_.end() ? nullptr : &it->second;
}

Order* OrderBook::find_mut(const std::string& id) {
  auto it = orders_.find(id);
  return it == orders_.end() ? nullptr : &it->second;
}

std::string OrderBook::order_of_request(const std::string& request_id) const {
  auto it = request_to_order_.find(request_id);
  return it == request_to_order_.end() ? std::string() : it->second;
}

std::map<std::string, PositionMap> OrderBook::positions() const {
  std::map<std::string, PositionMap> out;
  for (const auto& [id, o] : orders_) {
    const Qty cum = o.cum_qty();
    if (cum == 0) continue;
    out[o.owner_key()][o.symbol] += o.side == Side::Buy ? cum : -cum;
  }
  for (const auto& [sym, q] : unallocated_adjust_) out[kUnallocatedOwner][sym] += q;
  return out;
}

PositionMap OrderBook::account_positions() const {
  PositionMap total;
  for (const auto& [owner, pm] : positions())
    for (const auto& [sym, q] : pm) total[sym] += q;
  return total;
}

int64_t OrderBook::reserved_notional(const std::string& strategy) const {
  int64_t n = 0;
  for (const auto& [id, o] : orders_)
    if (o.owner == Ownership::Strategy && o.strategy == strategy && o.active()) n += o.leaves_qty() * o.price;
  return n;
}

std::vector<const Order*> OrderBook::unknown_orders() const {
  std::vector<const Order*> v;
  for (const auto& [id, o] : orders_)
    if (o.unknown()) v.push_back(&o);
  return v;
}

std::vector<const Order*> OrderBook::active_orders() const {
  std::vector<const Order*> v;
  for (const auto& [id, o] : orders_)
    if (o.active()) v.push_back(&o);
  return v;
}

}  // namespace gw
