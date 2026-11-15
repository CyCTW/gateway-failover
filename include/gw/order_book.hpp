// Order state machine (spec §7). One implementation serves three inputs: live reports,
// WAL replay and broker replay (shadow book). Every update is a monotone merge of facts,
// so applying the same set of reports in any order, any number of times, yields the
// same state (idempotent + commutative).
#pragma once

#include <map>
#include <set>
#include <string>
#include <vector>

#include "gw/types.hpp"

namespace gw {

inline const std::string kUnallocatedOwner = "__unallocated__";
inline const std::string kExternalOwner = "__external__";

enum class Ownership : uint8_t { Strategy, Unallocated, External };

struct Fill {
  std::string key;      // exec_id, or synthesized key "S|<bid>|<px>|<qty>|<time>#<n>"
  std::string cl_ord_id;
  Qty qty = 0;
  Price px = 0;
  TimeMs time = 0;
};

struct Order {
  // identity / static fields
  std::string cl_ord_id;
  std::string strategy;
  std::string account;
  std::string symbol;
  std::string broker_order_id;
  std::string channel;
  Side side = Side::Buy;
  Ownership owner = Ownership::Unallocated;
  std::string parent_id, algo_tag, risk_tag, tif;

  // amendable fields: highest amend sequence wins
  Price price = 0;
  Qty order_qty = 0;
  uint64_t mod_seq = 0;
  bool amend_uncertain = false;  // amend without seq/time: current value must come from query API

  // facts (monotone)
  bool has_request = false;  // we sent it (WAL request record)
  bool has_report = false;   // any broker report seen
  bool acked = false;        // broker knows the order
  bool canceled_seen = false;
  bool rejected_seen = false;
  bool failed = false;       // reconciliation decided it never reached the broker
  Qty canceled_qty = 0;
  std::map<std::string, Fill> fills;
  std::set<std::string> pending_requests;  // cancel/replace request ids awaiting an answer

  Qty cum_qty() const;
  Qty leaves_qty() const;  // order_qty - filled - canceled, never the broker's value
  OrdStatus status() const;
  bool active() const { return !is_terminal(status()); }
  // Request sent, nothing heard back: may be live or filled; risk stays reserved.
  bool unknown() const { return has_request && !has_report && !failed; }
  std::string owner_key() const;
};

struct ApplyResult {
  bool created = false;
  bool status_changed = false;
  bool invariant_violation = false;  // filled + canceled > order qty
  bool conflict = false;             // static fields disagree / same fill key with different values
  bool request_rejected = false;
  std::vector<Fill> new_fills;
  std::string detail;
};

class OrderBook {
 public:
  // Requests (from our own WAL).
  void apply_new(const NewOrderReq& r);
  void apply_cancel_req(const CancelReq& r);
  void apply_replace_req(const ReplaceReq& r);

  // `stream` scopes the occurrence counter for fills without an exec id: the k-th
  // occurrence of the same (order, px, qty, time) within a stream becomes key #k, so two
  // streams describing the same facts dedupe against each other.
  ApplyResult apply_report(const ExecReport& r, const std::string& stream);
  void apply_correction(const Correction& c);

  const Order* find(const std::string& cl_ord_id) const;
  Order* find_mut(const std::string& cl_ord_id);
  const std::map<std::string, Order>& orders() const { return orders_; }
  std::string order_of_request(const std::string& request_id) const;

  // owner key (strategy / __unallocated__ / __external__) -> symbol -> qty
  std::map<std::string, PositionMap> positions() const;
  PositionMap account_positions() const;  // summed over every owner
  // Worst-case reservation: leaves of every non-terminal order, including unknown ones.
  int64_t reserved_notional(const std::string& strategy) const;
  std::vector<const Order*> unknown_orders() const;
  std::vector<const Order*> active_orders() const;

 private:
  Order& get_or_create(const std::string& id, bool* created);

  std::map<std::string, Order> orders_;
  std::map<std::string, std::string> request_to_order_;
  std::map<std::string, int> synth_counts_;  // stream + base key -> occurrences
  PositionMap unallocated_adjust_;
};

std::string synth_fill_base(const ExecReport& r);

}  // namespace gw
