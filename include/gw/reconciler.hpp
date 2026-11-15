// Compares the local book with a shadow book built purely from broker replay (spec §9).
#pragma once

#include <string>
#include <vector>

#include "gw/clordid.hpp"
#include "gw/order_book.hpp"

namespace gw {

enum class DiffKind {
  Match,               // 1
  BrokerNewer,         // 2  normal: import broker facts
  LocalPendingMissing, // 3a local PendingNew, broker has nothing
  LocalAckedMissing,   // 3b local confirmed, broker has nothing  (anomaly)
  BrokerOnlyOurs,      // 4a broker-only, ClOrdID resolves to one of our strategies
  BrokerOnlyUnknown,   // 4b broker-only, cannot attribute
  BrokerOnlyExternal,  // 4c broker-only, other order-entry channel
  LocalNewer,          // 5  local has facts the broker replay lacks (never roll back)
  FieldConflict,       // 6  same order, contradictory fields
};
const char* to_string(DiffKind k);

struct OrderDiff {
  DiffKind kind = DiffKind::Match;
  std::string cl_ord_id;
  std::string strategy;    // resolved strategy for 4a
  bool broker_has_more = false;  // shadow has facts local lacks (import them)
  std::string detail;
};

struct ReconcileResult {
  std::vector<OrderDiff> diffs;  // one per order seen on either side
  size_t count(DiffKind k) const;
  std::vector<const OrderDiff*> of(DiffKind k) const;
};

struct ReconcileContext {
  const ClOrdIdCodec* codec = nullptr;
  std::string our_channel = "API";
};

ReconcileResult reconcile(const OrderBook& local, const OrderBook& shadow, const ReconcileContext& ctx);

struct PositionDiff {
  std::string symbol;
  Qty local = 0;
  Qty broker = 0;
};
// §9.4: sum of every local owner (+ opening positions) vs. broker position query.
std::vector<PositionDiff> check_positions(const PositionMap& local_total, const PositionMap& broker);

// The broker facts in `shadow` that `local` lacks, as synthetic reports (source
// `recovery`). Fills keep their resolved key as exec_id, so re-importing is idempotent.
std::vector<ExecReport> missing_facts(const Order* local, const Order& shadow);

}  // namespace gw
