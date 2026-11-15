// Gateway -> strategy push interface. v1 ships an in-process implementation; a socket
// or shared-memory transport can implement the same interface later (spec §16).
#pragma once

#include <string>
#include <vector>

#include "gw/order_book.hpp"

namespace gw {

struct OrderView {
  std::string cl_ord_id;
  std::string symbol;
  Side side = Side::Buy;
  Price price = 0;
  Qty order_qty = 0;
  Qty cum_qty = 0;
  Qty leaves_qty = 0;
  OrdStatus status = OrdStatus::PendingNew;
  bool unknown = false;  // sent, never confirmed
};

OrderView make_view(const Order& o);

// Sent after recovery (spec §8 step 8).
struct StrategySnapshot {
  std::string strategy;
  std::vector<OrderView> open_orders;
  std::vector<Fill> recovered_fills;  // fills learned during recovery (also pushed individually)
  PositionMap positions;
  uint64_t seq_cut = 0;  // incremental events after the snapshot have seq > seq_cut
};

class StrategyChannel {
 public:
  virtual ~StrategyChannel() = default;
  virtual void on_order_update(const std::string& strategy, const OrderView& v, uint64_t seq) = 0;
  virtual void on_fill(const std::string& strategy, const Fill& f, bool recovered, uint64_t seq) = 0;
  virtual void on_snapshot(const StrategySnapshot& s) = 0;
  virtual void on_trading_status(const std::string& strategy, bool enabled, const std::string& reason) = 0;
};

// Records everything; used by tests and as the default in-process transport.
class InProcessStrategyChannel : public StrategyChannel {
 public:
  struct FillEvent {
    std::string strategy;
    Fill fill;
    bool recovered;
    uint64_t seq;
  };
  struct StatusEvent {
    std::string strategy;
    bool enabled;
    std::string reason;
  };

  void on_order_update(const std::string& strategy, const OrderView& v, uint64_t) override {
    updates.push_back({strategy, v});
  }
  void on_fill(const std::string& strategy, const Fill& f, bool recovered, uint64_t seq) override {
    fills.push_back({strategy, f, recovered, seq});
  }
  void on_snapshot(const StrategySnapshot& s) override { snapshots.push_back(s); }
  void on_trading_status(const std::string& strategy, bool enabled, const std::string& reason) override {
    statuses.push_back({strategy, enabled, reason});
  }

  size_t fill_count(const std::string& strategy) const {
    size_t n = 0;
    for (const auto& f : fills) n += f.strategy == strategy;
    return n;
  }
  Qty filled_qty(const std::string& strategy) const {
    Qty n = 0;
    for (const auto& f : fills)
      if (f.strategy == strategy) n += f.fill.qty;
    return n;
  }

  std::vector<std::pair<std::string, OrderView>> updates;
  std::vector<FillEvent> fills;
  std::vector<StrategySnapshot> snapshots;
  std::vector<StatusEvent> statuses;
};

}  // namespace gw
