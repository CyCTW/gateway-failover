// Core data model shared by every component (spec §4, §6, §7).
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace gw {

using Qty = int64_t;
using Price = int64_t;   // fixed-point price ticks; the gateway never does float math on prices
using TimeMs = int64_t;  // milliseconds since an arbitrary epoch (sim clock in tests)

enum class Side : uint8_t { Buy = 1, Sell = 2 };

// Rank order PendingNew < New < PartiallyFilled < terminal (spec §7).
enum class OrdStatus : uint8_t { PendingNew = 0, New = 1, PartiallyFilled = 2, Filled = 3, Canceled = 4, Rejected = 5 };

inline int status_rank(OrdStatus s) {
  switch (s) {
    case OrdStatus::PendingNew: return 0;
    case OrdStatus::New: return 1;
    case OrdStatus::PartiallyFilled: return 2;
    default: return 3;
  }
}
inline bool is_terminal(OrdStatus s) { return status_rank(s) == 3; }
const char* to_string(OrdStatus s);

enum class ReportKind : uint8_t {
  Ack = 1,           // order accepted by broker
  Fill = 2,          // execution
  Canceled = 3,      // order canceled (by request, by exchange, by cancel-on-disconnect)
  Rejected = 4,      // new order rejected
  Replaced = 5,      // amend confirmed
  CancelReject = 6,  // cancel request rejected (expected outcome, spec §11)
  ReplaceReject = 7,
  Status = 8,        // status notification, not compared (spec §9.2)
};
const char* to_string(ReportKind k);

enum class ReportSource : uint8_t { Live = 1, Replay = 2, Recovery = 3 };

// One broker report, normalised. `cl_ord_id` always names the ORDER (its original
// ClOrdID); cancel/replace requests carry their own `request_id`.
struct ExecReport {
  ReportKind kind = ReportKind::Status;
  std::string cl_ord_id;
  std::string request_id;
  std::string broker_order_id;  // 委託書號
  std::string account;
  std::string symbol;
  Side side = Side::Buy;
  Price price = 0;
  Qty order_qty = 0;
  Qty last_qty = 0;   // Fill
  Price last_px = 0;  // Fill
  Qty cum_qty = 0;    // broker's cumulative view; informative only (spec §7: never trusted for leaves)
  std::string exec_id;  // 成交序號; may be empty for brokers that do not provide one
  TimeMs transact_time = 0;
  uint64_t mod_seq = 0;  // amend sequence for Replaced; 0 = broker gave none
  std::string channel;   // order-entry channel at the broker (API / WEB / PHONE ...)
  std::string text;
};

struct NewOrderReq {
  std::string cl_ord_id;
  std::string strategy;
  std::string account;
  std::string symbol;
  Side side = Side::Buy;
  Price price = 0;
  Qty qty = 0;
  std::string tif = "DAY";
  // Local-only information that cannot travel in the ClOrdID (spec §5).
  std::string parent_id;
  std::string algo_tag;
  std::string risk_tag;
};

struct CancelReq {
  std::string request_id;
  std::string cl_ord_id;
  std::string broker_order_id;
};

struct ReplaceReq {
  std::string request_id;
  std::string cl_ord_id;
  std::string broker_order_id;
  Price new_price = 0;
  Qty new_qty = 0;
};

// Corrections produced by reconciliation / operators (spec §6.1, source `recovery`).
enum class CorrectionKind : uint8_t {
  MarkFailed = 1,           // 3a: request never reached the broker; release risk
  AdoptOrder = 2,           // 4a: broker-only order whose ClOrdID decodes to one of our strategies
  Unallocated = 3,          // 4b: cannot attribute -> unallocated bucket
  External = 4,             // 4c: order from another channel; never touched, counted in positions
  UnallocatedPosition = 5,  // §9.4: account-level difference parked in unallocated
};

struct Correction {
  CorrectionKind kind = CorrectionKind::MarkFailed;
  std::string cl_ord_id;
  std::string strategy;
  std::string symbol;
  Qty qty = 0;
  std::string note;
};

// Control records: things the gateway must remember across a crash.
enum class ControlKind : uint8_t {
  MassCancelStart = 1,
  MassCancelDone = 2,
  AccountHalt = 3,
  OperatorRelease = 4,
  BatchCancelSent = 5,
};

struct ControlRecord {
  ControlKind kind = ControlKind::MassCancelStart;
  std::string scope;  // mass-cancel scope id, see MassCancelScope::id()
  std::string text;
};

// Broker-side snapshot of one working order (未成交委託查詢).
struct OpenOrderInfo {
  std::string cl_ord_id;
  std::string broker_order_id;
  std::string account;
  std::string symbol;
  std::string channel;
  Side side = Side::Buy;
  Price price = 0;
  Qty order_qty = 0;
  Qty cum_qty = 0;
  Qty leaves_qty = 0;
};

struct OrderQueryResult {
  bool found = false;
  OrdStatus status = OrdStatus::PendingNew;
  OpenOrderInfo info;
};

using PositionMap = std::map<std::string, Qty>;  // symbol -> signed net qty

enum class AlertLevel : uint8_t { Info, Warn, Critical };

class AlertSink {
 public:
  virtual ~AlertSink() = default;
  virtual void alert(AlertLevel level, const std::string& code, const std::string& message) = 0;
};

class Clock {
 public:
  virtual ~Clock() = default;
  virtual TimeMs now() const = 0;
};

class SimClock : public Clock {
 public:
  explicit SimClock(TimeMs start = 1'000'000) : now_(start) {}
  TimeMs now() const override { return now_; }
  void advance(TimeMs dt) { now_ += dt; }
  void set(TimeMs t) { now_ = t; }

 private:
  TimeMs now_;
};

class MarketData {
 public:
  virtual ~MarketData() = default;
  virtual std::optional<Price> last_price(const std::string& symbol) const = 0;
};

}  // namespace gw
