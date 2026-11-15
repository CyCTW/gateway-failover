// Protocol-neutral broker session interface (spec §3). A FIX or broker-specific adapter
// implements this; MockSession implements it for tests. Capabilities that differ per
// broker (§16: batch cancel, cancel-on-disconnect, second credentials) are flags.
#pragma once

#include <optional>
#include <string>
#include <vector>

#include "gw/types.hpp"

namespace gw {

struct BrokerCapabilities {
  bool batch_cancel = false;          // account-level mass cancel
  bool cancel_on_disconnect = false;  // exchange/broker deletes working orders on disconnect
  bool second_credentials = false;    // independent session for the kill switch
  bool cross_session_cancel = false;  // a session may cancel orders entered by another
  bool replay_end_marker = true;      // replay terminates with ReplayEnd (+ count)
};

enum class LoginResult { Ok, Rejected /* credentials, seq too low: do NOT retry */, Unreachable };

enum class BrokerEventType { Report, ReplayEnd, Disconnected };

struct BrokerEvent {
  BrokerEventType type = BrokerEventType::Report;
  ReportSource source = ReportSource::Live;  // Live or Replay
  ExecReport report;
  int64_t expected_count = -1;  // ReplayEnd: number of replay reports the broker sent; -1 unknown
  std::string reason;
};

class BrokerAdapter {
 public:
  virtual ~BrokerAdapter() = default;

  virtual BrokerCapabilities capabilities() const = 0;
  virtual LoginResult connect(const std::string& endpoint) = 0;
  virtual void disconnect() = 0;
  virtual bool connected() const = 0;
  // Flow control: false when the next message would exceed the broker's rate limit.
  virtual bool can_send() const = 0;

  // Return false if the message could not be handed to the transport.
  virtual bool send_new(const NewOrderReq& r) = 0;
  virtual bool send_cancel(const CancelReq& r) = 0;
  virtual bool send_replace(const ReplaceReq& r) = 0;
  virtual bool mass_cancel(const std::string& account) = 0;

  // Full-day replay from the beginning; cannot start from a sequence number (spec §3).
  virtual bool request_replay() = 0;

  // Synchronous queries; nullopt = query failed.
  virtual std::optional<std::vector<OpenOrderInfo>> query_open_orders(const std::string& account) = 0;
  virtual std::optional<OrderQueryResult> query_order(const std::string& cl_ord_id) = 0;
  virtual std::optional<PositionMap> query_positions(const std::string& account) = 0;

  virtual std::vector<BrokerEvent> poll() = 0;
};

}  // namespace gw
