// ClOrdID = gateway id (2 digits) + strategy short code (3 chars) + sequence (7 digits).
// Example: "01MMK0000042". Lets broker replay data restore strategy attribution even if
// the local WAL is lost (spec §5).
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>

namespace gw {

struct ClOrdIdParts {
  int gateway_id = 0;
  std::string code;  // strategy short code
  uint64_t seq = 0;
};

class ClOrdIdCodec {
 public:
  static constexpr size_t kCodeLen = 3;
  static constexpr size_t kSeqLen = 7;
  static constexpr size_t kLen = 2 + kCodeLen + kSeqLen;

  explicit ClOrdIdCodec(int gateway_id) : gateway_id_(gateway_id) {}

  // Short-code table (used when the strategy name is longer than the code field).
  void register_strategy(const std::string& strategy, const std::string& code);

  int gateway_id() const { return gateway_id_; }
  std::optional<std::string> code_of(const std::string& strategy) const;
  std::optional<std::string> strategy_of_code(const std::string& code) const;

  std::string make(const std::string& code, uint64_t seq) const;

  // Parses any well-formed id (any gateway). Returns nullopt for foreign/garbage ids.
  static std::optional<ClOrdIdParts> parse(const std::string& id);

  // Strategy name if the id was issued by THIS gateway for a known strategy.
  std::optional<std::string> resolve_strategy(const std::string& id) const;
  bool is_ours(const std::string& id) const;

 private:
  int gateway_id_;
  std::map<std::string, std::string> code_by_strategy_;
  std::map<std::string, std::string> strategy_by_code_;
};

}  // namespace gw
