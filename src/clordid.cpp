#include "gw/clordid.hpp"

#include <cctype>
#include <cstdio>
#include <stdexcept>

namespace gw {

void ClOrdIdCodec::register_strategy(const std::string& strategy, const std::string& code) {
  if (code.size() != kCodeLen) throw std::invalid_argument("strategy code must be 3 chars: " + code);
  for (char c : code)
    if (!std::isalnum(static_cast<unsigned char>(c))) throw std::invalid_argument("strategy code must be alnum");
  if (strategy_by_code_.count(code) && strategy_by_code_[code] != strategy)
    throw std::invalid_argument("duplicate strategy code " + code);
  code_by_strategy_[strategy] = code;
  strategy_by_code_[code] = strategy;
}

std::optional<std::string> ClOrdIdCodec::code_of(const std::string& strategy) const {
  auto it = code_by_strategy_.find(strategy);
  if (it == code_by_strategy_.end()) return std::nullopt;
  return it->second;
}

std::optional<std::string> ClOrdIdCodec::strategy_of_code(const std::string& code) const {
  auto it = strategy_by_code_.find(code);
  if (it == strategy_by_code_.end()) return std::nullopt;
  return it->second;
}

std::string ClOrdIdCodec::make(const std::string& code, uint64_t seq) const {
  if (seq >= 10'000'000ULL) throw std::overflow_error("ClOrdID sequence exhausted");
  char buf[kLen + 1];
  std::snprintf(buf, sizeof buf, "%02d%s%07llu", gateway_id_, code.c_str(), static_cast<unsigned long long>(seq));
  return buf;
}

std::optional<ClOrdIdParts> ClOrdIdCodec::parse(const std::string& id) {
  if (id.size() != kLen) return std::nullopt;
  for (size_t i = 0; i < 2; ++i)
    if (!std::isdigit(static_cast<unsigned char>(id[i]))) return std::nullopt;
  for (size_t i = 2; i < 2 + kCodeLen; ++i)
    if (!std::isalnum(static_cast<unsigned char>(id[i]))) return std::nullopt;
  for (size_t i = 2 + kCodeLen; i < kLen; ++i)
    if (!std::isdigit(static_cast<unsigned char>(id[i]))) return std::nullopt;
  ClOrdIdParts p;
  p.gateway_id = std::stoi(id.substr(0, 2));
  p.code = id.substr(2, kCodeLen);
  p.seq = std::stoull(id.substr(2 + kCodeLen));
  return p;
}

std::optional<std::string> ClOrdIdCodec::resolve_strategy(const std::string& id) const {
  auto p = parse(id);
  if (!p || p->gateway_id != gateway_id_) return std::nullopt;
  return strategy_of_code(p->code);
}

bool ClOrdIdCodec::is_ours(const std::string& id) const {
  auto p = parse(id);
  return p && p->gateway_id == gateway_id_;
}

}  // namespace gw
