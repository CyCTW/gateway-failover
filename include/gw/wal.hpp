// Append-only write-ahead log (spec §6).
//
// File layout:  "GWWAL001" | record* | zero padding (preallocated, mmap'ed)
// Record:       u32 body_len | u32 crc32(body) | body
// Body:         u64 seq | i64 ts | u8 type | payload
//
// Requests and reports share one log and one local sequence, so replay order is
// deterministic. Appends are memcpy into a MAP_SHARED mapping: a process crash leaves
// the data in the page cache; fsync is left to sync() / the OS. A torn or corrupt tail
// is detected on open and truncated (zeroed) before new appends.
#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "gw/types.hpp"

namespace gw {

enum class RecordType : uint8_t {
  NewOrder = 1,
  CancelOrder = 2,
  ReplaceOrder = 3,
  Report = 4,
  Correction = 5,
  Control = 6,
  Checkpoint = 7,  // reserved: order-book snapshot + log position (optional, not used yet)
};

struct WalRecord {
  uint64_t seq = 0;
  TimeMs ts = 0;
  RecordType type = RecordType::Control;
  std::string payload;
};

// Hook for shipping records to another machine (spec §6.3). Not implemented in v1.
class WalReplicator {
 public:
  virtual ~WalReplicator() = default;
  virtual void on_append(const WalRecord& rec, const std::string& encoded) = 0;
};

class Wal {
 public:
  explicit Wal(const std::string& path, size_t grow_bytes = 1 << 20);
  ~Wal();
  Wal(const Wal&) = delete;
  Wal& operator=(const Wal&) = delete;

  uint64_t append(RecordType type, TimeMs ts, const std::string& payload);
  const std::vector<WalRecord>& records() const { return records_; }
  uint64_t next_seq() const { return next_seq_; }
  size_t truncated_bytes() const { return truncated_bytes_; }
  void sync();
  void set_replicator(WalReplicator* r) { replicator_ = r; }
  const std::string& path() const { return path_; }

 private:
  void map(size_t size);
  void recover();

  std::string path_;
  size_t grow_bytes_;
  int fd_ = -1;
  uint8_t* base_ = nullptr;
  size_t mapped_ = 0;
  size_t end_ = 0;  // logical end
  uint64_t next_seq_ = 1;
  size_t truncated_bytes_ = 0;
  std::vector<WalRecord> records_;
  WalReplicator* replicator_ = nullptr;
};

uint32_t crc32(const uint8_t* data, size_t n);

// ---- payload codecs
std::string encode(const NewOrderReq&);
std::string encode(const CancelReq&);
std::string encode(const ReplaceReq&);
std::string encode(const ExecReport&, ReportSource src, TimeMs recv_time);
std::string encode(const Correction&);
std::string encode(const ControlRecord&);

NewOrderReq decode_new(const std::string&);
CancelReq decode_cancel(const std::string&);
ReplaceReq decode_replace(const std::string&);
ExecReport decode_report(const std::string&, ReportSource* src, TimeMs* recv_time);
Correction decode_correction(const std::string&);
ControlRecord decode_control(const std::string&);

}  // namespace gw
