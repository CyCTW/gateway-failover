#include "gw/wal.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>

namespace gw {

namespace {

constexpr char kMagic[8] = {'G', 'W', 'W', 'A', 'L', '0', '0', '1'};
constexpr size_t kHeader = 8;            // len + crc
constexpr size_t kBodyFixed = 8 + 8 + 1;  // seq + ts + type

void put_u32(uint8_t* p, uint32_t v) { std::memcpy(p, &v, 4); }
uint32_t get_u32(const uint8_t* p) {
  uint32_t v;
  std::memcpy(&v, p, 4);
  return v;
}

[[noreturn]] void sys_fail(const std::string& what) { throw std::runtime_error(what + ": " + std::strerror(errno)); }

// ---- tiny binary codec
class Writer {
 public:
  void u8(uint8_t v) { s_.push_back(static_cast<char>(v)); }
  void u64(uint64_t v) { s_.append(reinterpret_cast<const char*>(&v), 8); }
  void i64(int64_t v) { u64(static_cast<uint64_t>(v)); }
  void str(const std::string& v) {
    const uint32_t n = static_cast<uint32_t>(v.size());
    s_.append(reinterpret_cast<const char*>(&n), 4);
    s_ += v;
  }
  std::string take() { return std::move(s_); }

 private:
  std::string s_;
};

class Reader {
 public:
  explicit Reader(const std::string& s) : s_(s) {}
  uint8_t u8() {
    need(1);
    return static_cast<uint8_t>(s_[pos_++]);
  }
  uint64_t u64() {
    need(8);
    uint64_t v;
    std::memcpy(&v, s_.data() + pos_, 8);
    pos_ += 8;
    return v;
  }
  int64_t i64() { return static_cast<int64_t>(u64()); }
  std::string str() {
    need(4);
    uint32_t n;
    std::memcpy(&n, s_.data() + pos_, 4);
    pos_ += 4;
    need(n);
    std::string v = s_.substr(pos_, n);
    pos_ += n;
    return v;
  }

 private:
  void need(size_t n) {
    if (pos_ + n > s_.size()) throw std::runtime_error("WAL payload truncated");
  }
  const std::string& s_;
  size_t pos_ = 0;
};

}  // namespace

uint32_t crc32(const uint8_t* data, size_t n) {
  static uint32_t table[256];
  static bool init = false;
  if (!init) {
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int k = 0; k < 8; ++k) c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
      table[i] = c;
    }
    init = true;
  }
  uint32_t c = 0xFFFFFFFFu;
  for (size_t i = 0; i < n; ++i) c = table[(c ^ data[i]) & 0xFF] ^ (c >> 8);
  return c ^ 0xFFFFFFFFu;
}

Wal::Wal(const std::string& path, size_t grow_bytes) : path_(path), grow_bytes_(grow_bytes) {
  fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd_ < 0) sys_fail("open " + path);
  struct stat st {};
  if (::fstat(fd_, &st) != 0) sys_fail("fstat");
  size_t size = static_cast<size_t>(st.st_size);
  if (size < sizeof kMagic) {
    size = grow_bytes_;
    if (::ftruncate(fd_, static_cast<off_t>(size)) != 0) sys_fail("ftruncate");
    map(size);
    std::memcpy(base_, kMagic, sizeof kMagic);
  } else {
    map(size);
    if (std::memcmp(base_, kMagic, sizeof kMagic) != 0) throw std::runtime_error("not a gateway WAL: " + path);
  }
  recover();
}

Wal::~Wal() {
  if (base_) ::munmap(base_, mapped_);
  if (fd_ >= 0) ::close(fd_);
}

void Wal::map(size_t size) {
  if (base_) ::munmap(base_, mapped_);
  void* p = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
  if (p == MAP_FAILED) sys_fail("mmap");
  base_ = static_cast<uint8_t*>(p);
  mapped_ = size;
}

void Wal::recover() {
  size_t off = sizeof kMagic;
  while (off + kHeader <= mapped_) {
    const uint32_t len = get_u32(base_ + off);
    if (len == 0) break;  // clean end
    if (len < kBodyFixed || off + kHeader + len > mapped_) break;
    const uint8_t* body = base_ + off + kHeader;
    if (crc32(body, len) != get_u32(base_ + off + 4)) break;
    WalRecord rec;
    std::memcpy(&rec.seq, body, 8);
    std::memcpy(&rec.ts, body + 8, 8);
    rec.type = static_cast<RecordType>(body[16]);
    rec.payload.assign(reinterpret_cast<const char*>(body + kBodyFixed), len - kBodyFixed);
    next_seq_ = rec.seq + 1;
    records_.push_back(std::move(rec));
    off += kHeader + len;
  }
  // Anything non-zero past `off` is a torn/corrupt tail: count it and wipe it.
  size_t last_nonzero = off;
  for (size_t i = off; i < mapped_; ++i)
    if (base_[i] != 0) last_nonzero = i + 1;
  truncated_bytes_ = last_nonzero - off;
  if (truncated_bytes_) std::memset(base_ + off, 0, last_nonzero - off);
  end_ = off;
}

uint64_t Wal::append(RecordType type, TimeMs ts, const std::string& payload) {
  const size_t body_len = kBodyFixed + payload.size();
  const size_t need = kHeader + body_len;
  if (end_ + need + kHeader > mapped_) {  // keep room for a zero terminator
    const size_t size = mapped_ + std::max(grow_bytes_, need + kHeader);
    if (::ftruncate(fd_, static_cast<off_t>(size)) != 0) sys_fail("ftruncate");
    map(size);
  }
  WalRecord rec{next_seq_++, ts, type, payload};
  uint8_t* hdr = base_ + end_;
  uint8_t* body = hdr + kHeader;
  std::memcpy(body, &rec.seq, 8);
  std::memcpy(body + 8, &rec.ts, 8);
  body[16] = static_cast<uint8_t>(type);
  std::memcpy(body + kBodyFixed, payload.data(), payload.size());
  put_u32(hdr + 4, crc32(body, body_len));
  // Length goes in last: a record torn before this point reads as "end of log".
  put_u32(hdr, static_cast<uint32_t>(body_len));
  end_ += need;
  if (replicator_)
    replicator_->on_append(rec, std::string(reinterpret_cast<const char*>(hdr), need));
  return rec.seq;
}

void Wal::sync() {
  if (::msync(base_, mapped_, MS_SYNC) != 0) sys_fail("msync");
}

// ---------------------------------------------------------------- payload codecs

std::string encode(const NewOrderReq& r) {
  Writer w;
  w.str(r.cl_ord_id);
  w.str(r.strategy);
  w.str(r.account);
  w.str(r.symbol);
  w.u8(static_cast<uint8_t>(r.side));
  w.i64(r.price);
  w.i64(r.qty);
  w.str(r.tif);
  w.str(r.parent_id);
  w.str(r.algo_tag);
  w.str(r.risk_tag);
  return w.take();
}

NewOrderReq decode_new(const std::string& s) {
  Reader rd(s);
  NewOrderReq r;
  r.cl_ord_id = rd.str();
  r.strategy = rd.str();
  r.account = rd.str();
  r.symbol = rd.str();
  r.side = static_cast<Side>(rd.u8());
  r.price = rd.i64();
  r.qty = rd.i64();
  r.tif = rd.str();
  r.parent_id = rd.str();
  r.algo_tag = rd.str();
  r.risk_tag = rd.str();
  return r;
}

std::string encode(const CancelReq& r) {
  Writer w;
  w.str(r.request_id);
  w.str(r.cl_ord_id);
  w.str(r.broker_order_id);
  return w.take();
}

CancelReq decode_cancel(const std::string& s) {
  Reader rd(s);
  CancelReq r;
  r.request_id = rd.str();
  r.cl_ord_id = rd.str();
  r.broker_order_id = rd.str();
  return r;
}

std::string encode(const ReplaceReq& r) {
  Writer w;
  w.str(r.request_id);
  w.str(r.cl_ord_id);
  w.str(r.broker_order_id);
  w.i64(r.new_price);
  w.i64(r.new_qty);
  return w.take();
}

ReplaceReq decode_replace(const std::string& s) {
  Reader rd(s);
  ReplaceReq r;
  r.request_id = rd.str();
  r.cl_ord_id = rd.str();
  r.broker_order_id = rd.str();
  r.new_price = rd.i64();
  r.new_qty = rd.i64();
  return r;
}

std::string encode(const ExecReport& r, ReportSource src, TimeMs recv_time) {
  Writer w;
  w.u8(static_cast<uint8_t>(src));
  w.i64(recv_time);
  w.u8(static_cast<uint8_t>(r.kind));
  w.str(r.cl_ord_id);
  w.str(r.request_id);
  w.str(r.broker_order_id);
  w.str(r.account);
  w.str(r.symbol);
  w.u8(static_cast<uint8_t>(r.side));
  w.i64(r.price);
  w.i64(r.order_qty);
  w.i64(r.last_qty);
  w.i64(r.last_px);
  w.i64(r.cum_qty);
  w.str(r.exec_id);
  w.i64(r.transact_time);
  w.u64(r.mod_seq);
  w.str(r.channel);
  w.str(r.text);
  return w.take();
}

ExecReport decode_report(const std::string& s, ReportSource* src, TimeMs* recv_time) {
  Reader rd(s);
  ExecReport r;
  const auto source = static_cast<ReportSource>(rd.u8());
  const TimeMs t = rd.i64();
  if (src) *src = source;
  if (recv_time) *recv_time = t;
  r.kind = static_cast<ReportKind>(rd.u8());
  r.cl_ord_id = rd.str();
  r.request_id = rd.str();
  r.broker_order_id = rd.str();
  r.account = rd.str();
  r.symbol = rd.str();
  r.side = static_cast<Side>(rd.u8());
  r.price = rd.i64();
  r.order_qty = rd.i64();
  r.last_qty = rd.i64();
  r.last_px = rd.i64();
  r.cum_qty = rd.i64();
  r.exec_id = rd.str();
  r.transact_time = rd.i64();
  r.mod_seq = rd.u64();
  r.channel = rd.str();
  r.text = rd.str();
  return r;
}

std::string encode(const Correction& c) {
  Writer w;
  w.u8(static_cast<uint8_t>(c.kind));
  w.str(c.cl_ord_id);
  w.str(c.strategy);
  w.str(c.symbol);
  w.i64(c.qty);
  w.str(c.note);
  return w.take();
}

Correction decode_correction(const std::string& s) {
  Reader rd(s);
  Correction c;
  c.kind = static_cast<CorrectionKind>(rd.u8());
  c.cl_ord_id = rd.str();
  c.strategy = rd.str();
  c.symbol = rd.str();
  c.qty = rd.i64();
  c.note = rd.str();
  return c;
}

std::string encode(const ControlRecord& c) {
  Writer w;
  w.u8(static_cast<uint8_t>(c.kind));
  w.str(c.scope);
  w.str(c.text);
  return w.take();
}

ControlRecord decode_control(const std::string& s) {
  Reader rd(s);
  ControlRecord c;
  c.kind = static_cast<ControlKind>(rd.u8());
  c.scope = rd.str();
  c.text = rd.str();
  return c;
}

}  // namespace gw
