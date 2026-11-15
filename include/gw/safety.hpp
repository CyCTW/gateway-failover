// Shared kill flag (checked by the gateway before every send, spec §12.2 step 1) and
// heartbeat plumbing between gateway and watchdog (§13).
#pragma once

#include <optional>
#include <string>

#include "gw/types.hpp"

namespace gw {

class KillFlag {
 public:
  virtual ~KillFlag() = default;
  virtual bool tripped() const = 0;
  virtual void trip(const std::string& reason) = 0;
  virtual void clear() = 0;
};

class MemoryKillFlag : public KillFlag {
 public:
  bool tripped() const override { return tripped_; }
  void trip(const std::string& reason) override {
    tripped_ = true;
    reason_ = reason;
  }
  void clear() override { tripped_ = false; }
  const std::string& reason() const { return reason_; }

 private:
  bool tripped_ = false;
  std::string reason_;
};

// Flag = existence of a file. Works across processes on one host (or a shared mount).
class FileKillFlag : public KillFlag {
 public:
  explicit FileKillFlag(std::string path) : path_(std::move(path)) {}
  bool tripped() const override;
  void trip(const std::string& reason) override;
  void clear() override;

 private:
  std::string path_;
};

class Heartbeat {
 public:
  virtual ~Heartbeat() = default;
  virtual void beat(TimeMs now) = 0;
  virtual std::optional<TimeMs> last_beat() const = 0;
};

class MemoryHeartbeat : public Heartbeat {
 public:
  void beat(TimeMs now) override { last_ = now; }
  std::optional<TimeMs> last_beat() const override { return last_; }

 private:
  std::optional<TimeMs> last_;
};

class FileHeartbeat : public Heartbeat {
 public:
  explicit FileHeartbeat(std::string path) : path_(std::move(path)) {}
  void beat(TimeMs now) override;
  std::optional<TimeMs> last_beat() const override;

 private:
  std::string path_;
};

}  // namespace gw
