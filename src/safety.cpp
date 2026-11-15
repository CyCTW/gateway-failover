#include "gw/safety.hpp"

#include <cstdio>
#include <fstream>

#include "gw/strategy_channel.hpp"

namespace gw {

bool FileKillFlag::tripped() const {
  std::ifstream f(path_);
  return f.good();
}

void FileKillFlag::trip(const std::string& reason) {
  std::ofstream f(path_, std::ios::trunc);
  f << reason << "\n";
}

void FileKillFlag::clear() { std::remove(path_.c_str()); }

void FileHeartbeat::beat(TimeMs now) {
  const std::string tmp = path_ + ".tmp";
  {
    std::ofstream f(tmp, std::ios::trunc);
    f << now << "\n";
  }
  std::rename(tmp.c_str(), path_.c_str());  // atomic replace
}

std::optional<TimeMs> FileHeartbeat::last_beat() const {
  std::ifstream f(path_);
  TimeMs t;
  if (f >> t) return t;
  return std::nullopt;
}

OrderView make_view(const Order& o) {
  OrderView v;
  v.cl_ord_id = o.cl_ord_id;
  v.symbol = o.symbol;
  v.side = o.side;
  v.price = o.price;
  v.order_qty = o.order_qty;
  v.cum_qty = o.cum_qty();
  v.leaves_qty = o.leaves_qty();
  v.status = o.status();
  v.unknown = o.unknown();
  return v;
}

}  // namespace gw
