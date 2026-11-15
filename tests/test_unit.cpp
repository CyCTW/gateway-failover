// Unit tests: ClOrdID, state machine (incl. order-independence property), WAL,
// connection manager, recovery decision, reconciler classification.
#include <algorithm>
#include <cstdio>
#include <fstream>
#include <random>

#include "sim.hpp"

using namespace gw;

namespace {

ExecReport rep(ReportKind k, const std::string& id, Qty last_qty = 0, const std::string& exec = "", TimeMs t = 0) {
  ExecReport r;
  r.kind = k;
  r.cl_ord_id = id;
  r.broker_order_id = "B-" + id;
  r.account = "ACC1";
  r.symbol = "2330";
  r.side = Side::Buy;
  r.price = 100;
  r.order_qty = 10;
  r.last_qty = last_qty;
  r.last_px = 100;
  r.exec_id = exec;
  r.transact_time = t;
  r.channel = "API";
  return r;
}

std::string digest(const OrderBook& b) {
  std::string s;
  for (const auto& [id, o] : b.orders()) {
    s += id + ":" + to_string(o.status()) + ":" + std::to_string(o.cum_qty()) + ":" + std::to_string(o.leaves_qty()) +
         ":" + std::to_string(o.canceled_qty) + ":" + std::to_string(o.price) + "x" + std::to_string(o.order_qty) + "|";
    for (const auto& [k, f] : o.fills) s += k + ",";
    s += ";";
  }
  return s;
}

}  // namespace

// ------------------------------------------------------------------ ClOrdID

TEST("clordid: encode/parse roundtrip and attribution") {
  ClOrdIdCodec c(7);
  c.register_strategy("market_maker", "MMK");
  const std::string id = c.make("MMK", 42);
  CHECK_EQ(id, std::string("07MMK0000042"));
  auto p = ClOrdIdCodec::parse(id);
  REQUIRE(p.has_value());
  CHECK_EQ(p->gateway_id, 7);
  CHECK_EQ(p->seq, 42ull);
  CHECK(c.resolve_strategy(id) == std::optional<std::string>("market_maker"));
  CHECK(!c.resolve_strategy("08MMK0000042"));  // other gateway
  CHECK(!c.resolve_strategy("07XYZ0000001"));  // unknown code
  CHECK(!ClOrdIdCodec::parse("WEB-1"));
}

// ------------------------------------------------------------------ state machine

TEST("state machine: lifecycle, leaves computed locally, no regression after terminal") {
  OrderBook b;
  NewOrderReq n{"01MMK0000001", "market_maker", "ACC1", "2330", Side::Buy, 100, 10};
  b.apply_new(n);
  CHECK(b.find(n.cl_ord_id)->status() == OrdStatus::PendingNew);
  CHECK(b.find(n.cl_ord_id)->unknown());
  b.apply_report(rep(ReportKind::Ack, n.cl_ord_id), "live");
  auto r = rep(ReportKind::Fill, n.cl_ord_id, 4, "E1");
  r.cum_qty = 999;  // broker cum is ignored
  b.apply_report(r, "live");
  const Order* o = b.find(n.cl_ord_id);
  CHECK(o->status() == OrdStatus::PartiallyFilled);
  CHECK_EQ(o->leaves_qty(), 6);
  auto c = rep(ReportKind::Canceled, n.cl_ord_id);
  c.cum_qty = 4;
  b.apply_report(c, "live");
  CHECK(o->status() == OrdStatus::Canceled);
  CHECK_EQ(o->canceled_qty, 6);
  CHECK_EQ(o->leaves_qty(), 0);
  b.apply_report(rep(ReportKind::Ack, n.cl_ord_id), "live");  // late lower-rank report
  CHECK(o->status() == OrdStatus::Canceled);
  CHECK_EQ(b.reserved_notional("market_maker"), 0);
}

TEST("state machine: duplicate fills dedupe by exec id; invariant violation detected") {
  OrderBook b;
  b.apply_report(rep(ReportKind::Fill, "X", 6, "E1"), "live");
  auto res = b.apply_report(rep(ReportKind::Fill, "X", 6, "E1"), "live");
  CHECK(res.new_fills.empty());
  CHECK_EQ(b.find("X")->cum_qty(), 6);
  auto c = rep(ReportKind::Canceled, "X");
  c.cum_qty = 0;  // claims 10 canceled while 6 filled
  res = b.apply_report(c, "live");
  CHECK(res.invariant_violation);
}

TEST("state machine: fills without exec id use synthesized keys counted by occurrence") {
  OrderBook b;
  // Two genuinely distinct fills with identical (order, px, qty, time).
  b.apply_report(rep(ReportKind::Fill, "X", 2, "", 5), "live");
  b.apply_report(rep(ReportKind::Fill, "X", 2, "", 5), "live");
  CHECK_EQ(b.find("X")->cum_qty(), 4);
  // Another stream describing the same two facts must not double count.
  b.apply_report(rep(ReportKind::Fill, "X", 2, "", 5), "replay");
  b.apply_report(rep(ReportKind::Fill, "X", 2, "", 5), "replay");
  CHECK_EQ(b.find("X")->cum_qty(), 4);
}

TEST("state machine: amend uses highest sequence regardless of arrival order") {
  for (int flip = 0; flip < 2; ++flip) {
    OrderBook b;
    auto a1 = rep(ReportKind::Replaced, "X");
    a1.mod_seq = 1;
    a1.price = 101;
    a1.order_qty = 20;
    auto a2 = a1;
    a2.mod_seq = 2;
    a2.price = 102;
    a2.order_qty = 30;
    std::vector<ExecReport> v = {rep(ReportKind::Ack, "X"), a1, a2};
    if (flip) std::reverse(v.begin(), v.end());
    for (auto& r : v) b.apply_report(r, "live");
    CHECK_EQ(b.find("X")->price, 102);
    CHECK_EQ(b.find("X")->order_qty, 30);
  }
}

TEST("property: report application is idempotent and order-independent") {
  std::mt19937 rng(1234);
  for (int iter = 0; iter < 300; ++iter) {
    // Random history for 3 orders: ack, some fills, maybe cancel, maybe amend.
    std::vector<ExecReport> hist;
    int exec = 1;
    for (int k = 0; k < 3; ++k) {
      const std::string id = "O" + std::to_string(k);
      hist.push_back(rep(ReportKind::Ack, id));
      Qty cum = 0;
      const int nf = rng() % 4;
      for (int f = 0; f < nf; ++f) {
        const Qty q = 1 + rng() % 3;
        if (cum + q > 10) break;
        cum += q;
        const bool synth = rng() % 2;
        hist.push_back(rep(ReportKind::Fill, id, q, synth ? "" : "E" + std::to_string(exec++), 100 + f));
      }
      if (rng() % 2) {
        auto a = rep(ReportKind::Replaced, id);
        a.mod_seq = 1;
        a.order_qty = 12;
        hist.push_back(a);
      }
      if (rng() % 2) {
        auto c = rep(ReportKind::Canceled, id);
        c.order_qty = 10;
        c.cum_qty = cum;
        hist.push_back(c);
      }
    }
    OrderBook ref;
    for (auto& r : hist) ref.apply_report(r, "s");
    const std::string want = digest(ref);

    // Permute, and duplicate exec-id'd reports (dups of synthesized fills are not
    // distinguishable from real repeats by design, so they are not duplicated).
    auto perm = hist;
    for (auto& r : hist)
      if (!(r.kind == ReportKind::Fill && r.exec_id.empty()) && rng() % 3 == 0) perm.push_back(r);
    std::shuffle(perm.begin(), perm.end(), rng);
    OrderBook b;
    for (auto& r : perm) b.apply_report(r, "s");
    if (digest(b) != want) {
      std::fprintf(stderr, "    iter %d\n    want %s\n    got  %s\n", iter, want.c_str(), digest(b).c_str());
      CHECK(false);
      break;
    }
  }
}

// ------------------------------------------------------------------ WAL

TEST("wal: roundtrip of every record type") {
  const std::string path = sim::test_path("unit_roundtrip.wal");
  std::remove(path.c_str());
  NewOrderReq n{"01MMK0000001", "market_maker", "ACC1", "2330", Side::Sell, 101, 5, "IOC", "P1", "twap", "r"};
  ExecReport r = rep(ReportKind::Fill, n.cl_ord_id, 5, "E9", 77);
  {
    Wal w(path, 4096);
    w.append(RecordType::NewOrder, 1, encode(n));
    w.append(RecordType::Report, 2, encode(r, ReportSource::Live, 2));
    w.append(RecordType::Correction, 3, encode(Correction{CorrectionKind::AdoptOrder, "id", "s", "", 0, "n"}));
    w.append(RecordType::Control, 4, encode(ControlRecord{ControlKind::MassCancelStart, "ALL", ""}));
    for (int i = 0; i < 200; ++i) w.append(RecordType::Control, 5, std::string(100, 'x'));  // forces growth
  }
  Wal w(path, 4096);
  REQUIRE(w.records().size() == 204);
  CHECK_EQ(w.truncated_bytes(), 0u);
  CHECK_EQ(w.next_seq(), 205ull);
  NewOrderReq n2 = decode_new(w.records()[0].payload);
  CHECK_EQ(n2.algo_tag, std::string("twap"));
  CHECK(n2.side == Side::Sell);
  ReportSource src;
  ExecReport r2 = decode_report(w.records()[1].payload, &src, nullptr);
  CHECK(src == ReportSource::Live);
  CHECK_EQ(r2.exec_id, std::string("E9"));
  CHECK_EQ(decode_correction(w.records()[2].payload).strategy, std::string("s"));
  CHECK_EQ(decode_control(w.records()[3].payload).scope, std::string("ALL"));
}

TEST("wal: torn tail is detected, truncated, and appends continue (spec 14 #4)") {
  const std::string path = sim::test_path("unit_torn.wal");
  std::remove(path.c_str());
  size_t end_offset = 0;
  {
    Wal w(path, 4096);
    w.append(RecordType::Control, 1, "a");
    w.append(RecordType::Control, 2, "bb");
    end_offset = 8 + 2 * (8 + 17) + 1 + 2;
  }
  {
    // Simulate a crash mid-write: a length header promising more bytes than exist, plus junk.
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(static_cast<std::streamoff>(end_offset));
    const uint32_t len = 500;
    f.write(reinterpret_cast<const char*>(&len), 4);
    f.write("\x12\x34\x56\x78garbage", 11);
  }
  {
    Wal w(path, 4096);
    CHECK_EQ(w.records().size(), 2u);
    CHECK(w.truncated_bytes() > 0);
    w.append(RecordType::Control, 3, "ccc");
  }
  Wal w(path, 4096);
  REQUIRE(w.records().size() == 3);
  CHECK_EQ(w.records()[2].payload, std::string("ccc"));
  CHECK_EQ(w.truncated_bytes(), 0u);
}

TEST("wal: corrupted checksum stops replay at that record") {
  const std::string path = sim::test_path("unit_crc.wal");
  std::remove(path.c_str());
  {
    Wal w(path, 4096);
    w.append(RecordType::Control, 1, "first");
    w.append(RecordType::Control, 2, "second");
  }
  {
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(8 + 8 + 17 + 5 + 8 + 17);  // a payload byte of record 2
    f.write("X", 1);
  }
  Wal w(path, 4096);
  CHECK_EQ(w.records().size(), 1u);
  CHECK(w.truncated_bytes() > 0);
}

// ------------------------------------------------------------------ connection manager

TEST("reconnect: fast backoff on primary, rotation after 10s, 30s incident retries") {
  ReconnectPolicy p;
  p.jitter = 0;
  ConnectionManager cm(p, {"primary", "backup-line", "backup-host"});
  TimeMs t = 100'000;
  cm.on_connected(t);
  cm.on_disconnected(t);
  std::vector<std::pair<TimeMs, std::string>> attempts;
  for (TimeMs now = t; now < t + 130'000; now += 100) {
    if (auto ep = cm.due_attempt(now)) {
      attempts.push_back({now - t, *ep});
      cm.attempt_failed(now);
    }
  }
  REQUIRE(attempts.size() > 6);
  CHECK_EQ(attempts[0].first, 0);  // immediate
  CHECK_EQ(attempts[1].first, 1'000);
  CHECK_EQ(attempts[2].first, 3'000);
  CHECK_EQ(attempts[3].first, 7'000);
  for (auto& a : attempts)
    if (a.first < 10'000) CHECK_EQ(a.second, std::string("primary"));
  std::set<std::string> rotated;
  for (auto& a : attempts)
    if (a.first >= 10'000 && a.first < 60'000) rotated.insert(a.second);
  CHECK_EQ(rotated.size(), 3u);
  CHECK(cm.phase(t + 70'000) == ConnPhase::Incident);
  std::vector<TimeMs> incident;
  for (auto& a : attempts)
    if (a.first >= 60'000) incident.push_back(a.first);
  REQUIRE(incident.size() >= 2);
  CHECK_EQ(incident[1] - incident[0], 30'000);
}

TEST("reconnect: login rejected stops retries; flapping detected 3 in 10 min") {
  ConnectionManager cm(ReconnectPolicy{}, {"primary"});
  cm.on_disconnected(0);
  CHECK(cm.due_attempt(0).has_value());
  cm.on_login_rejected();
  CHECK(!cm.due_attempt(1'000'000).has_value());
  CHECK(cm.phase(5) == ConnPhase::Stopped);

  ConnectionManager f(ReconnectPolicy{}, {"primary"});
  TimeMs t = 1'000;
  for (int i = 0; i < 3; ++i) {
    f.on_connected(t);
    t += 60'000;
    f.on_disconnected(t);
  }
  CHECK(f.flapping(t));
  CHECK(!f.flapping(t + 700'000));
}

TEST("recovery decision: most conservative condition wins") {
  RecoveryPolicy p;
  RecoveryFacts f;
  CHECK(decide_recovery(p, f) == RecoveryAction::Resume);
  f.outage = 6'000;
  CHECK(decide_recovery(p, f) == RecoveryAction::CancelThenResume);
  f.outage = 1'000;
  f.price_check_enabled = true;
  f.price_move = 0.01;
  CHECK(decide_recovery(p, f) == RecoveryAction::CancelThenResume);
  f.price_move = std::nullopt;  // unknown => escalate
  CHECK(decide_recovery(p, f) == RecoveryAction::CancelThenResume);
  f.price_move = 0.001;
  f.reconcile_clean = false;
  CHECK(decide_recovery(p, f) == RecoveryAction::Halt);
  f.reconcile_clean = true;
  f.flapping = true;
  CHECK(decide_recovery(p, f) == RecoveryAction::Halt);
  f.flapping = false;
  f.outage = 90'000;
  CHECK(decide_recovery(p, f) == RecoveryAction::Halt);
}

// ------------------------------------------------------------------ reconciler

TEST("reconciler: classifies every 9.3 outcome") {
  ClOrdIdCodec codec(1);
  codec.register_strategy("market_maker", "MMK");
  OrderBook local, shadow;
  auto req = [&](const std::string& id) {
    local.apply_new(NewOrderReq{id, "market_maker", "ACC1", "2330", Side::Buy, 100, 10});
  };
  // 1 match
  req("01MMK0000001");
  local.apply_report(rep(ReportKind::Ack, "01MMK0000001"), "live");
  shadow.apply_report(rep(ReportKind::Ack, "01MMK0000001"), "replay");
  // 2 broker newer
  req("01MMK0000002");
  local.apply_report(rep(ReportKind::Ack, "01MMK0000002"), "live");
  shadow.apply_report(rep(ReportKind::Ack, "01MMK0000002"), "replay");
  shadow.apply_report(rep(ReportKind::Fill, "01MMK0000002", 3, "E2"), "replay");
  // 3a pending, missing
  req("01MMK0000003");
  // 3b acked, missing
  req("01MMK0000004");
  local.apply_report(rep(ReportKind::Ack, "01MMK0000004"), "live");
  // 4a ours, broker only
  shadow.apply_report(rep(ReportKind::Ack, "01MMK0000005"), "replay");
  // 4b undecodable
  shadow.apply_report(rep(ReportKind::Ack, "ZZZ-ORPHAN"), "replay");
  // 4c external channel
  auto ext = rep(ReportKind::Ack, "WEB-1");
  ext.channel = "WEB";
  shadow.apply_report(ext, "replay");
  // 5 local newer
  req("01MMK0000006");
  local.apply_report(rep(ReportKind::Fill, "01MMK0000006", 2, "E6"), "live");
  shadow.apply_report(rep(ReportKind::Ack, "01MMK0000006"), "replay");
  // 6 conflict
  req("01MMK0000007");
  local.apply_report(rep(ReportKind::Ack, "01MMK0000007"), "live");
  auto bad = rep(ReportKind::Ack, "01MMK0000007");
  bad.side = Side::Sell;
  shadow.apply_report(bad, "replay");

  ReconcileContext ctx{&codec, "API"};
  auto res = reconcile(local, shadow, ctx);
  auto kind_of = [&](const std::string& id) {
    for (auto& d : res.diffs)
      if (d.cl_ord_id == id) return d.kind;
    return DiffKind::Match;
  };
  CHECK(kind_of("01MMK0000001") == DiffKind::Match);
  CHECK(kind_of("01MMK0000002") == DiffKind::BrokerNewer);
  CHECK(kind_of("01MMK0000003") == DiffKind::LocalPendingMissing);
  CHECK(kind_of("01MMK0000004") == DiffKind::LocalAckedMissing);
  CHECK(kind_of("01MMK0000005") == DiffKind::BrokerOnlyOurs);
  CHECK(kind_of("ZZZ-ORPHAN") == DiffKind::BrokerOnlyUnknown);
  CHECK(kind_of("WEB-1") == DiffKind::BrokerOnlyExternal);
  CHECK(kind_of("01MMK0000006") == DiffKind::LocalNewer);
  CHECK(kind_of("01MMK0000007") == DiffKind::FieldConflict);

  // Importing missing facts brings local to the shadow's state, idempotently.
  for (int pass = 0; pass < 2; ++pass)
    for (auto& r : missing_facts(local.find("01MMK0000002"), *shadow.find("01MMK0000002")))
      local.apply_report(r, "recovery");
  CHECK_EQ(local.find("01MMK0000002")->cum_qty(), 3);
}

TEST("reconciler: account-level position check") {
  auto d = check_positions({{"2330", 5}, {"2317", 0}}, {{"2330", 5}, {"2454", -2}});
  REQUIRE(d.size() == 1);
  CHECK_EQ(d[0].symbol, std::string("2454"));
  CHECK_EQ(d[0].broker, -2);
}
