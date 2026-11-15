// Failover scenario tests against the mock broker (spec §14 "必測情境" + §9.3 outcomes).
#include <set>

#include "sim.hpp"

using namespace gw;
using sim::Sim;

namespace {
const std::string MM = "market_maker";
const std::string TR = "trend";
}  // namespace

TEST("baseline: clean start, order lifecycle, fills pushed once") {
  Sim s("baseline");
  s.boot_and_recover();
  CHECK(s.gw->trading_enabled(MM));
  const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
  s.run_for(50);
  CHECK(s.gw->book().find(id)->status() == OrdStatus::New);
  s.ex.faults().duplicate_live = true;
  s.ex.fill(id, 4, 100);
  s.run_for(50);
  CHECK_EQ(s.strat.fill_count(MM), 1u);
  CHECK_EQ(s.gw->book().find(id)->cum_qty(), 4);
  CHECK(s.gw->submit_cancel(MM, id).ok);
  s.run_for(50);
  CHECK(s.gw->book().find(id)->status() == OrdStatus::Canceled);
  CHECK_EQ(s.gw->book().reserved_notional(MM), 0);
}

TEST("#1a crash after send, before ack: broker had it -> restored as live order") {
  Sim s("s1a");
  s.boot_and_recover();
  const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
  s.crash();  // ack never processed
  CHECK(s.ex.order(id) != nullptr);
  s.boot();
  CHECK(s.gw->book().find(id)->unknown());  // WAL replay: request without report
  CHECK(!s.gw->submit_new(MM, OrderParams{"2330", Side::Buy, 100, 1}).ok);  // Recovering: no new orders
  REQUIRE(s.run_until([&] { return s.active(); }));
  const Order* o = s.gw->book().find(id);
  CHECK(o->status() == OrdStatus::New);
  CHECK(!o->broker_order_id.empty());
  CHECK(!s.gw->account_halted());
  const StrategySnapshot* snap = nullptr;
  for (const auto& sn : s.strat.snapshots)
    if (sn.strategy == MM) snap = &sn;
  REQUIRE(snap);
  REQUIRE(snap->open_orders.size() == 1u);
  CHECK(snap->open_orders[0].status == OrdStatus::New);
}

TEST("#1b crash after send, order lost in transit -> 3a: recheck, then failed, risk released") {
  Sim s("s1b");
  s.boot_and_recover();
  s.ex.faults().drop_next_new = true;
  const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
  CHECK_EQ(s.gw->book().reserved_notional(MM), 1000);  // unknown orders keep risk reserved
  s.crash();
  s.boot();
  REQUIRE(s.run_until([&] { return s.active(); }));
  CHECK_EQ(s.gw->last_reconcile().count(DiffKind::LocalPendingMissing), 1u);
  CHECK(s.gw->replay_rounds() >= 2);
  CHECK(s.gw->book().find(id)->failed);
  CHECK_EQ(s.gw->book().reserved_notional(MM), 0);
  CHECK(s.alerts.has("order_failed"));
  CHECK(!s.gw->account_halted());
}

TEST("#2 fills during an outage are pushed after recovery exactly once") {
  Sim s("s2");
  s.boot_and_recover();
  const auto a = s.order(MM, "2330", Side::Buy, 100, 10);
  const auto b = s.order(TR, "2317", Side::Sell, 50, 5);
  s.run_for(50);
  s.ex.fill(a, 3, 100);
  s.run_for(50);
  CHECK_EQ(s.strat.fill_count(MM), 1u);

  s.session->drop_link();
  s.ex.set_endpoint_up("primary", false);
  s.run_for(20);
  CHECK(!s.gw->trading_enabled(MM));
  CHECK(!s.gw->submit_new(MM, OrderParams{"2330", Side::Buy, 100, 1}).ok);
  s.ex.fill(a, 2, 101);  // happen while we are blind
  s.ex.fill(b, 5, 50);
  s.run_for(1'000);
  s.ex.set_endpoint_up("primary", true);
  REQUIRE(s.run_until([&] { return s.active(); }));

  CHECK_EQ(s.strat.fill_count(MM), 2u);
  CHECK_EQ(s.strat.filled_qty(MM), 5);
  CHECK_EQ(s.strat.fill_count(TR), 1u);
  int recovered = 0;
  for (auto& f : s.strat.fills) recovered += f.recovered;
  CHECK_EQ(recovered, 2);
  CHECK(s.gw->book().find(b)->status() == OrdStatus::Filled);
  CHECK(s.gw->trading_enabled(MM));  // short outage, clean reconcile -> resume

  // A restart after that must not re-push anything.
  const size_t before = s.strat.fills.size();
  s.crash();
  s.boot();
  REQUIRE(s.run_until([&] { return s.active(); }));
  CHECK_EQ(s.strat.fills.size(), before);
}

TEST("#3 replay order differs from live order (shuffled, merged, no exec ids): same final state") {
  for (int variant = 0; variant < 2; ++variant) {
    Sim s("s3_" + std::to_string(variant));
    if (variant == 1) s.ex.faults().omit_exec_id = true;
    s.boot_and_recover();
    std::vector<std::string> ids;
    for (int i = 0; i < 4; ++i) ids.push_back(s.order(MM, "2330", Side::Buy, 100 + i, 10));
    s.run_for(30);
    s.ex.fill(ids[0], 3, 100);
    s.ex.fill(ids[0], 3, 100);
    s.ex.fill(ids[1], 10, 101);
    s.run_for(30);
    CHECK(s.gw->submit_replace(MM, ids[2], 105, 8).ok);
    CHECK(s.gw->submit_cancel(MM, ids[3]).ok);
    s.run_for(30);
    s.crash();
    s.ex.faults().replay_shuffle = true;
    s.ex.faults().replay_merge = true;
    s.boot();
    REQUIRE(s.run_until([&] { return s.active(); }));
    CHECK(!s.gw->account_halted());
    CHECK_EQ(s.gw->book().find(ids[0])->cum_qty(), 6);
    CHECK(s.gw->book().find(ids[1])->status() == OrdStatus::Filled);
    CHECK_EQ(s.gw->book().find(ids[2])->price, 105);
    CHECK(s.gw->book().find(ids[3])->status() == OrdStatus::Canceled);
    CHECK_EQ(s.gw->last_reconcile().count(DiffKind::FieldConflict), 0u);
    CHECK_EQ(s.gw->last_reconcile().count(DiffKind::LocalNewer), 0u);
  }
}

TEST("#4 torn WAL tail: restart truncates it and carries on") {
  Sim s("s4");
  s.boot_and_recover();
  const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
  s.run_for(30);
  s.crash();
  {
    FILE* f = std::fopen(s.wal_path.c_str(), "r+b");
    REQUIRE(f);
    // Find logical end (first zero length) and write a half record there.
    std::vector<unsigned char> buf(1 << 20);
    const size_t n = std::fread(buf.data(), 1, buf.size(), f);
    size_t off = 8;
    while (off + 8 <= n) {
      uint32_t len;
      std::memcpy(&len, &buf[off], 4);
      if (!len) break;
      off += 8 + len;
    }
    std::fseek(f, static_cast<long>(off), SEEK_SET);
    const uint32_t len = 64;
    std::fwrite(&len, 4, 1, f);
    std::fwrite("\x01\x02\x03\x04partial", 1, 11, f);
    std::fclose(f);
  }
  s.boot();
  CHECK(s.gw->wal_truncated_bytes() > 0);
  REQUIRE(s.run_until([&] { return s.active(); }));
  CHECK(s.gw->book().find(id)->status() == OrdStatus::New);
  const auto id2 = s.order(MM, "2330", Side::Buy, 100, 1);
  CHECK(id2 != id);
}

TEST("9.3 #3b confirmed order missing from replay: recheck, then halt") {
  Sim s("s3b");
  s.boot_and_recover();
  const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
  s.run_for(30);
  s.crash();
  s.ex.faults().replay_filter = [id](const ExecReport& r) { return r.cl_ord_id != id; };
  s.boot();
  REQUIRE(s.run_until([&] { return s.recovered(); }));
  CHECK(s.gw->replay_rounds() >= 2);
  CHECK(s.gw->account_halted());
  CHECK(s.gw->state() == GwState::Halted);
  CHECK(!s.gw->trading_enabled(MM));
}

TEST("9.3 #4a WAL lost: strategy attribution restored from ClOrdID, no id reuse") {
  Sim s("s4a");
  s.boot_and_recover();
  const auto a = s.order(MM, "2330", Side::Buy, 100, 10);
  const auto b = s.order(TR, "2317", Side::Buy, 50, 4);
  s.run_for(30);
  s.ex.fill(a, 6, 100);
  s.run_for(30);
  s.crash();
  std::remove(s.wal_path.c_str());  // disk lost
  s.boot();
  REQUIRE(s.run_until([&] { return s.active(); }));
  CHECK_EQ(s.gw->last_reconcile().count(DiffKind::BrokerOnlyOurs), 2u);
  CHECK_EQ(s.gw->book().find(a)->strategy, MM);
  CHECK_EQ(s.gw->book().find(b)->strategy, TR);
  CHECK_EQ(s.pos(MM, "2330"), 6);
  CHECK(!s.gw->account_halted());
  const auto c = s.order(MM, "2330", Side::Buy, 100, 1);
  CHECK(c != a && c != b);
  CHECK(ClOrdIdCodec::parse(c)->seq > ClOrdIdCodec::parse(b)->seq);
}

TEST("9.3 #4b undecodable order on our channel: unallocated, cancelled, alerted") {
  Sim s("s4b");
  s.boot_and_recover();
  s.ex.inject_orphan("ACC1", "MANUAL-123", "2330", Side::Buy, 99, 3);
  s.run_for(2'000);
  const Order* o = s.gw->book().find("MANUAL-123");
  REQUIRE(o);
  CHECK(o->owner == Ownership::Unallocated);
  CHECK(s.alerts.has("unallocated_order"));
  CHECK(s.ex.order("MANUAL-123")->status == OrdStatus::Canceled);

  // Same thing discovered by replay after a restart.
  s.crash();
  s.ex.inject_orphan("ACC1", "MANUAL-456", "2330", Side::Buy, 99, 3);
  s.boot();
  REQUIRE(s.run_until([&] { return s.active(); }));
  s.run_for(2'000);
  CHECK(s.gw->book().find("MANUAL-456")->owner == Ownership::Unallocated);
  CHECK(s.ex.order("MANUAL-456")->status == OrdStatus::Canceled);
}

TEST("9.3 #4c external channel order: left alone, counted in account positions") {
  Sim s("s4c");
  s.boot_and_recover();
  const auto ext = s.ex.inject_external("ACC1", "2330", Side::Buy, 100, 5);
  s.ex.fill(ext, 2, 100);
  s.run_for(100);
  s.crash();
  s.boot();
  REQUIRE(s.run_until([&] { return s.active(); }));
  const Order* o = s.gw->book().find(ext);
  REQUIRE(o);
  CHECK(o->owner == Ownership::External);
  CHECK(s.ex.order(ext)->status == OrdStatus::PartiallyFilled);  // untouched
  CHECK(!s.gw->account_halted());                                 // account check includes it
  s.gw->request_mass_cancel(MassCancelScope::all());
  s.run_for(3'000);
  CHECK(s.ex.order(ext)->status == OrdStatus::PartiallyFilled);  // even a mass cancel skips it
}

TEST("9.3 #5 local ahead of broker: never rolled back, halt after recheck") {
  Sim s("s5");
  s.boot_and_recover();
  const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
  s.run_for(30);
  s.ex.fill(id, 4, 100);
  s.run_for(30);
  s.crash();
  s.ex.faults().replay_filter = [](const ExecReport& r) { return r.kind != ReportKind::Fill; };
  s.boot();
  REQUIRE(s.run_until([&] { return s.recovered(); }));
  CHECK(s.gw->replay_rounds() >= 2);
  CHECK_EQ(s.gw->book().find(id)->cum_qty(), 4);
  CHECK(s.gw->account_halted());
}

TEST("9.3 #6 field conflict on the same order: halt immediately") {
  Sim s("s6");
  s.boot_and_recover();
  const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
  s.run_for(30);
  s.crash();
  s.ex.faults().replay_mutate = [id](ExecReport& r) {
    if (r.cl_ord_id == id) r.side = Side::Sell;
  };
  s.boot();
  REQUIRE(s.run_until([&] { return s.recovered(); }));
  CHECK_EQ(s.gw->last_reconcile().count(DiffKind::FieldConflict), 1u);
  CHECK(s.gw->account_halted());
  CHECK_EQ(s.gw->replay_rounds(), 1);
}

TEST("9.3 #7 replay keeps failing: fall back to queries, stay halted") {
  Sim s("s7");
  s.boot_and_recover();
  const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
  s.run_for(30);
  s.crash();
  s.ex.faults().replay_fail = 100;
  s.boot();
  REQUIRE(s.run_until([&] { return s.recovered(); }));
  CHECK(s.alerts.has("replay_unavailable"));
  CHECK(s.gw->account_halted());
  CHECK(s.gw->book().find(id)->acked);  // from the open-order query
}

TEST("replay incomplete once / no end marker: retried or settled by silence timeout") {
  {
    Sim s("s_incomplete");
    s.boot_and_recover();
    s.order(MM, "2330", Side::Buy, 100, 10);
    s.run_for(30);
    s.crash();
    s.ex.faults().replay_drop_tail = 1;
    s.boot();
    s.run_for(50);
    s.ex.faults().replay_drop_tail = 0;
    REQUIRE(s.run_until([&] { return s.active(); }));
    CHECK(s.alerts.has("replay_failed"));
  }
  {
    Sim s("s_noend");
    s.ex.caps().replay_end_marker = false;
    s.boot();
    REQUIRE(s.run_until([&] { return s.active(); }));
    const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
    s.run_for(30);
    s.crash();
    s.boot();
    REQUIRE(s.run_until([&] { return s.active(); }));
    CHECK(s.gw->book().find(id)->status() == OrdStatus::New);
  }
}

TEST("#6 crash during mass cancel: resumed after restart, nothing missed or misjudged") {
  Sim s("s6mc");
  s.boot_and_recover();
  std::vector<std::string> ids;
  for (int i = 0; i < 6; ++i) ids.push_back(s.order(i % 2 ? TR : MM, "2330", Side::Buy, 100 + i, 10));
  s.run_for(30);
  s.ex.faults().rate_limit_per_tick = 1;
  s.gw->request_mass_cancel(MassCancelScope::all());
  CHECK(!s.gw->trading_enabled(MM));
  s.run_for(20);  // only a couple of cancels get out
  CHECK(s.exchange_open() > 0);
  // Cancels go out largest-notional first, so ids[0] (lowest price) is still working.
  CHECK(s.ex.fill(ids[0], 10, 100));  // fills while we are down
  s.crash();
  s.boot();
  REQUIRE(s.run_until([&] { return s.active() && !s.gw->mass_cancel_active(); }));
  CHECK(s.alerts.has("mass_cancel_done"));
  CHECK_EQ(s.exchange_open(), 0u);
  CHECK(s.gw->book().active_orders().empty());
  CHECK(s.gw->book().find(ids[0])->status() == OrdStatus::Filled);  // not misjudged as canceled
  CHECK_EQ(s.pos(MM, "2330"), 10);
  for (size_t i = 1; i < ids.size(); ++i) CHECK(s.gw->book().find(ids[i])->status() == OrdStatus::Canceled);
}

TEST("#7 flapping link (3 drops in 10 min) forces halt and cancels") {
  Sim s("s7flap");
  s.boot_and_recover();
  const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
  for (int i = 0; i < 3; ++i) {
    s.run_for(1'000);
    s.session->drop_link();
    REQUIRE(s.run_until([&] { return s.recovered(); }));
  }
  s.run_for(3'000);
  CHECK(!s.gw->trading_enabled(MM));
  CHECK(s.ex.order(id)->status == OrdStatus::Canceled);
  CHECK(!s.gw->submit_new(MM, OrderParams{"2330", Side::Buy, 100, 1}).ok);
  CHECK(s.gw->operator_release());
  s.run_for(50);
  CHECK(s.gw->trading_enabled(MM));
}

TEST("#8 kill switch while the gateway keeps sending: zero working orders at the end") {
  Sim s("s8");
  s.ex.caps().cross_session_cancel = true;
  s.boot_and_recover();
  for (int i = 0; i < 5; ++i) s.order(MM, "2330", Side::Buy, 100 + i, 10);
  auto ks_session = s.ex.open_session("ops", "ACC1", false);
  sim::RecordingAlerts ks_alerts;
  KillSwitchConfig kc;
  kc.accounts = {"ACC1"};
  KillSwitch ks(kc, *ks_session, ks_alerts);
  int sent_after_trip = 0;
  auto sleep = [&](TimeMs ms) {
    for (TimeMs t = 0; t < ms; t += 10) {
      // A misbehaving strategy keeps firing orders the whole time.
      auto r = s.gw->submit_new(MM, OrderParams{"2330", Side::Buy, 100, 1});
      if (r.ok) ++sent_after_trip;
      s.step(10);
    }
  };
  auto rep = ks.run([&] { s.kill.trip("ops kill"); }, sleep);
  CHECK(rep.success);
  CHECK_EQ(sent_after_trip, 0);
  CHECK_EQ(s.exchange_open(), 0u);
  CHECK(rep.residual.empty());
  // Idempotent: running again is harmless.
  auto rep2 = ks.run(nullptr, sleep);
  CHECK(rep2.success);
  CHECK_EQ(rep2.cancels_sent, 0);
}

TEST("kill switch: strategy scope only touches that strategy; stuck order reported") {
  Sim s("s8b");
  s.ex.caps().cross_session_cancel = true;
  s.boot_and_recover();
  const auto mm = s.order(MM, "2330", Side::Buy, 100, 10);
  const auto tr = s.order(TR, "2330", Side::Buy, 100, 10);
  s.run_for(30);
  auto ks_session = s.ex.open_session("ops", "ACC1", false);
  sim::RecordingAlerts ks_alerts;
  KillSwitchConfig kc;
  kc.accounts = {"ACC1"};
  kc.scope = KillSwitchConfig::Scope::Strategy;
  kc.strategy_code = "TRD";
  KillSwitch ks(kc, *ks_session, ks_alerts);
  auto rep = ks.run(nullptr, [&](TimeMs ms) { s.run_for(ms); });
  CHECK(rep.success);
  CHECK(s.ex.order(tr)->status == OrdStatus::Canceled);
  CHECK(s.ex.order(mm)->status == OrdStatus::New);

  s.ex.faults().cancel_reject_next = 1000;  // the broker refuses every cancel
  kc.scope = KillSwitchConfig::Scope::Account;
  KillSwitch ks2(kc, *ks_session, ks_alerts);
  auto rep2 = ks2.run(nullptr, [&](TimeMs ms) { s.run_for(ms); });
  CHECK(!rep2.success);
  CHECK_EQ(rep2.residual.size(), 1u);
  CHECK(!rep2.cancel_rejects.empty());
  CHECK_EQ(rep2.cancels_sent, kc.max_attempts_per_order);
}

TEST("#9 ClOrdIDs never repeat across restarts") {
  Sim s("s9");
  s.boot_and_recover();
  std::set<std::string> ids;
  for (int round = 0; round < 3; ++round) {
    for (int i = 0; i < 5; ++i) {
      auto id = s.order(round % 2 ? TR : MM, "2330", Side::Buy, 100, 1);
      CHECK(ids.insert(id).second);
    }
    auto c = *ids.begin();
    s.gw->submit_cancel(MM, c);  // request ids share the sequence
    s.crash();
    s.boot_and_recover();
  }
  CHECK_EQ(ids.size(), 15u);
}

TEST("#10 account-level position mismatch: diff parked in unallocated, trading stopped") {
  Sim s("s10");
  s.cfg.opening_positions = {{"2330", 100}};  // ops says we started long 100; broker disagrees
  s.boot();
  REQUIRE(s.run_until([&] { return s.recovered(); }));
  CHECK(s.alerts.has("position_mismatch"));
  CHECK(s.gw->account_halted());
  CHECK_EQ(s.pos(kUnallocatedOwner, "2330"), -100);
  CHECK(s.gw->state() == GwState::Halted);
  // The halt is durable: a restart does not silently resume trading.
  s.crash();
  s.boot();
  REQUIRE(s.run_until([&] { return s.recovered(); }));
  CHECK(s.gw->account_halted());
  CHECK(!s.gw->trading_enabled(MM));
}

TEST("outage longer than max_outage_for_resume: cancel_then_resume, auto resumes after cancels") {
  Sim s("s_long");
  s.boot_and_recover();
  const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
  s.run_for(30);
  s.session->drop_link();
  s.ex.set_endpoint_up("primary", false);
  s.run_for(8'000);
  s.ex.set_endpoint_up("primary", true);
  REQUIRE(s.run_until([&] { return s.recovered(); }));
  REQUIRE(s.run_until([&] { return s.gw->trading_enabled(MM); }, 20'000));
  CHECK(s.ex.order(id)->status == OrdStatus::Canceled);
  CHECK(!s.gw->account_halted());
}

TEST("price moved too much during outage: escalates even for a short outage") {
  for (int moved = 0; moved < 2; ++moved) {
    Sim s("s_px" + std::to_string(moved));
    sim::FakeMarket mkt;
    mkt.px["2330"] = 1000;
    s.market = &mkt;
    s.boot_and_recover();
    const auto id = s.order(MM, "2330", Side::Buy, 1000, 10);
    s.run_for(30);
    s.session->drop_link();
    mkt.px["2330"] = moved ? 1020 : 1002;  // +2% vs +0.2% (limit 0.5%)
    REQUIRE(s.run_until([&] { return s.recovered(); }));
    s.run_for(3'000);
    if (moved)
      CHECK(s.ex.order(id)->status == OrdStatus::Canceled);
    else
      CHECK(s.ex.order(id)->status == OrdStatus::New);
    CHECK(s.gw->trading_enabled(MM));  // cancel_then_resume comes back on its own
  }
}

TEST("restart with market data but no pre-crash price: conservatively cancels first") {
  Sim s("s_px_restart");
  sim::FakeMarket mkt;
  mkt.px["2330"] = 1000;
  s.market = &mkt;
  s.boot_and_recover();
  const auto id = s.order(MM, "2330", Side::Buy, 1000, 10);
  s.run_for(30);
  s.crash();
  s.boot();
  REQUIRE(s.run_until([&] { return s.recovered(); }));
  s.run_for(3'000);
  CHECK(s.ex.order(id)->status == OrdStatus::Canceled);
}

TEST("login rejected: stop reconnecting, alert, halt") {
  Sim s("s_login");
  s.ex.faults().login_reject = true;
  s.boot();
  s.run_for(30'000);
  CHECK(s.alerts.has("login_rejected"));
  CHECK_EQ(s.session->connect_attempts(), 1);
  CHECK(s.gw->account_halted());
}

TEST("reconnect rotates endpoints and raises incident after 60s") {
  Sim s("s_rotate");
  s.boot_and_recover();
  s.session->drop_link();
  s.ex.set_endpoint_up("primary", false);
  s.ex.set_endpoint_up("backup-line", false);
  s.ex.set_endpoint_up("backup-host", false);
  s.run_for(15'000, 50);
  CHECK(s.alerts.has("reconnect_alert_phase"));
  s.run_for(50'000, 50);
  CHECK(s.alerts.has("connection_incident"));
  s.ex.set_endpoint_up("backup-host", true);
  REQUIRE(s.run_until([&] { return s.recovered(); }, 120'000, 50));
  CHECK_EQ(s.session->last_endpoint(), std::string("backup-host"));
  // >60s outage: every strategy waits for manual confirmation (spec 10.3)
  s.run_for(2'000, 50);
  CHECK(!s.gw->trading_enabled(MM));
  CHECK(!s.gw->trading_enabled(TR));
}

TEST("strategy heartbeat loss cancels its orders; snapshot on return") {
  Sim s("s_strat");
  s.strategy_cfg(MM).heartbeat_timeout = 1'000;
  s.boot_and_recover();
  const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
  s.run_for(2'500);  // no heartbeats
  CHECK(s.alerts.has("strategy_lost"));
  CHECK(s.ex.order(id)->status == OrdStatus::Canceled);
  const size_t snaps = s.strat.snapshots.size();
  s.gw->strategy_heartbeat(MM);
  CHECK_EQ(s.strat.snapshots.size(), snaps + 1);
}

TEST("cancel reject is expected: logged only, positions unchanged") {
  Sim s("s_crej");
  s.boot_and_recover();
  const auto id = s.order(MM, "2330", Side::Buy, 100, 10);
  s.run_for(30);
  s.ex.faults().cancel_reject_next = 1;
  CHECK(s.gw->submit_cancel(MM, id).ok);
  s.run_for(30);
  CHECK(s.alerts.has("cancel_rejected"));
  CHECK(s.gw->book().find(id)->status() == OrdStatus::New);
  CHECK(!s.gw->account_halted());
}

TEST("watchdog: lost gateway heartbeat fires kill switch for high-risk setups") {
  Sim s("s_wd");
  s.boot_and_recover();
  WatchdogConfig wc;
  wc.heartbeat_timeout = 1'000;
  wc.kill_switch_on_loss = true;
  int kills = 0, restarts = 0;
  Watchdog wd(wc, s.hb, s.clock, s.alerts, [&] { ++kills; }, [&] { ++restarts; });
  s.run_for(500);
  wd.poll();
  CHECK_EQ(kills, 0);
  s.crash();  // heartbeats stop
  s.clock.advance(1'500);
  wd.poll();
  wd.poll();
  CHECK_EQ(kills, 1);
  CHECK_EQ(restarts, 1);
}
