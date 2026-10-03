from __future__ import annotations

import json
import tempfile
import threading
import unittest
import unittest.mock
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from civicflow.security import AccessContext

NOW = "2026-10-03T05:00:00Z"


class MarathonTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "marathon.sqlite3"
        self.app = CivicFlow.open(self.db_path, fixed_now=NOW)
        self.system = AccessContext.system("race-admin")
        self.ops = AccessContext(actor_id="ops", permissions=frozenset({"propose:marathon", "apply:marathon", "read:marathon"}))
        self.medical = AccessContext(actor_id="med-lead", permissions=frozenset({"confirm:marathon.medical", "read:marathon"}))
        self.transport = AccessContext(actor_id="trans-lead", permissions=frozenset({"confirm:marathon.transport", "read:marathon"}))
        self.field = AccessContext(actor_id="field-device", permissions=frozenset({"write:marathon.inbox"}))
        marathon = self.app.marathon
        for zone_id, wave_no in (("A", 1), ("B", 2), ("C", 3)):
            marathon.create_zone(self.system, zone_id, wave_no=wave_no, capacities={"start_slot": 3, "medical": 3, "supply": 3, "shuttle": 3}, request_key=f"zone-{zone_id}")
        marathon.activate_rules(self.system, {"elite": {"zones": ["A"]}, "standard": {"zones": ["A", "B", "C"]}, "charity": {"zones": ["C"]}}, request_key="rules-1")

    def tearDown(self):
        self.temp.cleanup()

    def reopen(self, fixed_now: str = NOW) -> None:
        self.app = CivicFlow.open(self.db_path, fixed_now=fixed_now)

    def register(self, runner_id: str, zone_id: str = "A", qclass: str = "standard") -> dict:
        return self.app.marathon.register_runner(self.system, runner_id, name=f"选手{runner_id}", qualification={"class": qclass, "best": "3:30:00"}, zone_id=zone_id, request_key=f"reg-{runner_id}")

    def confirmed_plan(self, moves: list[dict], key: str = "plan-1", reason: str = "道路容量变化") -> str:
        marathon = self.app.marathon
        plan = marathon.propose_plan(self.ops, moves, reason=reason, request_key=key)
        marathon.confirm_plan(self.medical, plan["plan_id"], role="medical", request_key=f"{key}-medical")
        marathon.confirm_plan(self.transport, plan["plan_id"], role="transport", request_key=f"{key}-transport")
        return plan["plan_id"]

    def test_register_snapshots_qualification_and_original_zone(self):
        runner = self.register("B0001")
        self.assertEqual(runner["qualification"], {"class": "standard", "best": "3:30:00"})
        self.assertEqual(runner["original_zone"], "A")
        self.assertEqual(runner["current_zone"], "A")
        self.assertEqual(runner["status"], "registered")
        self.assertEqual(len(runner["allocations"]), 4)
        self.assertTrue(all(item["status"] == "locked" for item in runner["allocations"]))
        zone = self.app.marathon.zone(self.system, "A")
        self.assertEqual(zone["usage"], {"start_slot": 1, "medical": 1, "supply": 1, "shuttle": 1})
        replay = self.app.marathon.register_runner(self.system, "B0001", name="选手B0001", qualification={"class": "standard", "best": "3:30:00"}, zone_id="A", request_key="reg-B0001")
        self.assertEqual(replay["runner_id"], "B0001")
        with self.assertRaises(ConflictError):
            self.app.marathon.register_runner(self.system, "B0001", name="换人", qualification={"class": "standard"}, zone_id="A", request_key="reg-other")

    def test_register_enforces_zone_capacity(self):
        self.register("B0001"); self.register("B0002"); self.register("B0003")
        with self.assertRaises(ConflictError):
            self.register("B0004")

    def test_register_rejects_unknown_class_and_wrong_zone(self):
        with self.assertRaises(ValidationError):
            self.register("B0001", qclass="vip")
        with self.assertRaises(ValidationError):
            self.register("B0002", zone_id="A", qclass="charity")

    def test_adjustable_range_follows_rule_version(self):
        self.register("B0001")
        first = self.app.marathon.adjustable_range(self.system, "B0001")
        self.assertTrue(first["movable"])
        self.assertEqual(first["rule_version"], 1)
        self.assertEqual([item["zone_id"] for item in first["zones"]], ["B", "C"])
        self.app.marathon.activate_rules(self.system, {"standard": {"zones": ["A", "B"]}}, request_key="rules-2")
        second = self.app.marathon.adjustable_range(self.system, "B0001")
        self.assertEqual(second["rule_version"], 2)
        self.assertEqual([item["zone_id"] for item in second["zones"]], ["B"])

    def test_plan_requires_two_distinct_confirms_and_no_self_approval(self):
        self.register("B0001")
        marathon = self.app.marathon
        plan = marathon.propose_plan(self.ops, [{"runner_id": "B0001", "to_zone": "B"}], reason="医疗点压力上升", request_key="p1")
        self.assertEqual(plan["state"], "proposed")
        proposer_as_medical = AccessContext(actor_id="ops", permissions=frozenset({"confirm:marathon.medical"}))
        with self.assertRaises(PermissionDenied):
            marathon.confirm_plan(proposer_as_medical, plan["plan_id"], role="medical", request_key="c-self")
        self.assertEqual(marathon.confirmation_queue(self.system), [])
        marathon.confirm_plan(self.medical, plan["plan_id"], role="medical", request_key="c-m")
        self.assertEqual(marathon.confirmation_queue(self.system), [])
        dual_role = AccessContext(actor_id="med-lead", permissions=frozenset({"confirm:marathon.medical", "confirm:marathon.transport"}))
        with self.assertRaises(PermissionDenied):
            marathon.confirm_plan(dual_role, plan["plan_id"], role="transport", request_key="c-t-same")
        marathon.confirm_plan(self.transport, plan["plan_id"], role="transport", request_key="c-t")
        queue = marathon.confirmation_queue(self.system)
        self.assertEqual([item["plan_id"] for item in queue], [plan["plan_id"]])
        self.assertEqual(queue[0]["state"], "confirmed")

    def test_propose_validates_rules_and_headroom(self):
        self.register("B0001", zone_id="C", qclass="charity")
        with self.assertRaises(ValidationError):
            self.app.marathon.propose_plan(self.ops, [{"runner_id": "B0001", "to_zone": "A"}], reason="越区", request_key="p-bad")
        for index in range(3):
            self.register(f"B100{index}", zone_id="B")
        self.register("B0002", zone_id="A")
        with self.assertRaises(ConflictError):
            self.app.marathon.propose_plan(self.ops, [{"runner_id": "B0002", "to_zone": "B"}], reason="满员", request_key="p-full")

    def test_apply_moves_runner_and_locks_all_resources(self):
        self.register("B0001"); self.register("B0002")
        plan_id = self.confirmed_plan([{"runner_id": "B0001", "to_zone": "B", "reason": "道路管制"}])
        applied = self.app.marathon.apply_plan(self.ops, plan_id)
        self.assertEqual(applied["state"], "applied")
        self.assertEqual(applied["applied_at"], NOW)
        runner = self.app.marathon.runner(self.system, "B0001")
        self.assertEqual(runner["current_zone"], "B")
        self.assertEqual(runner["original_zone"], "A")
        locked = [item for item in runner["allocations"] if item["status"] == "locked"]
        released = [item for item in runner["allocations"] if item["status"] == "released"]
        self.assertEqual(len(locked), 4)
        self.assertEqual(len(released), 4)
        self.assertTrue(all(item["zone_id"] == "B" for item in locked))
        self.assertTrue(all(item["zone_id"] == "A" for item in released))
        self.assertTrue(all(item["plan_id"] == plan_id for item in locked))
        self.assertEqual(self.app.marathon.zone(self.system, "A")["usage"]["start_slot"], 1)
        self.assertEqual(self.app.marathon.zone(self.system, "B")["usage"]["start_slot"], 1)
        replay = self.app.marathon.apply_plan(self.ops, plan_id)
        self.assertEqual(replay["state"], "applied")
        review = self.app.marathon.review_at(self.system, as_of=NOW)
        self.assertEqual(len(review["adjustments"]), 1)
        self.assertEqual(review["adjustments"][0]["reason"], "道路管制")
        self.assertEqual(review["zones"]["B0001"], "B")

    def test_checked_in_runner_cannot_move(self):
        self.register("B0001")
        self.app.marathon.check_in(self.system, "B0001")
        with self.assertRaises(ConflictError):
            self.app.marathon.propose_plan(self.ops, [{"runner_id": "B0001", "to_zone": "B"}], reason="x", request_key="p-ci")
        blocked = self.app.marathon.adjustable_range(self.system, "B0001")
        self.assertFalse(blocked["movable"])
        self.assertEqual(blocked["blocked_by"], "checked_in")

    def test_fired_wave_cannot_move_and_consumes_resources(self):
        self.register("B0001")
        fired = self.app.marathon.fire_wave(self.system, 1)
        self.assertEqual(fired["started"], ["B0001"])
        with self.assertRaises(ConflictError):
            self.app.marathon.propose_plan(self.ops, [{"runner_id": "B0001", "to_zone": "B"}], reason="x", request_key="p-fw")
        runner = self.app.marathon.runner(self.system, "B0001")
        self.assertEqual(runner["status"], "started")
        self.assertTrue(all(item["status"] == "consumed" for item in runner["allocations"]))
        again = self.app.marathon.fire_wave(self.system, 1)
        self.assertEqual(again["started"], [])

    def test_withdraw_releases_only_unconsumed_resources(self):
        self.register("B0001", zone_id="A")
        self.register("B0002", zone_id="B")
        self.app.marathon.fire_wave(self.system, 1)
        self.app.marathon.withdraw(self.system, "B0001", reason="赛中退赛")
        zone_a = self.app.marathon.zone(self.system, "A")
        self.assertEqual(zone_a["usage"], {"start_slot": 1, "medical": 1, "supply": 1, "shuttle": 1})
        self.app.marathon.withdraw(self.system, "B0002", reason="临时退赛")
        zone_b = self.app.marathon.zone(self.system, "B")
        self.assertEqual(zone_b["usage"], {"start_slot": 0, "medical": 0, "supply": 0, "shuttle": 0})
        runner = self.app.marathon.runner(self.system, "B0002")
        self.assertEqual(runner["status"], "withdrawn")
        replay = self.app.marathon.withdraw(self.system, "B0002", reason="重复退赛回执")
        self.assertEqual(replay["status"], "withdrawn")

    def test_callback_idempotent_and_conflict_freezes_only_related_runner(self):
        self.register("B0001"); self.register("B0002")
        marathon = self.app.marathon
        first = marathon.ingest_callback(self.field, source="checkin", source_key="gate-1", sequence=1, payload={"kind": "check_in", "runner_id": "B0001"}, occurred_at=NOW)
        self.assertEqual(first["status"], "accepted")
        duplicate = marathon.ingest_callback(self.field, source="checkin", source_key="gate-1", sequence=1, payload={"kind": "check_in", "runner_id": "B0001"}, occurred_at=NOW)
        self.assertEqual(duplicate["status"], "duplicate")
        conflict = marathon.ingest_callback(self.field, source="checkin", source_key="gate-1", sequence=1, payload={"kind": "check_in", "runner_id": "B0002"}, occurred_at=NOW)
        self.assertEqual(conflict["status"], "quarantined")
        self.assertEqual(conflict["frozen"], ["B0002"])
        first_runner = marathon.runner(self.system, "B0001")
        second_runner = marathon.runner(self.system, "B0002")
        self.assertEqual(first_runner["status"], "checked_in")
        self.assertFalse(first_runner["frozen"])
        self.assertEqual(second_runner["status"], "registered")
        self.assertTrue(second_runner["frozen"])
        self.assertEqual(self.app.verify()["inbox_conflicts"], 1)
        with self.assertRaises(ConflictError):
            marathon.propose_plan(self.ops, [{"runner_id": "B0002", "to_zone": "B"}], reason="x", request_key="p-frozen")
        marathon.unfreeze(self.system, "B0002", reason="人工核实为闸机重复上报")
        plan = marathon.propose_plan(self.ops, [{"runner_id": "B0002", "to_zone": "B"}], reason="核实后调整", request_key="p-unfrozen")
        self.assertEqual(plan["state"], "proposed")

    def test_stale_plan_blocked_when_receipt_races(self):
        self.register("B0001"); self.register("B0002")
        plan_id = self.confirmed_plan([{"runner_id": "B0001", "to_zone": "B"}])
        self.app.marathon.ingest_callback(self.field, source="checkin", source_key="gate-1", sequence=1, payload={"kind": "check_in", "runner_id": "B0002"}, occurred_at=NOW)
        self.assertEqual(self.app.marathon.confirmation_queue(self.system), [])
        with self.assertRaises(ConflictError):
            self.app.marathon.apply_plan(self.ops, plan_id)
        plan = self.app.marathon.plan(self.system, plan_id)
        self.assertEqual(plan["state"], "superseded")
        runner = self.app.marathon.runner(self.system, "B0001")
        self.assertEqual(runner["current_zone"], "A")
        self.assertEqual(self.app.marathon.zone(self.system, "B")["usage"]["start_slot"], 0)

    def test_rule_version_change_blocks_plan(self):
        self.register("B0001")
        plan_id = self.confirmed_plan([{"runner_id": "B0001", "to_zone": "B"}])
        self.app.marathon.activate_rules(self.system, {"standard": {"zones": ["A", "B", "C"]}}, request_key="rules-2")
        with self.assertRaises(ConflictError):
            self.app.marathon.apply_plan(self.ops, plan_id)
        self.assertEqual(self.app.marathon.plan(self.system, plan_id)["state"], "superseded")

    def test_failed_apply_leaves_no_half_state(self):
        self.register("B0001"); self.register("B0002")
        plan_id = self.confirmed_plan([{"runner_id": "B0001", "to_zone": "B"}, {"runner_id": "B0002", "to_zone": "B"}])
        marathon = self.app.marathon
        original = marathon._lock_allocations
        calls = {"count": 0}

        def fail_on_second(service, connection, **kwargs):
            calls["count"] += 1
            if calls["count"] == 2:
                raise RuntimeError("模拟落库中途故障")
            return original(connection, **kwargs)

        with unittest.mock.patch.object(type(marathon), "_lock_allocations", fail_on_second):
            with self.assertRaises(RuntimeError):
                marathon.apply_plan(self.ops, plan_id)
        for runner_id in ("B0001", "B0002"):
            runner = marathon.runner(self.system, runner_id)
            self.assertEqual(runner["current_zone"], "A")
            self.assertEqual(len([item for item in runner["allocations"] if item["status"] == "locked"]), 4)
            self.assertEqual(len([item for item in runner["allocations"] if item["status"] == "released"]), 0)
        self.assertEqual(marathon.zone(self.system, "A")["usage"]["start_slot"], 2)
        self.assertEqual(marathon.zone(self.system, "B")["usage"]["start_slot"], 0)
        self.assertEqual(marathon.plan(self.system, plan_id)["state"], "confirmed")
        review = marathon.review_at(self.system, as_of=NOW)
        self.assertEqual(review["adjustments"], [])

    def test_apply_rejects_tampered_plan_without_side_effects(self):
        self.register("B0001"); self.register("B0002")
        plan_id = self.confirmed_plan([{"runner_id": "B0001", "to_zone": "B"}, {"runner_id": "B0002", "to_zone": "B"}])
        with self.app.database.connect() as connection:
            connection.execute("UPDATE marathon_plans SET moves_json=? WHERE plan_id=?", (json.dumps([{"runner_id": "B0001", "to_zone": "B"}, {"runner_id": "B0002", "to_zone": "Z"}]), plan_id))
        with self.assertRaises(NotFoundError):
            self.app.marathon.apply_plan(self.ops, plan_id)
        self.assertEqual(self.app.marathon.runner(self.system, "B0001")["current_zone"], "A")
        self.assertEqual(self.app.marathon.runner(self.system, "B0002")["current_zone"], "A")
        self.assertEqual(self.app.marathon.zone(self.system, "B")["usage"]["start_slot"], 0)

    def test_medical_pressure_callback_reduces_capacity(self):
        self.register("B0001")
        result = self.app.marathon.ingest_callback(self.field, source="medical", source_key="mp-1", sequence=1, payload={"kind": "medical_pressure", "zone_id": "A", "capacity": 1}, occurred_at=NOW)
        self.assertEqual(result["status"], "accepted")
        self.assertEqual(self.app.marathon.zone(self.system, "A")["capacities"]["medical"], 1)
        with self.assertRaises(ConflictError):
            self.register("B0002", zone_id="A")
        with self.assertRaises(ConflictError):
            self.app.marathon.update_capacity(self.system, "A", kind="medical", capacity=0, reason="低于已占用")

    def test_wave_and_withdrawal_callbacks(self):
        self.register("B0001")
        marathon = self.app.marathon
        fired = marathon.ingest_callback(self.field, source="timing", source_key="start", sequence=1, payload={"kind": "wave_fired", "wave_no": 1}, occurred_at=NOW)
        self.assertEqual(fired["effect"]["started"], ["B0001"])
        withdrawn = marathon.ingest_callback(self.field, source="medical", source_key="amb-1", sequence=1, payload={"kind": "withdrawn", "runner_id": "B0001", "reason": "医疗退赛"}, occurred_at=NOW)
        self.assertEqual(withdrawn["status"], "accepted")
        runner = marathon.runner(self.system, "B0001")
        self.assertEqual(runner["status"], "withdrawn")
        self.assertEqual(marathon.zone(self.system, "A")["usage"]["medical"], 1)

    def test_review_at_reconstructs_point_in_time(self):
        self.register("B0001"); self.register("B0002")
        plan_id = self.confirmed_plan([{"runner_id": "B0001", "to_zone": "B", "reason": "医疗点压力上升"}])
        self.reopen("2026-10-03T05:10:00Z")
        self.app.marathon.apply_plan(self.ops, plan_id)
        self.reopen("2026-10-03T05:20:00Z")
        self.app.marathon.withdraw(self.system, "B0002", reason="临时退赛")
        early = self.app.marathon.review_at(self.system, as_of="2026-10-03T05:05:00Z")
        self.assertEqual(early["zones"], {"B0001": "A", "B0002": "A"})
        self.assertEqual(early["adjustments"], [])
        usage_a_early = {item["kind"]: item["used"] for item in early["usage"] if item["zone_id"] == "A"}
        self.assertEqual(usage_a_early["start_slot"], 2)
        capacity_a = {item["kind"]: item["capacity"] for item in early["capacities"] if item["zone_id"] == "A"}
        self.assertEqual(capacity_a["start_slot"], 3)
        mid = self.app.marathon.review_at(self.system, as_of="2026-10-03T05:15:00Z")
        self.assertEqual(mid["zones"]["B0001"], "B")
        self.assertEqual(len(mid["adjustments"]), 1)
        self.assertEqual(mid["adjustments"][0]["reason"], "医疗点压力上升")
        usage_b_mid = {item["kind"]: item["used"] for item in mid["usage"] if item["zone_id"] == "B"}
        self.assertEqual(usage_b_mid["medical"], 1)
        late = self.app.marathon.review_at(self.system, as_of="2026-10-03T05:25:00Z")
        usage_a_late = {item["kind"]: item["used"] for item in late["usage"] if item["zone_id"] == "A"}
        self.assertEqual(usage_a_late.get("start_slot", 0), 0)
        self.assertEqual(late["zones"]["B0001"], "B")

    def test_restart_preserves_queue_and_time_basis(self):
        self.register("B0001")
        plan_id = self.confirmed_plan([{"runner_id": "B0001", "to_zone": "B"}])
        self.reopen("2026-10-03T06:00:00Z")
        queue = self.app.marathon.confirmation_queue(self.system)
        self.assertEqual([item["plan_id"] for item in queue], [plan_id])
        runner = self.app.marathon.runner(self.system, "B0001")
        self.assertEqual(runner["created_at"], NOW)
        applied = self.app.marathon.apply_plan(self.ops, plan_id)
        self.assertEqual(applied["state"], "applied")
        self.assertEqual(applied["applied_at"], "2026-10-03T06:00:00Z")

    def test_concurrent_apply_and_field_receipt(self):
        self.register("B0001")
        plan_id = self.confirmed_plan([{"runner_id": "B0001", "to_zone": "B"}])
        barrier = threading.Barrier(2)
        outcome: dict[str, str] = {}

        def do_apply():
            barrier.wait()
            try:
                outcome["apply"] = self.app.marathon.apply_plan(self.ops, plan_id)["state"]
            except ConflictError:
                outcome["apply"] = "conflict"

        def do_checkin():
            barrier.wait()
            outcome["checkin"] = self.app.marathon.ingest_callback(self.field, source="checkin", source_key="gate-9", sequence=1, payload={"kind": "check_in", "runner_id": "B0001"}, occurred_at=NOW)["status"]

        threads = [threading.Thread(target=do_apply), threading.Thread(target=do_checkin)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        runner = self.app.marathon.runner(self.system, "B0001")
        plan = self.app.marathon.plan(self.system, plan_id)
        self.assertEqual(outcome["checkin"], "accepted")
        self.assertEqual(runner["status"], "checked_in")
        if outcome["apply"] == "applied":
            self.assertEqual(runner["current_zone"], "B")
            self.assertEqual(plan["state"], "applied")
            self.assertEqual(self.app.marathon.zone(self.system, "B")["usage"]["start_slot"], 1)
            self.assertEqual(self.app.marathon.zone(self.system, "A")["usage"]["start_slot"], 0)
        else:
            self.assertEqual(outcome["apply"], "conflict")
            self.assertEqual(runner["current_zone"], "A")
            self.assertEqual(plan["state"], "superseded")
            self.assertEqual(self.app.marathon.zone(self.system, "A")["usage"]["start_slot"], 1)
            self.assertEqual(self.app.marathon.zone(self.system, "B")["usage"]["start_slot"], 0)

    def test_permissions_are_enforced(self):
        self.register("B0001")
        outsider = AccessContext(actor_id="outsider", permissions=frozenset())
        with self.assertRaises(PermissionDenied):
            self.app.marathon.propose_plan(outsider, [{"runner_id": "B0001", "to_zone": "B"}], reason="x", request_key="p-denied")
        with self.assertRaises(PermissionDenied):
            self.app.marathon.runner(outsider, "B0001")
        with self.assertRaises(PermissionDenied):
            self.app.marathon.review_at(outsider, as_of=NOW)
        with self.assertRaises(PermissionDenied):
            self.app.marathon.ingest_callback(outsider, source="checkin", source_key="g", sequence=1, payload={"kind": "check_in", "runner_id": "B0001"}, occurred_at=NOW)


if __name__ == "__main__":
    unittest.main()
