from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from civicflow.application import CivicFlow
from civicflow.errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from civicflow.security import AccessContext


class RaceOrchestrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "race.sqlite3"
        self.app = CivicFlow.open(self.db_path, fixed_now="2026-09-28T12:00:00+08:00")
        self.ops = AccessContext.system("ops-01")
        self.med = AccessContext.system("med-01")
        self.traffic = AccessContext.system("traffic-01")
        self._setup_world()

    def tearDown(self):
        self.temp.cleanup()

    def _setup_world(self, *, zone_cap=10, res_cap=4):
        self.app.race.put_rule(
            self.ops,
            {"rule_version": "rules-2026", "wave_order": ["A", "B", "C"], "max_wave_delta": 1, "max_zone_delta": 1,
             "effective_from": "2026-01-01T00:00:00+08:00", "detail": {"name": "三枪分区规则"}},
            request_key="rule-2026",
        )
        self.zones = []
        for wave in ("A", "B", "C"):
            for ordinal in (1, 2):
                zone_id = f"zone-{wave}{ordinal}"
                self.app.race.configure_zone(self.ops, zone_id=zone_id, wave=wave, ordinal=ordinal, capacity=zone_cap, request_key=f"cfg-{zone_id}")
                self.zones.append(zone_id)
                for kind, prefix in (("medical", "med"), ("supply", "sup"), ("shuttle", "bus")):
                    self.app.race.register_resource(self.ops, resource_id=f"{prefix}-{wave}{ordinal}", kind=kind, scope_value=zone_id, capacity=res_cap, request_key=f"reg-{prefix}-{zone_id}")
        self.runners = {}
        initial = {"runner-001": "zone-A1", "runner-002": "zone-A2", "runner-003": "zone-B1",
                   "runner-004": "zone-B2", "runner-005": "zone-C1"}
        for i, (runner_id, zone_id) in enumerate(initial.items()):
            self.app.race.import_runner(
                self.ops, runner_id=runner_id, bib=str(1001 + i),
                qualification={"seed": i % 2, "certified": True, "entry": "2026-beijing"},
                zone_id=zone_id, request_key=f"imp-{runner_id}",
            )
            self.runners[runner_id] = zone_id

    def _propose_approve_ready(self, runner_id, to_zone, *, reason="医疗点压力上升", key="p"):
        plan = self.app.race.propose_plan(self.ops, moves=[{"runner_id": runner_id, "to_zone": to_zone}], reason=reason, request_key=f"propose-{key}")
        self.app.race.approve_plan(self.med, plan_id=plan["plan_id"], role="medical", approved=True, comment="ok", request_key=f"med-{key}")
        self.app.race.approve_plan(self.traffic, plan_id=plan["plan_id"], role="traffic", approved=True, comment="ok", request_key=f"tr-{key}")
        return self.app.race.get_plan(plan["plan_id"])

    def _held_count(self, plan_id=None):
        sql = "SELECT COUNT(*) AS n FROM race_allocations WHERE status='held'"
        params = ()
        if plan_id:
            sql += " AND plan_id=?"; params = (plan_id,)
        with self.app.database.connect() as c:
            return c.execute(sql, params).fetchone()["n"]

    # ---- 快照与规则范围 ---------------------------------------------------

    def test_qualification_and_original_zone_are_snapshotted(self):
        runner = self.app.race.get_runner("runner-003")
        self.assertEqual(runner["original_zone"], "zone-B1")
        self.assertEqual(runner["qualification"]["entry"], "2026-beijing")
        plan = self._propose_approve_ready("runner-003", "zone-A2", key="snap")
        self.app.race.apply_plan(self.ops, plan_id=plan["plan_id"], request_key="apply-snap")
        moved = self.app.race.get_runner("runner-003")
        self.assertEqual(moved["zone"], "zone-A2")
        self.assertEqual(moved["wave"], "A")
        # 原分区与资格快照永不被改写。
        self.assertEqual(moved["original_zone"], "zone-B1")
        self.assertEqual(moved["qualification"]["entry"], "2026-beijing")

    def test_adjustable_range_follows_rule_version(self):
        info = self.app.race.adjustable_targets("runner-003")  # B1，规则允许跨一枪、同枪邻区
        self.assertEqual(info["rule_version"], "rules-2026")
        self.assertTrue(info["adjustable"])
        self.assertEqual(set(info["allowed_zones"]), {"zone-A1", "zone-A2", "zone-B1", "zone-B2", "zone-C1", "zone-C2"})
        with self.assertRaises(ConflictError):  # C1 -> A1 跨两枪，超出规则
            self.app.race.propose_plan(self.ops, moves=[{"runner_id": "runner-005", "to_zone": "zone-A1"}], reason="违规跨枪", request_key="bad-wave")
        # 新规则版本在未来生效：跨两枪放开、同枪换区收紧。
        self.app.race.put_rule(
            self.ops,
            {"rule_version": "rules-2027", "wave_order": ["A", "B", "C"], "max_wave_delta": 2, "max_zone_delta": 0,
             "effective_from": "2027-01-01T00:00:00+08:00", "detail": {}},
            request_key="rule-2027",
        )
        future = self.app.race.adjustable_targets("runner-005", at="2027-06-01T00:00:00+08:00")
        self.assertIn("zone-A1", future["allowed_zones"])
        self.assertNotIn("zone-C2", future["allowed_zones"])
        current = self.app.race.adjustable_targets("runner-005")
        self.assertNotIn("zone-A1", current["allowed_zones"])

    # ---- 四类资源同时锁定 -------------------------------------------------

    def test_plan_holds_all_four_resource_kinds(self):
        plan = self.app.race.propose_plan(
            self.ops, moves=[{"runner_id": "runner-003", "to_zone": "zone-A2"}],
            reason="B1 医疗压力上升", request_key="hold-1",
        )
        view = self.app.race.get_plan(plan["plan_id"])
        self.assertEqual(set(view["held_resources"]), {"zone_capacity", "medical", "supply", "shuttle"})
        load = {item["resource_id"]: item for item in self.app.race.resource_load("medical", "zone-A2")}
        self.assertEqual(load["med-A2"]["held_or_used"], 1)

    def test_zone_capacity_shortfall_rolls_back_everything(self):
        self.app.race.set_road_capacity(self.ops, zone_id="zone-A2", capacity=2, request_key="shrink-a2")
        with self.assertRaises(ConflictError):
            self.app.race.propose_plan(
                self.ops,
                moves=[{"runner_id": "runner-003", "to_zone": "zone-A2"},
                       {"runner_id": "runner-004", "to_zone": "zone-A2"},
                       {"runner_id": "runner-005", "to_zone": "zone-A2"}],
                reason="批量前移", request_key="bulk-fail",
            )
        self.assertEqual(self.app.race.confirmation_queue(), [])
        self.assertEqual(self._held_count(), 0)  # 无任何预占残留
        self.assertEqual(self.app.race.get_runner("runner-003")["zone"], "zone-B1")
        self.assertEqual(self.app.race.get_runner("runner-004")["zone"], "zone-B2")
        self.assertEqual(self.app.race.get_runner("runner-005")["zone"], "zone-C1")

    def test_medical_capacity_shortfall_rolls_back_everything(self):
        # B2 现有选手已占一份医疗，医疗点容量压到 1，再向 B2 放人即不足。
        self.app.race.register_resource(self.ops, resource_id="med-B2", kind="medical", scope_value="zone-B2", capacity=1, request_key="med-b2-tight")
        self.app.race.assign_support(self.ops, runner_id="runner-004", kind="medical", resource_id="med-B2", request_key="assign-004-med")
        with self.assertRaises(ConflictError):
            self.app.race.propose_plan(
                self.ops, moves=[{"runner_id": "runner-003", "to_zone": "zone-B2"}],
                reason="医疗不足也必须整体失败", request_key="med-fail",
            )
        self.assertEqual(self._held_count(), 0)  # 连起跑区预占都不能留下
        self.assertEqual(self.app.race.get_runner("runner-003")["zone"], "zone-B1")

    # ---- 检录/发枪冻结 ----------------------------------------------------

    def test_checked_in_and_started_runners_are_frozen(self):
        self.app.race.receive_report(self.ops, source="checkin", runner_id="runner-001", sequence=1, payload={"gate": "G1"}, occurred_at=self.app.clock.now(), request_key="ci-1")
        r1 = self.app.race.get_runner("runner-001")
        self.assertEqual(r1["status"], "checked_in")
        self.assertTrue(r1["frozen"])
        self.assertEqual(self.app.race.adjustable_targets("runner-001")["allowed_zones"], [])
        with self.assertRaises(ConflictError):
            self.app.race.propose_plan(self.ops, moves=[{"runner_id": "runner-001", "to_zone": "zone-A2"}], reason="不应移动", request_key="freeze-1")
        # 发枪后同样冻结，并标记分区已发枪。
        self.app.race.receive_report(self.ops, source="timing", runner_id="runner-001", sequence=2, payload={"point": "start"}, occurred_at=self.app.clock.now(), request_key="gun-1")
        self.assertEqual(self.app.race.get_runner("runner-001")["status"], "started")
        with self.assertRaises(ConflictError):
            self.app.race.withdraw(self.ops, runner_id="runner-001", reason="发枪后不退赛", request_key="wd-1")

    # ---- 退赛只释放未消耗资源 ---------------------------------------------

    def test_withdraw_releases_only_unconsumed_support(self):
        self.app.race.assign_support(self.ops, runner_id="runner-003", kind="medical", resource_id="med-B1", request_key="a-med")
        self.app.race.assign_support(self.ops, runner_id="runner-003", kind="supply", resource_id="sup-B1", request_key="a-sup")
        self.app.race.receive_consumption(self.ops, runner_id="runner-003", kind="supply", quantity=1, request_key="c-sup")  # 补给已领
        result = self.app.race.withdraw(self.ops, runner_id="runner-003", reason="赛前高烧", request_key="wd-003")
        released_kinds = {item["kind"] for item in result["released"]}
        self.assertEqual(released_kinds, {"medical"})  # 未消耗的医疗保障释放
        self.assertNotIn("supply", released_kinds)  # 已领补给不退仓
        med_load = self.app.race.resource_load("medical", "zone-B1")[0]
        sup_load = self.app.race.resource_load("supply", "zone-B1")[0]
        self.assertEqual(med_load["held_or_used"], 0)
        self.assertEqual(sup_load["held_or_used"], 1)
        self.assertEqual(sup_load["consumed"], 1)
        self.assertEqual(self.app.race.get_runner("runner-003")["status"], "withdrawn")
        # 退赛幂等：重复请求返回同一结果。
        again = self.app.race.withdraw(self.ops, runner_id="runner-003", reason="赛前高烧", request_key="wd-003")
        self.assertEqual(again["status"], "withdrawn")

    def test_withdraw_blocked_while_plan_pending(self):
        self.app.race.propose_plan(self.ops, moves=[{"runner_id": "runner-003", "to_zone": "zone-A2"}], reason="待决中", request_key="pend")
        with self.assertRaises(ConflictError):
            self.app.race.withdraw(self.ops, runner_id="runner-003", reason="不能退", request_key="wd-block")

    # ---- 幂等回传与异文隔离 -----------------------------------------------

    def test_report_idempotent_by_source_sequence(self):
        payload = {"gate": "G1"}
        first = self.app.race.receive_report(self.ops, source="checkin", runner_id="runner-002", sequence=1, payload=payload, occurred_at=self.app.clock.now(), request_key="r-1")
        second = self.app.race.receive_report(self.ops, source="checkin", runner_id="runner-002", sequence=1, payload=payload, occurred_at=self.app.clock.now(), request_key="r-1-again")
        self.assertEqual(first["status"], "accepted")
        self.assertEqual(second["status"], "duplicate")

    def test_divergent_report_isolates_only_related_runner(self):
        self.app.race.receive_report(self.ops, source="timing", runner_id="runner-002", sequence=1, payload={"point": "start"}, occurred_at=self.app.clock.now(), request_key="t-1")
        with self.assertRaises(ConflictError):
            self.app.race.receive_report(self.ops, source="timing", runner_id="runner-002", sequence=1, payload={"point": "5k"}, occurred_at=self.app.clock.now(), request_key="t-1-other")
        # 只有异文选手被冻结隔离。
        self.assertTrue(self.app.race.get_runner("runner-002")["frozen"])
        self.assertFalse(self.app.race.get_runner("runner-003")["frozen"])
        with self.app.database.connect() as c:
            self.assertEqual(c.execute("SELECT COUNT(*) AS n FROM inbox_conflicts").fetchone()["n"], 1)
        # 其他人照常可以提出方案（A2 已随 runner-002 发枪，改去 B2）。
        self.app.race.propose_plan(self.ops, moves=[{"runner_id": "runner-003", "to_zone": "zone-B2"}], reason="别人不受影响", request_key="others-ok")

    # ---- 双角色确认 / 不得自批 --------------------------------------------

    def test_two_leads_must_confirm_and_proposer_cannot_self_approve(self):
        plan = self.app.race.propose_plan(self.ops, moves=[{"runner_id": "runner-003", "to_zone": "zone-A2"}], reason="需要会签", request_key="sign-1")
        with self.assertRaises(PermissionDenied):  # 申请人不得自批
            self.app.race.approve_plan(self.ops, plan_id=plan["plan_id"], role="medical", approved=True, request_key="self")
        self.app.race.approve_plan(self.med, plan_id=plan["plan_id"], role="medical", approved=True, request_key="m-1")
        self.assertEqual(self.app.race.get_plan(plan["plan_id"])["status"], "medical_approved")
        with self.assertRaises(ConflictError):  # 仅医疗确认不能生效
            self.app.race.apply_plan(self.ops, plan_id=plan["plan_id"], request_key="early")
        with self.assertRaises(ConflictError):  # 同一角色不能重复确认
            self.app.race.approve_plan(self.med, plan_id=plan["plan_id"], role="medical", approved=True, request_key="m-2")
        self.app.race.approve_plan(self.traffic, plan_id=plan["plan_id"], role="traffic", approved=True, request_key="t-1")
        self.assertEqual(self.app.race.get_plan(plan["plan_id"])["status"], "ready")
        applied = self.app.race.apply_plan(self.ops, plan_id=plan["plan_id"], request_key="go-1")
        self.assertEqual(applied["status"], "applied")
        # 生效幂等。
        again = self.app.race.apply_plan(self.ops, plan_id=plan["plan_id"], request_key="go-1")
        self.assertEqual(again["status"], "applied")

    def test_rejection_releases_holds_and_allows_new_plan(self):
        plan = self.app.race.propose_plan(self.ops, moves=[{"runner_id": "runner-003", "to_zone": "zone-A2"}], reason="可能被驳回", request_key="rej-1")
        self.app.race.approve_plan(self.med, plan_id=plan["plan_id"], role="medical", approved=False, comment="医疗点无覆盖", request_key="rej-med")
        self.assertEqual(self.app.race.get_plan(plan["plan_id"])["status"], "rejected")
        self.assertEqual(self._held_count(plan["plan_id"]), 0)
        self.assertEqual(self.app.race.get_runner("runner-003")["zone"], "zone-B1")  # 选手未动
        # 释放后可以重新提出方案。
        self.app.race.propose_plan(self.ops, moves=[{"runner_id": "runner-003", "to_zone": "zone-B2"}], reason="改去 B2", request_key="rej-2")

    # ---- 并发：旧方案不能落库、不留半状态 ----------------------------------

    def test_concurrent_checkin_blocks_stale_plan_without_half_state(self):
        plan = self._propose_approve_ready("runner-003", "zone-A2", key="conc")
        # 方案等待期间现场检录回传到达，选手被冻结。
        self.app.race.receive_report(self.ops, source="checkin", runner_id="runner-003", sequence=1, payload={"gate": "G2"}, occurred_at=self.app.clock.now(), request_key="ci-conc")
        with self.assertRaises(ConflictError):
            self.app.race.apply_plan(self.ops, plan_id=plan["plan_id"], request_key="apply-conc")
        stale = self.app.race.get_plan(plan["plan_id"])
        self.assertEqual(stale["status"], "rejected")
        runner = self.app.race.get_runner("runner-003")
        self.assertEqual(runner["zone"], "zone-B1")  # 选手没有换区
        self.assertEqual(runner["status"], "checked_in")
        # 保障任务没有指向新区：预占全部释放。
        self.assertEqual(self._held_count(plan["plan_id"]), 0)
        pointers = {a["zone_id"] for a in runner["allocations"]}
        self.assertNotIn("zone-A2", pointers)
        med_a2 = self.app.race.resource_load("medical", "zone-A2")[0]
        self.assertEqual(med_a2["available"], med_a2["capacity"])
        # 其他选手仍可正常编排。
        other = self._propose_approve_ready("runner-005", "zone-B2", key="conc-other")
        self.app.race.apply_plan(self.ops, plan_id=other["plan_id"], request_key="apply-other")
        self.assertEqual(self.app.race.get_runner("runner-005")["zone"], "zone-B2")

    def test_consumption_waits_for_pending_plan_then_follows_new_zone(self):
        plan = self._propose_approve_ready("runner-003", "zone-A2", key="recv")
        with self.assertRaises(ConflictError):  # 待决预占期间回执被挡，避免与旧方案交错
            self.app.race.receive_consumption(self.ops, runner_id="runner-003", kind="medical", request_key="recv-early")
        self.app.race.apply_plan(self.ops, plan_id=plan["plan_id"], request_key="apply-recv")
        receipt = self.app.race.receive_consumption(self.ops, runner_id="runner-003", kind="medical", request_key="recv-ok")
        self.assertEqual(receipt["status"], "consumed")
        med_a2 = self.app.race.resource_load("medical", "zone-A2")[0]
        self.assertEqual(med_a2["consumed"], 1)

    def test_support_moves_with_runner_and_keeps_journal(self):
        # 选手已有一份绑定在旧区的医疗保障。
        self.app.race.assign_support(self.ops, runner_id="runner-003", kind="medical", resource_id="med-B1", request_key="move-med")
        plan = self._propose_approve_ready("runner-003", "zone-A2", key="journ")
        self.app.race.apply_plan(self.ops, plan_id=plan["plan_id"], request_key="apply-journ")
        runner = self.app.race.get_runner("runner-003")
        med = next(a for a in runner["allocations"] if a["kind"] == "medical")
        self.assertEqual(med["resource_id"], "med-A2")  # 保障任务随人指向新区
        self.assertEqual(med["zone_id"], "zone-A2")
        self.assertEqual(self.app.race.resource_load("medical", "zone-B1")[0]["held_or_used"], 0)
        with self.app.database.connect() as c:
            row = c.execute("SELECT * FROM race_allocation_journal WHERE event='move' AND kind='medical' ORDER BY journal_id DESC LIMIT 1").fetchone()
            self.assertEqual(row["from_resource_id"], "med-B1")
            self.assertEqual(row["to_resource_id"], "med-A2")
            self.assertEqual(row["to_zone"], "zone-A2")

    def test_shrunk_medical_capacity_blocks_ready_plan(self):
        plan = self._propose_approve_ready("runner-003", "zone-A2", key="shrink")
        # 等待期间 A2 先有一份既有医疗占用，医疗点再把容量下调到 1。
        self.app.race.assign_support(self.ops, runner_id="runner-002", kind="medical", resource_id="med-A2", request_key="a2-med")
        self.app.race.register_resource(self.ops, resource_id="med-A2", kind="medical", scope_value="zone-A2", capacity=1, request_key="a2-tight")
        with self.assertRaises(ConflictError):
            self.app.race.apply_plan(self.ops, plan_id=plan["plan_id"], request_key="apply-shrink")
        self.assertEqual(self.app.race.get_plan(plan["plan_id"])["status"], "rejected")
        self.assertEqual(self.app.race.get_runner("runner-003")["zone"], "zone-B1")
        self.assertEqual(self._held_count(plan["plan_id"]), 0)

    def test_road_capacity_change_cannot_undercut_active_hold(self):
        self._propose_approve_ready("runner-003", "zone-A2", key="road")
        # A2 现存 1 人 + 待决预占 1 人，压缩到 1 必须被拒绝。
        with self.assertRaises(ConflictError):
            self.app.race.set_road_capacity(self.ops, zone_id="zone-A2", capacity=1, request_key="road-cut")

    # ---- 确认队列 / 持久化 / 复盘 -----------------------------------------

    def test_confirmation_queue_and_restart_persistence(self):
        plan = self.app.race.propose_plan(self.ops, moves=[{"runner_id": "runner-003", "to_zone": "zone-A2"}], reason="跨进程待办", request_key="persist")
        self.app.race.approve_plan(self.med, plan_id=plan["plan_id"], role="medical", approved=True, request_key="persist-m")
        # 进程退出后重新打开：待办队列、已做确认与时间依据都还在。
        reopened = CivicFlow.open(self.db_path, fixed_now="2026-09-28T12:05:00+08:00")
        queue = reopened.race.confirmation_queue()
        self.assertEqual(len(queue), 1)
        self.assertEqual(queue[0]["plan_id"], plan["plan_id"])
        self.assertEqual(queue[0]["status"], "medical_approved")
        reopened.race.approve_plan(self.traffic, plan_id=plan["plan_id"], role="traffic", approved=True, request_key="persist-t")
        reopened.race.apply_plan(self.ops, plan_id=plan["plan_id"], request_key="persist-apply")
        self.assertEqual(reopened.race.confirmation_queue(), [])
        self.assertEqual(reopened.race.get_runner("runner-003")["zone"], "zone-A2")

    def test_review_at_point_in_time_lists_zones_support_and_reasons(self):
        plan = self._propose_approve_ready("runner-003", "zone-A2", key="review")
        self.app.race.apply_plan(self.ops, plan_id=plan["plan_id"], request_key="apply-review")
        before = self.app.race.review_at(at="2026-09-28T11:59:59+08:00")
        self.assertEqual(before["changes"], [])  # 导入前无人可见
        after = self.app.race.review_at(at="2026-09-28T12:30:00+08:00")
        r3 = next(r for r in after["runners"] if r["runner_id"] == "runner-003")
        self.assertEqual(r3["zone"], "zone-A2")
        self.assertEqual(r3["original_zone"], "zone-B1")
        self.assertTrue(any(a["kind"] == "medical" and a["zone_id"] == "zone-A2" for a in r3["allocations"]))
        reasons = [c["reason"] for c in after["changes"] if c["runner_id"] == "runner-003"]
        self.assertTrue(any("重排方案生效" in r for r in reasons))
        recorded = next(p for p in after["plans"] if p["plan_id"] == plan["plan_id"])
        self.assertEqual(recorded["reason"], "医疗点压力上升")
        self.assertEqual(recorded["status"], "applied")

    def test_propose_is_idempotent_per_request_key(self):
        moves = [{"runner_id": "runner-003", "to_zone": "zone-A2"}]
        first = self.app.race.propose_plan(self.ops, moves=moves, reason="同一请求", request_key="idem")
        second = self.app.race.propose_plan(self.ops, moves=moves, reason="同一请求", request_key="idem")
        self.assertEqual(first["plan_id"], second["plan_id"])
        with self.assertRaises(ConflictError):
            self.app.race.propose_plan(self.ops, moves=[{"runner_id": "runner-003", "to_zone": "zone-B2"}], reason="内容变了", request_key="idem")


if __name__ == "__main__":
    unittest.main()
