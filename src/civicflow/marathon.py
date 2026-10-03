"""马拉松分枪分区与保障资源动态重排。

在协同事务平台之上提供赛事编排能力：

- 报名即保存选手资格快照与原分区快照，可调整范围按生效规则版本计算；
- 重排方案在同一事务内同时锁定起跑区容量、医疗救援覆盖、补给水位与接驳班次，
  任何一步失败整体回滚，不留选手已换区但保障任务仍指向旧区的半状态；
- 已检录或已发枪的选手不被普通重排改写，退赛只释放尚未消耗的资源；
- 计时、检录、医疗回传按来源序号幂等接收，异文隔离且只冻结关联选手；
- 方案由运营提出，医疗与交通负责人分别确认，申请人不得自批；
- 环境版本号阻止过期方案在现场回执并发时落库；
- 确认队列、每次调整的原因与全部时间依据持久化在 SQLite，进程退出不丢失。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Mapping

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, InvariantViolation, NotFoundError, PermissionDenied, ValidationError
from .identifiers import new_id, require_safe
from .idempotency import IdempotencyStore
from .inbox import Inbox
from .jsonutil import canonical_json
from .security import AccessContext, assert_distinct
from .timeutil import Clock, canonical_instant

RESOURCE_KINDS = ("start_slot", "medical", "supply", "shuttle")
RESOURCE_LABELS = {"start_slot": "起跑区容量", "medical": "医疗救援覆盖", "supply": "补给水位", "shuttle": "接驳班次"}
RUNNER_STATUSES = ("registered", "checked_in", "started", "finished", "withdrawn")
PLAN_STATES = ("proposed", "confirmed", "applied", "rejected", "withdrawn", "superseded")
CONFIRM_ROLES = ("medical", "transport")
CALLBACK_KINDS = ("check_in", "withdrawn", "wave_fired", "finished", "medical_pressure", "road_capacity")
MAX_PLAN_MOVES = 1000


@dataclass(frozen=True)
class MarathonService:
    """赛事编排领域服务；所有写操作在单个 SQLite 事务内完成。"""

    database: Database
    clock: Clock
    inbox: Inbox
    audit: AuditLog
    idempotency: IdempotencyStore

    # ------------------------------------------------------------------
    # 分区与保障资源
    # ------------------------------------------------------------------

    def create_zone(self, context: AccessContext, zone_id: str, *, wave_no: int, capacities: Mapping[str, int], request_key: str) -> dict:
        context.require("write:marathon.zones")
        zone_id = require_safe(zone_id, "分区标识")
        wave_no = self._wave_no(wave_no)
        normalized = self._validate_capacities(capacities)
        with self.database.transaction() as connection:
            def operation() -> dict:
                now = self.clock.now()
                try:
                    connection.execute("INSERT INTO marathon_zones(zone_id,wave_no,fired,created_at,updated_at) VALUES(?,?,0,?,?)", (zone_id, wave_no, now, now))
                except sqlite3.IntegrityError as exc:
                    raise ConflictError(f"分区 {zone_id} 已存在") from exc
                for kind in RESOURCE_KINDS:
                    connection.execute("INSERT INTO marathon_resources(zone_id,kind,capacity) VALUES(?,?,?)", (zone_id, kind, normalized[kind]))
                    self._capacity_event(connection, zone_id, kind, normalized[kind], now)
                self._bump(connection)
                self.audit.append(connection, actor_id=context.actor_id, action="create_zone", entity_type="marathon_zone", entity_id=zone_id, version=1, detail={"wave_no": wave_no, "capacities": normalized})
                return self._zone_dict(connection, zone_id)
            return self.idempotency.execute(connection, scope="marathon.create_zone", request_key=request_key, request={"zone_id": zone_id, "wave_no": wave_no, "capacities": normalized}, operation=operation)

    def update_capacity(self, context: AccessContext, zone_id: str, *, kind: str, capacity: int, reason: str) -> dict:
        context.require("write:marathon.zones")
        if not reason or not reason.strip():
            raise ValidationError("容量调整必须说明原因")
        with self.database.transaction() as connection:
            self._set_capacity(connection, zone_id, kind, capacity, self.clock.now(), actor=context.actor_id, reason=reason.strip())
            return self._zone_dict(connection, zone_id)

    def zone(self, context: AccessContext, zone_id: str) -> dict:
        context.require("read:marathon")
        with self.database.connect() as connection:
            return self._zone_dict(connection, zone_id)

    def zones(self, context: AccessContext) -> list[dict]:
        context.require("read:marathon")
        with self.database.connect() as connection:
            rows = connection.execute("SELECT zone_id FROM marathon_zones ORDER BY wave_no, zone_id").fetchall()
            return [self._zone_dict(connection, row["zone_id"]) for row in rows]

    # ------------------------------------------------------------------
    # 规则版本
    # ------------------------------------------------------------------

    def activate_rules(self, context: AccessContext, classes: Mapping[str, object], *, request_key: str) -> dict:
        context.require("write:marathon.rules")
        normalized = self._validate_classes(classes)
        with self.database.transaction() as connection:
            def operation() -> dict:
                known = {row["zone_id"] for row in connection.execute("SELECT zone_id FROM marathon_zones")}
                for class_name, spec in normalized.items():
                    unknown = [zone_id for zone_id in spec["zones"] if zone_id not in known]
                    if unknown:
                        raise ValidationError(f"类别 {class_name} 引用了未知分区: {', '.join(unknown)}")
                row = connection.execute("SELECT COALESCE(MAX(version_no),0) AS v FROM marathon_rules").fetchone()
                version_no = int(row["v"]) + 1
                connection.execute("UPDATE marathon_rules SET status='retired' WHERE status='active'")
                rule_id = new_id("mrule")
                now = self.clock.now()
                connection.execute("INSERT INTO marathon_rules(rule_id,version_no,payload_json,status,created_at,created_by) VALUES(?,?,?,?,?,?)", (rule_id, version_no, canonical_json({"classes": normalized}), "active", now, context.actor_id))
                self._bump(connection)
                self.audit.append(connection, actor_id=context.actor_id, action="activate_rules", entity_type="marathon_rule", entity_id=rule_id, version=version_no, detail={"classes": normalized})
                return {"rule_id": rule_id, "version_no": version_no, "status": "active", "classes": normalized, "created_at": now}
            return self.idempotency.execute(connection, scope="marathon.activate_rules", request_key=request_key, request={"classes": normalized}, operation=operation)

    def rules(self, context: AccessContext) -> dict:
        context.require("read:marathon")
        with self.database.connect() as connection:
            row = self._active_rule(connection)
            if row is None:
                raise NotFoundError("没有生效的规则版本")
            return {"rule_id": row["rule_id"], "version_no": row["version_no"], "status": row["status"], "classes": json.loads(row["payload_json"])["classes"], "created_at": row["created_at"]}

    # ------------------------------------------------------------------
    # 选手报名与现场状态
    # ------------------------------------------------------------------

    def register_runner(self, context: AccessContext, runner_id: str, *, name: str, qualification: Mapping[str, object], zone_id: str, request_key: str) -> dict:
        context.require("write:marathon.runners")
        runner_id = require_safe(runner_id, "选手号码")
        zone_id = require_safe(zone_id, "分区标识")
        if not isinstance(name, str) or not name.strip():
            raise ValidationError("选手姓名不能为空")
        if not isinstance(qualification, Mapping):
            raise ValidationError("资格快照必须是对象")
        qualification = dict(qualification)
        qclass = str(qualification.get("class", "")).strip()
        if not qclass:
            raise ValidationError("资格快照必须包含 class")
        qualification["class"] = qclass
        with self.database.transaction() as connection:
            def operation() -> dict:
                zone = self._zone(connection, zone_id)
                if zone["fired"]:
                    raise ConflictError(f"分区 {zone_id} 已发枪，不能报名")
                rule = self._active_rule(connection)
                if rule is None:
                    raise ValidationError("没有生效的规则版本，无法校验报名分区")
                classes = json.loads(rule["payload_json"])["classes"]
                if qclass not in classes:
                    raise ValidationError(f"未知资格类别: {qclass}")
                if zone_id not in classes[qclass]["zones"]:
                    raise ValidationError(f"规则版本 {rule['version_no']} 不允许 {qclass} 类选手进入分区 {zone_id}")
                self._assert_capacity(connection, {(zone_id, kind): 1 for kind in RESOURCE_KINDS})
                now = self.clock.now()
                try:
                    connection.execute("INSERT INTO marathon_runners(runner_id,name,qualification_json,original_zone,current_zone,status,frozen,version,created_at,updated_at) VALUES(?,?,?,?,?,'registered',0,1,?,?)", (runner_id, name.strip(), canonical_json(qualification), zone_id, zone_id, now, now))
                except sqlite3.IntegrityError as exc:
                    raise ConflictError(f"选手 {runner_id} 已存在") from exc
                self._lock_allocations(connection, zone_id=zone_id, subject_id=runner_id, plan_id=None, now=now)
                self._bump(connection)
                self.audit.append(connection, actor_id=context.actor_id, action="register", entity_type="marathon_runner", entity_id=runner_id, version=1, detail={"zone_id": zone_id, "qualification": qualification})
                return self._runner_dict(connection, runner_id)
            return self.idempotency.execute(connection, scope="marathon.register_runner", request_key=request_key, request={"runner_id": runner_id, "name": name.strip(), "qualification": qualification, "zone_id": zone_id}, operation=operation)

    def runner(self, context: AccessContext, runner_id: str) -> dict:
        context.require("read:marathon")
        with self.database.connect() as connection:
            return self._runner_dict(connection, runner_id)

    def check_in(self, context: AccessContext, runner_id: str) -> dict:
        context.require("write:marathon.checkin")
        with self.database.transaction() as connection:
            self._check_in(connection, require_safe(runner_id, "选手号码"), self.clock.now(), actor=context.actor_id)
            return self._runner_dict(connection, runner_id)

    def fire_wave(self, context: AccessContext, wave_no: int) -> dict:
        context.require("write:marathon.wave")
        with self.database.transaction() as connection:
            started = self._fire_wave(connection, self._wave_no(wave_no), self.clock.now(), actor=context.actor_id)
            return {"wave_no": self._wave_no(wave_no), "started": started}

    def withdraw(self, context: AccessContext, runner_id: str, *, reason: str) -> dict:
        context.require("write:marathon.withdraw")
        if not reason or not reason.strip():
            raise ValidationError("退赛必须说明原因")
        with self.database.transaction() as connection:
            self._withdraw(connection, require_safe(runner_id, "选手号码"), reason.strip(), self.clock.now(), actor=context.actor_id)
            return self._runner_dict(connection, runner_id)

    def unfreeze(self, context: AccessContext, runner_id: str, *, reason: str) -> dict:
        context.require("resolve:marathon.freeze")
        if not reason or not reason.strip():
            raise ValidationError("解冻必须说明原因")
        runner_id = require_safe(runner_id, "选手号码")
        with self.database.transaction() as connection:
            runner = self._runner(connection, runner_id)
            if runner["frozen"]:
                connection.execute("UPDATE marathon_runners SET frozen=0, version=version+1, updated_at=? WHERE runner_id=?", (self.clock.now(), runner_id))
                self._bump(connection)
                self.audit.append(connection, actor_id=context.actor_id, action="unfreeze", entity_type="marathon_runner", entity_id=runner_id, version=runner["version"] + 1, detail={"reason": reason.strip()})
            return self._runner_dict(connection, runner_id)

    # ------------------------------------------------------------------
    # 可调整范围
    # ------------------------------------------------------------------

    def adjustable_range(self, context: AccessContext, runner_id: str) -> dict:
        context.require("read:marathon")
        runner_id = require_safe(runner_id, "选手号码")
        with self.database.connect() as connection:
            runner = self._runner(connection, runner_id)
            rule = self._active_rule(connection)
            if rule is None:
                raise ValidationError("没有生效的规则版本，无法计算可调整范围")
            blocked = self._blocked_reason(connection, runner)
            base = {"runner_id": runner_id, "rule_version": rule["version_no"], "movable": blocked is None, "blocked_by": blocked, "zones": []}
            if blocked is not None:
                return base
            classes = json.loads(rule["payload_json"])["classes"]
            qclass = json.loads(runner["qualification_json"])["class"]
            allowed = [zone_id for zone_id in classes.get(qclass, {}).get("zones", []) if zone_id != runner["current_zone"]]
            options = []
            for zone_id in allowed:
                zone = self._zone(connection, zone_id)
                if zone["fired"]:
                    continue
                headroom = min(self._capacity(connection, zone_id, kind) - self._usage(connection, zone_id, kind) for kind in RESOURCE_KINDS)
                if headroom >= 1:
                    options.append({"zone_id": zone_id, "wave_no": zone["wave_no"], "headroom": headroom})
            options.sort(key=lambda item: (item["wave_no"], item["zone_id"]))
            base["zones"] = options
            return base

    # ------------------------------------------------------------------
    # 重排方案
    # ------------------------------------------------------------------

    def propose_plan(self, context: AccessContext, moves: list[Mapping[str, object]], *, reason: str, request_key: str) -> dict:
        context.require("propose:marathon")
        if not reason or not reason.strip():
            raise ValidationError("重排方案必须说明原因")
        normalized = self._validate_moves(moves)
        with self.database.transaction() as connection:
            def operation() -> dict:
                rule = self._active_rule(connection)
                if rule is None:
                    raise ValidationError("没有生效的规则版本，无法提出重排方案")
                classes = json.loads(rule["payload_json"])["classes"]
                self._validate_moves_against_state(connection, normalized, classes)
                plan_id = new_id("mplan")
                revision = self._revision(connection)
                now = self.clock.now()
                connection.execute("INSERT INTO marathon_plans(plan_id,rule_version,env_revision,state,moves_json,reason,proposed_by,version,created_at,updated_at) VALUES(?,?,?,'proposed',?,?,?,1,?,?)", (plan_id, rule["version_no"], revision, canonical_json(normalized), reason.strip(), context.actor_id, now, now))
                self.audit.append(connection, actor_id=context.actor_id, action="propose", entity_type="marathon_plan", entity_id=plan_id, version=1, detail={"moves": normalized, "reason": reason.strip(), "rule_version": rule["version_no"], "env_revision": revision})
                return self._plan_dict(connection.execute("SELECT * FROM marathon_plans WHERE plan_id=?", (plan_id,)).fetchone())
            return self.idempotency.execute(connection, scope="marathon.propose_plan", request_key=request_key, request={"moves": normalized, "reason": reason.strip()}, operation=operation)

    def confirm_plan(self, context: AccessContext, plan_id: str, *, role: str, request_key: str) -> dict:
        role = self._confirm_role(role)
        context.require(f"confirm:marathon.{role}")
        plan_id = require_safe(plan_id, "方案标识")
        with self.database.transaction() as connection:
            def operation() -> dict:
                plan = self._plan(connection, plan_id)
                if plan["state"] != "proposed":
                    raise ConflictError(f"方案状态为 {plan['state']}，不能确认")
                assert_distinct(plan["proposed_by"], context.actor_id)
                column = f"{role}_confirmed_by"
                if plan[column] is not None:
                    raise ConflictError(f"{role} 负责人已确认过该方案")
                other_column = "transport_confirmed_by" if role == "medical" else "medical_confirmed_by"
                if plan[other_column] == context.actor_id:
                    raise PermissionDenied("医疗与交通负责人必须分别确认")
                now = self.clock.now()
                both = plan[other_column] is not None
                state = "confirmed" if both else "proposed"
                connection.execute(f"UPDATE marathon_plans SET {column}=?, state=?, version=version+1, updated_at=? WHERE plan_id=?", (context.actor_id, state, now, plan_id))
                self.audit.append(connection, actor_id=context.actor_id, action="confirm", entity_type="marathon_plan", entity_id=plan_id, version=plan["version"] + 1, detail={"role": role, "state": state})
                return self._plan_dict(connection.execute("SELECT * FROM marathon_plans WHERE plan_id=?", (plan_id,)).fetchone())
            return self.idempotency.execute(connection, scope=f"marathon.confirm_plan:{plan_id}:{role}", request_key=request_key, request={"role": role}, operation=operation)

    def reject_plan(self, context: AccessContext, plan_id: str, *, role: str, reason: str, request_key: str) -> dict:
        role = self._confirm_role(role)
        context.require(f"confirm:marathon.{role}")
        if not reason or not reason.strip():
            raise ValidationError("驳回必须说明原因")
        plan_id = require_safe(plan_id, "方案标识")
        with self.database.transaction() as connection:
            def operation() -> dict:
                plan = self._plan(connection, plan_id)
                if plan["state"] != "proposed":
                    raise ConflictError(f"方案状态为 {plan['state']}，不能驳回")
                assert_distinct(plan["proposed_by"], context.actor_id)
                now = self.clock.now()
                connection.execute("UPDATE marathon_plans SET state='rejected', version=version+1, updated_at=? WHERE plan_id=?", (now, plan_id))
                self.audit.append(connection, actor_id=context.actor_id, action="reject", entity_type="marathon_plan", entity_id=plan_id, version=plan["version"] + 1, detail={"role": role, "reason": reason.strip()})
                return self._plan_dict(connection.execute("SELECT * FROM marathon_plans WHERE plan_id=?", (plan_id,)).fetchone())
            return self.idempotency.execute(connection, scope=f"marathon.reject_plan:{plan_id}:{role}", request_key=request_key, request={"role": role, "reason": reason.strip()}, operation=operation)

    def withdraw_plan(self, context: AccessContext, plan_id: str) -> dict:
        context.require("propose:marathon")
        plan_id = require_safe(plan_id, "方案标识")
        with self.database.transaction() as connection:
            plan = self._plan(connection, plan_id)
            if plan["proposed_by"] != context.actor_id:
                raise PermissionDenied("只有提出人可以撤回方案")
            if plan["state"] == "withdrawn":
                return self._plan_dict(plan)
            if plan["state"] != "proposed":
                raise ConflictError(f"方案状态为 {plan['state']}，不能撤回")
            now = self.clock.now()
            connection.execute("UPDATE marathon_plans SET state='withdrawn', version=version+1, updated_at=? WHERE plan_id=?", (now, plan_id))
            self.audit.append(connection, actor_id=context.actor_id, action="withdraw_plan", entity_type="marathon_plan", entity_id=plan_id, version=plan["version"] + 1, detail={})
            return self._plan_dict(connection.execute("SELECT * FROM marathon_plans WHERE plan_id=?", (plan_id,)).fetchone())

    def apply_plan(self, context: AccessContext, plan_id: str) -> dict:
        context.require("apply:marathon")
        plan_id = require_safe(plan_id, "方案标识")
        stale: str | None = None
        with self.database.transaction() as connection:
            plan = self._plan(connection, plan_id)
            if plan["state"] == "applied":
                return self._plan_dict(plan)
            if plan["state"] != "confirmed":
                raise ConflictError(f"方案状态为 {plan['state']}，不能落库")
            revision = self._revision(connection)
            rule = self._active_rule(connection)
            if rule is None or rule["version_no"] != plan["rule_version"]:
                stale = "规则版本已变化"
            elif revision != plan["env_revision"]:
                stale = "现场环境已变化"
            if stale is not None:
                now = self.clock.now()
                connection.execute("UPDATE marathon_plans SET state='superseded', version=version+1, updated_at=? WHERE plan_id=?", (now, plan_id))
                self.audit.append(connection, actor_id=context.actor_id, action="supersede", entity_type="marathon_plan", entity_id=plan_id, version=plan["version"] + 1, detail={"cause": stale})
            else:
                return self._apply(connection, plan, actor=context.actor_id)
        raise ConflictError(f"方案已过期（{stale}），禁止落库")

    def plan(self, context: AccessContext, plan_id: str) -> dict:
        context.require("read:marathon")
        with self.database.connect() as connection:
            return self._plan_dict(self._plan(connection, require_safe(plan_id, "方案标识")))

    def list_plans(self, context: AccessContext, *, state: str | None = None, limit: int = 100) -> list[dict]:
        context.require("read:marathon")
        if state is not None and state not in PLAN_STATES:
            raise ValidationError("未知方案状态")
        if limit < 1 or limit > 500:
            raise ValidationError("limit 必须在 1 到 500 之间")
        sql = "SELECT * FROM marathon_plans"
        params: list[object] = []
        if state is not None:
            sql += " WHERE state=?"
            params.append(state)
        sql += " ORDER BY created_at, plan_id LIMIT ?"
        params.append(limit)
        with self.database.connect() as connection:
            return [self._plan_dict(row) for row in connection.execute(sql, params)]

    def confirmation_queue(self, context: AccessContext) -> list[dict]:
        """发枪前指挥部看到的、此刻仍可继续推进的已确认方案队列。"""
        context.require("read:marathon")
        with self.database.connect() as connection:
            revision = self._revision(connection)
            rule = self._active_rule(connection)
            if rule is None:
                return []
            rows = connection.execute("SELECT * FROM marathon_plans WHERE state='confirmed' AND env_revision=? AND rule_version=? ORDER BY created_at, plan_id", (revision, rule["version_no"])).fetchall()
            return [self._plan_dict(row) for row in rows]

    # ------------------------------------------------------------------
    # 现场回传接入
    # ------------------------------------------------------------------

    def ingest_callback(self, context: AccessContext, *, source: str, source_key: str, sequence: int, payload: Mapping[str, object], occurred_at: str) -> dict:
        """按来源序号幂等接收计时、检录、医疗回传；异文隔离并只冻结关联选手。"""
        context.require("write:marathon.inbox")
        if not isinstance(payload, Mapping):
            raise ValidationError("回传内容必须是对象")
        payload = dict(payload)
        with self.database.transaction() as connection:
            result = self.inbox.accept(connection, source=source, source_key=source_key, sequence=sequence, payload=payload, occurred_at=occurred_at)
            if result["status"] == "duplicate":
                return {"status": "duplicate", "digest": result["digest"]}
            if result["status"] == "conflict":
                frozen = self._freeze_for_payload(connection, payload, actor=context.actor_id)
                return {"status": "quarantined", "digest": result["digest"], "frozen": frozen}
            effect = self._apply_effect(connection, payload, self.clock.now(), actor=context.actor_id)
            return {"status": "accepted", "digest": result["digest"], "effect": effect}

    # ------------------------------------------------------------------
    # 赛后复盘
    # ------------------------------------------------------------------

    def review_at(self, context: AccessContext, *, as_of: str) -> dict:
        """按选定时点核对分区归属、保障分配与每次调整的原因。"""
        context.require("history:marathon")
        instant = canonical_instant(as_of)
        with self.database.connect() as connection:
            zones: dict[str, str] = {}
            for row in connection.execute("SELECT runner_id, original_zone FROM marathon_runners WHERE created_at<=? ORDER BY runner_id", (instant,)):
                zones[row["runner_id"]] = row["original_zone"]
            adjustments = [dict(row) for row in connection.execute("SELECT plan_id, runner_id, from_zone, to_zone, reason, occurred_at FROM marathon_adjustments WHERE occurred_at<=? ORDER BY occurred_at, rowid", (instant,))]
            for adjustment in adjustments:
                zones[adjustment["runner_id"]] = adjustment["to_zone"]
            latest_alloc: dict[str, dict] = {}
            for row in connection.execute("SELECT allocation_id, zone_id, kind, subject_id, quantity, event FROM marathon_alloc_events WHERE occurred_at<=? ORDER BY event_id", (instant,)):
                latest_alloc[row["allocation_id"]] = dict(row)
            usage: dict[tuple[str, str], int] = {}
            for alloc in latest_alloc.values():
                if alloc["event"] in ("locked", "consumed"):
                    key = (alloc["zone_id"], alloc["kind"])
                    usage[key] = usage.get(key, 0) + int(alloc["quantity"])
            capacities: dict[tuple[str, str], int] = {}
            for row in connection.execute("SELECT zone_id, kind, capacity FROM marathon_capacity_events WHERE occurred_at<=? ORDER BY event_id", (instant,)):
                capacities[(row["zone_id"], row["kind"])] = int(row["capacity"])
            return {
                "as_of": instant,
                "zones": zones,
                "usage": [{"zone_id": zone_id, "kind": kind, "used": used} for (zone_id, kind), used in sorted(usage.items())],
                "capacities": [{"zone_id": zone_id, "kind": kind, "capacity": capacity} for (zone_id, kind), capacity in sorted(capacities.items())],
                "adjustments": adjustments,
            }

    # ------------------------------------------------------------------
    # 内部：环境版本
    # ------------------------------------------------------------------

    def _revision(self, connection: sqlite3.Connection) -> int:
        connection.execute("INSERT OR IGNORE INTO marathon_env(id, revision) VALUES(1, 0)")
        row = connection.execute("SELECT revision FROM marathon_env WHERE id=1").fetchone()
        return int(row["revision"])

    def _bump(self, connection: sqlite3.Connection) -> int:
        connection.execute("INSERT OR IGNORE INTO marathon_env(id, revision) VALUES(1, 0)")
        connection.execute("UPDATE marathon_env SET revision=revision+1 WHERE id=1")
        return self._revision(connection)

    # ------------------------------------------------------------------
    # 内部：读取助手
    # ------------------------------------------------------------------

    def _zone(self, connection: sqlite3.Connection, zone_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM marathon_zones WHERE zone_id=?", (zone_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"分区 {zone_id} 不存在")
        return row

    def _runner(self, connection: sqlite3.Connection, runner_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM marathon_runners WHERE runner_id=?", (runner_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"选手 {runner_id} 不存在")
        return row

    def _plan(self, connection: sqlite3.Connection, plan_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM marathon_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"方案 {plan_id} 不存在")
        return row

    def _active_rule(self, connection: sqlite3.Connection) -> sqlite3.Row | None:
        return connection.execute("SELECT * FROM marathon_rules WHERE status='active' ORDER BY version_no DESC LIMIT 1").fetchone()

    def _usage(self, connection: sqlite3.Connection, zone_id: str, kind: str) -> int:
        row = connection.execute("SELECT COALESCE(SUM(quantity),0) AS used FROM marathon_allocations WHERE zone_id=? AND kind=? AND status IN ('locked','consumed')", (zone_id, kind)).fetchone()
        return int(row["used"])

    def _capacity(self, connection: sqlite3.Connection, zone_id: str, kind: str) -> int:
        row = connection.execute("SELECT capacity FROM marathon_resources WHERE zone_id=? AND kind=?", (zone_id, kind)).fetchone()
        if row is None:
            raise InvariantViolation(f"分区 {zone_id} 缺少资源类型 {kind}")
        return int(row["capacity"])

    # ------------------------------------------------------------------
    # 内部：字典化
    # ------------------------------------------------------------------

    def _zone_dict(self, connection: sqlite3.Connection, zone_id: str) -> dict:
        zone = self._zone(connection, zone_id)
        capacities = {kind: self._capacity(connection, zone_id, kind) for kind in RESOURCE_KINDS}
        usage = {kind: self._usage(connection, zone_id, kind) for kind in RESOURCE_KINDS}
        return {"zone_id": zone["zone_id"], "wave_no": zone["wave_no"], "fired": bool(zone["fired"]), "capacities": capacities, "usage": usage, "created_at": zone["created_at"], "updated_at": zone["updated_at"]}

    def _runner_dict(self, connection: sqlite3.Connection, runner_id: str) -> dict:
        runner = self._runner(connection, runner_id)
        allocations = [dict(row) for row in connection.execute("SELECT allocation_id, zone_id, kind, quantity, status, plan_id FROM marathon_allocations WHERE subject_id=? ORDER BY kind, allocation_id", (runner_id,))]
        return {
            "runner_id": runner["runner_id"],
            "name": runner["name"],
            "qualification": json.loads(runner["qualification_json"]),
            "original_zone": runner["original_zone"],
            "current_zone": runner["current_zone"],
            "status": runner["status"],
            "frozen": bool(runner["frozen"]),
            "version": runner["version"],
            "created_at": runner["created_at"],
            "updated_at": runner["updated_at"],
            "allocations": allocations,
        }

    @staticmethod
    def _plan_dict(plan: sqlite3.Row) -> dict:
        return {
            "plan_id": plan["plan_id"],
            "state": plan["state"],
            "rule_version": plan["rule_version"],
            "env_revision": plan["env_revision"],
            "moves": json.loads(plan["moves_json"]),
            "reason": plan["reason"],
            "proposed_by": plan["proposed_by"],
            "medical_confirmed_by": plan["medical_confirmed_by"],
            "transport_confirmed_by": plan["transport_confirmed_by"],
            "version": plan["version"],
            "created_at": plan["created_at"],
            "updated_at": plan["updated_at"],
            "applied_at": plan["applied_at"],
        }

    # ------------------------------------------------------------------
    # 内部：校验
    # ------------------------------------------------------------------

    @staticmethod
    def _wave_no(value: object) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValidationError("枪次必须是正整数")
        return value

    @staticmethod
    def _validate_capacities(capacities: Mapping[str, int]) -> dict[str, int]:
        if not isinstance(capacities, Mapping):
            raise ValidationError("容量配置必须是对象")
        unknown = set(capacities) - set(RESOURCE_KINDS)
        if unknown:
            raise ValidationError("未知资源类型: " + ", ".join(sorted(str(item) for item in unknown)))
        missing = [kind for kind in RESOURCE_KINDS if kind not in capacities]
        if missing:
            raise ValidationError("缺少资源类型: " + ", ".join(missing))
        normalized = {}
        for kind in RESOURCE_KINDS:
            value = capacities[kind]
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValidationError(f"{RESOURCE_LABELS[kind]}容量必须是正整数")
            normalized[kind] = value
        return normalized

    @staticmethod
    def _validate_classes(classes: Mapping[str, object]) -> dict[str, dict]:
        if not isinstance(classes, Mapping) or not classes:
            raise ValidationError("规则必须包含至少一个资格类别")
        normalized: dict[str, dict] = {}
        for name, spec in classes.items():
            class_name = str(name).strip()
            if not class_name:
                raise ValidationError("资格类别名称不能为空")
            zones = spec.get("zones") if isinstance(spec, Mapping) else None
            if not isinstance(zones, (list, tuple)) or not zones:
                raise ValidationError(f"类别 {class_name} 必须给出可进入分区列表")
            zone_ids = []
            for zone_id in zones:
                zone_ids.append(require_safe(str(zone_id), "分区标识"))
            if len(set(zone_ids)) != len(zone_ids):
                raise ValidationError(f"类别 {class_name} 的分区列表存在重复")
            normalized[class_name] = {"zones": zone_ids}
        return normalized

    @staticmethod
    def _validate_moves(moves: object) -> list[dict]:
        if not isinstance(moves, (list, tuple)) or not moves:
            raise ValidationError("调整清单不能为空")
        if len(moves) > MAX_PLAN_MOVES:
            raise ValidationError(f"单次方案最多 {MAX_PLAN_MOVES} 条调整")
        normalized = []
        for item in moves:
            if not isinstance(item, Mapping):
                raise ValidationError("调整项必须是对象")
            runner_id = require_safe(str(item.get("runner_id", "")), "选手号码")
            to_zone = require_safe(str(item.get("to_zone", "")), "目标分区")
            entry = {"runner_id": runner_id, "to_zone": to_zone}
            note = item.get("reason")
            if note is not None and str(note).strip():
                entry["reason"] = str(note).strip()
            normalized.append(entry)
        return normalized

    @staticmethod
    def _confirm_role(role: str) -> str:
        role = str(role).strip()
        if role not in CONFIRM_ROLES:
            raise ValidationError("确认角色必须是 medical 或 transport")
        return role

    def _blocked_reason(self, connection: sqlite3.Connection, runner: sqlite3.Row) -> str | None:
        if runner["frozen"]:
            return "frozen"
        status = runner["status"]
        if status == "registered":
            zone = self._zone(connection, runner["current_zone"])
            return "fired" if zone["fired"] else None
        return {"checked_in": "checked_in", "started": "started", "finished": "finished", "withdrawn": "withdrawn"}.get(status, status)

    def _assert_movable(self, connection: sqlite3.Connection, runner: sqlite3.Row) -> None:
        blocked = self._blocked_reason(connection, runner)
        messages = {
            "frozen": "选手处于异文冻结状态，解除前不能重排",
            "checked_in": "选手已检录，不能被普通重排改写",
            "started": "选手已发枪上赛道，不能被普通重排改写",
            "finished": "选手已完赛，不能重排",
            "withdrawn": "选手已退赛，不能重排",
            "fired": "选手所在分区已发枪，不能重排",
        }
        if blocked is not None:
            raise ConflictError(messages.get(blocked, f"选手状态为 {blocked}，不能重排"))

    def _validate_moves_against_state(self, connection: sqlite3.Connection, moves: list[dict], classes: dict) -> None:
        deltas: dict[tuple[str, str], int] = {}
        seen: set[str] = set()
        for move in moves:
            runner_id = move["runner_id"]
            if runner_id in seen:
                raise ValidationError(f"选手 {runner_id} 在方案中重复出现")
            seen.add(runner_id)
            runner = self._runner(connection, runner_id)
            self._assert_movable(connection, runner)
            from_zone = runner["current_zone"]
            to_zone = move["to_zone"]
            if from_zone == to_zone:
                raise ValidationError(f"选手 {runner_id} 的目标分区与当前分区相同")
            zone = self._zone(connection, to_zone)
            if zone["fired"]:
                raise ConflictError(f"分区 {to_zone} 已发枪，不能调入")
            qclass = json.loads(runner["qualification_json"])["class"]
            allowed = classes.get(qclass, {}).get("zones", [])
            if to_zone not in allowed:
                raise ValidationError(f"规则不允许 {qclass} 类选手 {runner_id} 调整至分区 {to_zone}")
            for kind in RESOURCE_KINDS:
                deltas[(from_zone, kind)] = deltas.get((from_zone, kind), 0) - 1
                deltas[(to_zone, kind)] = deltas.get((to_zone, kind), 0) + 1
        self._assert_capacity(connection, deltas)

    def _assert_capacity(self, connection: sqlite3.Connection, deltas: Mapping[tuple[str, str], int]) -> None:
        for (zone_id, kind), delta in sorted(deltas.items()):
            if delta == 0:
                continue
            usage = self._usage(connection, zone_id, kind)
            capacity = self._capacity(connection, zone_id, kind)
            if usage + delta > capacity:
                raise ConflictError(f"分区 {zone_id} 的{RESOURCE_LABELS[kind]}不足")
            if usage + delta < 0:
                raise InvariantViolation(f"分区 {zone_id} 的{RESOURCE_LABELS[kind]}占用不能为负")

    # ------------------------------------------------------------------
    # 内部：资源占用与事件
    # ------------------------------------------------------------------

    def _lock_allocations(self, connection: sqlite3.Connection, *, zone_id: str, subject_id: str, plan_id: str | None, now: str) -> None:
        for kind in RESOURCE_KINDS:
            allocation_id = new_id("malloc")
            connection.execute("INSERT INTO marathon_allocations(allocation_id,zone_id,kind,subject_id,quantity,status,plan_id,created_at,updated_at) VALUES(?,?,?,?,1,'locked',?,?,?)", (allocation_id, zone_id, kind, subject_id, plan_id, now, now))
            self._alloc_event(connection, allocation_id, zone_id, kind, subject_id, 1, "locked", now)

    def _release_allocations(self, connection: sqlite3.Connection, *, subject_id: str, now: str, to_status: str = "released") -> int:
        rows = connection.execute("SELECT * FROM marathon_allocations WHERE subject_id=? AND status='locked'", (subject_id,)).fetchall()
        for row in rows:
            connection.execute("UPDATE marathon_allocations SET status=?, updated_at=? WHERE allocation_id=?", (to_status, now, row["allocation_id"]))
            self._alloc_event(connection, row["allocation_id"], row["zone_id"], row["kind"], row["subject_id"], row["quantity"], to_status, now)
        return len(rows)

    def _consume_allocations(self, connection: sqlite3.Connection, *, subject_id: str, now: str) -> int:
        rows = connection.execute("SELECT * FROM marathon_allocations WHERE subject_id=? AND status='locked'", (subject_id,)).fetchall()
        for row in rows:
            connection.execute("UPDATE marathon_allocations SET status='consumed', updated_at=? WHERE allocation_id=?", (now, row["allocation_id"]))
            self._alloc_event(connection, row["allocation_id"], row["zone_id"], row["kind"], row["subject_id"], row["quantity"], "consumed", now)
        return len(rows)

    @staticmethod
    def _alloc_event(connection: sqlite3.Connection, allocation_id: str, zone_id: str, kind: str, subject_id: str, quantity: int, event: str, occurred_at: str) -> None:
        connection.execute("INSERT INTO marathon_alloc_events(allocation_id,zone_id,kind,subject_id,quantity,event,occurred_at) VALUES(?,?,?,?,?,?,?)", (allocation_id, zone_id, kind, subject_id, quantity, event, occurred_at))

    @staticmethod
    def _capacity_event(connection: sqlite3.Connection, zone_id: str, kind: str, capacity: int, occurred_at: str) -> None:
        connection.execute("INSERT INTO marathon_capacity_events(zone_id,kind,capacity,occurred_at) VALUES(?,?,?,?)", (zone_id, kind, capacity, occurred_at))

    # ------------------------------------------------------------------
    # 内部：现场状态核心（供公开方法与回传共用）
    # ------------------------------------------------------------------

    def _check_in(self, connection: sqlite3.Connection, runner_id: str, now: str, *, actor: str) -> None:
        runner = self._runner(connection, runner_id)
        if runner["status"] == "checked_in":
            return
        if runner["status"] != "registered":
            raise ConflictError(f"选手状态为 {runner['status']}，不能检录")
        connection.execute("UPDATE marathon_runners SET status='checked_in', version=version+1, updated_at=? WHERE runner_id=?", (now, runner_id))
        self._bump(connection)
        self.audit.append(connection, actor_id=actor, action="check_in", entity_type="marathon_runner", entity_id=runner_id, version=runner["version"] + 1, detail={})

    def _withdraw(self, connection: sqlite3.Connection, runner_id: str, reason: str, now: str, *, actor: str) -> None:
        runner = self._runner(connection, runner_id)
        if runner["status"] == "withdrawn":
            return
        if runner["status"] == "finished":
            raise ConflictError("已完赛选手不能退赛")
        released = self._release_allocations(connection, subject_id=runner_id, now=now)
        connection.execute("UPDATE marathon_runners SET status='withdrawn', version=version+1, updated_at=? WHERE runner_id=?", (now, runner_id))
        self._bump(connection)
        self.audit.append(connection, actor_id=actor, action="withdraw", entity_type="marathon_runner", entity_id=runner_id, version=runner["version"] + 1, detail={"reason": reason, "released_allocations": released})

    def _fire_wave(self, connection: sqlite3.Connection, wave_no: int, now: str, *, actor: str) -> list[str]:
        zones = connection.execute("SELECT * FROM marathon_zones WHERE wave_no=? ORDER BY zone_id", (wave_no,)).fetchall()
        if not zones:
            raise NotFoundError(f"第 {wave_no} 枪没有对应分区")
        if all(zone["fired"] for zone in zones):
            return []
        zone_ids = [zone["zone_id"] for zone in zones]
        for zone in zones:
            if not zone["fired"]:
                connection.execute("UPDATE marathon_zones SET fired=1, updated_at=? WHERE zone_id=?", (now, zone["zone_id"]))
                self.audit.append(connection, actor_id=actor, action="fire_wave", entity_type="marathon_zone", entity_id=zone["zone_id"], version=wave_no, detail={"wave_no": wave_no})
        placeholders = ",".join("?" for _ in zone_ids)
        runners = connection.execute(f"SELECT * FROM marathon_runners WHERE current_zone IN ({placeholders}) AND status IN ('registered','checked_in') ORDER BY runner_id", zone_ids).fetchall()
        started = []
        for runner in runners:
            connection.execute("UPDATE marathon_runners SET status='started', version=version+1, updated_at=? WHERE runner_id=?", (now, runner["runner_id"]))
            self._consume_allocations(connection, subject_id=runner["runner_id"], now=now)
            started.append(runner["runner_id"])
        self._bump(connection)
        return started

    def _finish(self, connection: sqlite3.Connection, runner_id: str, now: str, *, actor: str) -> None:
        runner = self._runner(connection, runner_id)
        if runner["status"] == "finished":
            return
        if runner["status"] != "started":
            raise ConflictError(f"选手状态为 {runner['status']}，不能登记完赛")
        connection.execute("UPDATE marathon_runners SET status='finished', version=version+1, updated_at=? WHERE runner_id=?", (now, runner_id))
        self._bump(connection)
        self.audit.append(connection, actor_id=actor, action="finish", entity_type="marathon_runner", entity_id=runner_id, version=runner["version"] + 1, detail={})

    def _set_capacity(self, connection: sqlite3.Connection, zone_id: str, kind: str, capacity: int, now: str, *, actor: str, reason: str) -> None:
        zone_id = require_safe(zone_id, "分区标识")
        if kind not in RESOURCE_KINDS:
            raise ValidationError(f"未知资源类型: {kind}")
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 0:
            raise ValidationError("容量必须是非负整数")
        self._zone(connection, zone_id)
        usage = self._usage(connection, zone_id, kind)
        if capacity < usage:
            raise ConflictError(f"分区 {zone_id} 的{RESOURCE_LABELS[kind]}不能低于已占用 {usage}")
        connection.execute("UPDATE marathon_resources SET capacity=? WHERE zone_id=? AND kind=?", (capacity, zone_id, kind))
        self._capacity_event(connection, zone_id, kind, capacity, now)
        self._bump(connection)
        self.audit.append(connection, actor_id=actor, action="update_capacity", entity_type="marathon_zone", entity_id=zone_id, version=capacity, detail={"kind": kind, "capacity": capacity, "reason": reason})

    def _freeze_for_payload(self, connection: sqlite3.Connection, payload: Mapping[str, object], *, actor: str) -> list[str]:
        runner_id = payload.get("runner_id")
        if not isinstance(runner_id, str) or not runner_id.strip():
            return []
        runner_id = runner_id.strip()
        row = connection.execute("SELECT * FROM marathon_runners WHERE runner_id=?", (runner_id,)).fetchone()
        if row is None or row["frozen"]:
            return []
        now = self.clock.now()
        connection.execute("UPDATE marathon_runners SET frozen=1, version=version+1, updated_at=? WHERE runner_id=?", (now, runner_id))
        self._bump(connection)
        self.audit.append(connection, actor_id=actor, action="freeze", entity_type="marathon_runner", entity_id=runner_id, version=row["version"] + 1, detail={"cause": "回传异文隔离"})
        return [runner_id]

    def _apply_effect(self, connection: sqlite3.Connection, payload: Mapping[str, object], now: str, *, actor: str) -> dict:
        kind = payload.get("kind")
        if kind == "check_in":
            runner_id = require_safe(str(payload.get("runner_id", "")), "选手号码")
            self._check_in(connection, runner_id, now, actor=actor)
            return {"kind": kind, "runner_id": runner_id}
        if kind == "withdrawn":
            runner_id = require_safe(str(payload.get("runner_id", "")), "选手号码")
            reason = str(payload.get("reason", "")).strip() or "现场回传退赛"
            self._withdraw(connection, runner_id, reason, now, actor=actor)
            return {"kind": kind, "runner_id": runner_id}
        if kind == "wave_fired":
            wave_no = payload.get("wave_no")
            started = self._fire_wave(connection, self._wave_no(wave_no), now, actor=actor)
            return {"kind": kind, "wave_no": wave_no, "started": started}
        if kind == "finished":
            runner_id = require_safe(str(payload.get("runner_id", "")), "选手号码")
            self._finish(connection, runner_id, now, actor=actor)
            return {"kind": kind, "runner_id": runner_id}
        if kind in ("medical_pressure", "road_capacity"):
            zone_id = require_safe(str(payload.get("zone_id", "")), "分区标识")
            capacity = payload.get("capacity")
            resource_kind = "medical" if kind == "medical_pressure" else "start_slot"
            reason = str(payload.get("reason", "")).strip() or ("医疗点压力上升" if kind == "medical_pressure" else "道路容量变化")
            self._set_capacity(connection, zone_id, resource_kind, capacity, now, actor=actor, reason=reason)
            return {"kind": kind, "zone_id": zone_id, "capacity": capacity}
        raise ValidationError(f"未知回传类型: {kind}")

    # ------------------------------------------------------------------
    # 内部：方案落库
    # ------------------------------------------------------------------

    def _apply(self, connection: sqlite3.Connection, plan: sqlite3.Row, *, actor: str) -> dict:
        plan_id = plan["plan_id"]
        moves = json.loads(plan["moves_json"])
        rule = self._active_rule(connection)
        classes = json.loads(rule["payload_json"])["classes"] if rule is not None else {}
        self._validate_moves_against_state(connection, moves, classes)
        now = self.clock.now()
        for move in moves:
            runner = self._runner(connection, move["runner_id"])
            from_zone = runner["current_zone"]
            to_zone = move["to_zone"]
            self._release_allocations(connection, subject_id=move["runner_id"], now=now)
            connection.execute("UPDATE marathon_runners SET current_zone=?, version=version+1, updated_at=? WHERE runner_id=?", (to_zone, now, move["runner_id"]))
            self._lock_allocations(connection, zone_id=to_zone, subject_id=move["runner_id"], plan_id=plan_id, now=now)
            note = move.get("reason") or plan["reason"]
            connection.execute("INSERT INTO marathon_adjustments(adjustment_id,plan_id,runner_id,from_zone,to_zone,reason,occurred_at) VALUES(?,?,?,?,?,?,?)", (new_id("madj"), plan_id, move["runner_id"], from_zone, to_zone, note, now))
            self.audit.append(connection, actor_id=actor, action="move", entity_type="marathon_runner", entity_id=move["runner_id"], version=runner["version"] + 1, detail={"plan_id": plan_id, "from_zone": from_zone, "to_zone": to_zone, "reason": note})
        connection.execute("UPDATE marathon_plans SET state='applied', applied_at=?, version=version+1, updated_at=? WHERE plan_id=?", (now, now, plan_id))
        self._bump(connection)
        self.audit.append(connection, actor_id=actor, action="apply", entity_type="marathon_plan", entity_id=plan_id, version=plan["version"] + 1, detail={"moves": moves})
        return self._plan_dict(connection.execute("SELECT * FROM marathon_plans WHERE plan_id=?", (plan_id,)).fetchone())
