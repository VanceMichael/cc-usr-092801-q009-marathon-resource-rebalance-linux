"""赛事编排：起跑分区重排与医疗、补给、接驳保障同步。

核心约束：

* 选手资格与原分区在报名导入时快照保存，永不被改写；可调整范围由生效中的
  规则版本（允许跨越的枪次/分区数）计算。
* 重排方案在同一个事务内同时锁定起跑区容量、医疗救援覆盖、补给水位与接驳
  班次，任一容量不足则整体不落库。
* 已检录或已发枪的选手被冻结，普通重排不能改写；退赛只释放尚未消耗的资源。
* 计时、检录、医疗回传按 (来源, 选手, 序号) 幂等接收，异文只隔离关联选手，
  不波及其他人。
* 方案由运营人员提出，医疗与交通负责人分别确认，申请人不得自批；方案生效前
  再次核对基准版本与最新容量，旧方案在并发下被整体拒绝，不留下半状态。
* 所有状态都在 SQLite 事务内落盘，进程退出后待办方案与时间依据不丢失。
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .audit import AuditLog
from .database import Database
from .errors import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .identifiers import new_id, require_safe
from .idempotency import IdempotencyStore
from .jsonutil import canonical_json, digest_json
from .security import AccessContext
from .timeutil import Clock, canonical_instant

# 四类同步锁定的保障资源。
RESOURCE_KINDS = ("zone_capacity", "medical", "supply", "shuttle")
SUPPORT_KINDS = ("medical", "supply", "shuttle")
RESOURCE_LABELS = {
    "zone_capacity": "起跑区容量",
    "medical": "医疗救援覆盖",
    "supply": "补给水位",
    "shuttle": "接驳班次",
}
# 回传来源：计时、检录、医疗。
RUNNER_SOURCES = ("timing", "checkin", "medical")
RUNNER_STATUSES = ("registered", "checked_in", "started", "withdrawn", "injured")
FROZEN_STATUSES = ("checked_in", "started")
PLAN_STATUSES = ("proposed", "medical_approved", "ready", "applied", "rejected")
APPROVAL_ROLES = ("medical", "traffic")
ACTIVE_PLAN_STATUSES = ("proposed", "medical_approved", "ready")


@dataclass(frozen=True)
class RaceService:
    """赛事编排领域服务。所有写操作都在单个 BEGIN IMMEDIATE 事务内完成。"""

    database: Database
    clock: Clock
    audit: AuditLog
    idempotency: IdempotencyStore

    # ---- 规则版本 -------------------------------------------------------

    def put_rule(self, context: AccessContext, values: dict, *, request_key: str) -> dict:
        context.require("write:race-rules")
        version = require_safe(str(values.get("rule_version", "")), "规则版本")
        wave_order = values.get("wave_order")
        if not isinstance(wave_order, list) or not wave_order or any(not isinstance(w, str) or not w.strip() for w in wave_order):
            raise ValidationError("wave_order 必须是非空枪次序列")
        if len(set(wave_order)) != len(wave_order):
            raise ValidationError("wave_order 中枪次不能重复")
        max_wave_delta = int(values.get("max_wave_delta", 0))
        max_zone_delta = int(values.get("max_zone_delta", 0))
        if max_wave_delta < 0 or max_zone_delta < 0:
            raise ValidationError("允许跨越的枪次或分区数不能为负")
        effective_from = canonical_instant(values["effective_from"])
        detail = values.get("detail", {})
        if not isinstance(detail, dict):
            raise ValidationError("detail 必须是对象")
        with self.database.transaction() as connection:
            def operation() -> dict:
                if connection.execute("SELECT 1 FROM race_rules WHERE rule_version=?", (version,)).fetchone():
                    raise ConflictError(f"规则版本 {version} 已存在，规则不可覆盖")
                connection.execute(
                    "INSERT INTO race_rules(rule_version,wave_order_json,max_wave_delta,max_zone_delta,effective_from,detail_json,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (version, canonical_json(wave_order), max_wave_delta, max_zone_delta, effective_from, canonical_json(detail), context.actor_id, self.clock.now()),
                )
                self.audit.append(connection, actor_id=context.actor_id, action="race-rule-put", entity_type="race_rules", entity_id=version, version=1, detail={"max_wave_delta": max_wave_delta, "max_zone_delta": max_zone_delta})
                return self._get_rule(connection, version)
            return self.idempotency.execute(connection, scope="race-rule", request_key=request_key, request=values, operation=operation)

    def current_rule(self, *, at: str | None = None) -> dict:
        instant = canonical_instant(at) if at else self.clock.now()
        with self.database.connect() as connection:
            return self._rule_to_dict(self._rule_at(connection, instant))

    @staticmethod
    def _get_rule(connection, version: str) -> dict:
        row = connection.execute("SELECT * FROM race_rules WHERE rule_version=?", (version,)).fetchone()
        if not row:
            raise NotFoundError(f"规则版本 {version} 不存在")
        return RaceService._rule_to_dict(row)

    @staticmethod
    def _rule_to_dict(row) -> dict:
        return {
            "rule_version": row["rule_version"],
            "wave_order": json.loads(row["wave_order_json"]),
            "max_wave_delta": row["max_wave_delta"],
            "max_zone_delta": row["max_zone_delta"],
            "effective_from": row["effective_from"],
            "detail": json.loads(row["detail_json"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
        }

    # ---- 分区与保障资源台账 ----------------------------------------------

    def configure_zone(self, context: AccessContext, *, zone_id: str, wave: str, ordinal: int, capacity: int, request_key: str) -> dict:
        context.require("write:race-zones")
        require_safe(zone_id, "分区")
        if not isinstance(wave, str) or not wave.strip():
            raise ValidationError("枪次不能为空")
        if capacity < 0 or ordinal < 0:
            raise ValidationError("分区容量与序号不能为负")
        with self.database.transaction() as connection:
            def operation() -> dict:
                connection.execute(
                    "INSERT INTO race_zones(zone_id,wave,ordinal,capacity,status) VALUES(?,?,?,?,'open') "
                    "ON CONFLICT(zone_id) DO UPDATE SET wave=excluded.wave,ordinal=excluded.ordinal,capacity=excluded.capacity",
                    (zone_id, wave, ordinal, capacity),
                )
                self.audit.append(connection, actor_id=context.actor_id, action="race-zone-configure", entity_type="race_zones", entity_id=zone_id, version=1, detail={"wave": wave, "ordinal": ordinal, "capacity": capacity})
                return dict(connection.execute("SELECT * FROM race_zones WHERE zone_id=?", (zone_id,)).fetchone())
            return self.idempotency.execute(connection, scope=f"race-zone:{zone_id}", request_key=request_key, request={"wave": wave, "ordinal": ordinal, "capacity": capacity}, operation=operation)

    def set_road_capacity(self, context: AccessContext, *, zone_id: str, capacity: int, status: str = "open", request_key: str) -> dict:
        """道路容量变化时压缩或关闭分区容量；不能低于现存占用与待决方案预占。"""
        context.require("write:race-zones")
        if status not in ("open", "restricted", "closed"):
            raise ValidationError("分区状态只能是 open/restricted/closed")
        if capacity < 0:
            raise ValidationError("分区容量不能为负")
        with self.database.transaction() as connection:
            def operation() -> dict:
                zone = self._require_zone(connection, zone_id)
                active = self._zone_occupancy_after_active_plans(connection, zone_id)
                if capacity < active:
                    raise ConflictError(f"{RESOURCE_LABELS['zone_capacity']}无法压缩到 {capacity}，当前占用含待决预占为 {active}")
                connection.execute("UPDATE race_zones SET capacity=?,status=? WHERE zone_id=?", (capacity, status, zone_id))
                self.audit.append(connection, actor_id=context.actor_id, action="race-zone-capacity", entity_type="race_zones", entity_id=zone_id, version=1, detail={"capacity": capacity, "status": status, "previous": zone["capacity"]})
                return dict(connection.execute("SELECT * FROM race_zones WHERE zone_id=?", (zone_id,)).fetchone())
            return self.idempotency.execute(connection, scope=f"race-zone-cap:{zone_id}", request_key=request_key, request={"capacity": capacity, "status": status}, operation=operation)

    def register_resource(self, context: AccessContext, *, resource_id: str, kind: str, scope_value: str, capacity: int, detail: dict | None = None, request_key: str) -> dict:
        context.require("write:race-resources")
        require_safe(resource_id, "资源")
        if kind not in SUPPORT_KINDS:
            raise ValidationError("保障资源类型必须是 " + "/".join(SUPPORT_KINDS))
        if not isinstance(scope_value, str) or not scope_value.strip():
            raise ValidationError("资源作用域不能为空")
        if capacity < 0:
            raise ValidationError("资源容量不能为负")
        with self.database.transaction() as connection:
            def operation() -> dict:
                connection.execute(
                    "INSERT INTO race_resources(resource_id,kind,scope_value,capacity,detail_json) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(resource_id) DO UPDATE SET kind=excluded.kind,scope_value=excluded.scope_value,capacity=excluded.capacity,detail_json=excluded.detail_json",
                    (resource_id, kind, scope_value, capacity, canonical_json(detail or {})),
                )
                self.audit.append(connection, actor_id=context.actor_id, action="race-resource-register", entity_type="race_resources", entity_id=resource_id, version=1, detail={"kind": kind, "scope_value": scope_value, "capacity": capacity})
                return dict(connection.execute("SELECT * FROM race_resources WHERE resource_id=?", (resource_id,)).fetchone())
            return self.idempotency.execute(connection, scope=f"race-resource:{resource_id}", request_key=request_key, request={"kind": kind, "scope": scope_value, "capacity": capacity}, operation=operation)

    def resource_load(self, kind: str, scope_value: str | None = None) -> list[dict]:
        if kind not in RESOURCE_KINDS:
            raise ValidationError("资源类型必须是 " + "/".join(RESOURCE_KINDS))
        sql = "SELECT r.* FROM race_resources r WHERE r.kind=?"
        params: list[object] = [kind]
        if scope_value is not None:
            sql += " AND r.scope_value=?"; params.append(scope_value)
        sql += " ORDER BY r.resource_id"
        with self.database.connect() as connection:
            result = []
            for row in connection.execute(sql, params):
                used = connection.execute("SELECT COALESCE(SUM(quantity),0) AS n FROM race_allocations WHERE resource_id=? AND status IN ('held','confirmed','consumed')", (row["resource_id"],)).fetchone()["n"]
                consumed = connection.execute("SELECT COALESCE(SUM(quantity),0) AS n FROM race_consumptions WHERE resource_id=?", (row["resource_id"],)).fetchone()["n"]
                item = dict(row); item["held_or_used"] = int(used); item["consumed"] = int(consumed)
                item["available"] = row["capacity"] - int(used)
                result.append(item)
            return result

    # ---- 选手资格与原分区快照 --------------------------------------------

    def import_runner(self, context: AccessContext, *, runner_id: str, bib: str, qualification: dict, zone_id: str, request_key: str) -> dict:
        context.require("write:race-runners")
        require_safe(runner_id, "选手")
        bib = str(bib).strip()
        if not bib:
            raise ValidationError("号码布不能为空")
        if not isinstance(qualification, dict) or not qualification:
            raise ValidationError("资格信息不能为空")
        with self.database.transaction() as connection:
            def operation() -> dict:
                zone = self._require_zone(connection, zone_id)
                rule = self._rule_at(connection, self.clock.now())
                now = self.clock.now()
                connection.execute(
                    "INSERT INTO race_runners(runner_id,bib,qualification_json,original_zone,zone,wave,status,frozen,rule_version,version,created_at,updated_at,created_by,updated_by) "
                    "VALUES(?,?,?,?,?,?,'registered',0,?,1,?,?,?,?)",
                    (runner_id, bib, canonical_json(qualification), zone_id, zone_id, zone["wave"], rule["rule_version"], now, now, context.actor_id, context.actor_id),
                )
                connection.execute(
                    "INSERT INTO race_runner_history(runner_id,version,zone,wave,status,frozen,reason,rule_version,plan_id,valid_from,actor_id) VALUES(?,1,?,?,'registered',0,?,?,NULL,?,?)",
                    (runner_id, zone_id, zone["wave"], "报名导入，保存资格与原分区快照", rule["rule_version"], now, context.actor_id),
                )
                self.audit.append(connection, actor_id=context.actor_id, action="race-runner-import", entity_type="race_runners", entity_id=runner_id, version=1, detail={"zone": zone_id, "bib": bib})
                return self._runner_to_dict(connection.execute("SELECT * FROM race_runners WHERE runner_id=?", (runner_id,)).fetchone(), connection)
            return self.idempotency.execute(connection, scope="race-runner-import", request_key=request_key, request={"runner_id": runner_id, "bib": bib, "zone": zone_id, "qualification": qualification}, operation=operation)

    def get_runner(self, runner_id: str) -> dict:
        with self.database.connect() as connection:
            return self._runner_to_dict(self._require_runner(connection, runner_id), connection)

    def list_runners(self, *, zone_id: str | None = None, status: str | None = None, limit: int = 500) -> list[dict]:
        if limit < 1 or limit > 2000:
            raise ValidationError("limit 必须在 1 到 2000 之间")
        sql = "SELECT * FROM race_runners"; where = []; params: list[object] = []
        if zone_id:
            where.append("zone=?"); params.append(zone_id)
        if status:
            if status not in RUNNER_STATUSES:
                raise ValidationError("未知选手状态")
            where.append("status=?"); params.append(status)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY bib,runner_id LIMIT ?"; params.append(limit)
        with self.database.connect() as connection:
            return [self._runner_to_dict(row, connection) for row in connection.execute(sql, params)]

    def adjustable_targets(self, runner_id: str, *, at: str | None = None) -> dict:
        """按规则版本计算可调整的枪次与分区范围；冻结选手返回空范围。"""
        instant = canonical_instant(at) if at else self.clock.now()
        with self.database.connect() as connection:
            runner = self._require_runner(connection, runner_id)
            rule = self._rule_at(connection, instant)
            wave_index = {wave: i for i, wave in enumerate(rule["wave_order"])}
            if runner["wave"] not in wave_index:
                raise ValidationError(f"选手枪次 {runner['wave']} 不在规则版本 {rule['rule_version']} 的枪次序列中")
            current = self._require_zone(connection, runner["zone"])
            zones = sorted((dict(row) for row in connection.execute("SELECT * FROM race_zones WHERE status!='closed'")), key=lambda z: (wave_index.get(z["wave"], 1 << 30), z["ordinal"]))
            same_wave_zones = sorted((z for z in zones if z["wave"] == current["wave"]), key=lambda z: z["ordinal"])
            cur_ordinal = next(z["ordinal"] for z in same_wave_zones if z["zone_id"] == current["zone_id"])
            cur_wave_i = wave_index[runner["wave"]]
            locked = bool(runner["frozen"]) or runner["status"] in FROZEN_STATUSES
            allowed: list[str] = []
            if not locked:
                for zone in zones:
                    if zone["wave"] not in wave_index:
                        continue
                    if abs(wave_index[zone["wave"]] - cur_wave_i) > rule["max_wave_delta"]:
                        continue
                    if zone["wave"] == current["wave"] and abs(zone["ordinal"] - cur_ordinal) > rule["max_zone_delta"]:
                        continue
                    allowed.append(zone["zone_id"])
            return {
                "runner_id": runner_id,
                "rule_version": rule["rule_version"],
                "current_zone": runner["zone"],
                "frozen": bool(runner["frozen"]),
                "adjustable": not locked and runner["status"] != "withdrawn",
                "allowed_zones": allowed,
            }

    # ---- 现场回传：检录 / 发枪 / 医疗（幂等、异文隔离） -------------------

    def receive_report(self, context: AccessContext, *, source: str, runner_id: str, sequence: int, payload: dict, occurred_at: str, request_key: str) -> dict:
        context.require("write:race-reports")
        if source not in RUNNER_SOURCES:
            raise ValidationError("回传来源必须是 " + "/".join(RUNNER_SOURCES))
        require_safe(runner_id, "选手")
        if sequence < 0:
            raise ValidationError("来源序号不能为负数")
        if not isinstance(payload, dict):
            raise ValidationError("回传内容必须是对象")
        occurred_at = canonical_instant(occurred_at)
        digest = digest_json(payload)
        # 异文可能在主事务外先被识别：隔离记录与冻结必须落盘，不能随随后抛出的异常回滚。
        with self.database.connect() as connection:
            existing = connection.execute("SELECT payload_digest FROM inbox_messages WHERE source=? AND source_key=? AND sequence=?", (source, runner_id, sequence)).fetchone()
        if existing:
            if existing["payload_digest"] != digest:
                self._quarantine_runner(context.actor_id, source=source, runner_id=runner_id, sequence=sequence, existing_digest=existing["payload_digest"], incoming_digest=digest)
                raise ConflictError(f"{source} 序号 {sequence} 出现不同内容，已隔离选手 {runner_id}")
            return {"status": "duplicate", "runner_id": runner_id, "digest": digest}
        with self.database.transaction() as connection:
            def operation() -> dict:
                row = connection.execute("SELECT payload_digest FROM inbox_messages WHERE source=? AND source_key=? AND sequence=?", (source, runner_id, sequence)).fetchone()
                if row:  # 并发下由另一事务先行写入，再次判别。
                    if row["payload_digest"] != digest:
                        raise ConflictError(f"{source} 序号 {sequence} 并发出现不同内容，请重试以完成隔离")
                    return {"status": "duplicate", "runner_id": runner_id, "digest": digest}
                runner = self._require_runner(connection, runner_id)
                connection.execute(
                    "INSERT INTO inbox_messages(source,source_key,sequence,payload_digest,payload_json,occurred_at,received_at,status) VALUES(?,?,?,?,?,?,?,'accepted')",
                    (source, runner_id, sequence, digest, canonical_json(payload), occurred_at, self.clock.now()),
                )
                return self._apply_report(connection, runner, source, payload, occurred_at, context.actor_id)
            return self.idempotency.execute(connection, scope=f"race-report:{source}:{runner_id}", request_key=request_key, request={"sequence": sequence, "payload": payload, "occurred_at": occurred_at}, operation=operation)

    def _quarantine_runner(self, actor: str, *, source: str, runner_id: str, sequence: int, existing_digest: str, incoming_digest: str) -> None:
        """异文隔离：登记冲突并只冻结关联选手，不波及其他选手。"""
        with self.database.transaction() as connection:
            runner = self._require_runner(connection, runner_id)
            if not connection.execute("SELECT 1 FROM inbox_conflicts WHERE source=? AND source_key=? AND sequence=? AND existing_digest=? AND incoming_digest=?", (source, runner_id, sequence, existing_digest, incoming_digest)).fetchone():
                connection.execute("INSERT INTO inbox_conflicts(source,source_key,sequence,existing_digest,incoming_digest,received_at) VALUES(?,?,?,?,?,?)", (source, runner_id, sequence, existing_digest, incoming_digest, self.clock.now()))
            if not runner["frozen"]:
                self._update_runner(connection, runner, status=runner["status"], frozen=1, zone=runner["zone"], wave=runner["wave"], reason=f"{source} 回传异文隔离，冻结并等待人工核对", actor="system", plan_id=None)
                runner = self._require_runner(connection, runner_id)
            self._reject_active_plans_for_runner(connection, runner_id, actor="system", comment=f"{source} 回传异文隔离，涉及该选手的待决方案作废")
            self.audit.append(connection, actor_id=actor, action="race-report-conflict", entity_type="race_runners", entity_id=runner_id, version=runner["version"], detail={"source": source, "sequence": sequence})

    def _apply_report(self, connection, runner, source: str, payload: dict, occurred_at: str, actor: str) -> dict:
        new_status = runner["status"]; new_frozen = runner["frozen"]; reason = ""
        if source == "checkin":
            if runner["status"] == "registered":
                new_status = "checked_in"; new_frozen = 1; reason = "检录回传：选手已检录，分区冻结"
        elif source == "timing":
            if str(payload.get("point", "")) == "start" and runner["status"] in ("registered", "checked_in"):
                new_status = "started"; new_frozen = 1
                connection.execute("UPDATE race_zones SET status='fired',fired_at=COALESCE(fired_at,?) WHERE zone_id=? AND status!='fired'", (occurred_at, runner["zone"]))
                reason = "计时回传：选手已发枪"
        elif source == "medical":
            severity = str(payload.get("severity", ""))
            if severity not in ("", "low", "medium", "high"):
                raise ValidationError("severity 只能是 low/medium/high")
            if severity == "high":
                new_status = "injured"; new_frozen = 1; reason = "医疗回传：高优先级处置，选手冻结等待医疗调度"
        if new_status != runner["status"] or new_frozen != runner["frozen"]:
            self._update_runner(connection, runner, status=new_status, frozen=new_frozen, zone=runner["zone"], wave=runner["wave"], reason=reason or "现场回传", actor=actor, plan_id=None)
            runner = self._require_runner(connection, runner["runner_id"])
            if new_frozen:
                # 冻结后涉及该选手的待决方案基准已过期，整体作废并释放全部预占。
                self._reject_active_plans_for_runner(connection, runner["runner_id"], actor=actor, comment=f"{source} 回传后选手冻结，待决方案作废")
        return {"status": "accepted", "runner_id": runner["runner_id"], "runner_status": runner["status"], "frozen": bool(runner["frozen"]), "digest": digest_json(payload)}

    # ---- 退赛：只释放尚未消耗的资源 ---------------------------------------

    def withdraw(self, context: AccessContext, *, runner_id: str, reason: str, request_key: str) -> dict:
        context.require("write:race-runners")
        if not reason.strip():
            raise ValidationError("退赛必须填写原因")
        with self.database.transaction() as connection:
            def operation() -> dict:
                runner = self._require_runner(connection, runner_id)
                if runner["status"] in ("started", "withdrawn"):
                    raise ConflictError(f"当前状态 {runner['status']} 不能退赛")
                active = connection.execute(
                    "SELECT 1 FROM race_plan_items pi JOIN race_plans p ON p.plan_id=pi.plan_id "
                    "WHERE pi.runner_id=? AND p.status IN (" + ",".join("?" * len(ACTIVE_PLAN_STATUSES)) + ") LIMIT 1",
                    (runner_id, *ACTIVE_PLAN_STATUSES),
                ).fetchone()
                if active:
                    raise ConflictError("选手存在待决重排方案，需先处理方案")
                self._update_runner(connection, runner, status="withdrawn", frozen=runner["frozen"], zone=runner["zone"], wave=runner["wave"], reason=f"临时退赛：{reason.strip()}", actor=context.actor_id, plan_id=None)
                released = self._release_unconsumed(connection, runner_id, actor=context.actor_id, reason="退赛释放尚未消耗的保障")
                self.audit.append(connection, actor_id=context.actor_id, action="race-runner-withdraw", entity_type="race_runners", entity_id=runner_id, version=runner["version"] + 1, detail={"reason": reason.strip(), "released": released})
                result = self._runner_to_dict(self._require_runner(connection, runner_id), connection)
                result["released"] = released
                return result
            return self.idempotency.execute(connection, scope="race-withdraw", request_key=request_key, request={"runner_id": runner_id, "reason": reason}, operation=operation)

    def _release_unconsumed(self, connection, runner_id: str, *, actor: str, reason: str) -> list[dict]:
        released = []
        rows = connection.execute("SELECT * FROM race_allocations WHERE runner_id=? AND status IN ('held','confirmed')", (runner_id,)).fetchall()
        now = self.clock.now()
        for alloc in rows:
            connection.execute("UPDATE race_allocations SET status='released',version=version+1,updated_at=? WHERE allocation_id=?", (now, alloc["allocation_id"]))
            self._journal(connection, actor=actor, allocation_id=alloc["allocation_id"], event="release", kind=alloc["kind"], from_resource=alloc["resource_id"], to_resource=None, from_zone=alloc["zone_id"], to_zone=None, plan_id=alloc["plan_id"], detail={"reason": reason})
            released.append({"allocation_id": alloc["allocation_id"], "kind": alloc["kind"], "resource_id": alloc["resource_id"]})
        # consumed 状态（已有现场回执的实际消耗，如已领取的补给）保留，不退仓。
        return released

    # ---- 保障绑定与现场回执（消耗） ---------------------------------------

    def assign_support(self, context: AccessContext, *, runner_id: str, kind: str, resource_id: str, request_key: str) -> dict:
        """方案之外的保障绑定（如报名后的基准医疗/接驳安排）。"""
        context.require("write:race-allocations")
        if kind not in SUPPORT_KINDS:
            raise ValidationError("保障绑定类型必须是 " + "/".join(SUPPORT_KINDS))
        with self.database.transaction() as connection:
            def operation() -> dict:
                runner = self._require_runner(connection, runner_id)
                resource = self._require_resource(connection, resource_id, kind)
                if runner["status"] == "withdrawn":
                    raise ConflictError("已退赛选手不能绑定保障")
                if connection.execute("SELECT 1 FROM race_allocations WHERE runner_id=? AND kind=? AND status IN ('held','confirmed','consumed')", (runner_id, kind)).fetchone():
                    raise ConflictError(f"选手已存在{RESOURCE_LABELS[kind]}分配")
                self._take_capacity(connection, resource, 1)
                allocation_id = new_id("alloc")
                connection.execute(
                    "INSERT INTO race_allocations(allocation_id,kind,resource_id,runner_id,zone_id,quantity,status,plan_id,version,updated_at) VALUES(?,?,?,?,?,1,'confirmed',NULL,1,?)",
                    (allocation_id, kind, resource_id, runner_id, runner["zone"], self.clock.now()),
                )
                self._journal(connection, actor=context.actor_id, allocation_id=allocation_id, event="assign", kind=kind, from_resource=None, to_resource=resource_id, from_zone=None, to_zone=runner["zone"], plan_id=None)
                return {"allocation_id": allocation_id, "status": "confirmed", "kind": kind, "resource_id": resource_id}
            return self.idempotency.execute(connection, scope=f"race-assign:{runner_id}:{kind}", request_key=request_key, request={"resource_id": resource_id}, operation=operation)

    def receive_consumption(self, context: AccessContext, *, runner_id: str, kind: str, quantity: int = 1, occurred_at: str | None = None, request_key: str) -> dict:
        """现场回执：保障已实际消耗（如补给已领取）。

        待决重排方案已预占该选手保障时拒绝回执，逼使回执与方案在同一串行化
        事务层面分出先后，避免旧方案落库覆盖回执结果。
        """
        context.require("write:race-allocations")
        if kind not in SUPPORT_KINDS:
            raise ValidationError("现场回执类型必须是 " + "/".join(SUPPORT_KINDS))
        if quantity <= 0:
            raise ValidationError("消耗数量必须为正数")
        occurred_at = canonical_instant(occurred_at) if occurred_at else self.clock.now()
        with self.database.transaction() as connection:
            def operation() -> dict:
                runner = self._require_runner(connection, runner_id)
                held = connection.execute("SELECT 1 FROM race_allocations WHERE runner_id=? AND kind=? AND status='held' LIMIT 1", (runner_id, kind)).fetchone()
                if held:
                    raise ConflictError("该选手保障已被待决重排方案预占，请待方案生效或驳回后再回执")
                alloc = connection.execute("SELECT * FROM race_allocations WHERE runner_id=? AND kind=? AND status IN ('confirmed','consumed') ORDER BY version DESC LIMIT 1", (runner_id, kind)).fetchone()
                if not alloc:
                    raise NotFoundError(f"选手没有{RESOURCE_LABELS[kind]}分配，无法接收回执")
                already = connection.execute("SELECT COALESCE(SUM(quantity),0) AS n FROM race_consumptions WHERE resource_id=? AND runner_id=?", (alloc["resource_id"], runner_id)).fetchone()["n"]
                total = int(already) + quantity
                if total > alloc["quantity"]:
                    raise ConflictError(f"回执数量超过{RESOURCE_LABELS[kind]}分配额度")
                connection.execute(
                    "INSERT INTO race_consumptions(resource_id,runner_id,quantity,occurred_at,updated_at) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(resource_id,runner_id) DO UPDATE SET quantity=excluded.quantity,occurred_at=excluded.occurred_at,updated_at=excluded.updated_at",
                    (alloc["resource_id"], runner_id, total, occurred_at, self.clock.now()),
                )
                if alloc["status"] != "consumed":
                    connection.execute("UPDATE race_allocations SET status='consumed',version=version+1,updated_at=? WHERE allocation_id=?", (self.clock.now(), alloc["allocation_id"]))
                self._journal(connection, actor=context.actor_id, allocation_id=alloc["allocation_id"], event="consume", kind=kind, from_resource=alloc["resource_id"], to_resource=alloc["resource_id"], from_zone=alloc["zone_id"], to_zone=alloc["zone_id"], plan_id=None, detail={"quantity": quantity})
                return {"allocation_id": alloc["allocation_id"], "status": "consumed", "consumed": total}
            return self.idempotency.execute(connection, scope=f"race-consume:{runner_id}:{kind}", request_key=request_key, request={"quantity": quantity, "occurred_at": occurred_at}, operation=operation)

    # ---- 重排方案：提出 / 双角色确认 / 原子生效 ----------------------------

    def propose_plan(self, context: AccessContext, *, moves: list[dict], reason: str, request_key: str) -> dict:
        context.require("propose:race-plans")
        if not reason.strip():
            raise ValidationError("重排方案必须说明原因")
        cleaned = self._clean_moves(moves)
        with self.database.transaction() as connection:
            def operation() -> dict:
                pairs = []
                for move in cleaned:
                    runner = self._require_runner(connection, move["runner_id"])
                    target = self._require_zone(connection, move["to_zone"])
                    if runner["frozen"] or runner["status"] in FROZEN_STATUSES:
                        raise ConflictError(f"选手 {runner['runner_id']} 已检录/发枪或被冻结，不能普通重排")
                    if runner["status"] == "withdrawn":
                        raise ConflictError(f"选手 {runner['runner_id']} 已退赛")
                    if target["status"] in ("closed", "fired"):
                        raise ConflictError(f"目标分区 {target['zone_id']} 已关闭或发枪")
                    if target["zone_id"] == runner["zone"]:
                        raise ValidationError(f"选手 {runner['runner_id']} 已在分区 {target['zone_id']}，无需调整")
                    rule = self._rule_at(connection, self.clock.now())
                    self._assert_within_rule(rule, runner, target)
                    pairs.append((runner, target))
                for runner, _ in pairs:
                    active = connection.execute(
                        "SELECT 1 FROM race_plan_items pi JOIN race_plans p ON p.plan_id=pi.plan_id "
                        "WHERE pi.runner_id=? AND p.status IN (" + ",".join("?" * len(ACTIVE_PLAN_STATUSES)) + ") LIMIT 1",
                        (runner["runner_id"], *ACTIVE_PLAN_STATUSES),
                    ).fetchone()
                    if active:
                        raise ConflictError(f"选手 {runner['runner_id']} 已存在待决重排方案")
                plan_id = new_id("plan")
                now = self.clock.now()
                # 同时锁定四类资源；不足则抛错，本事务回滚，不留任何预占或方案。
                self._hold_requirements(connection, pairs, plan_id, now)
                base_digest = self._base_digest([runner for runner, _ in pairs])
                connection.execute(
                    "INSERT INTO race_plans(plan_id,rule_versions_json,status,base_digest,reason,proposed_by,created_at,version) VALUES(?,?,'proposed',?,?,?,?,1)",
                    (plan_id, canonical_json(sorted({runner["rule_version"] for runner, _ in pairs})), base_digest, reason.strip(), context.actor_id, now),
                )
                for role in APPROVAL_ROLES:
                    connection.execute("INSERT INTO race_plan_approvals(plan_id,role,status) VALUES(?,?,'waiting')", (plan_id, role))
                for seq, (runner, target) in enumerate(pairs):
                    connection.execute(
                        "INSERT INTO race_plan_items(plan_id,seq,runner_id,action,from_zone,to_zone,base_version,base_zone,base_frozen,base_status) VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (plan_id, seq, runner["runner_id"], "move", runner["zone"], target["zone_id"], runner["version"], runner["zone"], runner["frozen"], runner["status"]),
                    )
                self.audit.append(connection, actor_id=context.actor_id, action="race-plan-propose", entity_type="race_plans", entity_id=plan_id, version=1, detail={"runners": len(pairs), "reason": reason.strip()})
                return self._plan_view(connection, plan_id)
            return self.idempotency.execute(connection, scope="race-plan-propose", request_key=request_key, request={"moves": cleaned, "reason": reason}, operation=operation)

    def approve_plan(self, context: AccessContext, *, plan_id: str, role: str, approved: bool, comment: str = "", request_key: str = "") -> dict:
        context.require("approve:race-plans")
        if role not in APPROVAL_ROLES:
            raise ValidationError("确认角色必须是 " + "/".join(APPROVAL_ROLES))
        with self.database.transaction() as connection:
            def operation() -> dict:
                plan = self._require_plan(connection, plan_id)
                if plan["status"] not in ACTIVE_PLAN_STATUSES:
                    raise ConflictError(f"方案状态为 {plan['status']}，不能再确认")
                # 申请人不得自批。
                if plan["proposed_by"] == context.actor_id:
                    raise PermissionDenied("重排申请人不能确认自己的方案")
                approval = connection.execute("SELECT * FROM race_plan_approvals WHERE plan_id=? AND role=?", (plan_id, role)).fetchone()
                if approval["status"] != "waiting":
                    raise ConflictError(f"{role} 已做出确认")
                now = self.clock.now()
                if not approved:
                    connection.execute("UPDATE race_plan_approvals SET status='rejected',decided_by=?,decided_at=?,comment=? WHERE plan_id=? AND role=?", (context.actor_id, now, comment.strip(), plan_id, role))
                    self._reject_plan(connection, plan, actor=context.actor_id, comment=f"{role} 负责人驳回：{comment.strip() or '未说明'}")
                    return self._plan_view(connection, plan_id)
                connection.execute("UPDATE race_plan_approvals SET status='approved',decided_by=?,decided_at=?,comment=? WHERE plan_id=? AND role=?", (context.actor_id, now, comment.strip(), plan_id, role))
                remaining = connection.execute("SELECT COUNT(*) AS n FROM race_plan_approvals WHERE plan_id=? AND status='waiting'", (plan_id,)).fetchone()["n"]
                if remaining == 0:
                    new_status = "ready"
                elif role == "medical":
                    new_status = "medical_approved"
                else:
                    new_status = "proposed"
                connection.execute("UPDATE race_plans SET status=?,version=version+1,decided_at=? WHERE plan_id=?", (new_status, now, plan_id))
                self.audit.append(connection, actor_id=context.actor_id, action="race-plan-approve", entity_type="race_plans", entity_id=plan_id, version=plan["version"] + 1, detail={"role": role, "status": new_status})
                return self._plan_view(connection, plan_id)
            key = request_key or f"{context.actor_id}:{role}:{approved}"
            return self.idempotency.execute(connection, scope=f"race-plan-approve:{plan_id}:{role}", request_key=key, request={"approved": approved, "comment": comment}, operation=operation)

    def apply_plan(self, context: AccessContext, *, plan_id: str, request_key: str) -> dict:
        context.require("apply:race-plans")
        # 未集齐双确认属于调用时机不对，直接拒绝，不作废方案；已生效的请求靠幂等键重放。
        prior = self.get_plan(plan_id)
        if prior["status"] not in ("ready", "applied"):
            raise ConflictError(f"方案状态为 {prior['status']}，医疗与交通负责人均确认后才能生效")
        try:
            with self.database.transaction() as connection:
                def operation() -> dict:
                    plan = self._require_plan(connection, plan_id)
                    if plan["status"] != "ready":
                        raise ConflictError(f"方案状态为 {plan['status']}，医疗与交通负责人均确认后才能生效")
                    items = connection.execute("SELECT * FROM race_plan_items WHERE plan_id=? ORDER BY seq", (plan_id,)).fetchall()
                    pairs = []
                    for item in items:
                        runner = self._require_runner(connection, item["runner_id"])
                        # 阻止旧方案落库：基准版本、分区、冻结标志、状态任一被现场回传改变即整体失败。
                        if (runner["version"], runner["zone"], runner["frozen"], runner["status"]) != (item["base_version"], item["base_zone"], item["base_frozen"], item["base_status"]):
                            raise ConflictError(f"选手 {runner['runner_id']} 状态已变化，旧方案不能落库")
                        target = self._require_zone(connection, item["to_zone"])
                        pairs.append((runner, target))
                    # 道路容量/医疗点容量可能在方案等待期间被压缩或关闭，按最新容量重新校验。
                    self._verify_holds_against_current_capacity(connection, pairs, plan_id)
                    now = self.clock.now()
                    for runner, target in pairs:
                        self._move_support(connection, runner, target, plan_id, now, context.actor_id)
                        self._update_runner(connection, runner, status=runner["status"], frozen=runner["frozen"], zone=target["zone_id"], wave=target["wave"], reason=f"重排方案生效：{plan['reason']}", actor=context.actor_id, plan_id=plan_id)
                    # 起跑区预占转为已消耗记录；选手行本身即为分区占用事实。
                    connection.execute("UPDATE race_allocations SET status='consumed',version=version+1,updated_at=? WHERE plan_id=? AND kind='zone_capacity' AND status='held'", (now, plan_id))
                    connection.execute("UPDATE race_plans SET status='applied',applied_at=?,applied_by=?,version=version+1 WHERE plan_id=?", (now, context.actor_id, plan_id))
                    self.audit.append(connection, actor_id=context.actor_id, action="race-plan-apply", entity_type="race_plans", entity_id=plan_id, version=plan["version"] + 1, detail={"runners": len(items)})
                    return self._plan_view(connection, plan_id)
                return self.idempotency.execute(connection, scope="race-plan-apply", request_key=request_key, request={"plan_id": plan_id}, operation=operation)
        except ConflictError:
            # 失败事务已回滚（选手与保障均未改动）；在独立事务中作废方案并释放预占，
            # 不让旧方案挂在确认队列里，也不留选手已换区、任务指旧区的半状态。
            self.abort_plan(plan_id, actor=context.actor_id, comment="生效校验失败：并发现场变化或容量不足，方案整体作废")
            raise

    def abort_plan(self, plan_id: str, *, actor: str, comment: str) -> None:
        """在独立事务中作废待决方案并释放全部预占。"""
        with self.database.transaction() as connection:
            plan = connection.execute("SELECT * FROM race_plans WHERE plan_id=?", (plan_id,)).fetchone()
            if plan and plan["status"] in ACTIVE_PLAN_STATUSES:
                self._reject_plan(connection, plan, actor=actor, comment=comment)

    def get_plan(self, plan_id: str) -> dict:
        with self.database.connect() as connection:
            return self._plan_view(connection, plan_id)

    def confirmation_queue(self) -> list[dict]:
        """发枪前指挥部可继续推进的确认队列：待确认/已就绪待生效的方案。"""
        with self.database.connect() as connection:
            rows = connection.execute("SELECT * FROM race_plans WHERE status IN ('proposed','medical_approved','ready') ORDER BY created_at,plan_id").fetchall()
            return [self._plan_to_dict(row, connection) for row in rows]

    # ---- 方案的资源锁定与迁移 ---------------------------------------------

    def _hold_requirements(self, connection, pairs: list, plan_id: str, now: str) -> None:
        # 1) 起跑区容量：模拟方案生效后的占用 = 现占用 - 本方案移出 + 本方案移入 + 其他待决方案预占。
        inbound: dict[str, int] = {}; outbound: dict[str, int] = {}
        for runner, target in pairs:
            inbound[target["zone_id"]] = inbound.get(target["zone_id"], 0) + 1
            outbound[runner["zone"]] = outbound.get(runner["zone"], 0) + 1
        for zone_id, incoming in inbound.items():
            zone = self._require_zone(connection, zone_id)
            if zone["status"] == "closed":
                raise ConflictError(f"目标分区 {zone_id} 已关闭")
            current = connection.execute("SELECT COUNT(*) AS n FROM race_runners WHERE zone=? AND status!='withdrawn'", (zone_id,)).fetchone()["n"]
            other_held = connection.execute("SELECT COALESCE(SUM(quantity),0) AS n FROM race_allocations WHERE resource_id=? AND status='held'", (self._zone_resource_id(zone_id),)).fetchone()["n"]
            required = int(current) - outbound.get(zone_id, 0) + incoming + int(other_held)
            if required > zone["capacity"]:
                raise ConflictError(f"{RESOURCE_LABELS['zone_capacity']}不足：分区 {zone_id} 容量 {zone['capacity']}，方案后需 {required}")
        # 2) 医疗/补给/接驳：逐选手锁定目标分区对应资源；选手已占同一资源的不重复锁定。
        demand: dict[str, int] = {}; resolved: list[tuple] = []
        for runner, target in pairs:
            for kind in SUPPORT_KINDS:
                resource = self._map_resource(connection, kind, target)
                if resource is None:
                    raise ConflictError(f"分区 {target['zone_id']} 缺少{RESOURCE_LABELS[kind]}资源")
                existing = connection.execute("SELECT resource_id FROM race_allocations WHERE runner_id=? AND kind=? AND status IN ('held','confirmed','consumed') LIMIT 1", (runner["runner_id"], kind)).fetchone()
                if existing and existing["resource_id"] == resource["resource_id"]:
                    resolved.append((runner, target, kind, resource, False))
                    continue
                demand[resource["resource_id"]] = demand.get(resource["resource_id"], 0) + 1
                resolved.append((runner, target, kind, resource, True))
        for resource_id, extra in demand.items():
            used = connection.execute("SELECT COALESCE(SUM(quantity),0) AS n FROM race_allocations WHERE resource_id=? AND status IN ('held','confirmed','consumed')", (resource_id,)).fetchone()["n"]
            capacity = connection.execute("SELECT capacity FROM race_resources WHERE resource_id=?", (resource_id,)).fetchone()["capacity"]
            if int(used) + extra > int(capacity):
                kind = connection.execute("SELECT kind FROM race_resources WHERE resource_id=?", (resource_id,)).fetchone()["kind"]
                raise ConflictError(f"{RESOURCE_LABELS[kind]}不足：资源 {resource_id} 容量 {capacity}，还需 {extra}")
        # 全部通过后才写预占。
        for zone_id, incoming in inbound.items():
            connection.execute(
                "INSERT INTO race_allocations(allocation_id,kind,resource_id,runner_id,zone_id,quantity,status,plan_id,version,updated_at) VALUES(?,?,?,?,?,?,'held',?,1,?)",
                (new_id("alloc"), "zone_capacity", self._zone_resource_id(zone_id), f"plan:{plan_id}:{zone_id}", zone_id, incoming, plan_id, now),
            )
        for runner, target, kind, resource, needs_hold in resolved:
            if not needs_hold:
                continue
            connection.execute(
                "INSERT INTO race_allocations(allocation_id,kind,resource_id,runner_id,zone_id,quantity,status,plan_id,version,updated_at) VALUES(?,?,?,?,?,1,'held',?,1,?)",
                (new_id("alloc"), kind, resource["resource_id"], runner["runner_id"], target["zone_id"], plan_id, now),
            )

    def _verify_holds_against_current_capacity(self, connection, pairs: list, plan_id: str) -> None:
        inbound: dict[str, int] = {}; outbound: dict[str, int] = {}
        for runner, target in pairs:
            inbound[target["zone_id"]] = inbound.get(target["zone_id"], 0) + 1
            outbound[runner["zone"]] = outbound.get(runner["zone"], 0) + 1
        for zone_id, incoming in inbound.items():
            zone = self._require_zone(connection, zone_id)
            if zone["status"] in ("closed", "fired"):
                raise ConflictError(f"目标分区 {zone_id} 已关闭或发枪，方案不能落库")
            current = connection.execute("SELECT COUNT(*) AS n FROM race_runners WHERE zone=? AND status!='withdrawn'", (zone_id,)).fetchone()["n"]
            other_held = connection.execute("SELECT COALESCE(SUM(quantity),0) AS n FROM race_allocations WHERE resource_id=? AND status='held' AND plan_id!=?", (self._zone_resource_id(zone_id), plan_id)).fetchone()["n"]
            required = int(current) - outbound.get(zone_id, 0) + incoming + int(other_held)
            if required > zone["capacity"]:
                raise ConflictError(f"{RESOURCE_LABELS['zone_capacity']}在等待期间变化：分区 {zone_id} 容量 {zone['capacity']}，方案后需 {required}")
        for runner, target in pairs:
            for kind in SUPPORT_KINDS:
                held = connection.execute("SELECT resource_id FROM race_allocations WHERE plan_id=? AND runner_id=? AND kind=? AND status='held'", (plan_id, runner["runner_id"], kind)).fetchone()
                if not held:
                    continue  # 提出时已由既有同资源分配覆盖。
                used = connection.execute("SELECT COALESCE(SUM(quantity),0) AS n FROM race_allocations WHERE resource_id=? AND status IN ('held','confirmed','consumed')", (held["resource_id"],)).fetchone()["n"]
                capacity = connection.execute("SELECT capacity FROM race_resources WHERE resource_id=?", (held["resource_id"],)).fetchone()["capacity"]
                if int(used) > int(capacity):
                    raise ConflictError(f"{RESOURCE_LABELS[kind]}在等待期间变化：资源 {held['resource_id']} 容量 {capacity}")

    def _move_support(self, connection, runner, target_zone, plan_id: str, now: str, actor: str) -> None:
        for kind in SUPPORT_KINDS:
            held = connection.execute("SELECT * FROM race_allocations WHERE plan_id=? AND runner_id=? AND kind=? AND status='held'", (plan_id, runner["runner_id"], kind)).fetchone()
            existing = connection.execute("SELECT * FROM race_allocations WHERE runner_id=? AND kind=? AND status IN ('confirmed','consumed') AND plan_id IS NULL ORDER BY CASE status WHEN 'confirmed' THEN 0 ELSE 1 END,version DESC LIMIT 1", (runner["runner_id"], kind)).fetchone()
            if held and existing and existing["status"] == "confirmed":
                # 既有未消耗保障随选手迁移到预占资源，预占行随后释放，容量净额不变。
                if existing["resource_id"] != held["resource_id"] or existing["zone_id"] != target_zone["zone_id"]:
                    connection.execute("UPDATE race_allocations SET resource_id=?,zone_id=?,version=version+1,updated_at=? WHERE allocation_id=?", (held["resource_id"], target_zone["zone_id"], now, existing["allocation_id"]))
                    self._journal(connection, actor=actor, allocation_id=existing["allocation_id"], event="move", kind=kind, from_resource=existing["resource_id"], to_resource=held["resource_id"], from_zone=runner["zone"], to_zone=target_zone["zone_id"], plan_id=plan_id)
                connection.execute("UPDATE race_allocations SET status='released',version=version+1,updated_at=? WHERE allocation_id=?", (now, held["allocation_id"]))
                self._journal(connection, actor=actor, allocation_id=held["allocation_id"], event="release", kind=kind, from_resource=held["resource_id"], to_resource=None, from_zone=target_zone["zone_id"], to_zone=None, plan_id=plan_id, detail={"reason": "预占由既有保障迁移承接"})
            elif held:
                # 无既有分配（或已有消耗记录，历史保留），预占转为选手在新区的正式保障。
                connection.execute("UPDATE race_allocations SET status='confirmed',zone_id=?,version=version+1,updated_at=? WHERE allocation_id=?", (target_zone["zone_id"], now, held["allocation_id"]))
                self._journal(connection, actor=actor, allocation_id=held["allocation_id"], event="move", kind=kind, from_resource=held["resource_id"], to_resource=held["resource_id"], from_zone=runner["zone"], to_zone=target_zone["zone_id"], plan_id=plan_id)
            else:
                # 提出时确认选手已绑定目标资源：只校正分区归属。
                if existing and existing["zone_id"] != target_zone["zone_id"] and existing["status"] == "confirmed":
                    connection.execute("UPDATE race_allocations SET zone_id=?,version=version+1,updated_at=? WHERE allocation_id=?", (target_zone["zone_id"], now, existing["allocation_id"]))
                    self._journal(connection, actor=actor, allocation_id=existing["allocation_id"], event="move", kind=kind, from_resource=existing["resource_id"], to_resource=existing["resource_id"], from_zone=runner["zone"], to_zone=target_zone["zone_id"], plan_id=plan_id)

    def _reject_plan(self, connection, plan, *, actor: str, comment: str) -> None:
        now = self.clock.now()
        connection.execute("UPDATE race_plans SET status='rejected',decided_at=COALESCE(decided_at,?),version=version+1 WHERE plan_id=?", (now, plan["plan_id"]))
        held_rows = connection.execute("SELECT * FROM race_allocations WHERE plan_id=? AND status='held'", (plan["plan_id"],)).fetchall()
        for alloc in held_rows:
            connection.execute("UPDATE race_allocations SET status='released',version=version+1,updated_at=? WHERE allocation_id=?", (now, alloc["allocation_id"]))
            self._journal(connection, actor=actor, allocation_id=alloc["allocation_id"], event="release", kind=alloc["kind"], from_resource=alloc["resource_id"], to_resource=None, from_zone=alloc["zone_id"], to_zone=None, plan_id=plan["plan_id"], detail={"reason": comment})
        self.audit.append(connection, actor_id=actor, action="race-plan-reject", entity_type="race_plans", entity_id=plan["plan_id"], version=plan["version"] + 1, detail={"reason": comment})

    def _reject_active_plans_for_runner(self, connection, runner_id: str, *, actor: str, comment: str) -> None:
        rows = connection.execute(
            "SELECT p.* FROM race_plans p JOIN race_plan_items pi ON pi.plan_id=p.plan_id "
            "WHERE pi.runner_id=? AND p.status IN (" + ",".join("?" * len(ACTIVE_PLAN_STATUSES)) + ") ORDER BY p.created_at",
            (runner_id, *ACTIVE_PLAN_STATUSES),
        ).fetchall()
        for plan in rows:
            self._reject_plan(connection, plan, actor=actor, comment=comment)

    # ---- 赛后复盘：定时点核对 ---------------------------------------------

    def review_at(self, *, at: str) -> dict:
        """赛后复盘：在任意定时点核对分区、保障分配与每次调整的原因。"""
        instant = canonical_instant(at)
        with self.database.connect() as connection:
            runners = []
            for row in connection.execute("SELECT * FROM race_runners ORDER BY bib,runner_id"):
                version_row = connection.execute(
                    "SELECT * FROM race_runner_history WHERE runner_id=? AND valid_from<=? ORDER BY valid_from DESC,version DESC LIMIT 1",
                    (row["runner_id"], instant),
                ).fetchone()
                if not version_row:
                    continue
                runners.append({
                    "runner_id": row["runner_id"],
                    "bib": row["bib"],
                    "original_zone": row["original_zone"],
                    "zone": version_row["zone"],
                    "wave": version_row["wave"],
                    "status": version_row["status"],
                    "frozen": bool(version_row["frozen"]),
                    "rule_version": version_row["rule_version"],
                    "allocations": self._allocations_as_of(connection, row["runner_id"], instant),
                })
            changes = [
                {"at": h["valid_from"], "runner_id": h["runner_id"], "version": h["version"], "zone": h["zone"], "wave": h["wave"], "status": h["status"], "reason": h["reason"], "plan_id": h["plan_id"], "actor_id": h["actor_id"]}
                for h in connection.execute("SELECT * FROM race_runner_history WHERE valid_from<=? AND version>1 ORDER BY valid_from,runner_id,version", (instant,))
            ]
            plans = [
                {"plan_id": p["plan_id"], "status": p["status"], "reason": p["reason"], "proposed_by": p["proposed_by"], "created_at": p["created_at"], "applied_at": p["applied_at"]}
                for p in connection.execute("SELECT * FROM race_plans WHERE created_at<=? ORDER BY created_at", (instant,))
            ]
            return {"as_of": instant, "zones": [dict(z) for z in connection.execute("SELECT * FROM race_zones ORDER BY wave,ordinal")], "runners": runners, "changes": changes, "plans": plans}

    def _allocations_as_of(self, connection, runner_id: str, instant: str) -> list[dict]:
        result = []
        for alloc in connection.execute("SELECT * FROM race_allocations WHERE runner_id=? AND kind!=? AND runner_id NOT LIKE 'plan:%'", (runner_id, "zone_capacity")):
            first = connection.execute("SELECT MIN(at) AS at FROM race_allocation_journal WHERE allocation_id=?", (alloc["allocation_id"],)).fetchone()["at"]
            if not first or first > instant:
                continue
            released = connection.execute("SELECT 1 FROM race_allocation_journal WHERE allocation_id=? AND event='release' AND at<=? LIMIT 1", (alloc["allocation_id"], instant)).fetchone()
            consumed = connection.execute("SELECT 1 FROM race_allocation_journal WHERE allocation_id=? AND event='consume' AND at<=? LIMIT 1", (alloc["allocation_id"], instant)).fetchone()
            last_move = connection.execute("SELECT * FROM race_allocation_journal WHERE allocation_id=? AND event IN ('assign','move') AND at<=? ORDER BY at DESC,journal_id DESC LIMIT 1", (alloc["allocation_id"], instant)).fetchone()
            status = "released" if released else ("consumed" if consumed else "active")
            result.append({
                "allocation_id": alloc["allocation_id"],
                "kind": alloc["kind"],
                "resource_id": last_move["to_resource_id"] if last_move and last_move["to_resource_id"] else alloc["resource_id"],
                "zone_id": last_move["to_zone"] if last_move and last_move["to_zone"] else alloc["zone_id"],
                "status_at": status,
                "plan_id": alloc["plan_id"],
            })
        return result

    # ---- 内部工具 --------------------------------------------------------

    @staticmethod
    def _zone_resource_id(zone_id: str) -> str:
        return f"zone:{zone_id}"

    def _clean_moves(self, moves: list[dict]) -> list[dict]:
        if not isinstance(moves, list) or not moves:
            raise ValidationError("调整列表不能为空")
        cleaned = []
        seen = set()
        for move in moves:
            if not isinstance(move, dict):
                raise ValidationError("调整项必须是对象")
            runner_id = require_safe(str(move.get("runner_id", "")), "选手")
            to_zone = require_safe(str(move.get("to_zone", "")), "目标分区")
            if runner_id in seen:
                raise ValidationError(f"选手 {runner_id} 在同一方案中出现多次")
            seen.add(runner_id)
            cleaned.append({"runner_id": runner_id, "to_zone": to_zone})
        return cleaned

    def _zone_occupancy_after_active_plans(self, connection, zone_id: str) -> int:
        current = connection.execute("SELECT COUNT(*) AS n FROM race_runners WHERE zone=? AND status!='withdrawn'", (zone_id,)).fetchone()["n"]
        held = connection.execute("SELECT COALESCE(SUM(quantity),0) AS n FROM race_allocations WHERE resource_id=? AND status='held'", (self._zone_resource_id(zone_id),)).fetchone()["n"]
        return int(current) + int(held)

    def _map_resource(self, connection, kind: str, zone) -> dict | None:
        row = connection.execute("SELECT * FROM race_resources WHERE kind=? AND scope_value=? ORDER BY resource_id LIMIT 1", (kind, zone["zone_id"])).fetchone()
        if row:
            return dict(row)
        wave_row = connection.execute("SELECT * FROM race_resources WHERE kind=? AND scope_value=? ORDER BY resource_id LIMIT 1", (kind, zone["wave"])).fetchone()
        return dict(wave_row) if wave_row else None

    def _take_capacity(self, connection, resource: dict, quantity: int) -> None:
        used = connection.execute("SELECT COALESCE(SUM(quantity),0) AS n FROM race_allocations WHERE resource_id=? AND status IN ('held','confirmed','consumed')", (resource["resource_id"],)).fetchone()["n"]
        if int(used) + quantity > resource["capacity"]:
            raise ConflictError(f"{RESOURCE_LABELS[resource['kind']]}不足：资源 {resource['resource_id']} 容量 {resource['capacity']}")

    @staticmethod
    def _assert_within_rule(rule: dict, runner, target) -> None:
        wave_order = rule["wave_order"]
        if runner["wave"] not in wave_order or target["wave"] not in wave_order:
            raise ValidationError("枪次不在规则版本的枪次序列中")
        if abs(wave_order.index(target["wave"]) - wave_order.index(runner["wave"])) > rule["max_wave_delta"]:
            raise ConflictError(f"目标枪次 {target['wave']} 超出规则 {rule['rule_version']} 允许的 {rule['max_wave_delta']} 枪")

    def _base_digest(self, runners) -> str:
        basis = [{"runner_id": r["runner_id"], "version": r["version"], "zone": r["zone"], "frozen": r["frozen"], "status": r["status"]} for r in sorted(runners, key=lambda r: r["runner_id"])]
        return digest_json(basis)

    def _update_runner(self, connection, runner, *, status: str, frozen: int, zone: str, wave: str, reason: str, actor: str, plan_id: str | None) -> None:
        version = runner["version"] + 1
        now = self.clock.now()
        changed = connection.execute(
            "UPDATE race_runners SET zone=?,wave=?,status=?,frozen=?,version=?,updated_at=?,updated_by=? WHERE runner_id=? AND version=?",
            (zone, wave, status, int(frozen), version, now, actor, runner["runner_id"], runner["version"]),
        ).rowcount
        if changed != 1:
            raise ConflictError("选手并发修改导致版本变化")
        connection.execute(
            "INSERT INTO race_runner_history(runner_id,version,zone,wave,status,frozen,reason,rule_version,plan_id,valid_from,actor_id) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (runner["runner_id"], version, zone, wave, status, int(frozen), reason, runner["rule_version"], plan_id, now, actor),
        )

    def _journal(self, connection, *, actor: str, allocation_id: str, event: str, kind: str, from_resource: str | None, to_resource: str | None, from_zone: str | None, to_zone: str | None, plan_id: str | None, detail: dict | None = None) -> None:
        connection.execute(
            "INSERT INTO race_allocation_journal(at,actor_id,allocation_id,event,kind,from_resource_id,to_resource_id,from_zone,to_zone,quantity,plan_id,detail_json) VALUES(?,?,?,?,?,?,?,?,?,1,?,?)",
            (self.clock.now(), actor, allocation_id, event, kind, from_resource, to_resource, from_zone, to_zone, plan_id, canonical_json(detail or {})),
        )

    @staticmethod
    def _require_zone(connection, zone_id: str) -> dict:
        row = connection.execute("SELECT * FROM race_zones WHERE zone_id=?", (zone_id,)).fetchone()
        if not row:
            raise NotFoundError(f"分区 {zone_id} 不存在")
        return dict(row)

    @staticmethod
    def _require_runner(connection, runner_id: str):
        row = connection.execute("SELECT * FROM race_runners WHERE runner_id=?", (runner_id,)).fetchone()
        if not row:
            raise NotFoundError(f"选手 {runner_id} 不存在")
        return row

    @staticmethod
    def _require_resource(connection, resource_id: str, kind: str) -> dict:
        row = connection.execute("SELECT * FROM race_resources WHERE resource_id=? AND kind=?", (resource_id, kind)).fetchone()
        if not row:
            raise NotFoundError(f"{RESOURCE_LABELS[kind]}资源 {resource_id} 不存在")
        return dict(row)

    @staticmethod
    def _require_plan(connection, plan_id: str):
        row = connection.execute("SELECT * FROM race_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if not row:
            raise NotFoundError(f"重排方案 {plan_id} 不存在")
        return row

    def _rule_at(self, connection, instant: str) -> dict:
        row = connection.execute("SELECT * FROM race_rules WHERE effective_from<=? ORDER BY effective_from DESC,rule_version DESC LIMIT 1", (instant,)).fetchone()
        if not row:
            raise NotFoundError("当前时点没有生效的规则版本")
        return self._rule_to_dict(row)

    def _runner_to_dict(self, row, connection=None) -> dict:
        result = {
            "runner_id": row["runner_id"],
            "bib": row["bib"],
            "qualification": json.loads(row["qualification_json"]),
            "original_zone": row["original_zone"],
            "zone": row["zone"],
            "wave": row["wave"],
            "status": row["status"],
            "frozen": bool(row["frozen"]),
            "rule_version": row["rule_version"],
            "version": row["version"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        if connection is not None:
            result["allocations"] = [
                {"allocation_id": a["allocation_id"], "kind": a["kind"], "resource_id": a["resource_id"], "zone_id": a["zone_id"], "status": a["status"], "plan_id": a["plan_id"]}
                for a in connection.execute("SELECT * FROM race_allocations WHERE runner_id=? AND status IN ('held','confirmed','consumed') AND kind!=? ORDER BY kind", (row["runner_id"], "zone_capacity"))
            ]
        return result

    def _plan_view(self, connection, plan_id: str) -> dict:
        return self._plan_to_dict(self._require_plan(connection, plan_id), connection)

    def _plan_to_dict(self, row, connection) -> dict:
        items = [dict(item) for item in connection.execute("SELECT runner_id,action,from_zone,to_zone,base_version FROM race_plan_items WHERE plan_id=? ORDER BY seq", (row["plan_id"],))]
        approvals = {
            a["role"]: {"status": a["status"], "decided_by": a["decided_by"], "decided_at": a["decided_at"], "comment": a["comment"]}
            for a in connection.execute("SELECT * FROM race_plan_approvals WHERE plan_id=?", (row["plan_id"],))
        }
        held = connection.execute("SELECT kind,COUNT(*) AS n FROM race_allocations WHERE plan_id=? AND status='held' GROUP BY kind", (row["plan_id"],)).fetchall()
        return {
            "plan_id": row["plan_id"],
            "status": row["status"],
            "reason": row["reason"],
            "proposed_by": row["proposed_by"],
            "rule_versions": json.loads(row["rule_versions_json"]),
            "created_at": row["created_at"],
            "decided_at": row["decided_at"],
            "applied_at": row["applied_at"],
            "applied_by": row["applied_by"],
            "items": items,
            "approvals": approvals,
            "held_resources": {h["kind"]: h["n"] for h in held},
        }
