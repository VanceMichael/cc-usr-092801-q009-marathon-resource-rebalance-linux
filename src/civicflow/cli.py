"""命令行入口。"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .application import CivicFlow
from .cases import CaseService
from .security import AccessContext


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2))


def demo(app: CivicFlow) -> dict:
    context = AccessContext.system("demo-operator")
    cases = CaseService(app.repository)
    created = cases.create(context, {"case_type": "协同事项", "subject": "示例联合处置", "owner_org": "org:demo", "priority": "high", "opened_at": app.clock.now()}, request_key="demo-case")
    accepted = app.inbox.receive(source="demo", source_key=created["entity_id"], sequence=1, payload={"kind": "opened"}, occurred_at=app.clock.now())
    reservation = app.reservations.reserve(resource_id="room:joint", subject_id=created["entity_id"], quantity=2, capacity=10, start_at="2026-09-28T10:00:00+08:00", end_at="2026-09-28T11:00:00+08:00", actor=context.actor_id)
    debit = app.ledger.post(journal_key="demo", account="coordination", currency="CNY", amount="12.34", direction="debit", reference="demo-debit", actor=context.actor_id)
    credit = app.ledger.post(journal_key="demo", account="coordination", currency="CNY", amount="12.34", direction="credit", reference="demo-credit", actor=context.actor_id)
    message = app.outbox.enqueue(topic="case.opened", aggregate_id=created["entity_id"], payload={"case_id": created["entity_id"]})
    return {"case": created, "inbox": accepted, "reservation": reservation, "entries": [debit, credit], "balance": app.ledger.balance("demo", currency="CNY"), "message_id": message, "verification": app.verify()}


def _setup_race_demo(app: CivicFlow) -> dict:
    """构造北马三枪分区场景：4.2 万人取少量样本演示完整编排链路。"""
    operator = AccessContext.system("ops-01")
    app.race.put_rule(operator, {"rule_version": "rules-2026", "wave_order": ["A", "B", "C"], "max_wave_delta": 1, "max_zone_delta": 1, "effective_from": "2026-01-01T00:00:00+08:00", "detail": {"name": "2026 北京马拉松三枪规则"}}, request_key="rule-1")
    zones = {}
    for wave, ordinals in (("A", (1, 2)), ("B", (1, 2)), ("C", (1, 2))):
        for ordinal in ordinals:
            zone_id = f"zone-{wave}{ordinal}"
            app.race.configure_zone(operator, zone_id=zone_id, wave=wave, ordinal=ordinal, capacity=3, request_key=f"zone-{zone_id}")
            zones[zone_id] = zone_id
    for zone_id in zones:
        for kind, prefix in (("medical", "med"), ("supply", "sup"), ("shuttle", "bus")):
            app.race.register_resource(operator, resource_id=f"{prefix}-{zone_id}", kind=kind, scope_value=zone_id, capacity=4, request_key=f"res-{prefix}-{zone_id}")
    runners = []
    for i, zone_id in enumerate(("zone-A1", "zone-A2", "zone-B1", "zone-B2", "zone-C1")):
        runner_id = f"runner-{i+1:03d}"
        app.race.import_runner(operator, runner_id=runner_id, bib=str(1000 + i), qualification={"seed": i % 2, "certified": True}, zone_id=zone_id, request_key=f"runner-{runner_id}")
        runners.append(runner_id)
    return {"zones": list(zones), "runners": runners}


def race_demo(app: CivicFlow) -> dict:
    setup = _setup_race_demo(app)
    operator = AccessContext.system("ops-01")
    medical_lead = AccessContext.system("medical-lead")
    traffic_lead = AccessContext.system("traffic-lead")
    # runner-003 从 B1 调整到 A2，规则允许跨一枪且分区相邻；四类资源同时锁定。
    plan = app.race.propose_plan(operator, moves=[{"runner_id": "runner-003", "to_zone": "zone-A2"}], reason="医疗点 B1 压力上升，前移一名选手至 A2", request_key="plan-1")
    app.race.approve_plan(medical_lead, plan_id=plan["plan_id"], role="medical", approved=True, comment="医疗覆盖已核对", request_key="med-ok")
    app.race.approve_plan(traffic_lead, plan_id=plan["plan_id"], role="traffic", approved=True, comment="接驳班次已核对", request_key="traffic-ok")
    applied = app.race.apply_plan(operator, plan_id=plan["plan_id"], request_key="apply-1")
    queue = app.race.confirmation_queue()
    return {"setup": setup, "plan": applied, "confirmation_queue": queue, "review_before_gun": app.race.review_at(at=app.clock.now())}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="协同事务平台")
    parser.add_argument("--db", default=os.getenv("CIVICFLOW_DB", "civicflow.sqlite3"))
    parser.add_argument("--now", default=None, help="测试或演示使用的固定时间")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("demo")
    commands.add_parser("verify")
    commands.add_parser("list-cases")
    commands.add_parser("race-demo", help="构造三枪分区编排演示并输出发枪前确认队列")
    commands.add_parser("race-queue", help="查看可继续推进的重排确认队列")
    review = commands.add_parser("race-review", help="按定时点复盘分区、保障与调整原因")
    review.add_argument("--at", default=None, help="复盘时点（ISO 8601），默认当前时间")
    args = parser.parse_args(argv)
    app = CivicFlow.open(Path(args.db), fixed_now=args.now)
    if args.command == "demo": emit(demo(app))
    elif args.command == "verify": emit(app.verify())
    elif args.command == "list-cases": emit(CaseService(app.repository).list_current(AccessContext.system("cli")))
    elif args.command == "race-demo": emit(race_demo(app))
    elif args.command == "race-queue": emit(app.race.confirmation_queue())
    elif args.command == "race-review": emit(app.race.review_at(at=args.at or app.clock.now()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
