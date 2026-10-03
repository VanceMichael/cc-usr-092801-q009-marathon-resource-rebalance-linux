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


def marathon_demo(app: CivicFlow) -> dict:
    marathon = app.marathon
    operations = AccessContext(actor_id="ops-lead", permissions=frozenset({"propose:marathon", "apply:marathon", "read:marathon"}))
    medical_lead = AccessContext(actor_id="medical-lead", permissions=frozenset({"confirm:marathon.medical", "read:marathon"}))
    transport_lead = AccessContext(actor_id="transport-lead", permissions=frozenset({"confirm:marathon.transport", "read:marathon"}))
    context = AccessContext.system("marathon-admin")
    for zone_id, wave_no in (("A", 1), ("B", 2), ("C", 3)):
        marathon.create_zone(context, zone_id, wave_no=wave_no, capacities={"start_slot": 14000, "medical": 200, "supply": 14000, "shuttle": 300}, request_key=f"demo-zone-{zone_id}")
    rules = marathon.activate_rules(context, {"elite": {"zones": ["A"]}, "standard": {"zones": ["A", "B", "C"]}, "charity": {"zones": ["C"]}}, request_key="demo-rules-1")
    marathon.register_runner(context, "B0001", name="示例选手一", qualification={"class": "standard", "best": "3:29:58"}, zone_id="A", request_key="demo-runner-1")
    marathon.register_runner(context, "B0002", name="示例选手二", qualification={"class": "standard", "best": "3:41:10"}, zone_id="A", request_key="demo-runner-2")
    adjustable = marathon.adjustable_range(context, "B0001")
    plan = marathon.propose_plan(operations, [{"runner_id": "B0001", "to_zone": "B", "reason": "一号道临时管制"}], reason="道路容量变化，均衡一枪密度", request_key="demo-plan-1")
    marathon.confirm_plan(medical_lead, plan["plan_id"], role="medical", request_key="demo-confirm-medical")
    marathon.confirm_plan(transport_lead, plan["plan_id"], role="transport", request_key="demo-confirm-transport")
    queue = marathon.confirmation_queue(context)
    applied = marathon.apply_plan(operations, plan["plan_id"])
    receipt = marathon.ingest_callback(context, source="checkin", source_key="gate-01", sequence=1, payload={"kind": "check_in", "runner_id": "B0002"}, occurred_at=app.clock.now())
    review = marathon.review_at(context, as_of=app.clock.now())
    return {"rules": rules, "adjustable": adjustable, "plan": applied, "queue_size": len(queue), "receipt": receipt, "review": review, "verification": app.verify()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="协同事务平台")
    parser.add_argument("--db", default=os.getenv("CIVICFLOW_DB", "civicflow.sqlite3"))
    parser.add_argument("--now", default=None, help="测试或演示使用的固定时间")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("demo")
    commands.add_parser("verify")
    commands.add_parser("list-cases")
    commands.add_parser("marathon-demo")
    commands.add_parser("marathon-queue")
    args = parser.parse_args(argv)
    app = CivicFlow.open(Path(args.db), fixed_now=args.now)
    if args.command == "demo": emit(demo(app))
    elif args.command == "verify": emit(app.verify())
    elif args.command == "list-cases": emit(CaseService(app.repository).list_current(AccessContext.system("cli")))
    elif args.command == "marathon-demo": emit(marathon_demo(app))
    elif args.command == "marathon-queue": emit(app.marathon.confirmation_queue(AccessContext.system("cli")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
