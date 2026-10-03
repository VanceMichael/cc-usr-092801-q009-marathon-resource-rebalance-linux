"""SQLite 连接、事务和数据库初始化。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = r"""
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS entities (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id)
);
CREATE TABLE IF NOT EXISTS entity_versions (
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_key TEXT NOT NULL,
    PRIMARY KEY(entity_type, entity_id, version)
);
CREATE INDEX IF NOT EXISTS entity_versions_asof ON entity_versions(entity_type, entity_id, valid_from, version);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    scope TEXT NOT NULL,
    request_key TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, request_key)
);
CREATE TABLE IF NOT EXISTS audit_entries (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    previous_digest TEXT NOT NULL,
    entry_digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS inbox_messages (
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    payload_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY(source, source_key, sequence)
);
CREATE TABLE IF NOT EXISTS inbox_conflicts (
    conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_key TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    existing_digest TEXT NOT NULL,
    incoming_digest TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox_messages (
    message_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    available_at TEXT NOT NULL,
    lease_until TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    delivered_at TEXT
);
CREATE INDEX IF NOT EXISTS outbox_ready ON outbox_messages(status, available_at, lease_until);
CREATE TABLE IF NOT EXISTS journal_entries (
    entry_id TEXT PRIMARY KEY,
    journal_key TEXT NOT NULL,
    account TEXT NOT NULL,
    currency TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    direction TEXT NOT NULL,
    reference TEXT NOT NULL,
    reversed_entry_id TEXT,
    occurred_at TEXT NOT NULL,
    posted_by TEXT NOT NULL,
    FOREIGN KEY(reversed_entry_id) REFERENCES journal_entries(entry_id)
);
CREATE INDEX IF NOT EXISTS journal_reference ON journal_entries(journal_key, reference, occurred_at);
CREATE TABLE IF NOT EXISTS resource_reservations (
    reservation_id TEXT PRIMARY KEY,
    resource_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    status TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_by TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reservation_window ON resource_reservations(resource_id, start_at, end_at, status);
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    run_at TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    lease_until TEXT,
    last_error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS jobs_due ON scheduled_jobs(status, run_at, lease_until);

-- 赛事编排：规则版本决定可调整范围，所有时间列均为带时区的规范时刻。
CREATE TABLE IF NOT EXISTS race_rules (
    rule_version TEXT PRIMARY KEY,
    wave_order_json TEXT NOT NULL,
    max_wave_delta INTEGER NOT NULL,
    max_zone_delta INTEGER NOT NULL,
    effective_from TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS race_zones (
    zone_id TEXT PRIMARY KEY,
    wave TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    capacity INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    fired_at TEXT
);
CREATE INDEX IF NOT EXISTS race_zones_wave ON race_zones(wave, ordinal);
CREATE TABLE IF NOT EXISTS race_runners (
    runner_id TEXT PRIMARY KEY,
    bib TEXT NOT NULL,
    qualification_json TEXT NOT NULL,
    original_zone TEXT NOT NULL,
    zone TEXT NOT NULL,
    wave TEXT NOT NULL,
    status TEXT NOT NULL,
    frozen INTEGER NOT NULL DEFAULT 0,
    rule_version TEXT NOT NULL,
    version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS race_runners_bib ON race_runners(bib);
CREATE TABLE IF NOT EXISTS race_runner_history (
    runner_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    zone TEXT NOT NULL,
    wave TEXT NOT NULL,
    status TEXT NOT NULL,
    frozen INTEGER NOT NULL,
    reason TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    plan_id TEXT,
    valid_from TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    PRIMARY KEY(runner_id, version)
);
CREATE INDEX IF NOT EXISTS race_runner_history_time ON race_runner_history(valid_from);
CREATE TABLE IF NOT EXISTS race_resources (
    resource_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    scope_value TEXT NOT NULL,
    capacity INTEGER NOT NULL,
    detail_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS race_resources_scope ON race_resources(kind, scope_value);
CREATE TABLE IF NOT EXISTS race_allocations (
    allocation_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    runner_id TEXT NOT NULL,
    zone_id TEXT NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL,
    plan_id TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS race_alloc_resource ON race_allocations(resource_id, status);
CREATE INDEX IF NOT EXISTS race_alloc_runner ON race_allocations(runner_id, kind, status);
CREATE INDEX IF NOT EXISTS race_alloc_active ON race_allocations(runner_id, kind, status);
CREATE TABLE IF NOT EXISTS race_consumptions (
    resource_id TEXT NOT NULL,
    runner_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(resource_id, runner_id)
);
CREATE TABLE IF NOT EXISTS race_allocation_journal (
    journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    allocation_id TEXT NOT NULL,
    event TEXT NOT NULL,
    kind TEXT NOT NULL,
    from_resource_id TEXT,
    to_resource_id TEXT,
    from_zone TEXT,
    to_zone TEXT,
    quantity INTEGER NOT NULL DEFAULT 1,
    plan_id TEXT,
    detail_json NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS race_alloc_journal_time ON race_allocation_journal(at);
CREATE TABLE IF NOT EXISTS race_plans (
    plan_id TEXT PRIMARY KEY,
    rule_versions_json TEXT NOT NULL,
    status TEXT NOT NULL,
    base_digest TEXT NOT NULL,
    reason TEXT NOT NULL,
    proposed_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    decided_at TEXT,
    applied_at TEXT,
    applied_by TEXT,
    version INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS race_plans_status ON race_plans(status, created_at);
CREATE TABLE IF NOT EXISTS race_plan_items (
    plan_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    runner_id TEXT NOT NULL,
    action TEXT NOT NULL,
    from_zone TEXT NOT NULL,
    to_zone TEXT,
    base_version INTEGER NOT NULL,
    base_zone TEXT NOT NULL,
    base_frozen INTEGER NOT NULL,
    base_status TEXT NOT NULL,
    PRIMARY KEY(plan_id, runner_id)
);
CREATE TABLE IF NOT EXISTS race_plan_approvals (
    plan_id TEXT NOT NULL,
    role TEXT NOT NULL,
    status TEXT NOT NULL,
    decided_by TEXT,
    decided_at TEXT,
    comment TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(plan_id, role)
);
"""


class Database:
    def __init__(self, path: str | Path):
        self.path = str(path)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA)

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()
