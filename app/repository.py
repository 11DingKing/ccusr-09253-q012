"""服务端业务模块。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .core.replay import Event as CoreEvent
from .core.replay import EventType
from .models import Event as EventModel
from .models import Freeze, InboxBatch, Plan


def get_plan(db: Session, plan_version: str) -> Plan | None:
    return db.get(Plan, plan_version)


def upsert_plan(
    db: Session,
    *,
    plan_version: str,
    iana_timezone: str,
    required_seconds: int,
) -> Plan:
    stmt = sqlite_insert(Plan).values(
        plan_version=plan_version,
        iana_timezone=iana_timezone,
        required_seconds=required_seconds,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["plan_version"],
        set_={
            "iana_timezone": iana_timezone,
            "required_seconds": required_seconds,
        },
    )
    db.execute(stmt)
    db.commit()
    plan = db.get(Plan, plan_version)
    assert plan is not None
    return plan


def _to_core_event(row: EventModel) -> CoreEvent:
    return CoreEvent(
        event_id=row.event_id,
        plan_version=row.plan_version,
        event_type=EventType(row.event_type),
        student_id=row.student_id,
        payload=dict(row.payload),
        created_at=row.created_at,
    )


def add_events(
    db: Session,
    *,
    plan_version: str,
    events: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    """写入事件但不提交，事务边界由调用方控制。"""
    accepted: list[str] = []
    duplicates: list[str] = []
    for e in events:
        stmt = sqlite_insert(EventModel).values(
            event_id=e["event_id"],
            plan_version=plan_version,
            student_id=e["student_id"],
            event_type=e["event_type"],
            payload=e["payload"],
        )
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["event_id", "plan_version"]
        ).returning(EventModel.id)
        inserted_id = db.execute(stmt).scalar_one_or_none()
        if inserted_id is not None:
            accepted.append(e["event_id"])
        else:
            duplicates.append(e["event_id"])
    return accepted, duplicates


def insert_events(
    db: Session,
    *,
    plan_version: str,
    events: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    """执行确定性的业务处理。"""
    accepted, duplicates = add_events(db, plan_version=plan_version, events=events)
    db.commit()
    return accepted, duplicates


def load_events(db: Session, plan_version: str) -> list[CoreEvent]:
    stmt = select(EventModel).where(EventModel.plan_version == plan_version)
    rows = db.execute(stmt).scalars().all()
    return [_to_core_event(r) for r in rows]


def load_events_up_to(
    db: Session, plan_version: str, max_event_id: str
) -> list[CoreEvent]:
    """执行确定性的业务处理。"""
    stmt = (
        select(EventModel)
        .where(EventModel.plan_version == plan_version)
        .where(EventModel.event_id <= max_event_id)
    )
    rows = db.execute(stmt).scalars().all()
    return [_to_core_event(r) for r in rows]


def max_event_id(db: Session, plan_version: str) -> str | None:
    stmt = (
        select(EventModel.event_id)
        .where(EventModel.plan_version == plan_version)
        .order_by(EventModel.event_id.desc())
        .limit(1)
    )
    return db.execute(stmt).scalar_one_or_none()


def get_freeze(
    db: Session, plan_version: str, freeze_id: str
) -> Freeze | None:
    return db.get(Freeze, (plan_version, freeze_id))


def list_freezes(db: Session, plan_version: str) -> list[Freeze]:
    stmt = (
        select(Freeze)
        .where(Freeze.plan_version == plan_version)
        .order_by(Freeze.freeze_id)
    )
    return list(db.execute(stmt).scalars().all())


def insert_freeze(
    db: Session,
    *,
    plan_version: str,
    freeze_id: str,
    snapshot: dict[str, Any],
    event_cutoff_id: str | None,
) -> Freeze | None:
    """执行确定性的业务处理。"""
    stmt = sqlite_insert(Freeze).values(
        plan_version=plan_version,
        freeze_id=freeze_id,
        snapshot=snapshot,
        event_cutoff_id=event_cutoff_id,
    )
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["plan_version", "freeze_id"]
    ).returning(Freeze.plan_version)
    inserted = db.execute(stmt).scalar_one_or_none()
    db.commit()
    if inserted is not None:
        return db.get(Freeze, (plan_version, freeze_id))
    return None


def get_inbox_batch(
    db: Session, plan_version: str, batch_id: str
) -> InboxBatch | None:
    return db.get(InboxBatch, (plan_version, batch_id))


def insert_inbox_batch(
    db: Session,
    *,
    plan_version: str,
    batch_id: str,
    source: str | None,
    content_hash: str,
    events: list[Any],
    validation: dict[str, Any],
    preview: dict[str, Any],
) -> InboxBatch | None:
    """写入待审批批次；同一 (plan_version, batch_id) 重复写入时返回 None。"""
    stmt = sqlite_insert(InboxBatch).values(
        plan_version=plan_version,
        batch_id=batch_id,
        status="pending",
        source=source,
        content_hash=content_hash,
        events=events,
        validation=validation,
        preview=preview,
        decision=None,
    )
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["plan_version", "batch_id"]
    ).returning(InboxBatch.plan_version)
    inserted = db.execute(stmt).scalar_one_or_none()
    db.commit()
    if inserted is not None:
        return db.get(InboxBatch, (plan_version, batch_id))
    return None


def claim_inbox_batch(
    db: Session,
    *,
    plan_version: str,
    batch_id: str,
    expect_status: str,
    new_status: str,
    decision: dict[str, Any] | None,
    updated_at: datetime,
) -> bool:
    """按期望状态做 CAS 迁移；不提交，事务边界由调用方控制。"""
    stmt = (
        update(InboxBatch)
        .where(InboxBatch.plan_version == plan_version)
        .where(InboxBatch.batch_id == batch_id)
        .where(InboxBatch.status == expect_status)
        .values(status=new_status, decision=decision, updated_at=updated_at)
    )
    result = db.execute(stmt)
    return result.rowcount == 1


def set_inbox_decision(
    db: Session,
    *,
    plan_version: str,
    batch_id: str,
    decision: dict[str, Any],
    updated_at: datetime,
) -> None:
    """在已持有写事务的前提下补充审批结果；不提交。"""
    stmt = (
        update(InboxBatch)
        .where(InboxBatch.plan_version == plan_version)
        .where(InboxBatch.batch_id == batch_id)
        .values(decision=decision, updated_at=updated_at)
    )
    db.execute(stmt)
