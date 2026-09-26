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


def insert_events(
    db: Session,
    *,
    plan_version: str,
    events: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    """执行确定性的业务处理。"""
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


def list_freezes(db: Session, plan_version: str) -> list[Freeze]:
    stmt = (
        select(Freeze)
        .where(Freeze.plan_version == plan_version)
        .order_by(Freeze.freeze_id)
    )
    return list(db.execute(stmt).scalars().all())


def list_canonical_event_ids(db: Session, plan_version: str) -> set[str]:
    stmt = select(EventModel.event_id).where(EventModel.plan_version == plan_version)
    return set(db.execute(stmt).scalars().all())


def list_pending_inbox_event_ids(
    db: Session, plan_version: str, *, exclude_batch_id: str | None = None
) -> set[str]:
    """汇总仍处于待审批状态的隔离批次中的事件标识。"""
    stmt = select(InboxBatch.events).where(
        InboxBatch.plan_version == plan_version,
        InboxBatch.status == "pending",
    )
    if exclude_batch_id is not None:
        stmt = stmt.where(InboxBatch.batch_id != exclude_batch_id)
    event_ids: set[str] = set()
    for (events,) in db.execute(stmt).all():
        for event in events or []:
            event_id = event.get("event_id")
            if event_id:
                event_ids.add(event_id)
    return event_ids


def get_inbox_batch(
    db: Session, plan_version: str, batch_id: str
) -> InboxBatch | None:
    return db.get(InboxBatch, (plan_version, batch_id))


def insert_inbox_batch(
    db: Session,
    *,
    plan_version: str,
    batch_id: str,
    source: str,
    events: list[dict[str, Any]],
    summary: dict[str, Any],
    impact: dict[str, Any],
    rule_signature: str,
) -> InboxBatch | None:
    """写入新的隔离批次；批次标识冲突时返回 None（幂等）。"""
    stmt = sqlite_insert(InboxBatch).values(
        plan_version=plan_version,
        batch_id=batch_id,
        source=source,
        status="pending",
        events=events,
        summary=summary,
        impact=impact,
        rule_signature=rule_signature,
    )
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["plan_version", "batch_id"]
    ).returning(InboxBatch.plan_version)
    inserted = db.execute(stmt).scalar_one_or_none()
    db.commit()
    if inserted is not None:
        return db.get(InboxBatch, (plan_version, batch_id))
    return None


def claim_inbox_batch_decision(
    db: Session,
    plan_version: str,
    batch_id: str,
    *,
    target_status: str,
    decision: dict[str, Any],
    decided_at: datetime,
) -> InboxBatch | None:
    """以条件更新抢占批次决策权，只有 pending 批次能被抢占一次。"""
    stmt = (
        update(InboxBatch)
        .where(
            InboxBatch.plan_version == plan_version,
            InboxBatch.batch_id == batch_id,
            InboxBatch.status == "pending",
        )
        .values(
            status=target_status,
            decision=decision,
            decided_at=decided_at,
        )
    )
    result = db.execute(stmt)
    db.flush()
    if result.rowcount != 1:
        db.rollback()
        return None
    db.expire_all()
    return db.get(InboxBatch, (plan_version, batch_id))


def finalize_inbox_batch(
    db: Session,
    plan_version: str,
    batch_id: str,
    *,
    summary: dict[str, Any],
    impact: dict[str, Any],
    decision: dict[str, Any],
) -> None:
    batch = db.get(InboxBatch, (plan_version, batch_id))
    assert batch is not None
    batch.summary = summary
    batch.impact = impact
    batch.decision = decision
    db.flush()


def insert_events_in_transaction(
    db: Session,
    *,
    plan_version: str,
    events: list[dict[str, Any]],
) -> tuple[list[str], list[str]]:
    """在调用方事务内写入正式事件流，不自行提交。"""
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
    db.flush()
    return accepted, duplicates
