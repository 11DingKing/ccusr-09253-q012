"""服务端业务模块。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from .core.inbox import (
    InboxStatus,
    classify_batch,
    content_fingerprint,
    simulate_impact,
)
from .core.snapshot import Snapshot, build_snapshot, diff_snapshots, explain_student
from .models import InboxBatch
from .repository import (
    add_events,
    claim_inbox_batch,
    get_freeze,
    get_inbox_batch,
    get_plan,
    insert_events,
    insert_freeze,
    insert_inbox_batch,
    list_freezes,
    load_events,
    load_events_up_to,
    max_event_id,
    set_inbox_decision,
    upsert_plan,
)


class PlanNotFoundError(Exception):
    pass


class FreezeConflictError(Exception):
    pass


class FreezeNotFoundError(Exception):
    pass


class BatchNotFoundError(Exception):
    pass


class BatchStateConflictError(Exception):
    pass


class BatchPayloadMismatchError(Exception):
    pass


def get_plan_plain(db: Session, plan_version: str) -> dict[str, Any] | None:
    plan = get_plan(db, plan_version)
    if plan is None:
        return None
    return {
        "plan_version": plan.plan_version,
        "iana_timezone": plan.iana_timezone,
        "required_seconds": plan.required_seconds,
    }


def ensure_plan(
    db: Session,
    *,
    plan_version: str,
    iana_timezone: str,
    required_seconds: int,
) -> dict[str, Any]:
    plan = upsert_plan(
        db,
        plan_version=plan_version,
        iana_timezone=iana_timezone,
        required_seconds=required_seconds,
    )
    return {
        "plan_version": plan.plan_version,
        "iana_timezone": plan.iana_timezone,
        "required_seconds": plan.required_seconds,
    }


def _require_plan(db: Session, plan_version: str):
    plan = get_plan(db, plan_version)
    if plan is None:
        raise PlanNotFoundError(f"plan version '{plan_version}' is not registered")
    return plan


def import_events(
    db: Session, *, plan_version: str, events: list[dict[str, Any]]
) -> dict[str, Any]:
    _require_plan(db, plan_version)
    accepted, duplicates = insert_events(
        db, plan_version=plan_version, events=events
    )
    return {
        "accepted": len(accepted),
        "duplicates": duplicates,
        "rejected": [],
    }


def current_snapshot(db: Session, plan_version: str) -> Snapshot:
    plan = _require_plan(db, plan_version)
    events = load_events(db, plan_version)
    return build_snapshot(
        events,
        plan_version=plan_version,
        timezone_name=plan.iana_timezone,
        required_seconds=plan.required_seconds,
    )


def student_progress(
    db: Session, plan_version: str, student_id: str
) -> dict[str, Any] | None:
    snap = current_snapshot(db, plan_version)
    return explain_student(snap, student_id)


def freeze_semester(
    db: Session, *, plan_version: str, freeze_id: str
) -> tuple[Snapshot, bool]:
    """执行确定性的业务处理。"""
    plan = _require_plan(db, plan_version)
    existing = get_freeze(db, plan_version, freeze_id)
    if existing is not None:
        return Snapshot.from_dict(existing.snapshot), False

    cutoff = max_event_id(db, plan_version)
    events = load_events(db, plan_version)
    snap = build_snapshot(
        events,
        plan_version=plan_version,
        timezone_name=plan.iana_timezone,
        required_seconds=plan.required_seconds,
        freeze_id=freeze_id,
        event_cutoff_id=cutoff,
    )
    row = insert_freeze(
        db,
        plan_version=plan_version,
        freeze_id=freeze_id,
        snapshot=snap.to_dict(),
        event_cutoff_id=cutoff,
    )
    if row is None:
        existing = get_freeze(db, plan_version, freeze_id)
        assert existing is not None
        return Snapshot.from_dict(existing.snapshot), False
    return snap, True


def get_frozen_snapshot(
    db: Session, plan_version: str, freeze_id: str
) -> Snapshot:
    _require_plan(db, plan_version)
    row = get_freeze(db, plan_version, freeze_id)
    if row is None:
        raise FreezeNotFoundError(
            f"freeze '{freeze_id}' for plan '{plan_version}' does not exist"
        )
    return Snapshot.from_dict(row.snapshot)


def explain_frozen_student(
    db: Session, plan_version: str, freeze_id: str, student_id: str
) -> dict[str, Any] | None:
    snap = get_frozen_snapshot(db, plan_version, freeze_id)
    return explain_student(snap, student_id)


def diff_freezes(
    db: Session, plan_version: str, old_freeze_id: str, new_freeze_id: str
) -> dict[str, Any]:
    old = get_frozen_snapshot(db, plan_version, old_freeze_id)
    new = get_frozen_snapshot(db, plan_version, new_freeze_id)
    return diff_snapshots(old, new)


def _aware(value: datetime | None) -> datetime | None:
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _inbox_out(batch: InboxBatch) -> dict[str, Any]:
    return {
        "plan_version": batch.plan_version,
        "batch_id": batch.batch_id,
        "status": batch.status,
        "source": batch.source,
        "content_hash": batch.content_hash,
        "validation": batch.validation,
        "preview": batch.preview,
        "decision": batch.decision,
        "created_at": _aware(batch.created_at),
        "updated_at": _aware(batch.updated_at),
    }


def upload_inbox_batch(
    db: Session,
    *,
    plan_version: str,
    batch_id: str,
    source: str | None,
    events: list[Any],
) -> tuple[dict[str, Any], bool]:
    """上传迟到批次：结构校验、去重、影响模拟后写入隔离收件箱。

    相同 batch_id 重复提交时返回原处理结果；内容不同则视为冲突。
    """
    plan = _require_plan(db, plan_version)
    fingerprint = content_fingerprint(events)

    existing = get_inbox_batch(db, plan_version, batch_id)
    if existing is not None:
        if existing.content_hash != fingerprint:
            raise BatchPayloadMismatchError(
                f"inbox batch '{batch_id}' already exists with different content"
            )
        return _inbox_out(existing), False

    official = load_events(db, plan_version)
    validation = classify_batch(events, {e.event_id for e in official})
    frozen = [
        (row.freeze_id, row.event_cutoff_id, Snapshot.from_dict(row.snapshot))
        for row in list_freezes(db, plan_version)
    ]
    preview = simulate_impact(
        plan_version=plan_version,
        timezone_name=plan.iana_timezone,
        required_seconds=plan.required_seconds,
        official_events=official,
        new_events=validation["valid_events"],
        frozen_snapshots=frozen,
    )
    row = insert_inbox_batch(
        db,
        plan_version=plan_version,
        batch_id=batch_id,
        source=source,
        content_hash=fingerprint,
        events=events,
        validation=validation,
        preview=preview,
    )
    if row is None:
        # 并发提交了相同 batch_id，以先入库的结果为准。
        existing = get_inbox_batch(db, plan_version, batch_id)
        assert existing is not None
        if existing.content_hash != fingerprint:
            raise BatchPayloadMismatchError(
                f"inbox batch '{batch_id}' already exists with different content"
            )
        return _inbox_out(existing), False
    return _inbox_out(row), True


def _require_inbox_batch(db: Session, plan_version: str, batch_id: str) -> InboxBatch:
    batch = get_inbox_batch(db, plan_version, batch_id)
    if batch is None:
        raise BatchNotFoundError(
            f"inbox batch '{batch_id}' for plan '{plan_version}' does not exist"
        )
    return batch


def get_inbox_batch_view(
    db: Session, plan_version: str, batch_id: str
) -> dict[str, Any]:
    _require_plan(db, plan_version)
    return _inbox_out(_require_inbox_batch(db, plan_version, batch_id))


def inbox_batch_diff(
    db: Session, plan_version: str, batch_id: str
) -> dict[str, Any]:
    """差异查询：返回上传时固化下来的影响模拟结果。"""
    out = get_inbox_batch_view(db, plan_version, batch_id)
    preview = out["preview"]
    return {
        "plan_version": plan_version,
        "batch_id": batch_id,
        "status": out["status"],
        "students_affected": preview["students_affected"],
        "student_changes": preview["student_changes"],
        "freezes_affected": preview["freezes_affected"],
    }


def _claim_batch(
    db: Session,
    *,
    plan_version: str,
    batch_id: str,
    new_status: InboxStatus,
    decision: dict[str, Any] | None,
    now: datetime,
) -> bool:
    # 先回滚掉可能存在的读事务，确保 CAS 在新写事务中执行，
    # 避免并发下读到旧快照。
    db.rollback()
    try:
        return claim_inbox_batch(
            db,
            plan_version=plan_version,
            batch_id=batch_id,
            expect_status=InboxStatus.PENDING.value,
            new_status=new_status.value,
            decision=decision,
            updated_at=now,
        )
    except OperationalError as exc:
        db.rollback()
        raise BatchStateConflictError(
            f"inbox batch '{batch_id}' is being decided concurrently; retry"
        ) from exc


def approve_inbox_batch(
    db: Session,
    *,
    plan_version: str,
    batch_id: str,
    actor: str | None = None,
    note: str = "",
) -> tuple[dict[str, Any], bool]:
    """批准批次：把通过校验的事件与状态迁移放在同一事务中原子写入。

    重复批准返回原处理结果；已拒绝的批次不允许再批准。
    """
    _require_plan(db, plan_version)
    now = datetime.now(timezone.utc)
    claimed = _claim_batch(
        db,
        plan_version=plan_version,
        batch_id=batch_id,
        new_status=InboxStatus.APPROVED,
        decision=None,
        now=now,
    )
    if not claimed:
        db.rollback()
        batch = _require_inbox_batch(db, plan_version, batch_id)
        if batch.status == InboxStatus.APPROVED.value:
            return _inbox_out(batch), False
        raise BatchStateConflictError(
            f"inbox batch '{batch_id}' is already {batch.status}"
        )

    batch = _require_inbox_batch(db, plan_version, batch_id)
    valid_events = batch.validation.get("valid_events", [])
    accepted, skipped = add_events(db, plan_version=plan_version, events=valid_events)
    decision = {
        "actor": actor,
        "note": note,
        "committed_event_ids": accepted,
        "skipped_duplicates": skipped,
        "committed_at": now.isoformat().replace("+00:00", "Z"),
    }
    set_inbox_decision(
        db,
        plan_version=plan_version,
        batch_id=batch_id,
        decision=decision,
        updated_at=now,
    )
    db.commit()
    return _inbox_out(_require_inbox_batch(db, plan_version, batch_id)), True


def reject_inbox_batch(
    db: Session,
    *,
    plan_version: str,
    batch_id: str,
    reason: str,
    actor: str | None = None,
) -> tuple[dict[str, Any], bool]:
    """拒绝批次：保留摘要与理由，事件不参与重放。重复拒绝返回原处理结果。"""
    _require_plan(db, plan_version)
    reason = (reason or "").strip()
    if not reason:
        raise ValueError("reject reason must not be blank")
    now = datetime.now(timezone.utc)
    decision = {
        "actor": actor,
        "reason": reason,
        "rejected_at": now.isoformat().replace("+00:00", "Z"),
    }
    claimed = _claim_batch(
        db,
        plan_version=plan_version,
        batch_id=batch_id,
        new_status=InboxStatus.REJECTED,
        decision=decision,
        now=now,
    )
    if claimed:
        db.commit()
        return _inbox_out(_require_inbox_batch(db, plan_version, batch_id)), True
    db.rollback()
    batch = _require_inbox_batch(db, plan_version, batch_id)
    if batch.status == InboxStatus.REJECTED.value:
        return _inbox_out(batch), False
    raise BatchStateConflictError(
        f"inbox batch '{batch_id}' is already {batch.status}"
    )
