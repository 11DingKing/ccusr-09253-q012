"""服务端业务模块。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import ValidationError
from sqlalchemy.orm import Session

from .core.replay import Event as CoreEvent
from .core.replay import EventType
from .core.snapshot import Snapshot, build_snapshot, diff_snapshots, explain_student
from .repository import (
    claim_inbox_batch_decision,
    finalize_inbox_batch,
    get_freeze,
    get_inbox_batch,
    get_plan,
    insert_events,
    insert_events_in_transaction,
    insert_freeze,
    insert_inbox_batch,
    list_canonical_event_ids,
    list_freezes,
    list_pending_inbox_event_ids,
    load_events,
    load_events_up_to,
    max_event_id,
    upsert_plan,
)
from .schemas import CheckinPayload, LeaveCorrectionPayload, MentorConfirmPayload


class PlanNotFoundError(Exception):
    pass


class FreezeConflictError(Exception):
    pass


class FreezeNotFoundError(Exception):
    pass


class InboxBatchNotFoundError(Exception):
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


# ---------------------------------------------------------------------------
# 隔离收件箱：迟到批次先校验、去重、模拟影响，审批后原子写入正式事件流
# ---------------------------------------------------------------------------


class InboxBatchAlreadyDecidedError(Exception):
    """批次已被处理（批准或拒绝），并发请求只有一个能胜出。"""


class InboxDecisionError(Exception):
    """审批请求本身不合法，例如缺少拒绝理由。"""


_PAYLOAD_MODELS = {
    EventType.CHECKIN: CheckinPayload,
    EventType.MENTOR_CONFIRM: MentorConfirmPayload,
    EventType.LEAVE_CORRECTION: LeaveCorrectionPayload,
}


def rule_signature_of(plan: Any) -> str:
    """预览时生效的规则签名，用于审批时识别跨版本规则变更。"""
    return f"{plan.iana_timezone}|{plan.required_seconds}"


def _validate_raw_event(raw: Any, index: int) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """结构校验：字段齐全、类型合法、负载可解析。返回 (规范化事件, 拒绝记录)。"""
    label = f"#{index}"
    if not isinstance(raw, dict):
        return None, {"ref": label, "event_id": None, "reason": "event must be an object"}
    event_id = raw.get("event_id")
    if isinstance(event_id, str) and event_id:
        label = event_id
    student_id = raw.get("student_id")
    event_type = raw.get("event_type")
    payload = raw.get("payload")

    if not isinstance(event_id, str) or not event_id:
        return None, {"ref": label, "event_id": event_id, "reason": "event_id is required"}
    if not isinstance(student_id, str) or not student_id:
        return None, {"ref": label, "event_id": event_id, "reason": "student_id is required"}
    try:
        event_type_enum = EventType(event_type)
    except (ValueError, TypeError):
        return None, {
            "ref": label,
            "event_id": event_id,
            "reason": f"unknown event_type: {event_type!r}",
        }
    if not isinstance(payload, dict):
        return None, {"ref": label, "event_id": event_id, "reason": "payload must be an object"}

    try:
        normalized_payload = _PAYLOAD_MODELS[event_type_enum](**payload).model_dump(
            mode="json"
        )
    except ValidationError as exc:
        reason = "; ".join(
            f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}"
            for err in exc.errors()
        )
        return None, {"ref": label, "event_id": event_id, "reason": reason}

    if event_type_enum == EventType.MENTOR_CONFIRM and not normalized_payload.get(
        "checkin_event_id"
    ):
        return None, {
            "ref": label,
            "event_id": event_id,
            "reason": "mentor_confirm requires checkin_event_id",
        }

    return (
        {
            "event_id": event_id,
            "event_type": event_type_enum.value,
            "student_id": student_id,
            "payload": normalized_payload,
        },
        None,
    )


def _simulate_impact(
    db: Session,
    plan: Any,
    unique_events: list[dict[str, Any]],
) -> dict[str, Any]:
    """在不写入正式事件流的前提下模拟批次接纳后的影响。"""
    canonical = load_events(db, plan.plan_version)
    baseline = build_snapshot(
        canonical,
        plan_version=plan.plan_version,
        timezone_name=plan.iana_timezone,
        required_seconds=plan.required_seconds,
    )
    simulated_at = datetime.now(timezone.utc)
    extra = [
        CoreEvent(
            event_id=e["event_id"],
            plan_version=plan.plan_version,
            event_type=EventType(e["event_type"]),
            student_id=e["student_id"],
            payload=e["payload"],
            created_at=simulated_at,
        )
        for e in unique_events
    ]
    projected = build_snapshot(
        canonical + extra,
        plan_version=plan.plan_version,
        timezone_name=plan.iana_timezone,
        required_seconds=plan.required_seconds,
    )
    diff = diff_snapshots(baseline, projected)
    frozen_revisions = [
        {"freeze_id": row.freeze_id, "event_cutoff_id": row.event_cutoff_id}
        for row in list_freezes(db, plan.plan_version)
    ]
    affected_students = sorted({e["student_id"] for e in unique_events})
    return {
        "rules": {
            "timezone": plan.iana_timezone,
            "required_seconds": plan.required_seconds,
        },
        "baseline_event_cutoff_id": max(
            (e.event_id for e in canonical), default=None
        ),
        "frozen_snapshots": frozen_revisions,
        "frozen_snapshots_remain_immutable": True,
        "submitted_student_ids": affected_students,
        "student_changes": diff["student_changes"],
        "students_affected": diff["students_affected"],
        "stale": False,
    }


def upload_inbox_batch(
    db: Session,
    *,
    plan_version: str,
    batch_id: str,
    source: str,
    raw_events: list[Any],
) -> tuple[dict[str, Any], bool]:
    """上传迟到批次。重复提交（相同 batch_id）返回原处理结果。

    返回 (批次视图, 是否新建)。
    """
    existing = get_inbox_batch(db, plan_version, batch_id)
    if existing is not None:
        return _batch_to_dict(existing, returned_existing=True), False

    plan = _require_plan(db, plan_version)

    rejected: list[dict[str, Any]] = []
    valid: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_events):
        normalized, rejection = _validate_raw_event(raw, index)
        if normalized is None:
            rejected.append(rejection)
        else:
            valid.append(normalized)

    canonical_ids = list_canonical_event_ids(db, plan_version)
    other_pending_ids = list_pending_inbox_event_ids(
        db, plan_version, exclude_batch_id=batch_id
    )

    unique_events: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    seen_in_batch: set[str] = set()
    valid_owner: dict[str, str] = {e["event_id"]: e["student_id"] for e in valid}
    canonical_owner = {
        e.event_id: e.student_id for e in load_events(db, plan_version)
    }
    for event in valid:
        event_id = event["event_id"]
        if event_id in seen_in_batch:
            duplicates.append({"event_id": event_id, "reason": "duplicate_in_batch"})
            continue
        seen_in_batch.add(event_id)
        if event_id in canonical_ids:
            duplicates.append({"event_id": event_id, "reason": "already_accepted"})
            continue
        if event_id in other_pending_ids:
            duplicates.append(
                {"event_id": event_id, "reason": "pending_in_another_batch"}
            )
            continue
        if event["event_type"] == EventType.MENTOR_CONFIRM.value:
            target = event["payload"]["checkin_event_id"]
            owner = valid_owner.get(target) or canonical_owner.get(target)
            if owner is not None and owner != event["student_id"]:
                rejected.append(
                    {
                        "ref": event_id,
                        "event_id": event_id,
                        "reason": (
                            "mentor_confirm target belongs to another student"
                        ),
                    }
                )
                continue
        unique_events.append(event)

    signature = rule_signature_of(plan)
    impact = _simulate_impact(db, plan, unique_events)
    summary = {
        "submitted": len(raw_events),
        "valid_unique": len(unique_events),
        "duplicate_count": len(duplicates),
        "rejected_count": len(rejected),
        "student_ids": sorted({e["student_id"] for e in unique_events}),
        "event_ids": [e["event_id"] for e in unique_events],
        "duplicates": duplicates,
        "rejected": rejected,
    }

    row = insert_inbox_batch(
        db,
        plan_version=plan_version,
        batch_id=batch_id,
        source=source,
        events=unique_events,
        summary=summary,
        impact=impact,
        rule_signature=signature,
    )
    if row is None:
        # 并发上传相同 batch_id，原结果优先。
        existing = get_inbox_batch(db, plan_version, batch_id)
        assert existing is not None
        return _batch_to_dict(existing, returned_existing=True), False
    return _batch_to_dict(row), True


def _require_inbox_batch(db: Session, plan_version: str, batch_id: str):
    _require_plan(db, plan_version)
    batch = get_inbox_batch(db, plan_version, batch_id)
    if batch is None:
        raise InboxBatchNotFoundError(
            f"inbox batch '{batch_id}' for plan '{plan_version}' does not exist"
        )
    return batch


def _live_impact(db: Session, plan: Any, batch: Any) -> dict[str, Any]:
    """对待审批批次按当前正式事件流与规则实时重算影响。"""
    canonical_ids = list_canonical_event_ids(db, plan.plan_version)
    other_pending = list_pending_inbox_event_ids(
        db, plan.plan_version, exclude_batch_id=batch.batch_id
    )
    candidates = [
        e
        for e in (batch.events or [])
        if e["event_id"] not in canonical_ids and e["event_id"] not in other_pending
    ]
    impact = _simulate_impact(db, plan, candidates)
    stored_ids = set((batch.summary or {}).get("event_ids", []))
    live_ids = {e["event_id"] for e in candidates}
    impact["stale"] = live_ids != stored_ids or (
        rule_signature_of(plan) != batch.rule_signature
    )
    return impact


def get_inbox_batch_view(db: Session, plan_version: str, batch_id: str) -> dict[str, Any]:
    batch = _require_inbox_batch(db, plan_version, batch_id)
    view = _batch_to_dict(batch)
    if batch.status == "pending":
        plan = _require_plan(db, plan_version)
        view["impact"] = _live_impact(db, plan, batch)
    return view


def get_inbox_batch_diff(db: Session, plan_version: str, batch_id: str) -> dict[str, Any]:
    batch = _require_inbox_batch(db, plan_version, batch_id)
    if batch.status == "pending":
        plan = _require_plan(db, plan_version)
        impact = _live_impact(db, plan, batch)
    else:
        impact = dict(batch.impact or {})
    return {
        "plan_version": plan_version,
        "batch_id": batch_id,
        "status": batch.status,
        "rules": impact.get("rules"),
        "baseline_event_cutoff_id": impact.get("baseline_event_cutoff_id"),
        "frozen_snapshots": impact.get("frozen_snapshots", []),
        "frozen_snapshots_remain_immutable": impact.get(
            "frozen_snapshots_remain_immutable", True
        ),
        "submitted_student_ids": impact.get("submitted_student_ids", []),
        "student_changes": impact.get("student_changes", []),
        "students_affected": impact.get("students_affected", 0),
        "stale": bool(impact.get("stale")),
        "decision": batch.decision,
    }


def approve_inbox_batch(
    db: Session,
    *,
    plan_version: str,
    batch_id: str,
    actor: str,
) -> dict[str, Any]:
    """批准批次：条件更新抢占决策权，事件与决策在同一事务原子提交。"""
    actor = (actor or "").strip()
    if not actor:
        raise InboxDecisionError("actor is required")
    decided_at = datetime.now(timezone.utc)
    batch = _require_inbox_batch(db, plan_version, batch_id)
    preview_signature = batch.rule_signature

    claimed = claim_inbox_batch_decision(
        db,
        plan_version,
        batch_id,
        target_status="approved",
        decision={
            "action": "approved",
            "actor": actor,
            "decided_at": decided_at.isoformat().replace("+00:00", "Z"),
        },
        decided_at=decided_at,
    )
    if claimed is None:
        raise InboxBatchAlreadyDecidedError(batch.status)

    try:
        # 事务内重新读取规则与正式事件流：识别跨版本规则与并发批次冲突。
        plan = _require_plan(db, plan_version)
        applied_signature = rule_signature_of(plan)
        canonical_events = load_events(db, plan_version)
        canonical_ids = {e.event_id for e in canonical_events}
        to_insert: list[dict[str, Any]] = []
        late_duplicates: list[dict[str, Any]] = []
        for event in batch.events or []:
            if event["event_id"] in canonical_ids:
                late_duplicates.append(
                    {"event_id": event["event_id"], "reason": "accepted_elsewhile"}
                )
            else:
                to_insert.append(event)

        baseline = build_snapshot(
            canonical_events,
            plan_version=plan_version,
            timezone_name=plan.iana_timezone,
            required_seconds=plan.required_seconds,
        )
        baseline_cutoff = max(canonical_ids, default=None)
        accepted, write_duplicates = insert_events_in_transaction(
            db, plan_version=plan_version, events=to_insert
        )
        late_duplicates.extend(
            {"event_id": event_id, "reason": "conflict_at_commit"}
            for event_id in write_duplicates
        )
        projected = build_snapshot(
            load_events(db, plan_version),
            plan_version=plan_version,
            timezone_name=plan.iana_timezone,
            required_seconds=plan.required_seconds,
        )
        diff = diff_snapshots(baseline, projected)
        impact = {
            "rules": {
                "timezone": plan.iana_timezone,
                "required_seconds": plan.required_seconds,
            },
            "baseline_event_cutoff_id": baseline_cutoff,
            "frozen_snapshots": [
                {"freeze_id": row.freeze_id, "event_cutoff_id": row.event_cutoff_id}
                for row in list_freezes(db, plan_version)
            ],
            "frozen_snapshots_remain_immutable": True,
            "submitted_student_ids": (batch.impact or {}).get(
                "submitted_student_ids", []
            ),
            "student_changes": diff["student_changes"],
            "students_affected": diff["students_affected"],
            "stale": applied_signature != preview_signature,
        }
        summary = dict(batch.summary or {})
        summary["late_duplicates"] = late_duplicates
        decision = {
            "action": "approved",
            "actor": actor,
            "decided_at": decided_at.isoformat().replace("+00:00", "Z"),
            "accepted": accepted,
            "duplicates": late_duplicates,
            "preview_rule_signature": preview_signature,
            "applied_rule_signature": applied_signature,
            "rules_changed": applied_signature != preview_signature,
        }
        finalize_inbox_batch(
            db,
            plan_version,
            batch_id,
            summary=summary,
            impact=impact,
            decision=decision,
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    return get_inbox_batch_view(db, plan_version, batch_id)


def reject_inbox_batch(
    db: Session,
    *,
    plan_version: str,
    batch_id: str,
    actor: str,
    reason: str,
) -> dict[str, Any]:
    """拒绝批次：保留摘要与理由，但事件永不参与重放。"""
    actor = (actor or "").strip()
    reason = (reason or "").strip()
    if not actor:
        raise InboxDecisionError("actor is required")
    if not reason:
        raise InboxDecisionError("rejection reason is required")
    _require_inbox_batch(db, plan_version, batch_id)
    decided_at = datetime.now(timezone.utc)
    decision = {
        "action": "rejected",
        "actor": actor,
        "reason": reason,
        "decided_at": decided_at.isoformat().replace("+00:00", "Z"),
    }
    claimed = claim_inbox_batch_decision(
        db,
        plan_version,
        batch_id,
        target_status="rejected",
        decision=decision,
        decided_at=decided_at,
    )
    if claimed is None:
        current = get_inbox_batch(db, plan_version, batch_id)
        raise InboxBatchAlreadyDecidedError(
            current.status if current is not None else "unknown"
        )
    db.commit()
    return get_inbox_batch_view(db, plan_version, batch_id)


def _batch_to_dict(row: Any, *, returned_existing: bool = False) -> dict[str, Any]:
    return {
        "plan_version": row.plan_version,
        "batch_id": row.batch_id,
        "source": row.source,
        "status": row.status,
        "summary": dict(row.summary or {}),
        "impact": dict(row.impact or {}),
        "decision": dict(row.decision) if row.decision else None,
        "rule_signature": row.rule_signature,
        "created_at": row.created_at,
        "decided_at": row.decided_at,
        "returned_existing": returned_existing,
    }
