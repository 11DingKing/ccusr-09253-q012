"""隔离收件箱纯逻辑：结构校验、去重、内容指纹与影响模拟。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Iterable

from .replay import Event, EventType
from .snapshot import Snapshot, build_snapshot, diff_snapshots

MAX_TEXT_LENGTH = 128


class InboxStatus(StrEnum):
    """隔离批次的状态机：待审批 -> 已批准 / 已拒绝。"""

    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


_VALID_EVENT_TYPES = frozenset(t.value for t in EventType)


def content_fingerprint(events: list[Any]) -> str:
    """对提交内容生成确定性指纹，用于识别重复提交。"""
    canonical = json.dumps(
        events,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_checkin_payload(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    start: datetime | None = None
    end: datetime | None = None
    for key in ("check_in_at", "check_out_at"):
        raw = payload.get(key)
        if not isinstance(raw, str):
            errors.append(f"payload.{key} must be an RFC 3339 timestamp string")
            continue
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            errors.append(f"payload.{key} must be a valid ISO 8601 timestamp")
            continue
        if parsed.tzinfo is None:
            errors.append(f"payload.{key} must be timezone-aware (RFC 3339)")
            continue
        if key == "check_in_at":
            start = parsed
        else:
            end = parsed
    if start is not None and end is not None and end <= start:
        errors.append("payload.check_out_at must be after payload.check_in_at")
    return errors


def validate_event(event: Any) -> list[str]:
    """校验单个事件结构，返回错误列表（空列表表示合法）。"""
    if not isinstance(event, dict):
        return ["event must be a JSON object"]
    errors: list[str] = []

    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        errors.append("event_id must be a non-empty string")
    elif len(event_id) > MAX_TEXT_LENGTH:
        errors.append(f"event_id must be at most {MAX_TEXT_LENGTH} characters")

    event_type = event.get("event_type")
    if event_type not in _VALID_EVENT_TYPES:
        errors.append(f"event_type must be one of {sorted(_VALID_EVENT_TYPES)}")

    student_id = event.get("student_id")
    if not isinstance(student_id, str) or not student_id.strip():
        errors.append("student_id must be a non-empty string")
    elif len(student_id) > MAX_TEXT_LENGTH:
        errors.append(f"student_id must be at most {MAX_TEXT_LENGTH} characters")

    payload = event.get("payload")
    if not isinstance(payload, dict):
        errors.append("payload must be a JSON object")
        return errors

    if event_type == EventType.CHECKIN.value:
        errors.extend(_validate_checkin_payload(payload))
    elif event_type == EventType.MENTOR_CONFIRM.value:
        target = payload.get("checkin_event_id")
        if not isinstance(target, str) or not target.strip():
            errors.append("payload.checkin_event_id must be a non-empty string")
    elif event_type == EventType.LEAVE_CORRECTION.value:
        seconds = payload.get("adjustment_seconds")
        if isinstance(seconds, bool) or not isinstance(seconds, int):
            errors.append("payload.adjustment_seconds must be an integer")
    return errors


def classify_batch(events: list[Any], existing_event_ids: set[str]) -> dict[str, Any]:
    """结构校验 + 去重：批次内去重，并对照正式事件流去重。"""
    valid: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []
    seen: set[str] = set()
    for event in events:
        errors = validate_event(event)
        if errors:
            invalid.append(
                {
                    "event_id": event.get("event_id")
                    if isinstance(event, dict)
                    else None,
                    "errors": errors,
                }
            )
            continue
        event_id = event["event_id"]
        if event_id in seen:
            duplicates.append({"event_id": event_id, "reason": "duplicate_in_batch"})
            continue
        if event_id in existing_event_ids:
            duplicates.append({"event_id": event_id, "reason": "already_in_stream"})
            continue
        seen.add(event_id)
        valid.append(event)
    return {
        "received_count": len(events),
        "valid_count": len(valid),
        "valid_event_ids": [e["event_id"] for e in valid],
        "valid_events": valid,
        "duplicates": duplicates,
        "invalid": invalid,
    }


def to_core_events(
    events: Iterable[dict[str, Any]], *, plan_version: str
) -> list[Event]:
    """把通过校验的事件字典转换为重放用的核心事件。"""
    placeholder = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return [
        Event(
            event_id=e["event_id"],
            plan_version=plan_version,
            event_type=EventType(e["event_type"]),
            student_id=e["student_id"],
            payload=dict(e["payload"]),
            created_at=placeholder,
        )
        for e in events
    ]


def simulate_impact(
    *,
    plan_version: str,
    timezone_name: str,
    required_seconds: int,
    official_events: list[Event],
    new_events: list[dict[str, Any]],
    frozen_snapshots: list[tuple[str, str | None, Snapshot]],
) -> dict[str, Any]:
    """模拟接纳批次后的影响：对比实时快照，并检查各冻结快照是否会被回溯改变。"""
    incoming = to_core_events(new_events, plan_version=plan_version)
    combined = list(official_events) + incoming

    before = build_snapshot(
        list(official_events),
        plan_version=plan_version,
        timezone_name=timezone_name,
        required_seconds=required_seconds,
    )
    after = build_snapshot(
        combined,
        plan_version=plan_version,
        timezone_name=timezone_name,
        required_seconds=required_seconds,
    )
    live_diff = diff_snapshots(before, after)

    freezes_affected: list[dict[str, Any]] = []
    for freeze_id, cutoff, frozen in frozen_snapshots:
        if cutoff is None:
            # 冻结时事件流为空，迟到事件不会落入其截止范围。
            continue
        replayed = build_snapshot(
            combined,
            plan_version=plan_version,
            timezone_name=timezone_name,
            required_seconds=required_seconds,
            freeze_id=freeze_id,
            event_cutoff_id=cutoff,
        )
        freeze_diff = diff_snapshots(frozen, replayed)
        if freeze_diff["students_affected"]:
            freezes_affected.append(
                {
                    "freeze_id": freeze_id,
                    "event_cutoff_id": cutoff,
                    "students_affected": freeze_diff["students_affected"],
                    "student_changes": freeze_diff["student_changes"],
                }
            )

    return {
        "official_event_count": len(official_events),
        "new_event_count": len(incoming),
        "students_affected": live_diff["students_affected"],
        "student_changes": live_diff["student_changes"],
        "freezes_affected": freezes_affected,
    }
