"""服务端业务模块。"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session

from . import services
from .db import get_db
from .schemas import (
    DiffOut,
    EventBatchIn,
    FreezeIn,
    ImportResult,
    InboxApproveIn,
    InboxBatchIn,
    InboxBatchOut,
    InboxDiffOut,
    InboxRejectIn,
    PlanIn,
    PlanOut,
    SnapshotOut,
    StudentProgressOut,
)

router = APIRouter(prefix="/api")


@router.post("/plans", response_model=PlanOut, status_code=status.HTTP_201_CREATED)
def create_plan(body: PlanIn, db: Session = Depends(get_db)) -> Any:
    return services.ensure_plan(
        db,
        plan_version=body.plan_version,
        iana_timezone=body.iana_timezone,
        required_seconds=body.required_seconds,
    )


@router.get("/plans/{plan_version}", response_model=PlanOut)
def read_plan(plan_version: str, db: Session = Depends(get_db)) -> Any:
    plan = services.get_plan_plain(db, plan_version)
    if plan is None:
        raise HTTPException(status_code=404, detail="plan not found")
    return plan


@router.post(
    "/plans/{plan_version}/events",
    response_model=ImportResult,
    status_code=status.HTTP_201_CREATED,
)
def post_events(
    plan_version: str, body: EventBatchIn, db: Session = Depends(get_db)
) -> Any:
    try:
        return services.import_events(
            db,
            plan_version=plan_version,
            events=[e.model_dump() for e in body.events],
        )
    except services.PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/plans/{plan_version}/snapshot",
    response_model=SnapshotOut,
)
def get_snapshot(plan_version: str, db: Session = Depends(get_db)) -> Any:
    try:
        snap = services.current_snapshot(db, plan_version)
        return snap.to_dict()
    except services.PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/plans/{plan_version}/students/{student_id}/progress",
    response_model=StudentProgressOut,
)
def get_progress(
    plan_version: str, student_id: str, db: Session = Depends(get_db)
) -> Any:
    try:
        result = services.student_progress(db, plan_version, student_id)
    except services.PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="student not found")
    return result


@router.post(
    "/plans/{plan_version}/freezes/{freeze_id}",
    response_model=SnapshotOut,
    status_code=status.HTTP_201_CREATED,
)
def post_freeze(
    plan_version: str,
    freeze_id: str,
    body: FreezeIn,
    db: Session = Depends(get_db),
) -> Any:
    try:
        snap, _ = services.freeze_semester(
            db, plan_version=plan_version, freeze_id=freeze_id
        )
        return snap.to_dict()
    except services.PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/plans/{plan_version}/freezes/{freeze_id}",
    response_model=SnapshotOut,
)
def get_freeze(
    plan_version: str, freeze_id: str, db: Session = Depends(get_db)
) -> Any:
    try:
        snap = services.get_frozen_snapshot(db, plan_version, freeze_id)
        return snap.to_dict()
    except services.PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except services.FreezeNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/plans/{plan_version}/freezes/{freeze_id}/explain/{student_id}",
    response_model=StudentProgressOut,
)
def explain_freeze_student(
    plan_version: str,
    freeze_id: str,
    student_id: str,
    db: Session = Depends(get_db),
) -> Any:
    try:
        result = services.explain_frozen_student(
            db, plan_version, freeze_id, student_id
        )
    except (services.PlanNotFoundError, services.FreezeNotFoundError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(status_code=404, detail="student not found")
    return result


@router.get(
    "/plans/{plan_version}/freezes/{freeze_id}/diff/{other_freeze_id}",
    response_model=DiffOut,
)
def get_diff(
    plan_version: str,
    freeze_id: str,
    other_freeze_id: str,
    db: Session = Depends(get_db),
) -> Any:
    try:
        return services.diff_freezes(
            db, plan_version, freeze_id, other_freeze_id
        )
    except (services.PlanNotFoundError, services.FreezeNotFoundError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/plans/{plan_version}/inbox/batches",
    response_model=InboxBatchOut,
)
def upload_inbox_batch(
    plan_version: str,
    body: InboxBatchIn,
    response: Response,
    db: Session = Depends(get_db),
) -> Any:
    """上传迟到批次：校验、去重、影响模拟后进入隔离收件箱。"""
    try:
        result, created = services.upload_inbox_batch(
            db,
            plan_version=plan_version,
            batch_id=body.batch_id,
            source=body.source,
            events=body.events,
        )
    except services.PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except services.BatchPayloadMismatchError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    response.status_code = (
        status.HTTP_201_CREATED if created else status.HTTP_200_OK
    )
    return result


@router.get(
    "/plans/{plan_version}/inbox/batches/{batch_id}",
    response_model=InboxBatchOut,
)
def read_inbox_batch(
    plan_version: str, batch_id: str, db: Session = Depends(get_db)
) -> Any:
    """预览批次：校验报告、影响模拟与审批结果。"""
    try:
        return services.get_inbox_batch_view(db, plan_version, batch_id)
    except (services.PlanNotFoundError, services.BatchNotFoundError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/plans/{plan_version}/inbox/batches/{batch_id}/diff",
    response_model=InboxDiffOut,
)
def read_inbox_batch_diff(
    plan_version: str, batch_id: str, db: Session = Depends(get_db)
) -> Any:
    """差异查询：批次被接纳后会影响哪些学生与冻结快照。"""
    try:
        return services.inbox_batch_diff(db, plan_version, batch_id)
    except (services.PlanNotFoundError, services.BatchNotFoundError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/plans/{plan_version}/inbox/batches/{batch_id}/approve",
    response_model=InboxBatchOut,
)
def approve_inbox_batch(
    plan_version: str,
    batch_id: str,
    body: InboxApproveIn | None = None,
    db: Session = Depends(get_db),
) -> Any:
    """批准批次：通过校验的事件原子写入正式事件流。"""
    actor = body.actor if body else None
    note = body.note if body else ""
    try:
        result, _ = services.approve_inbox_batch(
            db,
            plan_version=plan_version,
            batch_id=batch_id,
            actor=actor,
            note=note,
        )
    except (services.PlanNotFoundError, services.BatchNotFoundError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except services.BatchStateConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return result


@router.post(
    "/plans/{plan_version}/inbox/batches/{batch_id}/reject",
    response_model=InboxBatchOut,
)
def reject_inbox_batch(
    plan_version: str,
    batch_id: str,
    body: InboxRejectIn,
    db: Session = Depends(get_db),
) -> Any:
    """拒绝批次：保留摘要与理由，事件不参与重放。"""
    try:
        result, _ = services.reject_inbox_batch(
            db,
            plan_version=plan_version,
            batch_id=batch_id,
            reason=body.reason,
            actor=body.actor,
        )
    except (services.PlanNotFoundError, services.BatchNotFoundError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except services.BatchStateConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return result
