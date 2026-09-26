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
    InboxBatchIn,
    InboxBatchOut,
    InboxDecisionIn,
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


# ---------------------------------------------------------------------------
# 隔离收件箱：迟到批次的上传、预览、批准、拒绝与差异查询
# ---------------------------------------------------------------------------


@router.post(
    "/plans/{plan_version}/inbox",
    response_model=InboxBatchOut,
)
def upload_inbox_batch(
    plan_version: str,
    body: InboxBatchIn,
    response: Response,
    db: Session = Depends(get_db),
) -> Any:
    try:
        result, created = services.upload_inbox_batch(
            db,
            plan_version=plan_version,
            batch_id=body.batch_id,
            source=body.source,
            raw_events=body.events,
        )
    except services.PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    response.status_code = (
        status.HTTP_201_CREATED if created else status.HTTP_200_OK
    )
    return result


@router.get(
    "/plans/{plan_version}/inbox/{batch_id}",
    response_model=InboxBatchOut,
)
def preview_inbox_batch(
    plan_version: str, batch_id: str, db: Session = Depends(get_db)
) -> Any:
    try:
        return services.get_inbox_batch_view(db, plan_version, batch_id)
    except services.PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except services.InboxBatchNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/plans/{plan_version}/inbox/{batch_id}/diff",
    response_model=InboxDiffOut,
)
def get_inbox_batch_diff(
    plan_version: str, batch_id: str, db: Session = Depends(get_db)
) -> Any:
    try:
        return services.get_inbox_batch_diff(db, plan_version, batch_id)
    except services.PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except services.InboxBatchNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/plans/{plan_version}/inbox/{batch_id}/approve",
    response_model=InboxBatchOut,
)
def approve_inbox_batch(
    plan_version: str,
    batch_id: str,
    body: InboxDecisionIn,
    db: Session = Depends(get_db),
) -> Any:
    try:
        return services.approve_inbox_batch(
            db,
            plan_version=plan_version,
            batch_id=batch_id,
            actor=body.actor,
        )
    except services.PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except services.InboxBatchNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except services.InboxDecisionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except services.InboxBatchAlreadyDecidedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post(
    "/plans/{plan_version}/inbox/{batch_id}/reject",
    response_model=InboxBatchOut,
)
def reject_inbox_batch(
    plan_version: str,
    batch_id: str,
    body: InboxRejectIn,
    db: Session = Depends(get_db),
) -> Any:
    try:
        return services.reject_inbox_batch(
            db,
            plan_version=plan_version,
            batch_id=batch_id,
            actor=body.actor,
            reason=body.reason,
        )
    except services.PlanNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except services.InboxBatchNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except services.InboxDecisionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except services.InboxBatchAlreadyDecidedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
