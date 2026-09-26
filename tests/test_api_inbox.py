"""隔离收件箱：迟到批次的上传、预览、批准、拒绝与差异查询。"""

from __future__ import annotations

import threading

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import services
from tests.conftest import NY_PLAN, SHANGHAI_PLAN, TestSessionLocal


def _create_plan(client, plan=SHANGHAI_PLAN):
    resp = client.post("/api/plans", json=plan)
    assert resp.status_code == 201, resp.text


def _checkin(
    eid,
    student,
    start="2024-03-15T08:00:00+08:00",
    end="2024-03-15T10:00:00+08:00",
    activity_type="regular",
):
    return {
        "event_id": eid,
        "event_type": "checkin",
        "student_id": student,
        "payload": {
            "activity_id": "A1",
            "activity_type": activity_type,
            "check_in_at": start,
            "check_out_at": end,
        },
    }


def _import_events(client, pv, events):
    resp = client.post(f"/api/plans/{pv}/events", json={"events": events})
    assert resp.status_code == 201, resp.text
    return resp.json()


def _upload(client, pv, batch_id, events, source=None, expected=201):
    body = {"batch_id": batch_id, "events": events}
    if source is not None:
        body["source"] = source
    resp = client.post(f"/api/plans/{pv}/inbox/batches", json=body)
    assert resp.status_code == expected, resp.text
    return resp.json()


def _approve(client, pv, batch_id, expected=200):
    resp = client.post(f"/api/plans/{pv}/inbox/batches/{batch_id}/approve", json={})
    assert resp.status_code == expected, resp.text
    return resp.json()


def _reject(client, pv, batch_id, reason="来源未授权", expected=200):
    resp = client.post(
        f"/api/plans/{pv}/inbox/batches/{batch_id}/reject",
        json={"reason": reason},
    )
    assert resp.status_code == expected, resp.text
    return resp.json()


def test_upload_preview_and_approve_with_partial_conflicts(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    official = [_checkin(f"E-1{i:02d}", f"S{i}") for i in range(5)]
    _import_events(client, pv, official)
    client.post(f"/api/plans/{pv}/freezes/F-01", json={})

    # 迟到事件的事件号排在冻结截止点之前（数周前的签到）。
    late = [
        _checkin(
            f"E-05{i}",
            f"S{i}",
            start="2024-02-01T08:00:00+08:00",
            end="2024-02-01T10:00:00+08:00",
        )
        for i in range(5)
    ]
    batch_events = (
        late
        + [official[0], official[1]]  # 已在正式事件流中
        + [late[0], late[1]]  # 批次内重复
        + [
            {  # 时间戳缺时区
                "event_id": "BAD-1",
                "event_type": "checkin",
                "student_id": "S9",
                "payload": {
                    "check_in_at": "2024-03-15 08:00:00",
                    "check_out_at": "2024-03-15T10:00:00+08:00",
                },
            },
            {  # 未知事件类型
                "event_id": "BAD-2",
                "event_type": "teleport",
                "student_id": "S9",
                "payload": {},
            },
            {  # 结束早于开始
                "event_id": "BAD-3",
                "event_type": "checkin",
                "student_id": "S9",
                "payload": {
                    "check_in_at": "2024-03-15T10:00:00+08:00",
                    "check_out_at": "2024-03-15T08:00:00+08:00",
                },
            },
        ]
    )
    body = _upload(client, pv, "B-1", batch_events, source="ext-partner")
    assert body["status"] == "pending"
    assert body["source"] == "ext-partner"

    validation = body["validation"]
    assert validation["received_count"] == 12
    assert validation["valid_count"] == 5
    assert sorted(validation["valid_event_ids"]) == [f"E-05{i}" for i in range(5)]
    dup_pairs = sorted(
        (d["event_id"], d["reason"]) for d in validation["duplicates"]
    )
    assert dup_pairs == sorted(
        [
            ("E-100", "already_in_stream"),
            ("E-101", "already_in_stream"),
            ("E-050", "duplicate_in_batch"),
            ("E-051", "duplicate_in_batch"),
        ]
    )
    assert {i["event_id"] for i in validation["invalid"]} == {
        "BAD-1",
        "BAD-2",
        "BAD-3",
    }

    preview = body["preview"]
    assert preview["new_event_count"] == 5
    assert preview["students_affected"] == 5
    assert [f["freeze_id"] for f in preview["freezes_affected"]] == ["F-01"]
    assert preview["freezes_affected"][0]["students_affected"] == 5

    # 预览接口返回同一份固化下来的模拟结果。
    got = client.get(f"/api/plans/{pv}/inbox/batches/B-1")
    assert got.status_code == 200
    assert got.json()["preview"] == preview
    assert got.json()["decision"] is None

    # 差异查询接口暴露影响摘要。
    diff = client.get(f"/api/plans/{pv}/inbox/batches/B-1/diff").json()
    assert diff["batch_id"] == "B-1"
    assert diff["status"] == "pending"
    assert diff["students_affected"] == 5
    assert diff["freezes_affected"][0]["freeze_id"] == "F-01"

    # 批准只写入通过校验的事件。
    approved = _approve(client, pv, "B-1")
    assert approved["status"] == "approved"
    assert sorted(approved["decision"]["committed_event_ids"]) == [
        f"E-05{i}" for i in range(5)
    ]
    assert approved["decision"]["skipped_duplicates"] == []

    # 实时快照反映迟到事件，冻结快照保持不变。
    live = client.get(f"/api/plans/{pv}/snapshot").json()
    totals = {s["student_id"]: s["total_seconds"] for s in live["students"]}
    assert all(totals[f"S{i}"] == 4 * 3600 for i in range(5))
    frozen = client.get(f"/api/plans/{pv}/freezes/F-01").json()
    assert all(s["total_seconds"] == 7200 for s in frozen["students"])

    # 非法事件从未进入正式事件流。
    assert client.get(f"/api/plans/{pv}/students/S9/progress").status_code == 404


def test_reupload_returns_original_result(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    events = [_checkin("E-201", "S1")]
    first = _upload(client, pv, "B-1", events)

    again = _upload(client, pv, "B-1", events, expected=200)
    assert again == first

    _approve(client, pv, "B-1")
    third = _upload(client, pv, "B-1", events, expected=200)
    assert third["status"] == "approved"
    assert third["decision"]["committed_event_ids"] == ["E-201"]

    # 重复提交不会产生任何额外效果。
    live = client.get(f"/api/plans/{pv}/snapshot").json()
    assert len(live["students"]) == 1
    assert live["students"][0]["total_seconds"] == 7200


def test_reupload_with_different_content_conflicts(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _upload(client, pv, "B-1", [_checkin("E-201", "S1")])
    resp = client.post(
        f"/api/plans/{pv}/inbox/batches",
        json={"batch_id": "B-1", "events": [_checkin("E-202", "S1")]},
    )
    assert resp.status_code == 409


def test_approve_is_idempotent(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _upload(
        client,
        pv,
        "B-1",
        [
            _checkin("E-201", "S1"),
            _checkin(
                "E-202",
                "S1",
                start="2024-03-16T08:00:00+08:00",
                end="2024-03-16T10:00:00+08:00",
            ),
        ],
    )
    first = _approve(client, pv, "B-1")
    assert sorted(first["decision"]["committed_event_ids"]) == ["E-201", "E-202"]

    second = _approve(client, pv, "B-1")
    assert second["status"] == "approved"
    assert second["decision"] == first["decision"]

    live = client.get(f"/api/plans/{pv}/snapshot").json()
    assert live["students"][0]["total_seconds"] == 4 * 3600
    assert len(live["students"][0]["checkins"]) == 2


def test_reject_keeps_summary_and_stays_out_of_replay(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _upload(client, pv, "B-1", [_checkin("E-201", "S1")])

    rejected = _reject(client, pv, "B-1", reason="来源未授权")
    assert rejected["status"] == "rejected"
    assert rejected["decision"]["reason"] == "来源未授权"

    # 摘要（校验报告 + 影响模拟）与理由都保留，可继续查询。
    record = client.get(f"/api/plans/{pv}/inbox/batches/B-1").json()
    assert record["status"] == "rejected"
    assert record["decision"]["reason"] == "来源未授权"
    assert record["validation"]["valid_count"] == 1
    assert record["preview"]["students_affected"] == 1

    # 被拒批次不参与重放。
    live = client.get(f"/api/plans/{pv}/snapshot").json()
    assert live["students"] == []

    # 拒绝后不能再批准；重复拒绝返回原处理结果。
    _approve(client, pv, "B-1", expected=409)
    again = _reject(client, pv, "B-1", reason="换一个理由")
    assert again["decision"]["reason"] == "来源未授权"

    live = client.get(f"/api/plans/{pv}/snapshot").json()
    assert live["students"] == []


def test_reject_requires_reason(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _upload(client, pv, "B-1", [_checkin("E-201", "S1")])
    resp = client.post(f"/api/plans/{pv}/inbox/batches/B-1/reject", json={})
    assert resp.status_code == 422
    resp = client.post(
        f"/api/plans/{pv}/inbox/batches/B-1/reject", json={"reason": "   "}
    )
    assert resp.status_code == 422


def test_unknown_batch_and_plan_return_404(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    assert client.get(f"/api/plans/{pv}/inbox/batches/NOPE").status_code == 404
    assert client.get(f"/api/plans/{pv}/inbox/batches/NOPE/diff").status_code == 404
    assert (
        client.post(f"/api/plans/{pv}/inbox/batches/NOPE/approve", json={}).status_code
        == 404
    )
    assert (
        client.post(
            f"/api/plans/{pv}/inbox/batches/NOPE/reject",
            json={"reason": "x"},
        ).status_code
        == 404
    )
    assert (
        client.post(
            "/api/plans/NOPE/inbox/batches",
            json={"batch_id": "B-1", "events": []},
        ).status_code
        == 404
    )
    assert client.get("/api/plans/NOPE/inbox/batches/B-1").status_code == 404


def test_large_batch_with_partial_conflicts(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    official = [_checkin(f"E-{1000 + i}", f"S{i:03d}") for i in range(100)]
    _import_events(client, pv, official)

    late = [
        _checkin(
            f"E-{i:04d}",
            f"S{i:03d}",
            start="2024-01-08T08:00:00+08:00",
            end="2024-01-08T11:00:00+08:00",
        )
        for i in range(100)
    ]
    stream_dups = official[:50]
    batch_dups = late[:50]
    invalid = [
        {
            "event_id": f"BAD-{i:03d}",
            "event_type": "checkin",
            "student_id": "SX",
            "payload": {"check_in_at": "n/a", "check_out_at": "n/a"},
        }
        for i in range(50)
    ]
    body = _upload(client, pv, "B-BIG", late + stream_dups + batch_dups + invalid)

    validation = body["validation"]
    assert validation["received_count"] == 250
    assert validation["valid_count"] == 100
    assert len(validation["duplicates"]) == 100
    assert len(validation["invalid"]) == 50
    assert body["preview"]["students_affected"] == 100

    approved = _approve(client, pv, "B-BIG")
    assert len(approved["decision"]["committed_event_ids"]) == 100
    assert approved["decision"]["skipped_duplicates"] == []

    live = client.get(f"/api/plans/{pv}/snapshot").json()
    assert len(live["students"]) == 100
    first = next(s for s in live["students"] if s["student_id"] == "S000")
    assert first["total_seconds"] == 7200 + 3 * 3600


def test_overlapping_batches_second_commit_skips_duplicates(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    shared = _checkin("E-900", "S1")
    _upload(client, pv, "B-1", [shared, _checkin("E-901", "S1")])
    _upload(client, pv, "B-2", [shared, _checkin("E-902", "S2")])

    _approve(client, pv, "B-1")
    second = _approve(client, pv, "B-2")
    assert second["decision"]["committed_event_ids"] == ["E-902"]
    assert second["decision"]["skipped_duplicates"] == ["E-900"]

    live = client.get(f"/api/plans/{pv}/snapshot").json()
    assert {s["student_id"] for s in live["students"]} == {"S1", "S2"}


def test_empty_batch_is_valid(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    body = _upload(client, pv, "B-EMPTY", [])
    assert body["validation"]["valid_count"] == 0
    assert body["preview"]["students_affected"] == 0
    approved = _approve(client, pv, "B-EMPTY")
    assert approved["status"] == "approved"
    assert approved["decision"]["committed_event_ids"] == []


def test_non_object_events_are_reported_invalid(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    body = _upload(client, pv, "B-WEIRD", ["garbage", 42, None, _checkin("E-1", "S1")])
    invalid = body["validation"]["invalid"]
    assert len(invalid) == 3
    assert all(entry["event_id"] is None for entry in invalid)
    assert body["validation"]["valid_event_ids"] == ["E-1"]


def test_late_mentor_confirm_impact_preview(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _import_events(
        client,
        pv,
        [
            _checkin(
                "E-800",
                "S1",
                start="2024-03-15T08:00:00+08:00",
                end="2024-03-15T12:00:00+08:00",
                activity_type="internship",
            ),
            # 更晚的事件把冻结截止点推到 E-900。
            _checkin("E-900", "S2"),
        ],
    )
    client.post(f"/api/plans/{pv}/freezes/F-01", json={})

    # 迟到的导师确认：排在目标签到之后（重放才生效），但在冻结截止点之前。
    confirm = {
        "event_id": "E-850",
        "event_type": "mentor_confirm",
        "student_id": "S1",
        "payload": {"checkin_event_id": "E-800"},
    }
    body = _upload(client, pv, "B-MC", [confirm])
    change = next(
        c for c in body["preview"]["student_changes"] if c["student_id"] == "S1"
    )
    assert change["fields"]["confirmed_seconds"] == {"before": 0, "after": 4 * 3600}
    assert change["fields"]["pending_seconds"] == {"before": 4 * 3600, "after": 0}
    assert body["preview"]["freezes_affected"][0]["freeze_id"] == "F-01"

    _approve(client, pv, "B-MC")
    progress = client.get(f"/api/plans/{pv}/students/S1/progress").json()
    assert progress["confirmed_seconds"] == 4 * 3600
    assert progress["pending_seconds"] == 0

    frozen = client.get(f"/api/plans/{pv}/freezes/F-01").json()
    assert frozen["students"][0]["pending_seconds"] == 4 * 3600


def test_cross_version_batches_are_isolated(client):
    _create_plan(client, SHANGHAI_PLAN)
    ny_plan = dict(NY_PLAN)
    ny_plan["required_seconds"] = 3600
    _create_plan(client, ny_plan)
    sh = SHANGHAI_PLAN["plan_version"]
    ny = ny_plan["plan_version"]

    events = [
        _checkin(
            "E-501",
            "S1",
            start="2024-03-15T08:00:00+08:00",
            end="2024-03-15T10:30:00+08:00",
        )
    ]
    sh_batch = _upload(client, sh, "B-1", events)
    ny_batch = _upload(client, ny, "B-1", events)

    # 同一批次在两个方案下按各自规则模拟：2.5 小时只够纽约方案的门槛。
    sh_after = sh_batch["preview"]["student_changes"][0]["after"]
    ny_after = ny_batch["preview"]["student_changes"][0]["after"]
    assert sh_after["meets_requirement"] is False
    assert ny_after["meets_requirement"] is True

    # 只批准上海方案的批次，纽约方案的批次与事件流不受影响。
    _approve(client, sh, "B-1")
    assert client.get(f"/api/plans/{ny}/inbox/batches/B-1").json()["status"] == "pending"
    assert client.get(f"/api/plans/{ny}/snapshot").json()["students"] == []
    sh_live = client.get(f"/api/plans/{sh}/snapshot").json()
    assert sh_live["students"][0]["total_seconds"] == 9000

    # 同一 event_id 可以在另一方案的正式事件流中独立存在。
    result = _import_events(client, ny, events)
    assert result["accepted"] == 1


def test_concurrent_approval_commits_exactly_once(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _upload(client, pv, "B-RACE", [_checkin(f"E-3{i:02d}", "S1") for i in range(10)])

    results: list[tuple[bool, list[str]]] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def _approve():
        session = TestSessionLocal()
        try:
            out, committed = services.approve_inbox_batch(
                session, plan_version=pv, batch_id="B-RACE", actor="t", note=""
            )
            with lock:
                results.append((committed, out["decision"]["committed_event_ids"]))
        except Exception as exc:  # pragma: no cover - 失败时暴露
            with lock:
                errors.append(exc)
        finally:
            session.close()

    threads = [threading.Thread(target=_approve) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    # 只有一个线程真正执行了写入，其余拿到同一份已批准结果。
    assert sum(1 for committed, _ in results if committed) == 1
    decisions = {tuple(sorted(ids)) for _, ids in results}
    assert len(decisions) == 1
    assert len(decisions.pop()) == 10

    live = client.get(f"/api/plans/{pv}/snapshot").json()
    assert len(live["students"][0]["checkins"]) == 10


def test_concurrent_approve_and_reject_settle_in_one_state(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _upload(client, pv, "B-RACE2", [_checkin("E-401", "S1")])

    wins: list[str] = []
    conflicts: list[str] = []
    lock = threading.Lock()

    def _approve():
        session = TestSessionLocal()
        try:
            services.approve_inbox_batch(session, plan_version=pv, batch_id="B-RACE2")
            with lock:
                wins.append("approved")
        except services.BatchStateConflictError:
            with lock:
                conflicts.append("approve")
        finally:
            session.close()

    def _reject():
        session = TestSessionLocal()
        try:
            services.reject_inbox_batch(
                session, plan_version=pv, batch_id="B-RACE2", reason="重复批次"
            )
            with lock:
                wins.append("rejected")
        except services.BatchStateConflictError:
            with lock:
                conflicts.append("reject")
        finally:
            session.close()

    threads = [
        threading.Thread(target=_approve),
        threading.Thread(target=_reject),
        threading.Thread(target=_approve),
        threading.Thread(target=_reject),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = client.get(f"/api/plans/{pv}/inbox/batches/B-RACE2").json()
    live = client.get(f"/api/plans/{pv}/snapshot").json()
    if final["status"] == "approved":
        assert "rejected" not in wins
        assert len(live["students"]) == 1
    else:
        assert final["status"] == "rejected"
        assert "approved" not in wins
        assert live["students"] == []
    # 至少有一方胜出，失败方全部收到状态冲突。
    assert len(wins) >= 1
    assert len(conflicts) >= 1


def test_state_survives_restart(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _import_events(client, pv, [_checkin("E-700", "S1")])
    _upload(
        client,
        pv,
        "B-APPROVED",
        [
            _checkin(
                "E-701",
                "S1",
                start="2024-03-16T08:00:00+08:00",
                end="2024-03-16T10:00:00+08:00",
            )
        ],
    )
    _approve(client, pv, "B-APPROVED")
    _upload(client, pv, "B-REJECTED", [_checkin("E-702", "S2")])
    _reject(client, pv, "B-REJECTED", reason="来源未授权")
    _upload(client, pv, "B-PENDING", [_checkin("E-703", "S3")])

    # 模拟重启：在同一个数据库文件上新建引擎与会话。
    engine2 = create_engine(
        "sqlite:///./practice_hours_test.db",
        connect_args={"check_same_thread": False, "timeout": 30},
        future=True,
    )
    session2_factory = sessionmaker(
        bind=engine2, autoflush=False, autocommit=False, future=True
    )
    try:
        session = session2_factory()
        try:
            approved = services.get_inbox_batch_view(session, pv, "B-APPROVED")
            assert approved["status"] == "approved"
            assert approved["decision"]["committed_event_ids"] == ["E-701"]

            rejected = services.get_inbox_batch_view(session, pv, "B-REJECTED")
            assert rejected["status"] == "rejected"
            assert rejected["decision"]["reason"] == "来源未授权"

            snap = services.current_snapshot(session, pv)
            totals = {s["student_id"]: s["total_seconds"] for s in snap.students}
            assert totals == {"S1": 4 * 3600}

            # 待审批批次在重启后仍可正常批准并写入事件流。
            _, committed = services.approve_inbox_batch(
                session, plan_version=pv, batch_id="B-PENDING"
            )
            assert committed is True
            snap = services.current_snapshot(session, pv)
            assert {s["student_id"] for s in snap.students} == {"S1", "S3"}
        finally:
            session.close()
    finally:
        engine2.dispose()
