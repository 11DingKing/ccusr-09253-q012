"""隔离收件箱：迟到批次的上传、预览、批准、拒绝与差异查询。"""

from __future__ import annotations

import threading

import pytest

from tests.conftest import NY_PLAN, SHANGHAI_PLAN


def _create_plan(client, plan=SHANGHAI_PLAN):
    resp = client.post("/api/plans", json=plan)
    assert resp.status_code == 201, resp.text


def _checkin(eid, student, start, end, activity_type="regular"):
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


def _one_hour_checkin(eid, student, day, hour):
    return _checkin(
        eid,
        student,
        f"2024-04-{day:02d}T{hour:02d}:00:00+08:00",
        f"2024-04-{day:02d}T{hour + 1:02d}:00:00+08:00",
    )


def _upload(client, plan_version, batch_id, events, source="external-signin"):
    return client.post(
        f"/api/plans/{plan_version}/inbox",
        json={"batch_id": batch_id, "source": source, "events": events},
    )


def test_upload_validates_dedupes_and_previews_impact(client):
    """大批量部分冲突：结构校验、三类去重与影响预览，正式事件流不受影响。"""
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]

    # 正式事件流已有 3 条 S1 的签到（满足 3 小时要求），并冻结 F-01。
    client.post(
        f"/api/plans/{pv}/events",
        json={
            "events": [
                _checkin(
                    f"E-00{i}",
                    "S1",
                    f"2024-03-1{i}T08:00:00+08:00",
                    f"2024-03-1{i}T09:00:00+08:00",
                )
                for i in (1, 2, 3)
            ]
        },
    )
    client.post(f"/api/plans/{pv}/freezes/F-01", json={})

    events = []
    # 50 条有效唯一事件，分布在 S1..S5。
    for i in range(50):
        events.append(
            _one_hour_checkin(f"L-{i:03d}", f"S{(i % 5) + 1}", day=1 + i // 8, hour=8 + i % 8)
        )
    # 3 条批次内重复。
    events += [
        _one_hour_checkin("L-000", "S1", 1, 8),
        _one_hour_checkin("L-001", "S2", 1, 9),
        _one_hour_checkin("L-002", "S3", 1, 10),
    ]
    # 2 条与正式事件流冲突。
    events += [
        _checkin("E-001", "S1", "2024-03-11T08:00:00+08:00", "2024-03-11T09:00:00+08:00"),
        _checkin("E-002", "S1", "2024-03-12T08:00:00+08:00", "2024-03-12T09:00:00+08:00"),
    ]
    # 5 条结构非法。
    events += [
        {"event_id": "BAD-1", "event_type": "checkin", "student_id": "S1",
         "payload": {"check_in_at": "2024-04-01 08:00:00", "check_out_at": "2024-04-01 09:00:00"}},
        {"event_id": "BAD-2", "event_type": "teleport", "student_id": "S1", "payload": {}},
        {"event_id": "BAD-3", "event_type": "checkin", "student_id": "S1",
         "payload": {"check_in_at": "2024-04-01T09:00:00+08:00", "check_out_at": "2024-04-01T08:00:00+08:00"}},
        {"event_id": "BAD-4", "event_type": "checkin",
         "payload": {"check_in_at": "2024-04-01T08:00:00+08:00", "check_out_at": "2024-04-01T09:00:00+08:00"}},
        {"event_id": "BAD-5", "event_type": "mentor_confirm", "student_id": "S1",
         "payload": {"checkin_event_id": ""}},
    ]

    resp = _upload(client, pv, "B-BIG", events)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["status"] == "pending"
    assert body["returned_existing"] is False

    summary = body["summary"]
    assert summary["submitted"] == 60
    assert summary["valid_unique"] == 50
    assert summary["rejected_count"] == 5
    assert summary["duplicate_count"] == 5
    assert {r["event_id"] for r in summary["rejected"]} == {
        "BAD-1", "BAD-2", "BAD-3", "BAD-4", "BAD-5",
    }
    dup_reasons = {}
    for d in summary["duplicates"]:
        dup_reasons.setdefault(d["reason"], set()).add(d["event_id"])
    assert dup_reasons["duplicate_in_batch"] == {"L-000", "L-001", "L-002"}
    assert dup_reasons["already_accepted"] == {"E-001", "E-002"}

    # 影响预览：5 个学生受影响，冻结快照 F-01 被点名但保持不可变。
    impact = body["impact"]
    assert impact["students_affected"] == 5
    assert impact["submitted_student_ids"] == ["S1", "S2", "S3", "S4", "S5"]
    assert impact["frozen_snapshots"] == [
        {"freeze_id": "F-01", "event_cutoff_id": "E-003"}
    ]
    assert impact["frozen_snapshots_remain_immutable"] is True
    s1_change = next(
        c for c in impact["student_changes"] if c["student_id"] == "S1"
    )
    assert s1_change["fields"]["total_seconds"]["before"] == 10800
    assert s1_change["fields"]["total_seconds"]["after"] == 10800 + 10 * 3600

    # 预览接口可重复查询，且正式事件流与学生进度均未改变。
    preview = client.get(f"/api/plans/{pv}/inbox/B-BIG").json()
    assert preview["impact"]["students_affected"] == 5
    live = client.get(f"/api/plans/{pv}/snapshot").json()
    assert len(live["students"]) == 1
    progress = client.get(f"/api/plans/{pv}/students/S1/progress").json()
    assert progress["total_seconds"] == 10800


def test_duplicate_upload_returns_original_result(client):
    """重复提交同一 batch_id 返回原处理结果，不重复计数。"""
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    events = [_one_hour_checkin("L-001", "S1", 1, 8)]

    first = _upload(client, pv, "B-DUP", events)
    assert first.status_code == 201
    second = _upload(client, pv, "B-DUP", events)
    assert second.status_code == 200
    assert second.json()["returned_existing"] is True
    assert second.json()["summary"] == first.json()["summary"]
    assert second.json()["status"] == "pending"

    # 批准后重复上传仍返回原（已批准）结果。
    client.post(f"/api/plans/{pv}/inbox/B-DUP/approve", json={"actor": "registrar"})
    third = _upload(client, pv, "B-DUP", events)
    assert third.status_code == 200
    assert third.json()["status"] == "approved"
    assert third.json()["returned_existing"] is True


def test_approve_writes_atomically_and_freeze_stays_immutable(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    client.post(
        f"/api/plans/{pv}/events",
        json={
            "events": [
                _checkin("E-01", "S1", "2024-03-15T08:00:00+08:00", "2024-03-15T10:00:00+08:00")
            ]
        },
    )
    client.post(f"/api/plans/{pv}/freezes/F-01", json={})

    _upload(
        client,
        pv,
        "B-APPROVE",
        [
            _one_hour_checkin("L-01", "S1", 2, 8),
            {
                "event_id": "L-02",
                "event_type": "leave_correction",
                "student_id": "S1",
                "payload": {"adjustment_seconds": 600, "reason": "late make-up"},
            },
        ],
    )
    resp = client.post(
        f"/api/plans/{pv}/inbox/B-APPROVE/approve", json={"actor": "registrar"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "approved"
    assert body["decision"]["action"] == "approved"
    assert body["decision"]["actor"] == "registrar"
    assert sorted(body["decision"]["accepted"]) == ["L-01", "L-02"]
    assert body["decision"]["rules_changed"] is False

    # 事件原子进入正式流：快照一次性反映整个批次。
    live = client.get(f"/api/plans/{pv}/snapshot").json()
    assert live["students"][0]["total_seconds"] == 7200 + 3600 + 600
    assert live["event_cutoff_id"] is None
    # 冻结快照保持不可变。
    frozen = client.get(f"/api/plans/{pv}/freezes/F-01").json()
    assert frozen["students"][0]["total_seconds"] == 7200

    # 差异查询反映批准时的实际写入。
    diff = client.get(f"/api/plans/{pv}/inbox/B-APPROVE/diff").json()
    assert diff["status"] == "approved"
    assert diff["students_affected"] == 1
    change = diff["student_changes"][0]
    assert change["fields"]["total_seconds"]["before"] == 7200
    assert change["fields"]["total_seconds"]["after"] == 7200 + 3600 + 600

    # 已批准批次不可再次批准。
    again = client.post(
        f"/api/plans/{pv}/inbox/B-APPROVE/approve", json={"actor": "registrar"}
    )
    assert again.status_code == 409


def test_reject_keeps_summary_but_never_replays(client):
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _upload(client, pv, "B-REJ", [_one_hour_checkin("L-01", "S9", 2, 8)])

    # 缺少理由的拒绝请求不合法。
    bad = client.post(
        f"/api/plans/{pv}/inbox/B-REJ/reject",
        json={"actor": "registrar", "reason": "  "},
    )
    assert bad.status_code == 422

    resp = client.post(
        f"/api/plans/{pv}/inbox/B-REJ/reject",
        json={"actor": "registrar", "reason": "source not trusted"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "rejected"
    assert body["decision"]["reason"] == "source not trusted"
    # 摘要与影响预览保留供审计。
    assert body["summary"]["valid_unique"] == 1
    assert body["impact"]["students_affected"] == 1

    # 被拒批次的事件永不进入重放。
    live = client.get(f"/api/plans/{pv}/snapshot").json()
    assert live["students"] == []
    diff = client.get(f"/api/plans/{pv}/inbox/B-REJ/diff").json()
    assert diff["status"] == "rejected"
    assert diff["decision"]["reason"] == "source not trusted"

    # 被拒批次不可再批准或再次拒绝。
    assert client.post(
        f"/api/plans/{pv}/inbox/B-REJ/approve", json={"actor": "registrar"}
    ).status_code == 409
    assert client.post(
        f"/api/plans/{pv}/inbox/B-REJ/reject",
        json={"actor": "registrar", "reason": "again"},
    ).status_code == 409


def test_pending_batches_dedupe_against_each_other(client):
    """跨批次冲突：后上传的批次去重，先批准者胜出，事件只入账一次。"""
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    shared = _one_hour_checkin("L-SHARED", "S1", 2, 8)

    _upload(client, pv, "B-A", [shared])
    resp_b = _upload(client, pv, "B-B", [shared, _one_hour_checkin("L-B1", "S2", 3, 8)])
    summary_b = resp_b.json()["summary"]
    assert summary_b["valid_unique"] == 1
    assert summary_b["duplicates"] == [
        {"event_id": "L-SHARED", "reason": "pending_in_another_batch"}
    ]

    client.post(f"/api/plans/{pv}/inbox/B-A/approve", json={"actor": "registrar"})
    # B-B 的候选集在上传时已排除 L-SHARED，预览只覆盖 S2。
    preview_b = client.get(f"/api/plans/{pv}/inbox/B-B").json()
    assert preview_b["impact"]["submitted_student_ids"] == ["S2"]

    client.post(f"/api/plans/{pv}/inbox/B-B/approve", json={"actor": "registrar"})
    live = client.get(f"/api/plans/{pv}/snapshot").json()
    totals = {s["student_id"]: s["total_seconds"] for s in live["students"]}
    assert totals == {"S1": 3600, "S2": 3600}


def test_direct_import_makes_pending_preview_stale(client):
    """上传后正式流直接入账同 id 事件：预览标记过期，批准时记为迟到重复。"""
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _upload(
        client,
        pv,
        "B-C",
        [_one_hour_checkin("L-C1", "S1", 2, 8), _one_hour_checkin("L-C2", "S2", 3, 8)],
    )
    # 同一事件经正式通道直接入账。
    client.post(
        f"/api/plans/{pv}/events",
        json={"events": [_one_hour_checkin("L-C1", "S1", 2, 8)]},
    )

    preview = client.get(f"/api/plans/{pv}/inbox/B-C").json()
    assert preview["impact"]["stale"] is True
    assert preview["impact"]["submitted_student_ids"] == ["S2"]

    approved = client.post(
        f"/api/plans/{pv}/inbox/B-C/approve", json={"actor": "registrar"}
    ).json()
    assert approved["status"] == "approved"
    assert approved["decision"]["accepted"] == ["L-C2"]
    assert approved["decision"]["duplicates"] == [
        {"event_id": "L-C1", "reason": "accepted_elsewhile"}
    ]
    live = client.get(f"/api/plans/{pv}/snapshot").json()
    totals = {s["student_id"]: s["total_seconds"] for s in live["students"]}
    assert totals == {"S1": 3600, "S2": 3600}


def test_concurrent_approval_only_one_wins(client):
    """并发审批：条件更新保证只有一个请求真正写入，其余收到冲突。"""
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _upload(
        client,
        pv,
        "B-CONC",
        [_one_hour_checkin(f"L-{i}", "S1", 1 + i, 8) for i in range(5)],
    )

    from app import services
    from tests.conftest import TestSessionLocal

    outcomes: list[str] = []
    lock = threading.Lock()

    def _approve():
        session = TestSessionLocal()
        try:
            try:
                services.approve_inbox_batch(
                    session, plan_version=pv, batch_id="B-CONC", actor="registrar"
                )
                outcome = "approved"
            except services.InboxBatchAlreadyDecidedError:
                outcome = "conflict"
            with lock:
                outcomes.append(outcome)
        finally:
            session.close()

    threads = [threading.Thread(target=_approve) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes.count("approved") == 1
    assert outcomes.count("conflict") == 3

    # 每条事件恰好写入一次。
    live = client.get(f"/api/plans/{pv}/snapshot").json()
    assert live["students"][0]["total_seconds"] == 5 * 3600
    view = client.get(f"/api/plans/{pv}/inbox/B-CONC").json()
    assert view["status"] == "approved"
    assert len(view["decision"]["accepted"]) == 5


def test_cross_plan_rules_and_rule_drift(client):
    """跨版本规则：批次按各自方案的时区与要求模拟；规则变更在批准时被标记。"""
    _create_plan(client)
    _create_plan(client, NY_PLAN)
    sh, ny = SHANGHAI_PLAN["plan_version"], NY_PLAN["plan_version"]

    # 同一 event_id 可在不同方案下各自隔离审批。
    _upload(client, sh, "B-SH", [_one_hour_checkin("L-X", "S1", 2, 8)])
    _upload(
        client,
        ny,
        "B-NY",
        [_checkin("L-X", "S1", "2024-04-02T08:00:00-04:00", "2024-04-02T09:00:00-04:00")],
    )
    client.post(f"/api/plans/{sh}/inbox/B-SH/approve", json={"actor": "registrar"})
    ny_view = client.get(f"/api/plans/{ny}/inbox/B-NY").json()
    assert ny_view["status"] == "pending"
    assert ny_view["impact"]["rules"]["timezone"] == "America/New_York"
    assert ny_view["impact"]["stale"] is False
    ny_live = client.get(f"/api/plans/{ny}/snapshot").json()
    assert ny_live["students"] == []

    # 规则漂移：上传后调整 required_seconds，批准时记录两个签名。
    _upload(client, ny, "B-DRIFT", [_checkin(
        "L-D", "S2", "2024-04-03T08:00:00-04:00", "2024-04-03T09:00:00-04:00"
    )])
    changed_plan = dict(NY_PLAN, required_seconds=999999)
    assert client.post("/api/plans", json=changed_plan).status_code == 201
    preview = client.get(f"/api/plans/{ny}/inbox/B-DRIFT").json()
    assert preview["impact"]["stale"] is True
    approved = client.post(
        f"/api/plans/{ny}/inbox/B-DRIFT/approve", json={"actor": "registrar"}
    ).json()
    assert approved["decision"]["rules_changed"] is True
    assert (
        approved["decision"]["preview_rule_signature"]
        != approved["decision"]["applied_rule_signature"]
    )
    assert approved["impact"]["stale"] is True


def test_inbox_requires_existing_plan_and_batch(client):
    resp = client.post(
        "/api/plans/NOPE/inbox",
        json={"batch_id": "B-1", "events": []},
    )
    assert resp.status_code == 404
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    assert client.get(f"/api/plans/{pv}/inbox/B-MISSING").status_code == 404
    assert client.get(f"/api/plans/{pv}/inbox/B-MISSING/diff").status_code == 404
    assert client.post(
        f"/api/plans/{pv}/inbox/B-MISSING/approve", json={"actor": "r"}
    ).status_code == 404
    assert client.post(
        f"/api/plans/{pv}/inbox/B-MISSING/reject",
        json={"actor": "r", "reason": "x"},
    ).status_code == 404


def test_restart_recovers_pending_and_decided_batches(client):
    """重启恢复：新连接（模拟进程重启）后，待审批与已决策批次状态完整。"""
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _upload(client, pv, "B-PENDING", [_one_hour_checkin("L-01", "S1", 2, 8)])
    _upload(client, pv, "B-DONE", [_one_hour_checkin("L-02", "S2", 3, 8)])
    client.post(f"/api/plans/{pv}/inbox/B-DONE/approve", json={"actor": "registrar"})
    client.post(
        f"/api/plans/{pv}/inbox/B-PENDING/reject",
        json={"actor": "registrar", "reason": "duplicate source"},
    )
    _upload(client, pv, "B-STILL", [_one_hour_checkin("L-03", "S3", 4, 8)])

    # 用全新的引擎与会话模拟服务重启（同一 SQLite 文件）。
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from app import services

    engine2 = create_engine(
        "sqlite:///./practice_hours_test.db",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    Session2 = sessionmaker(bind=engine2, autoflush=False, autocommit=False, future=True)
    session = Session2()
    try:
        rejected = services.get_inbox_batch_view(session, pv, "B-PENDING")
        assert rejected["status"] == "rejected"
        assert rejected["decision"]["reason"] == "duplicate source"

        approved = services.get_inbox_batch_view(session, pv, "B-DONE")
        assert approved["status"] == "approved"
        assert approved["decision"]["accepted"] == ["L-02"]

        pending = services.get_inbox_batch_view(session, pv, "B-STILL")
        assert pending["status"] == "pending"
        assert pending["impact"]["students_affected"] == 1

        # 正式事件流只包含已批准批次的事件。
        snap = services.current_snapshot(session, pv)
        assert [s["student_id"] for s in snap.students] == ["S2"]

        # 重启后仍可完成审批。
        result = services.approve_inbox_batch(
            session, plan_version=pv, batch_id="B-STILL", actor="registrar"
        )
        assert result["status"] == "approved"
        snap = services.current_snapshot(session, pv)
        assert sorted(s["student_id"] for s in snap.students) == ["S2", "S3"]
    finally:
        session.close()
        engine2.dispose()


def test_approval_rolls_back_on_crash(client, db, monkeypatch):
    """审批中途崩溃：事务回滚，批次保持待审批，可安全重试。"""
    _create_plan(client)
    pv = SHANGHAI_PLAN["plan_version"]
    _upload(client, pv, "B-CRASH", [_one_hour_checkin("L-01", "S1", 2, 8)])

    from app import services

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated crash mid-approval")

    monkeypatch.setattr(services, "insert_events_in_transaction", _boom)
    with pytest.raises(RuntimeError):
        services.approve_inbox_batch(
            db, plan_version=pv, batch_id="B-CRASH", actor="registrar"
        )

    view = services.get_inbox_batch_view(db, pv, "B-CRASH")
    assert view["status"] == "pending"
    assert services.current_snapshot(db, pv).students == []

    monkeypatch.undo()
    result = services.approve_inbox_batch(
        db, plan_version=pv, batch_id="B-CRASH", actor="registrar"
    )
    assert result["status"] == "approved"
    snap = services.current_snapshot(db, pv)
    assert snap.students[0]["total_seconds"] == 3600
