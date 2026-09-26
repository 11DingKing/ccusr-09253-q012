# 实训学时合规与冻结服务

该服务汇聚学员签到、导师确认和请假修正事件，按培养方案与时区重放学时状态，并保存可追溯的学期冻结快照。项目还提供导师分配、证明材料、豁免复核、规则版本、名额、通知和数据留存等领域模块，供后续业务扩展时复用统一的状态与审计约束。

## 隔离收件箱

外部签到来源可能在冻结前补送数周前的迟到事件。这类批次先进入隔离收件箱，完成结构校验、去重（批次内以及对照正式事件流）与影响模拟，教务人员确认影响范围后再决定是否接纳：

- `POST /api/plans/{plan_version}/inbox/batches` 上传批次并返回校验报告与影响模拟；重复提交同一 `batch_id` 返回原处理结果，内容不同则返回 409。
- `GET /api/plans/{plan_version}/inbox/batches/{batch_id}` 预览批次：校验报告、影响模拟（会影响哪些学生与冻结快照）以及审批结果。
- `GET /api/plans/{plan_version}/inbox/batches/{batch_id}/diff` 查询批次的影响差异。
- `POST /api/plans/{plan_version}/inbox/batches/{batch_id}/approve` 批准批次，通过校验的事件与状态迁移在同一事务中原子写入正式事件流；重复批准返回原处理结果。
- `POST /api/plans/{plan_version}/inbox/batches/{batch_id}/reject` 拒绝批次，保留摘要与理由但不参与重放；重复拒绝返回原处理结果。

批次状态持久化在 `inbox_batches` 表中，服务重启后待审批、已批准与已拒绝的批次均保持原状。

## 运行方式

默认数据保存在项目目录的 SQLite 文件中。安装依赖后执行 `uvicorn app.main:app --host 127.0.0.1 --port 8000`，健康检查地址为 `/health`，业务接口位于 `/api`。

## 测试

```bash
python3 -m pytest -q
```

## 编译检查

```bash
python3 -m compileall -q app tests
```

测试覆盖事件幂等导入、跨时区与跨日学时合并、实习确认、负向修正、冻结快照和差异查询，以及隔离收件箱的上传校验、大批量部分冲突、并发审批、跨版本规则与重启恢复；运行过程中不需要单独的数据库或网络服务。
