# 部署、发布、恢复与事故手册

## 首次启动 / 日常停止

1. `make setup && make verify && make up`，检查 Compose 和 Flink REST。
2. `make smoke`，初始 live release 只有业务对账通过才激活。
3. 查看 Grafana、`/v1/quality` 样本和 `/v1/reconcile`；单看 RUNNING 不足以验收。
4. 暂停实验可停止 collector 和 worker；`make down` 保留卷，但再次提交已产生输出的作业必须从保存点恢复，不能重新以空状态使用同一 release。

开发环境的 PostgreSQL 账号 / ClickHouse 密码保存在 Compose 中，均为本地合成数据默认值。对外部署须替换配置；CI 不使用任何外部账号。

## Savepoint 与状态升级

运行 `.venv/bin/python scripts/drills.py --scenario savepoint-rescale` 会对归因作业创建持久 savepoint，取消旧作业，上传本地新 JAR，用 `allowNonRestoredState=false` 从保存点恢复并行度 3，检查实际并行度与恢复来源，发送数据并对账。原有清洗作业继续运行。

手动升级两作业时，对每个 job 调用 `POST /jobs/<id>/savepoints`，保存返回路径和 job ID；先停止 collector，等待未完成事务、归档和 sink 收敛。然后保存点、停止作业，替换镜像 / JAR，调用 `POST /jars/<id>/run`，明确 `entryClass`、`savepointPath`、`allowNonRestoredState=false`。恢复后检查 `/jobs/<id>/checkpoints` 的 restored 记录、业务键和 oracle。

固定 UID 和状态描述符在 Java 代码中带 `-v1`。有状态 JSON 使用 Flink String serializer，计时器状态使用 Long / Boolean；可加新状态，但修改已有描述符或序列化形式需要专门迁移测试。不可使用“忽略未恢复状态”来掩盖不兼容，也不能在升级时同时更换 Flink 与 Kafka connector 版本。

本地 session cluster 的 JobManager 未配置 HA。worker 中断可以从 JobManager 管理的持久 checkpoint 恢复；JobManager 被删除后需操作者提供已记录的 savepoint / checkpoint，不能根据容器名称推断状态安全。新建历史全量任务使用新 release。

已有 release 缺少运行作业时，bootstrap 会拒绝空状态提交。可在启动时指定：

```bash
CLEAN_SAVEPOINT='file:/opt/flink/state/savepoints/清洗保存点' \
ATTRIBUTION_SAVEPOINT='file:/opt/flink/state/savepoints/归因保存点' make up
```

作业失败但 JobManager 仍在时，可直接使用保留 checkpoint：

```bash
.venv/bin/python scripts/restore_job.py --job-id <失败作业ID> \
  --entry-class io.adpulse.AttributionJob --parallelism 3
```

JobManager 已重建时额外传 `--state-path <持久路径>`。恢复脚本要求新 checkpoint 完成；仍需再次做业务对账。

## 规则与修正版本

1. 复制 `contracts/rules-v1.json`，改 rule_version，填写 rollback_version，保留明确 policy_version 和有效边界。仅声明式字段映射、过滤和阈值，不执行任意脚本。
2. `adpulse replay --from-s3 --rules <文件> --release <新ID> --publish`：检查归档、冻结规则 hash、创建独立 topic、发布完整值、验证 sink 后标记 validated。
3. 比较两个版本的事件数、指标键、关联原因与维度变动。规则自身 oracle 一致不能证明错误过滤合理。
4. `adpulse activate <新ID> --reason '<变更原因>'` 原子切换；回退用相同命令指定已验证旧 ID。审计在 PostgreSQL release_audit 中。
5. 每日可执行 `scripts/daily_replay.py` 生成受影响历史的完整回放候选；默认不自动推广。

## 定位顺序

| 现象 | 先看证据 | 处置 / 验收 |
| --- | --- | --- |
| 新鲜度上升 | per-receipt P95、sink heartbeat、offset lag、Flink backpressure | 从 sink 逆向检查；恢复后核对输入清单与逻辑结果 |
| 转化未匹配 | pending 数、reason、click_id 原始轨迹、watermark | 辨别等待、非法时间关系和缺失；超时后到达走新 release |
| schema 异常 | rule_version、error_code、sample_ref、quarantine raw | 修正规则 / SDK 映射后回放，保留原始错误样本 |
| 重复率增高 | 两种重复码、client_batch_id、event_id / 业务键 | 不把重试当作业务增长；核对重复边界和持久回放 |
| 分区空闲 | 各 operator watermark、Kafka partition 实际流量 | 已启用 idleness；恢复时监控迟到修正信号 |
| 热点活动 | subtask busy / backpressure、唯一 click_id 分布 | 关联键不能随机加盐；可结合指标聚合可分盐再汇总 |
| checkpoint 失败 | checkpoint duration / bytes / exception、磁盘与存储延迟 | 先保存证据，再考虑资源 / 间隔调整；恢复须对账 |
| CDC 断开 | connector tasks、replication slot retained_bytes、源端 WAL | 恢复 connector 并核对 create/update/delete；不盲删 slot |
| 归档落后 | archive heartbeat、manifest 和接收清单缺口 | 停止新版本推广，恢复归档，完成 checksum 与覆盖核验 |
| 数据量仅新规则骤降 | release 对比、冻结规则 hash | 停止推广 / 指针回退，独立回放修正 |
| Kafka 历史不足 | 目标 offset 可读范围、已验收的 S3 manifest | 新 release 从归档重建，报告无法覆盖的接收范围 |

告警按 alertname / job 聚合；critical 抑制相同 job 的 warning，避免依赖故障重复通知。本地 Alertmanager 接收器不发送外部消息，只显示 firing / resolved 状态；外部 webhook 须按目标环境配置。

## 自动演练

`scripts/drills.py --scenario all` 保存每类事故的结构化报告。包含真实组件上的重复、坏 schema、转化先到、sink 提交前崩溃、worker SIGKILL、归档中断、错误规则发布和回退、CDC 暂停恢复及删除、savepoint 扩容；事件时间长边界另由真实 Flink harness 验证。

不要在其他项目的 Compose 文件下运行演练脚本。脚本固定本项目 `deployment/compose.yaml` 和 adpulse 服务，不删除数据卷。涉及 source 数据的注入全部为合成标识。

## 已验证的运行时迁移

`scripts/migrate_runtime.py` 用于旧版本仍正常运行时的一次性迁移。先保存旧 JAR/Compose/Dockerfile，再构建新 JAR 和镜像。脚本确认运行中的 ClickHouse 正在使用指定 source volume、target 不存在、两作业健康，才暂停 collector。

```bash
make java-test
docker compose -f deployment/compose.yaml build jobmanager
.venv/bin/python scripts/migrate_runtime.py \
  --source-volume adpulse_clickhouse-data \
  --target-volume adpulse_clickhouse-data-v26
```

已迁移过的环境不要重跑这个命令；默认目标已存在时会拒绝。脚本暂停接入 → 完整对账 → 顺序保存并取消两作业 → 停止 sink/API/数据库 → 冷拷贝到新卷 → 启动目标版本 → 五表指纹校验 → strict 恢复 → 新 checkpoint → 新事件对账。每一步写 journal，失败时不会自动开放接入。

回退先保持 collector/sink 停止，保存当前失败现场；使用旧镜像和未被升级写入的原卷，恢复与该边界配套的旧 JAR/两个保存点。**禁止把 24.8 二进制指向已由 26.3 写入的新卷。** 若新版本已经接收并确认更多数据，旧卷缺少后续写入，不能仅切卷宣称恢复成功：必须从 Kafka/S3 补齐并核验接收覆盖及业务结果，或创建新的完整 replay release。

本轮实测了冷拷贝、版本切换、状态恢复和业务对账；保留了回退材料，未执行一次带新增流量的完整降级回退实验。具体保存点及卷名见 `docs/evidence/operations/migration.json`。

## 隔离 quorum 实验

`.venv/bin/python scripts/quorum_drill.py` 只操作 `adpulse-quorum` Compose 项目，collector 在 28088；broker 在 29092–29094。实验包含有意 SIGKILL，结束会启动实验服务，随后可 `docker compose -f deployment/compose.quorum.yaml stop` 停止，保留数据。不要用 `down -v` 删除证据卷。该实验不会将默认业务管道迁移为 RF=3。


## 独立容量配置

默认 Compose 使用每 TaskManager 2 GiB 的 Flink 进程预算、两作业并行度各 2，供初始启动和托管 CI。`deployment/capacity.env` 明确选择每 TaskManager **8 GiB**、清洗并行度 2、归因并行度 6，两个 worker 共 8 个 slot。它是有额外资源成本的本地容量配置；Docker 容器没有因此获得独立物理机或生产 SLA。

对已有 RUNNING 作业切换时使用保留状态的操作：

```bash
.venv/bin/python scripts/capacity_restore.py --output artifacts/capacity-restore
# 此后对该运行环境执行 Compose up/config 时显式使用同一配置：
docker compose --env-file deployment/capacity.env -f deployment/compose.yaml config
```

脚本暂停接入、保存并取消两项作业、重建 Flink 容器、严格恢复 2/6 并行度并等待新 checkpoint。失败保留 journal 和保存点，不回落为空状态。恢复后必须执行完整业务对账，再开始新测量。已有输出的环境不要直接运行不带容量配置的 `make up`，它会改变进程配置；后续需要重新建容器时先保存状态。新空白环境可用 `docker compose --env-file deployment/capacity.env -f deployment/compose.yaml up -d --build --wait`，但已有 release 仍受禁止空状态提交的保护。

sink 最大批次为 2,000 条 Kafka 消息，poll 最多等待一秒后也会返回部分批次。身份/路由查询按 JSON 转义后 UTF-8 大小拆为最多 64 KiB 参数；完整批次的 offset/hash 与 key/partition 冲突检查保留，只有全部同步写入成功后才提交位点。`scripts/sink_acceptance.py` 验证真实数据库的大参数、重试与冲突边界；`scripts/drills.py --scenario sink-replay` 验证实际 Kafka 位点边界。worker 故障目标由当前 RUNNING 子任务分配决定，不能假定某个固定容器必然承载任务。
