# AdPulse · 广告行为实时数仓与质量治理平台

[English overview](README.en.md) · [真实演示录屏](docs/demo/adpulse-local-demo.mp4) · [运行时迁移与故障验证](docs/operations-validation.md)

根据 [`docs/project-brief.md`](docs/project-brief.md) 实现的可运行数据工程项目。输入仅为合成广告事件；不包含真实广告或用户数据。

真实链路：**Python generator → HTTP collector → Kafka → Flink SQL / Java → Kafka 完整值结果 → ClickHouse**。另有 PostgreSQL / Debezium 历史维度、S3 兼容归档、独立 Python oracle、发布版本校正、Prometheus / Grafana。

**0.3.0 更新**：新增磁盘对账、归档 receipt 索引、带过期检查的后台检查快照及每进程查询并发限制。固定 2 CPU / 4 GiB、20 万条输入的 3 组对照中，oracle 进程峰值 RSS 中位数降低 86.3%，代价为 2.29 倍耗时与约 605 MiB 临时磁盘。trace / 监控服务成本与预计算成本分开记录，见 [本轮实现与实测](docs/scaling-validation.md)。完整历史 **3,833,704** 条输入在 4 GiB 容器中全量核对通过，91 项回归和最新托管 Docker CI 通过。原有 [分页接口契约](docs/query-api.md) 和 [岗位 / 技术依据](docs/market-and-technology-2026-09-06.md)保留。

## 快速启动

需要 Python 3.12、Java 17 或可编译 Java 17 的 JDK、Maven 3.9、uv、Docker Engine / Compose。空白完整开发栈建议预留至少 12 GB 内存和 20 GB 磁盘；历史数据、索引与磁盘 oracle 另占存储，不能把此数当长期容量上限。首次启动会下载固定版本镜像和依赖。

```bash
make setup
make verify
make up
make smoke
```

`make up` 构建 JAR 和镜像，初始化 Kafka topics、ClickHouse 表、CDC connector、对象存储及两个 Flink 作业。`make smoke` 发送合成数据，核对完整接收清单、归档、清洗去向、指标、转化关联和 checkpoint；通过后激活 `live-v1`。容器启动成功本身不会激活未经业务验收的发布版本。

| 入口 | 地址 / 本地凭据 |
| --- | --- |
| Grafana 看板 | http://localhost:13000/d/adpulse-overview · `admin / adpulse-local` |
| 查询 / 治理 API | http://localhost:8080/docs |
| Collector OpenAPI | http://localhost:8088/docs |
| Flink 作业与 checkpoint | http://localhost:18081 |
| Prometheus / Alertmanager | http://localhost:19090 / http://localhost:19093 |
| MinIO 原始归档 | http://localhost:19001 · `adpulse / adpulse-local-storage` |
| Debezium REST | http://localhost:18083/connectors |

这些是仅绑定 `127.0.0.1` 的本地实验配置。对外部署时需接入鉴权、TLS、外部密钥管理和独立基础设施；本仓库没有公开发布服务。

## 不启动 Docker 的正确性演示

```bash
make demo
```

输出 `artifacts/demo/report.json`、业务 ground truth、归档清单和 SQLite 完整值重试核验。这个模式明确标记 `flink_executed=false`；真实 Flink 的证据来自 `make java-test` 和 `make smoke`。

## 接入、查询与版本修正

```bash
# 创建可重复的数据、注入传输错误并发送；truth 与 transport 分别保存
.venv/bin/python -m adpulse.cli generate --users 1000 --scenario mixed \
  --output artifacts/input --send http://localhost:8088

# 最近一次后台接收 / 清洗 / 归档检查；响应包含检查时间
curl http://localhost:8080/v1/reconcile

# cohort 必须明确；金额另按币种查询
curl 'http://localhost:8080/v1/metrics?release=live-v1&cohort=click'
curl 'http://localhost:8080/v1/associations?release=live-v1&status=unmatched'
curl http://localhost:8080/v1/quality

# 从已校验 S3 归档计算独立完整 release；验收后才切换
.venv/bin/python -m adpulse.cli replay --from-s3 --release replay-20260906 \
  --output artifacts/replay-20260906 --publish --activate

# 原版本保留，可原子回退，审计原因必填
.venv/bin/python -m adpulse.cli activate live-v1 --reason 'return to provisional live metrics'
```

`/v1/metrics` 和 `/v1/associations` 默认每页 100 条，最多 500 条；读取全部结果必须跟随 `next_cursor`。游标保持筛选条件和 release；live 数据的跨页一致性边界见接口说明。

转化回放和规则变化创建独立 release/topic。结果查询按完整键 `argMax(payload, output_offset)` 取最新完整值；不把 ClickHouse 后台合并当作即时去重。修正释放完整快照，旧维度键在新 release 中显式归零。发布指针、归档完成、写入完成分别检查。

## 故障与负载验证

```bash
.venv/bin/python scripts/drills.py --scenario all
# 或仅运行一个：sink-replay / worker-restart / archive-gap / rule-rollback
# cdc-recovery / savepoint-rescale / duplicates / schema / conversion-first / late-boundary

.venv/bin/python scripts/loadtest.py --rate 100 --seconds 1800 --output artifacts/load-100.json
.venv/bin/python scripts/loadtest.py --rate 1000 --seconds 1800 --output artifacts/load-1000.json
```

演练使用本项目容器，会中断相应本地服务，并在 `finally` 中恢复。`late-boundary` 使用真正 Flink operator harness 推进事件时间，保留实际 24 小时业务边界；不是等待两小时墙钟，也不冒充 processing-time TTL 测试。其他演练在真实 Compose 组件上执行。最新实测结果与限制见 [`docs/validation.md`](docs/validation.md)。

正常停止：`make down`，保留数据卷。**重启 JobManager 或更新 Flink 作业必须执行保存点恢复流程**，见运行手册；当前本地 session cluster 没有配置 JobManager HA。禁止把已存在的线上 release 作为空状态新作业直接继续运行。

## 工程目录

| 路径 | 职责 |
| --- | --- |
| `adpulse/generator.py` / `generator/` | 业务真值、关联数据与传输错误注入 |
| `adpulse/collector.py` / `collector/` | Kafka 事务确认后的接收响应 |
| `contracts/` | JSON Schema、冻结规则、时间配置 |
| `flink-jobs/` | 清洗 SQL、双键关联、有界去重、RocksDB 状态与完整值聚合 |
| `adpulse/storage.py`, `worker.py` / `sink-writer/` | ClickHouse 批量写入、重试与归档 worker |
| `adpulse/oracle.py`, `disk.py`, `reconciliation.py` | 独立内存 / 磁盘批量对账 |
| `adpulse/archive_index.py`, `inspection.py` | 归档检索索引、后台检查和带时间戳的 HTTP 读取模型 |
| `adpulse/releases.py`, `cli.py`, `api.py` | 修正发布、审计、原子切换和服务接口 |
| `deployment/`, `monitoring/` | 固定版本 Compose、CDC、告警、Grafana |
| `tests/`, `scripts/` | Python / Java 验收、集成、故障、负载脚本 |
| `docs/` | 契约、指标、交付语义、容量、发布恢复手册、实测证据 |

默认完整栈保持单 broker；独立三 broker Docker 实验已验证 leader 故障、quorum 丢失与事务恢复。Flink 1.20.5 / ClickHouse 26.3 LTS 迁移及录屏已完成；100/1,000 两档各 30 分钟已完成，1,000 档 P95 19.370 秒（2×8 GiB 进程预算、归因并行度 6）；云端 Docker CI 已通过，详见运维验收。所有副本仍位于单宿主机，不能据此声称物理机或可用区容错。
