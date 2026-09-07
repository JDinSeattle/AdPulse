# 运行时迁移、负载与故障验证

本轮于 2026-09-06（America/Los_Angeles）执行，部分报告 UTC 日期为 2026-09-07。环境为本地 Linux / Docker，业务输入全部为合成广告事件。此前的短测、失败记录和查询基线保留在原验收文档中。本页逐项区分实际完成与尚未完成的验证。

## 选择依据与优先级

| 工作 | 岗位能力 / 工程价值 | 验收 |
| --- | --- | --- |
| Flink 补丁与 ClickHouse LTS 迁移 | 安全更新、有状态服务维护、可回退变更 | 两作业 strict savepoint 恢复；旧/新 DB 全表逻辑指纹；历史及新增事件 oracle |
| 两档各 30 分钟输入 | 性能验证、背压及容量判断 | 接收速率 ≥目标 95%；完整可见覆盖；逐 receipt P95 ≤60 秒；checkpoint 持续成功；无容器 OOM / 重启 |
| 三 broker 故障 | 分布式系统、副本、事务边界和恢复 | RF=3/minISR=2；leader SIGKILL 后成功接收；失去 quorum 返回 503；恢复后逐 offset/hash 对账 |
| CI 与演示录屏 | 可部署、可复现、工程交付 | 新环境启动完整栈、独立回放、查询和 worker 故障；真实看板/作业界面视频 |

岗位来源及样本限制见 [前轮研究](market-and-technology-2026-09-06.md)。新增运维工作直接补充这些样本重复出现的可靠实现、性能度量和自动化要求，不增加模型服务或 Kubernetes 关键词项目。

官方技术来源均于 2026-09-06 访问：

- [Flink 1.20.5 公告，2026-06-08](https://flink.apache.org/2026/06/08/apache-flink-1.20.5-release-announcement/)包含 RocksDB Compactor 资源泄漏等修复。下载列表将该版列为 06-03，与公告日期不一致；本页以公告标注为准。保留 Java 17、状态 UID 和 Kafka connector 3.3.0-1.20，避免把 API/connector 大版本迁移与补丁维护混在一起。[官方状态升级流程](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/upgrading/)。
- [ClickHouse 26.3.32.14 LTS，2026-09-06](https://github.com/ClickHouse/ClickHouse/releases/tag/v26.3.32.14-lts)；[官方升级指南](https://clickhouse.com/docs/guides/oss/update)允许跨一年采用停机升级或中间版本。本项目单节点选择停机冷拷贝到新卷，保留旧卷，未把旧版直接降级写回新卷。检查了 [2025 changelog](https://clickhouse.com/docs/resources/changelogs/oss/2025) 的 JSON 类型、统计数据格式及 ORDER BY 变化；本项目用 String payload、显式非空主键，没有原生 JSON 列或列统计格式迁移。2026 长页面经网页工具读取受大小限制，目标版本另通过真实数据、查询预算与读写回归验收，未声称完整审计全部中间版本。
- [Kafka 3.9 replication](https://kafka.apache.org/39/design/design/#replication)、[KRaft 运维](https://kafka.apache.org/39/operations/kraft/)：隔离实验采用三个 combined broker/controller，失去两台同时失去数据写入 quorum 与 controller 多数派；不能单独归因为 ISR 门槛，也不是生产拓扑推荐。
- [Playwright 视频文档](https://playwright.dev/docs/videos)，录屏工具单独锁定 1.62.1，不加入 Python 服务镜像。

## 迁移：已验证

Flink **1.20.3 → 1.20.5**；ClickHouse **24.8.14.39 → 26.3.32.14 LTS**。两个 Flink 作业从原保存点恢复，清洗并行度 2、归因并行度 3，`allowNonRestoredState=false`，均完成新 checkpoint。原 ClickHouse 卷 `adpulse_clickhouse-data` 保留；服务切换到冷拷贝的新卷 `adpulse_clickhouse-data-v26`。

五表校验覆盖 results 46,550、deliveries 207,458、quality 128,186、receipts 1,415、visibility 321,285 条逻辑行。ReplacingMergeTree 使用 FINAL；visibility 保留所有行。SHA-256 包含全部业务字段，忽略 results/quality 的插入时间。新旧版本 JSON UInt64 默认输出不同导致第一次拒绝；固定 `output_format_json_quote_64bit_integers=0` 后五表完全一致，没有忽略发生差异的业务字段。该默认值切换可在 [26.3 release 对应的官方设置历史](https://github.com/ClickHouse/ClickHouse/blob/v26.3.32.14-lts/src/Core/SettingsChangesHistory.cpp#L305) 中定位到 25.8。

新增 22 条混合事件后，累计 **128,188** 条已确认事件全部归档、分类；**1,211** 个指标键与 **21,672** 条关联和独立 oracle 完全一致。真实 DB 查询验收通过 21 次并发指针切换、最新状态过滤、游标重试、完整分页及 scan/response 预算拒绝。

证据：[迁移报告](evidence/operations/migration.json)、[首次拒绝记录](evidence/operations/migration-initial-rejected.json)、[迁移后查询回归](evidence/operations/query-after-migration.json)。首次拒绝是验证工具输出格式不一致，不是数据损坏，也不构成性能提升。

## 三 broker：已验证，单宿主机

独立 `deployment/compose.quorum.yaml`，RF=3、minISR=2、`acks=all`、`read_committed`。一次真实实验：

- SIGKILL 当前 raw 分区 leader，**12.007 秒**后已取得新的成功接收确认。
- 再杀一台，接入在 **30.006 秒**后返回 HTTP 503；没有向客户端宣称提交成功。
- 重启两台，等待 ISR=3，重新初始化相同事务 ID；**124** 条已确认 raw 和 **3** 份 manifest 的 partition/offset/SHA-256 全部匹配；未确认失败批次不可见。

[结构化报告](evidence/operations/quorum.json)包含前、中、后的 leader/replica/ISR 元数据。原始实验容器停止、卷保留。默认完整链路仍为单 broker；三 broker 实验验证接入事务边界，不证明 ClickHouse/MinIO/PostgreSQL 或完整链路高可用，不覆盖物理机、可用区或网络分区故障。

## 长测与录屏

[真实浏览器录屏](demo/adpulse-local-demo.mp4)已完成：60.24 秒，1440×1000，含 Grafana、作业拓扑和 checkpoint；附 [文件 hash 与章节](evidence/operations/recording.json)。录屏检查发现的未初始化 watermark 无效日期已修正，过滤哨兵值并改用有效 watermark。视频展示 UI；定量结论以独立报告为准。

两档各 1,800 秒的实验正在执行，最终报告完成后在此列出。每分钟记录实际接收、可见积压、P95、Docker CPU/内存、checkpoint 大小/时长/计数。报告中 `passed` 与 `sustained_target_duration_met` 分开；只有时长与所有验收条件同时满足才能称本地 30 分钟验证通过。

## 云端边界

已实现 `.github/workflows/ci.yml`：固定 actions SHA、Python 3.12 / Java 17、锁定依赖、完整 Docker 链路、独立回放、真实查询预算、worker 故障和 artifact。没有成功的 GitHub Actions run URL 之前，云端验证状态保持未验证。即使托管 runner 执行成功，也只属于云端 CI 上的容器集成测试，不是 AWS/GCP/Azure 托管服务或生产部署。

30 分钟输入不能证明 26 小时状态保留周期的容量稳态；本地副本不能证明跨主机可靠性；无账单就不报告云成本。上述边界应在经历库和英文简历中保留。
