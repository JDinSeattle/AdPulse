# 运行时迁移、负载与故障验证

本轮于 2026-09-06（America/Los_Angeles）执行，部分报告 UTC 日期为 2026-09-07。环境为本地 Linux / Docker，业务输入全部为合成广告事件。此前的短测、失败记录和查询基线保留在原验收文档中。本页逐项区分实际完成与尚未完成的验证。

## 最终完成结果

| 负载 | 配置 | 输入时长 | 确认并全部可见 | 实测速率 | 全体 receipt P95 |
| --- | --- | --- | --- | --- | --- |
| 100 target | 2×2 GiB 进程预算，归因并行度 3，batch 200；Bloom 前 | 1,800.000 秒 | 180,002 | 100.00/s | 27.517 秒 |
| 1,000 target | 2×8 GiB 进程预算，归因并行度 6，batch 2000；Bloom / 64 KiB 参数 | 1,800.000 秒 | 1,800,211 | 1,000.12/s | 19.370 秒 |

两档完成各 30 分钟，满足各自运行的速率、新鲜度、完整可见和 checkpoint 验收。最终 1,000 档两个作业新增成功 checkpoint 分别为 182, 182，期间失败/恢复增量均为零，容器身份、启动时间与重启计数保持一致。配置预算不是实际 RSS；两档配置和历史状态不同，不计算同资源性能提升百分比。完整数据见 [100 报告](evidence/operations/steady-100.json)、[1,000 报告](evidence/operations/steady-1000.json)及 [采样图](evidence/operations/load-timeline.png)。

包含四次失败输入在内，累计 **3,833,704** 条已确认输入的归档、分类和来源对账完整；**6,538** 个指标键、**646,242** 条转化关联与独立 oracle 完全一致。该 [最终对账](evidence/operations/final-reconciliation.json)是全历史检查，和负载 prefix 的可见性测量分开。

当前运行代码的 [成功云端 CI](https://github.com/JDinSeattle/AdPulse/actions/runs/34075884766)在默认资源配置上完成真实完整栈、回放、查询预算、大参数批次、实际 worker 中断和 sink 提交前崩溃；[报告及下载 artifact hash](evidence/operations/cloud-ci-latest.json)保留原始计数。worker 回到 RUNNING 为 20.212 秒，业务对账 41.332 秒；sink 故障实际退出码 77，恢复后累计 340 条输入业务结果一致。不是托管数据库、跨宿主机部署或生产服务。

四次失败/早停、一次云端故障目标选择失败均保留，不能改写为成功容量指标。30 分钟输入未覆盖 26 小时状态保留周期；真实跨物理机/可用区故障、云服务部署与账单、生产 SLA 仍未验证。

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

100 events/s 档已完成 1,800 秒：180,002 条确认输入全部可见，实测速率 100.0 events/s，逐 receipt P95 27.517 秒，两作业各新增 183 次成功 checkpoint，期间无 checkpoint 失败、作业恢复、容器 OOM 或重启。见 [完整报告](evidence/operations/steady-100.json)。

首次 1,000 events/s 在约四分钟采样时积压达到 141,610，已可见子集 P95 127.896 秒，因此停止输入并保留 [失败报告](evidence/operations/steady-1000-rejected.json)。最终 309,585 条已提交输入全部处理；失败期间的最终 P95 为 474.016 秒，不能写成通过或稳态能力。

[实际线程及 Prometheus 采样](evidence/operations/rocksdb-diagnosis.json)显示聚合任务 busy 998 ms/s，两条采样线程在 RocksDB MapState 点查询中。基于这一诊断推断，启用 10-bit whole-key full Bloom filter 并缓存每事件重复读取的 aggregate 值。遵循 [RocksDB 官方点查询调优说明](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)，接受 filter 的内存/空间成本。新增原生刷盘、重开、命中/未命中与遍历回归，两作业通过 [strict canonical savepoint 恢复](evidence/operations/bloom-restore.json)，全历史 **617,840** 条输入与独立 oracle [再次匹配](evidence/operations/after-bloom-reconciliation.json)。

Bloom 修改后的第二次输入仍出现下游积压，572.695 秒时停止；572,779 条全部可见，最终 P95 54.065 秒、最大 64.385 秒。P95 未越过 60 秒，但没有完成目标时长，因此 [本次报告](evidence/operations/steady-1000-bloom-only.json)仍不具备 30 分钟容量资格。[第二次诊断](evidence/operations/sink-diagnosis.json)显示 Flink aggregate 已非持续满忙，而 sink 的 quality/results 消费积压增加。将 sink 最大批次从 200 调为 2,000 条 Kafka 消息，保留一秒 poll 返回部分批次、同步写入及写入后提交位点。此处消息数不是单张表行数；依据 [ClickHouse 同步批量写入指南](https://clickhouse.com/docs/concepts/best-practices/selecting-an-insert-strategy)摊薄查询/写入固定开销，代价是单批内存和重放范围增加。没有启用不等待落盘的异步确认。

[全历史 1,190,619 条输入](evidence/operations/after-bloom-only-reconciliation.json)对账通过后，部署批次调整并重新开始 1,000 events/s 完整长测（后续按真实失败进一步修复参数边界并扩容；最终配置与结果见上表）。100 档使用 Bloom 前 / batch=200 配置；最终 1,000 档使用 Bloom / batch=2000，具体 [环境与配置](evidence/operations/environment.json)单列。前后状态规模、配置和宿主机负载不同，不能据此计算受控性能提升百分比。每分钟记录实际接收、可见积压、P95、Docker CPU/内存、checkpoint 大小/时长/计数。报告中 `passed` 与 `sustained_target_duration_met` 分开；只有时长与所有验收条件同时满足才能称本地 30 分钟验证通过。

## 云端边界

公共仓库 [JDinSeattle/AdPulse](https://github.com/JDinSeattle/AdPulse) 已建立；[首次 GitHub Actions](https://github.com/JDinSeattle/AdPulse/actions/runs/34072376249) 成功，运行于 GitHub 托管的 4-CPU Linux runner。固定 actions SHA、Python 3.12 / Java 17，执行锁定依赖、完整 Docker 链路、独立回放、真实查询预算、worker 故障并上传 artifact。[下载核验后的证据](evidence/operations/cloud-ci-initial.json)包含 198 条初始对账及故障后累计 278 条对账；worker 19.347 秒回到 RUNNING、42.982 秒完成业务对账。启动和故障期间确有失败 checkpoint，报告保留原计数。该 run 对应初始公开 commit；后续修改需要其自己的 CI 结果。

大批次调优还触发了真实的 HTTP 参数边界：旧的整批 route-key 参数被 ClickHouse 拒绝为 `Field value too long`，sink 保持批次并停止提交消费位点。[拒绝证据](evidence/operations/sink-large-parameter-rejected.json)保留。`storage.lookup_parameters` 对校验身份去重，并按 JSON 转义后 UTF-8 字节数限制每个参数最多 64 KiB；写入仍按 2,000 条消息成批，逐 offset 哈希及 key/partition 冲突检查全部保留。[真实 ClickHouse 大批次回归](evidence/operations/sink-large-batch.json)复现 384,001 字节旧参数拒绝，验证 6 个最大 65,473 字节参数、2,000 条逻辑结果/投递身份、相同批次重试、冲突内容与分区变更拒绝。该脚本使用合成 Kafka 消息元数据，实际 broker 位点故障由独立 sink-replay 演练覆盖。完整本地回归为 **60 Python + 10 Java = 70** 项；原生 RocksDB 在 Java 项内。

云端结果仅属于托管 CI 上的容器集成测试，不是 AWS/GCP/Azure 托管服务或生产部署。

30 分钟输入不能证明 26 小时状态保留周期的容量稳态；本地副本不能证明跨主机可靠性；无账单就不报告云成本。上述边界应在经历库和英文简历中保留。
