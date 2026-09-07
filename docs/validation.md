# 实际验收记录 · 2026-09-06

> 本文保留原轮次的历史结果与当时限制。后续运行时迁移、长测、三 broker 故障和录屏状态见 [运维验收](operations-validation.md)。

所有以下结果均来自本仓库实际执行。实验只有本机合成广告数据，未使用外部广告平台。原始结构化证据保存在 [`evidence/`](evidence/)；较大的输入、输出、归档和运行日志在 gitignore 排除的 `artifacts/` 中。

## 正确性与完整链路

- **31 项 Python 测试、9 项 Flink operator 测试通过**。覆盖独立 oracle、事件 / 业务去重、24 小时闭区间、跨身份隔离、多转化点击去重、币种、历史维度、schema / 规则白名单、可靠接收失败、归档损坏、sink 重试、checkpoint / rescale 和晚到修正。
- 最终真实链路累计 **128,139 条已确认接收记录**；**128,139 条归档、128,139 条清洗去向记录**，没有无法解释的接收缺口。
- **1,154 个完整指标键、21,667 条转化关联**与独立 Python oracle 一致。见 [`final-integration.json`](evidence/final-integration.json)。
- 从全部已验证归档发布 `replay-verified-20260906`，sink 逐键核验通过，执行了原子激活并切回 `live-v1`；两个版本保留。见 [`replay-verified.json`](evidence/replay-verified.json)。
- Grafana 的 **16 个面板查询**在有输入期间均返回真实监控数据，Prometheus 的 **7 个抓取目标**健康。支持按活动、国家 / 地区、应用版本筛选。
- 最终代码部署后再次从归档独立计算，同时核对实时与已发布回放，两者均无差异。两个 Flink 作业均有实际恢复记录且持续完成 checkpoint；API、collector、Grafana 和全部监控目标健康。见 [`final-service-audit.json`](evidence/final-service-audit.json)。Prometheus 的 8 条告警规则通过 `promtool` 校验。

## 故障演练

| 演练 | 执行环境 | 结果 / 证据 |
| --- | --- | --- |
| event_id 与新 event_id 的业务重复 | 真实 Compose | [通过](evidence/duplicates.json)，最终结果不重复累计 |
| SDK 坏 schema 混入正常事件 | 真实 Compose | [通过](evidence/schema.json)，隔离且接收去向完整 |
| 转化先到、点击和曝光后到 | 真实 Compose | [通过](evidence/conversion-first.json)，等待后正确关联 |
| sink 写入后、提交位点前退出 | 真实 Compose | [通过](evidence/sink-replay.json)，实际写入 28 条结果后退出 77，重放后对账一致 |
| TaskManager SIGKILL | 真实 Compose | [通过](evidence/worker-restart.json)，21.286 秒恢复运行，39.040 秒完成业务追赶 |
| 原始归档中断 | 真实 Compose | [通过](evidence/archive-gap.json)，观测到 65 条缺口，恢复后全部补齐 |
| 错误过滤规则推广与回退 | 真实 Compose | [通过](evidence/rule-rollback.json)，发现曝光减少，旧版本未被污染 |
| CDC 暂停、WAL 恢复、更新与删除 | 真实 Compose | [通过](evidence/cdc-recovery.json)，实际观察 c / u / d |
| Savepoint 恢复并行度 2 → 3 | 真实 Compose | [通过](evidence/savepoint-rescale.json)，检查实际并行度、恢复路径和业务结果 |
| 2 小时等待 / 24 小时业务边界 / 冻结修正 | Flink operator harness | [通过](evidence/late-boundary.json)，显式推进事件时间，未冒充墙钟 TTL 验证 |

演练期间确实发现并修复了 CDC tombstone 的空值反序列化错误。修复后从原持久 checkpoint 恢复，而非空状态重算；[恢复证据](evidence/cdc-checkpoint-recovery.json)中保留了原作业 ID、checkpoint 路径、恢复作业和新 checkpoint 成功记录。

## 负载短测与一次实际调优

配置：单 Kafka broker，两个 TaskManager，每个 process memory 2 GiB / 4 slots，清洗并行度 2，归因并行度 3，RocksDB 托管内存与持久 checkpoint，checkpoint 间隔 10 秒。

| 目标档位 | 实际接收速率 | 输入时长 | 接收记录 | 可见覆盖 | 逐 receipt P95 |
| --- | --- | --- | --- | --- | --- |
| 100 events/s | 101.23 events/s | 60 秒 | 6,074 | 100% | 34.509 秒 |
| 1000 events/s，调优前 | 1006.95 events/s | 60 秒 | 60,417 | 追赶等待截止时 24,286 条 | 207.863 秒，仅已可见子集；未达标 |
| 1000 events/s，调优后 | 1003.28 events/s | 60 秒 | 60,197 | 100% | 25.260 秒 |

原始报告：[100 档](evidence/load-100.json)、[1000 档失败记录](evidence/load-1000-before-tuning.json)、[1000 档调优后](evidence/load-1000-tuned.json)。失败短测的未完成记录随后全部追平，并纳入最终 128,139 条完整对账，没有隐藏或删除失败样本。

调优依据是线程栈中 RocksDB.put 阻塞，以及[实际 write stall 日志](evidence/rocksdb-write-stalls.txt)。多个状态列族共享本地小 slot 内存，默认 64 MiB memtable 反复触发 flush 等待。`LocalRocksOptions` 改为 8 MiB memtable、256 KiB arena、4 个 write buffers / background jobs，继续使用受管理内存。另移除 worker 每批每分区的阻塞 broker 指标请求，并将 archive batch 调为 1000。通过[原 checkpoint 恢复](evidence/rocksdb-tuning-recovery.json)应用配置，未清空历史状态。

这些是 **60 秒短测**。没有执行每档 30–60 分钟稳态、完整 26 小时状态容量验证、多 broker / ISR 故障、云成本计量或生产 SLA 认证。`scripts/loadtest.py` 支持 1800 秒及更长实验，并明确记录 `sustained_target_duration_met`。不能把以上速率写成长期稳态结论。

## 版本与可复现性

Flink 1.20.3，Kafka connector 3.3.0-1.20，Java 17 字节码 / Java 17 容器，Kafka 3.9.0，PostgreSQL 16.8，Debezium 3.0.8.Final，ClickHouse 24.8.14.39。镜像实际 digest 与架构在 [`deployment/images.lock.json`](../deployment/images.lock.json)，Python 依赖在 `requirements.lock`，Java 依赖与 Jackson BOM 在 `flink-jobs/pom.xml`。

本地集群未配置 JobManager HA。重建 JobManager 需明确提供保存点；bootstrap 会阻止已验证 release 空状态重启。大规模回放当前仍是内存 Python oracle，归档查找采用完整校验扫描，需按容量升级索引和分区复算。

浏览器连接工具未能初始化，因此未生成 UI 截图或录屏；已通过 Grafana 配置 API、Prometheus 查询和业务 API 验证接口与看板数据。录屏和长时稳态实验应作为后续运行验收单独记录。
