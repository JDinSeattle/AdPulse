# 交付语义与修正正确性

| 边界 | 实现 | 重试 / 恢复含义 | 验证 |
| --- | --- | --- | --- |
| HTTP → Kafka | raw 与 receipts 同事务、acks=all、幂等 producer | 提交后才返回 202；HTTP 重试可能新增接收记录 | collector 失败测试、接收清单对账 |
| Kafka → Flink → Kafka | read_committed、checkpoint、KafkaSink EXACTLY_ONCE、唯一事务前缀 | 对应状态和输出事务共同恢复；仍须业务去重 | 两作业 checkpoint 与 worker 恢复 |
| Kafka → ClickHouse | 完整值、同步插入后提交 offset | 允许重复物理行；读取最新逻辑值 | 注入写入成功后、commit 前退出 77 |
| 原始归档 | 内容 hash 命名数据对象，第二步不可变 manifest，最后 commit offset | 无 manifest 的对象不参与回放；同 offset 不同内容拒绝 | checksum、manifest、归档中断演练 |
| 版本发布 | building → validated → active；PG 事务指针与审计 | 旧版本保留，新 release 完整隔离 | 回放验收与规则回退 |

Kafka 事务 timeout 900 秒，checkpoint 间隔 10 秒、超时 120 秒；工作进程故障恢复应在事务范围内完成。事务可见性带来等待，新鲜度需包含这段时间。local broker 副本为 1 / min ISR 1，仅验证本地逻辑和应用恢复。

结果 Kafka key 是 `m:<metric_key>` 或 `a:<association_key>`。固定 release topic 的分区数为 3，同一 key 固定一个 partition；不能把不同分区 offset 相互比较。sink 在批内及数据库检查 key 路由与重复 offset hash。变更分区数、key 策略、规则或回放范围必须创建新 release。

ClickHouse `ReplacingMergeTree(output_offset)` 只是存储策略，查询明确采用 `argMax(payload, output_offset)`，按 release + key 先还原完整行，再汇总业务值。聚合器先按贡献业务身份替换，再输出完整计数，避免不可重试的 `count += 1` 消息。没有会重复累加的增量物化视图。

归档检查逐条比对 topic / partition / offset / sha256。Kafka offset 间隔可能含事务控制记录，不能相减当成业务记录数。缺少客户端独立上报证据时，只能判定平台已确认接收之后的遗漏；流量突然下降本身不证明 SDK 丢失。

回放从原始归档按 received_at / batch_id / index 排序，固定完整规则快照和输入 manifest hash；在所选范围内持久去重。新 release 输出完整结果，并为上一版本中已消失的维度键写零值。输出到 ClickHouse 与预期快照逐键一致、归档完整后，才能标记 validated。激活与回退必须附审计原因。

正确性验收不等于业务规则正确。错误过滤规则也可能产生“计算与自身规则一致”的结果，所以规则演练额外比较新旧版本曝光变化，然后演示回退。这一差异比较不能被 sink 对账替代。

`visibility` 表记录每个 clean receipt 首次被完整指标快照包含后可见的时间。查询对同一 release / receipt 取最早可见时间，排除 sink 重放造成的重复样本。逐事件系统新鲜度与 event_time 延迟分别计算。被过滤、隔离、去重或转交修正的记录通过质量去向核对，不混入健康按时事件的新鲜度 SLO。

技术依据：[Flink Kafka 事务与 checkpoint](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/)、[ClickHouse ReplacingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree)、[Debezium PostgreSQL snapshot / WAL](https://debezium.io/documentation/reference/3.0/connectors/postgresql.html)。
