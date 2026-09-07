# 0.2.0 实际验收 · 2026-09-06

> 本文保留原轮次的历史结果与当时限制。后续运行时迁移、长测、三 broker 故障和录屏状态见 [运维验收](operations-validation.md)。

本次更新增加有界结果 API、固定 release 的签名游标、查询预算 / 监控和 HTTP 依赖维护。需求与取舍见[研究记录](market-and-technology-2026-09-06.md)，接口变更见[迁移说明](query-api.md)。原来的 [0.1.0 验收记录](validation.md) 作为历史证据保留，没有覆盖其失败样本或改变短测口径。

## 已执行

| 验收 | 实际结果 | 证据 |
| --- | --- | --- |
| 完整回归 | **51 项 Python + 9 项 Java = 60 项测试通过**；ruff、compileall 通过 | [verification.json](evidence/refresh/verification.json)、[JUnit XML](evidence/refresh/python-tests.xml) |
| 真实 DB 版本语义 | 两个隔离合成 release，37 个逻辑键含旧 pending、新 matched 和重复 offset；pending 查询没有复活旧状态 | [query-acceptance.json](evidence/refresh/query-acceptance.json) |
| 真实 PostgreSQL 并发切换 | 21 次测试指针切换；全部后续页固定原 release，同一游标重试一致，正常服务的活动指针未变 | 同上 |
| 数据库资源拒绝 | 实际触发扫描行数、响应字节预算；返回失败，没有部分成功页 | 同上；单元测试另覆盖客户端流式响应截断 / 损坏 JSON |
| 完整分页遍历 | 已验证 replay 的 **21,667 条关联 / 44 页，1,154 个指标键 / 3 页**与完整最新快照逐项一致 | 同上 |
| 部署后真实 HTTP | 8 并发、24 次同游标请求全部一致；500 上限、OpenAPI 0.2.0、查询指标和 18 个 Grafana 面板已核实 | [deployed-http.json](evidence/refresh/deployed-http.json) |
| 原业务链路回归 | 升级后 collector 确认 27 条新混合事件；累计 **128,166 条接收 / 归档 / 清洗去向**、**1,183 指标键 / 21,670 关联**与独立 oracle 对账一致，收敛 27.664 秒 | [live-smoke.json](evidence/refresh/live-smoke.json) |
| 依赖与告警 | 运行容器为 AdPulse 0.2.0 / FastAPI 0.141.1 / Starlette 1.6.0；`promtool` 校验 9 条规则通过 | [verification.json](evidence/refresh/verification.json) |

DB 并发场景使用真实 PostgreSQL / ClickHouse 和 ASGI TestClient，但数据与故障是合成的；HTTP 并发场景使用已部署服务。二者没有混称为生产故障或云端运行。新增 Grafana 面板通过配置和查询接口核验，未生成 UI 截图 / 录屏。

## 同环境性能对照

机器为 i9-13900K、32 逻辑 CPU、约 123 GiB 内存，Linux 7.0.0-29；共享工作站上其他项目容器继续运行，没有独占机器或绑核。Python 3.12.13、ClickHouse 24.8.14.39、FastAPI 0.141.1、Starlette 1.6.0。三种算法均在同一环境、同一不可变 `replay-verified-20260906` 上运行；每种先预热一次，交替顺序测量五轮，全部启用 `tracemalloc`。

计时包含同步端点内部的数据库请求、网络读取和 Python 解析；不包含 ASGI 网络往返及最终 JSON 序列化。峰值为 **Python traced allocations，不是进程 RSS / ClickHouse 内存**。响应大小另对结果 JSON 紧凑编码后计量。

| 算法 | 返回条数 | 中位耗时 | Python 峰值分配中位数 | 响应字节中位数 |
| --- | --- | --- | --- | --- |
| 旧端点的完整物化算法 | 21,667 | 554.5 ms | 71,271,122 B | 12,717,463 B |
| 旧算法读取全部后截前 100 条 | 100 | **621.7 ms** | **71,271,154 B** | 58,464 B |
| 新的数据库端首 100 条分页 | 100 | **36.6 ms** | **442,695 B** | 58,889 B |

原始逐轮数据：[query-benchmark.json](evidence/refresh/query-benchmark.json)。改动前、旧 HTTP 依赖下的独立基线也保留于 [query-baseline.json](evidence/refresh/query-baseline.json)。对等的前 100 条业务记录完全相同；新的响应略大，是因为包含游标与一致性元数据。原全量响应变小首先来自有界接口契约，**不能把返回更少记录解释为完整导出加速**。内存和端点耗时的比较使用相同前 100 条任务，证明避免全量载入 Python 的收益；没有把收益归因于框架升级。

```bash
make verify
make query-check RELEASE=replay-verified-20260906
make query-benchmark RELEASE=replay-verified-20260906
# 其他机器先 make up / make smoke，并发布自己的 immutable replay，替换 RELEASE。
```

## 保留的边界与待办

- live 游标只固定 release，跨页不提供 MVCC；需要可重复的完整导出时使用 validated replay。
- 某些旧契约字符串没有上限。超大业务键无法生成有界游标时返回明确 422；不静默丢弃。治理归档接口尚未全部分页。
- 查询 LIMIT 不意味着只扫描 500 行；server budgets 是查询级保护，不是并发总量或进程级硬隔离。
- Git 提交历史不可读取，源码以 [SHA-256 manifest](evidence/refresh/source-manifest.json) 记录，没有伪造 commit 或远程链接。
- Flink 1.20.3、Kafka 3.9.0、Debezium 3.0.8、ClickHouse 24.8 仍是原本地链路版本。ClickHouse 24.8 已超官方 LTS 支持窗口；本次没有声称全栈已维护到生产安全基线。
- 两条 TestClient / anyio 弃用提示保留；hosted CI 尚未运行。
- 没有新增 30–60 分钟稳态、26 小时容量、多 broker / 宿主机容错、云部署 / 账单、真实广告流量或 GPU / 模型 serving 证据。旧 1000 events/s、P95 25.26 秒仍只是一分钟短测。

经历库仅写入上述可支持事实、英文 bullets、指标来源和面试边界；新增稳定项目 ID `project-local-adpulse`，不把本地 portfolio 项目转为工作经历。
