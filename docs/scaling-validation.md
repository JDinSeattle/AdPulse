# 本机容量与查询成本优化

本轮范围：磁盘对账、归档检索与后台监控、固定资源性能对照。基线为公开提交 `c3020d6f8ee314b6d09ec326e05e30e42f869edd`；不改变现有 Flink 作业状态，不包含跨主机 HA 或 26 小时长测。

## 优先级与验收

1. SQLite 磁盘索引和 oracle：逐对象校验 hash、offset 和 receipt 引用；有界解析、缓存和差异样本；对比原独立 oracle 的全部输出，并对现有真实本地 Docker 数据复算。
2. HTTP trace / reconcile / metrics：索引查询和后台预计算；明确检查时间、过期与失败，禁止把过期成功当实时成功；验证并发、后台失败和大数据规模下的请求成本。
3. 固定资源：独立 Docker 容器设置相同 CPU / 内存 / swap 上限，固定输入 SHA、初始索引和代码；交替多次测量 elapsed、CPU、RSS/cgroup peak、磁盘与结果摘要。预处理/索引构建成本单列；不能把缓存请求与冷构建混为同一工作量。

## 技术依据

官方文档于 2026-09-06/07 访问：[SQLite 缓存和临时存储](https://www.sqlite.org/pragma.html)、[WAL 单写者与读快照](https://www.sqlite.org/wal.html)、[Docker 资源限制](https://docs.docker.com/engine/containers/resource_constraints/)、[ClickHouse 查询资源限制](https://clickhouse.com/docs/operations/settings/query-complexity)。使用 Python 标准库 SQLite，接受额外磁盘和序列化成本；page cache 配置不是进程硬限制，实验另以 Docker cgroup 约束。

结果将在实际验证完成后填写，计划中的优化收益不作为简历指标。

## 实现与一致性边界

- 原 `oracle.calculate` 的 Python 容器路径保留为小样本参考，新增磁盘容器参数。磁盘路径仍独立于 Flink 实现，复用同一业务规则计算流程；修改前冻结 fixture 和所有质量/指标/关联输出的摘要用于防止共同回归。
- 归档逐份提交清单加载，验证对象 SHA、逐 offset/hash、receipt 引用；单清单 8 MiB、单行 2 MiB、对象 64 MiB，超限拒绝而非截断。每份对象完整校验后才提交 SQLite。增量模式信任先前验证的不可变对象，`--audit` 重读全部对象；清单删除会报错。索引是可重建缓存，不替代归档权威。
- SQLite 使用磁盘临时表、32 MiB page cache、禁用 mmap；大集合、排序、去重、结果比较和差异计数都在磁盘上，返回最多 100 个差异样本。广播维表另限制 10,000 条 / 16 MiB 输入，超限拒绝。进程总内存仍由 Docker 实验单独测量。
- HTTP trace 使用 receipt 索引及后台同步的质量记录；reconcile / quality 读取后台结果。响应含检查时间，冷启动、失败或过期返回 503；缺失 receipt 的 404 只针对已检查索引。
- `/metrics` 只读本地检查结果和进程查询指标；监控检查每次结束后等待 30 秒，归档/来源检查等待 120 秒，两个进程独立运行。超过 90 / 600 秒或上一轮失败时，不输出旧业务 gauge，并置 `adpulse_inspection_snapshot_ready` 为 0。新增告警覆盖这种情况。完成时间与检查起始时间均记录；这些不是跨来源原子快照。
- 分页请求限制每 API 进程最多 4 个正在执行的查询；繁忙时立即返回 503 / Retry-After。单请求 SQL 预算继续保留；此限制不是跨副本全局准入。

## 已发现并修复的实际问题

[首次拒绝记录](evidence/scaling/initial-rejected.json)：SQLite 优化器为 receipt 查询选择了 kind-only 索引，导致大规模反连接不可接受；使用完整 receipt/kind 复合索引并约束关键查询计划。监控 pending 数通过完整 payload 的最新值视图聚合时触发 ClickHouse 错误 241；改为按 key 聚合最新 status 标量后计数，保持“最新值后过滤”语义。失败尝试不参与性能指标。
