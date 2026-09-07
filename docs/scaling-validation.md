# 本机容量与查询成本优化

本轮范围：磁盘对账、归档检索与后台监控、固定资源性能对照。基线为公开提交 `c3020d6f8ee314b6d09ec326e05e30e42f869edd`；不改变现有 Flink 作业状态，不包含跨主机 HA 或 26 小时长测。

## 优先级与验收

1. SQLite 磁盘索引和 oracle：逐对象校验 hash、offset 和 receipt 引用；有界解析、缓存和差异样本；对比原独立 oracle 的全部输出，并对现有真实本地 Docker 数据复算。
2. HTTP trace / reconcile / metrics：索引查询和后台预计算；明确检查时间、过期与失败，禁止把过期成功当实时成功；验证并发、后台失败和大数据规模下的请求成本。
3. 固定资源：独立 Docker 容器设置相同 CPU / 内存 / swap 上限，固定输入 SHA、初始索引和代码；交替多次测量 elapsed、CPU、RSS/cgroup peak、磁盘与结果摘要。预处理/索引构建成本单列；不能把缓存请求与冷构建混为同一工作量。

## 技术依据

官方文档于 2026-09-06/07 访问：[SQLite 缓存和临时存储](https://www.sqlite.org/pragma.html)、[WAL 单写者与读快照](https://www.sqlite.org/wal.html)、[Docker 资源限制](https://docs.docker.com/engine/containers/resource_constraints/)、[ClickHouse 查询资源限制](https://clickhouse.com/docs/operations/settings/query-complexity)。使用 Python 标准库 SQLite，接受额外磁盘和序列化成本；page cache 配置不是进程硬限制，实验另以 Docker cgroup 约束。

当前改动延续 [官方早期岗位样本与项目覆盖分析](market-and-technology-2026-09-06.md)：优先用可复现的资源约束、失败处理和数据核对展示 Backend / Cloud Infrastructure 能力。没有新增模型服务或 Kubernetes，也没有因热度引入新依赖。已完成的结果如下；未通过的尝试单独保留。

## 实现与一致性边界

- 原 `oracle.calculate` 的 Python 容器路径保留为小样本参考，新增磁盘容器参数。磁盘路径仍独立于 Flink 实现，复用同一业务规则计算流程；修改前冻结 fixture 和所有质量/指标/关联输出的摘要用于防止共同回归。
- 归档逐份提交清单加载，验证对象 SHA、逐 offset/hash、receipt 引用；单清单 8 MiB、单行 2 MiB、对象 64 MiB，超限拒绝而非截断。每份对象完整校验后才提交 SQLite。增量模式信任先前验证的不可变对象，`--audit` 重读全部对象；清单删除会报错。索引是可重建缓存，不替代归档权威。
- SQLite 使用磁盘临时表、32 MiB page cache、禁用 mmap；大集合、排序、去重、结果比较和差异计数都在磁盘上，返回最多 100 个差异样本。广播维表另限制 10,000 条 / 16 MiB 输入，超限拒绝。进程总内存仍由 Docker 实验单独测量。
- HTTP trace 使用 receipt 索引及后台同步的质量记录；reconcile / quality 读取后台结果。响应含检查时间，冷启动、失败或过期返回 503；缺失 receipt 的 404 只针对已检查索引。
- `/metrics` 只读本地检查结果和进程查询指标；监控检查每次结束后等待 30 秒，归档/来源检查等待 120 秒，两个进程独立运行。超过 90 / 600 秒或上一轮失败时，不输出旧业务 gauge，并置 `adpulse_inspection_snapshot_ready` 为 0。新增告警覆盖这种情况。完成时间与检查起始时间均记录；这些不是跨来源原子快照。
- 分页请求限制每 API 进程最多 4 个正在执行的查询；繁忙时立即返回 503 / Retry-After。单请求 SQL 预算继续保留；此限制不是跨副本全局准入。

## 已发现并修复的实际问题

[首次拒绝记录](evidence/scaling/initial-rejected.json)：SQLite 优化器为 receipt 查询选择了 kind-only 索引，导致大规模反连接不可接受；使用完整 receipt/kind 复合索引并约束关键查询计划。监控 pending 数通过完整 payload 的最新值视图聚合时触发 ClickHouse 错误 241；改为按 key 聚合最新 status 标量后计数，保持“最新值后过滤”语义。失败尝试不参与性能指标。

进一步的失败与修复均保留原边界：

- [监控旧反连接失败](evidence/scaling/serving-initial-rejected.json)：全量 `NOT IN` 集合触及查询预算，改为有界 grace-hash anti-join；真实缺失/重复边界和全部历史检查见 [查询验收](evidence/scaling/monitoring-query.json)。
- [维表识别失败](evidence/scaling/full-second-rejected.json)：quality/quarantine 被误归为 CDC 输入，触发维表上限；现在只扫描实际 campaign_versions 主题，并测试大量质量记录不消耗维表预算。
- [HTTP 慢消费者失败](evidence/scaling/coverage-stream-rejected.json)：SQLite 写入使多 GB 响应长时间未读完，收到 ProtocolError；先完整流入最大 8 GiB 临时文件，再逐行写索引。失败、截断或超限不会发布成功，临时文件自动删除。
- [CI WAL 失败](evidence/scaling/ci-first-rejected.json)：最后一个检查连接关闭后，只读卷阻止 WAL 读者重建辅助文件。已用独立文件复现并修复：API 数据库连接仍为 `mode=ro`，共享卷允许 sidecar 写入，因此不声称文件系统写隔离。官方 [只读 WAL 条件](https://www.sqlite.org/wal.html#read_only_databases)于 2026-09-07 核对。
- [完整关联聚合失败](evidence/scaling/full-third-rejected.json)：旧查询对完整 payload 做 `argMax`，实际复现 [错误 241 / 512 MiB](evidence/scaling/reference-query-rejections.json)。现在只把核对字段的 JSON 值及业务 key 放入同一个最新 offset tuple，64 MiB 后允许聚合外排，临时查询磁盘上限 4 GiB。依据 [ClickHouse 外部聚合文档](https://clickhouse.com/docs/reference/statements/select/group-by#group-by-in-external-memory)，访问日期 2026-09-07。
- [键投影预检拒绝](evidence/scaling/full-fourth-rejected.json)：存储 output_key 包含类型前缀，不能替代 payload 中的业务 key。改为从最新 payload 中保留原字段和 JSON 类型；[真实等价性检查](evidence/scaling/reference-query.json)比较了不可变版本的全部 1,154 个指标键和 21,667 条关联，零差异。

## 固定资源对照结果

固定 200,000 条混合合成输入、83 批；输入 SHA 和逐文件清单见 [fixture](evidence/scaling/fixture-200k.json)。每组 3 次，交替执行 `memory-1, disk-1, disk-2, memory-2, memory-3, disk-3`。同一镜像 ID、2 CPU quota、CPU affinity `2,4`、4 GiB cgroup、无额外 swap。每次新建磁盘索引和 scratch；进程小规模预热不读 fixture；不清除宿主机 page cache，磁盘和其他项目仍共享。不是独占硬件，也不是 Flink 吞吐实验。

[完整参考计算对照](evidence/scaling/paired-oracle.json)包含读取/校验归档、排序、全量业务计算及全部输出 hash；质量、指标、关联的所有结果一致。

| 三次中位数 | 原内存版 | 磁盘版 |
| --- | ---: | ---: |
| 耗时 | 30.718 s | 70.367 s |
| 进程峰值 RSS | 1,010,892 KiB | 138,444 KiB |
| 实验 scratch / 索引磁盘 | 0 | 634,052,608 B |

进程峰值 RSS 中位数降低 **86.3%**，耗时为原来的 **2.29 倍**，新增约 **604.7 MiB** 磁盘。cgroup peak 包括文件缓存，下降幅度远小于 RSS；报告逐次保留该值和 OOM 计数。不能把 RSS 降幅写成宿主机、Flink 或整套系统的内存降幅。

[HTTP 对照](evidence/scaling/paired-serving.json)使用实际 Uvicorn HTTP、真实共享 ClickHouse/PostgreSQL/Flink 依赖，但归档为相同 200k 本地 fixture，经生产 LocalObjects 适配器读取。这些 fixture receipt 不存在于业务库，所以两侧质量结果均为空；真正全量 lineage 的非空 trace 另行验收。每个接口每个版本共 15 次请求。

| 请求路径 | 原请求中位数 | 索引/缓存请求中位数 | 原 / 新请求中的 ClickHouse 调用总数 |
| --- | ---: | ---: | ---: |
| trace | 4,344.13 ms | 2.36 ms | 15 / 0 |
| metrics | 1,690.56 ms | 2.10 ms | 90 / 0 |

候选版本初始化中位数 **11.904 s**，每次含 6 次 ClickHouse 调用，后续刷新仍有成本。这表示工作移到索引/后台，并接受有日期的结果；不能写成相同新鲜度的实时查询提速。冻结测量源码与当前已执行函数的关联见 [源码比对](evidence/scaling/source-equivalence.json)。

## 实际 HTTP 与后台检查

[部署后验收](evidence/scaling/inspection-http.json)使用实际 S3 归档索引和 ClickHouse 质量记录：383 万条累计历史覆盖完整；16 个客户端发起 48 个 trace/reconcile/metrics 请求，均通过，包含真实非空 raw + quality trace。混合请求中位数 15.22 ms、最大 20.81 ms；这是一轮并发验收，不能当容量极限或生产 SLA。

[独立故障验收](evidence/scaling/inspection-failure.json)通过实际 loopback HTTP，使用刻意失败/过期的合成检查文件及进程内许可耗尽，验证 503、Retry-After、旧业务 gauge 消失和恢复。不是实际数据库饱和或断网实验。单元测试另验证异常退出释放许可和流中断清理。

[后台实测快照](evidence/scaling/background-coverage.json)：一次复用已验证索引的全量 lineage/quality 刷新耗时 **332.9 s**。采样时 SQLite 主文件约 **11.63 GiB**，WAL 约 **2.90 GiB**；两者会随历史增长和 checkpoint 时机变化。该进程的 1 GiB cgroup 包括 page cache，峰值接近预算，OOM kill 为 0。目前仍每轮重建 lineage/quality，未实现增量质量同步；不要用毫秒级 HTTP 响应掩盖后台分钟级成本。

## 复现命令

在已运行的本机环境中执行以下独立验收；报告拒绝覆盖已有文件。CI 脚本仅适用于新的临时 runner，会主动拒绝现有业务栈。

```bash
make verify
.venv/bin/python scripts/disk_reconcile.py --output artifacts/reproduce/full.json
.venv/bin/python scripts/inspection_acceptance.py --output artifacts/reproduce/http.json
.venv/bin/python scripts/inspection_failure_acceptance.py --output artifacts/reproduce/failure.json
.venv/bin/python scripts/reconciliation_query_acceptance.py \
  --release replay-verified-20260906 --output artifacts/reproduce/reference-query.json
```

磁盘核对按清单增量验证；完整重读不可变对象加 `--audit`。必须另提供足够的索引、oracle scratch、WAL 和 HTTP 临时文件空间。旧 `adpulse replay --publish` 仍为内存全快照发布路径，不把本次磁盘核对误当成该命令的容量改造。

在可写 Git clone 中准备同一基线，再运行固定资源对照：

```bash
mkdir -p artifacts/reproduce/baseline
git archive c3020d6f8ee314b6d09ec326e05e30e42f869edd | tar -x -C artifacts/reproduce/baseline
.venv/bin/python scripts/prepare_scaling_fixture.py --records 200000 --output artifacts/reproduce/fixture
.venv/bin/python scripts/scaling_benchmark.py \
  --fixture artifacts/reproduce/fixture --baseline artifacts/reproduce/baseline --candidate . \
  --baseline-commit c3020d6f8ee314b6d09ec326e05e30e42f869edd \
  --cpuset 2,4 --repeats 3 --workload oracle --output artifacts/reproduce/paired-oracle
.venv/bin/python scripts/scaling_benchmark.py \
  --fixture artifacts/reproduce/fixture --baseline artifacts/reproduce/baseline --candidate . \
  --baseline-commit c3020d6f8ee314b6d09ec326e05e30e42f869edd \
  --cpuset 2,4 --repeats 3 --workload serving --output artifacts/reproduce/paired-serving
```

CPU affinity 必须按实际机器可用 CPU 调整并记录，不会因设置相同 quota 自动得到独占核心。候选源码测量期间必须保持冻结；新结果的镜像 ID、源文件 hash 和环境以新报告为准。

## 回归与托管 CI

本机 `make verify` 通过 **81 Python + 10 Java = 91** 项；[验证记录](evidence/scaling/verification.json)保留命令和日志 hash。一次受限沙箱的 ASGI TestClient 停滞已中断，不计为成功；允许本地连接后完整验证通过。Starlette TestClient 弃用和 Maven shade 重叠警告仍存在。

最终代码提交 `b184d5f435b5cec09b79dff528845fca14b04fd8` 的 [GitHub Actions](https://github.com/JDinSeattle/AdPulse/actions/runs/34082912455) 已成功。下载核验了 11 份环境/验收报告，并将当前 80 份运行、测试、监控与部署源文件和该提交逐字节比较；[证据汇总](evidence/scaling/cloud-ci.json)包含原报告、hash、91 项回归、真实完整栈核对、索引 HTTP、故障/过期返回、字段投影、回放、分页预算、worker SIGKILL 和 sink 提交前崩溃恢复。后续文档/证据提交不改变这些运行源文件。该环境是托管 runner 上的 Docker 集成，不是托管云业务服务。

完整数据核对也可以直接施加本次的容器预算：

```bash
mkdir -p artifacts/reproduce/full
docker run --rm --network adpulse_default --user "$(id -u):$(id -g)" \
  --cpus 2 --memory 4g --memory-swap 4g \
  --mount "type=bind,src=$PWD,dst=/work,readonly" \
  --mount "type=bind,src=$PWD/artifacts/reproduce/full,dst=/output" \
  -w /work -e PYTHONPATH=/work -e ADPULSE_ROOT=/work \
  -e CLICKHOUSE_URL=http://clickhouse:8123 -e S3_ENDPOINT=http://minio:9000 \
  adpulse-python:0.3.0 python scripts/profile_reconcile.py \
  --index /output/archive.sqlite --output /output/full.json
```

此命令第一次运行包含冷索引构建；本轮完整历史最终重跑复用已校验索引，从新的 oracle scratch 开始，不能直接比较两者 wall time。保留已有报告，后续运行使用新的 `--output` 路径。

监控配置另通过 [Promtool / 实际热加载验收](evidence/scaling/monitoring-config.json)：10 条规则合法，两个 inspection-ready 序列均为 1。新增的两个 Grafana panel 已维护 JSON；本轮没有新增录屏，既有录屏仍对应前轮版本。

## 完整历史最终核对

[最终实际 Docker 报告](evidence/scaling/full-reconciliation.json)：**3,833,704** 条已确认输入全部归档并分类，**6,538** 个指标键、**646,242** 条关联与磁盘独立参考完全一致，完整差异计数为 **0**。这不是抽样检查。

独立容器限制 2 CPU / 4 GiB、无额外 swap；从新 oracle scratch 复算，用时 **977.294 s**（约 16.29 分钟），进程峰值 RSS **173,176 KiB**（169.12 MiB）。cgroup peak **4 GiB**，包含文件缓存，触发内存回收但 OOM / OOM kill 为 0；不能把 169 MiB RSS 当成整个容器峰值。

[退出后资源与磁盘记录](evidence/scaling/full-resources.json)：oracle scratch **7,370,182,656 B**（6.86 GiB），离线索引主文件 **10,169,950,208 B**（9.47 GiB）。复用了前次完整校验的不可变归档索引，未把最初冷索引构建计入 977 秒；未声称本轮执行 --audit 或与旧内存版的完整历史耗时作成对比较。冻结源文件见 [v5 清单](evidence/scaling/full-source-v5.json)。业务输入在核对期间静止；多个来源仍不是原子一致性快照。

剩余边界：旧发布回放仍为内存全快照；后台质量/lineage 全量刷新、索引和 scratch 磁盘继续增长；未做 26 小时保留状态稳态、真实数据库饱和容量、跨主机/可用区容错或托管云业务部署。本轮没有新增录屏。
