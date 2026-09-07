# 状态容量与负载记录方法

## 容量模型

需要分别记录事件序列化字节数、唯一事件 / 业务键率、每秒曝光 / 点击 / 转化数、状态保留时长、RocksDB 本地磁盘和 checkpoint 实际大小。

设 R 为接收 events/s，U 为唯一比例，I/C/V 为三种事件占比，B 为序列化状态平均字节数，O 为 RocksDB 索引、LSM 和对象开销系数：

- 两层实时去重近似 `2 × R × U × 600 × 单键字节数`。
- 曝光状态近似 `R × U × I × 93600 × B`，点击关联状态近似 `R × U × C × 93600 × B`。
- 待匹配最坏约 `R × U × V × 7200 × B`，并受缺点击比例影响。
- 指标贡献去重还保留窗口内业务贡献身份，不能只计算总计数对象大小；一个事件可能形成多个 cohort / counter 贡献。
- 以上字节量乘实测 O，另算 WAL、checkpoint、归档、Kafka 副本与保留开销。不能把 O 随意设为 1 当成结果。

例如 R=1000、U=1、C=0.3、点击 B=600 字节时，单点击状态原始序列化体积约 16.85 GB（十进制），尚未包含 RocksDB / checkpoint / 指标状态。这是算术容量示例，不是当前机器的实测状态大小。

当前聚合按完整指标键集中，贡献以 MapState 保存；大活动可能成为聚合热点。版本一提供热点生成器和 subtask 观测，不声称已经实现二阶段加盐聚合。后续只有在负载证据表明确有瓶颈时，才拆分可结合聚合，click_id 关联键保持不变。

维度历史为广播小表；回放 oracle 为内存小样本实现。完整归档扫描与 HTTP 大结果查询适合实验规模，增长到长时间千万级事件时，应分页读取、外排 / DuckDB 分区复算、维护归档索引，并对查询增加范围约束。

## 实测流程

1. 每档运行 `scripts/loadtest.py --rate 100|1000 --seconds 1800`，记录目标与实际 events/s、确认接收数、逐 receipt 可见覆盖、P95、最大值。
2. 同时采集 `docker stats`、Flink checkpoint size/duration、RocksDB / state 目录磁盘、Kafka / ClickHouse 数据量与 Prometheus 时间段。
3. 区分接收到结果可见的系统延迟和事件发生到结果可见的业务延迟。生成器故意延迟不能混成系统瓶颈。
4. 重复 worker 中断，分别报告任务回到 RUNNING 与业务追赶完成时间；允许使用与真实配置等价的边界数据验证长业务窗口。
5. 每百万输入事件 CPU 秒、存储写入、对象存储 API 请求数与云账单分别计算；没有云账单就不虚构成本结论。

实测报告 `sustained_target_duration_met` 只有运行至少 1800 秒才为 true。短测只能作为冒烟 / 性能趋势，不能写成 30–60 分钟稳态验收。

## 本轮长测与图表复现

`loadtest.py` 现在拒绝覆盖报告，并写相邻的 `.samples.jsonl`。采样间隔按实际耗时记录，包含 collector 接收速率、累计已可见子集的逐 receipt P95、尚不可见数、Docker memory/CPU 与 checkpoint bytes/duration/count。最终 P95 必须等到该实验所有接收 receipt 可见；不能使用中间已可见子集代表全体。

```bash
uv run --no-project --python 3.12 --with matplotlib==3.10.8 \
  python scripts/plot_load.py \
  docs/evidence/operations/steady-100.json \
  docs/evidence/operations/steady-1000.json
```

绘图依赖独立于服务锁文件。输出 PNG/PDF，可直接追溯每个点到采样 JSONL。Docker memory、checkpoint state_size 与前轮 `tracemalloc` Python 分配峰值是不同指标，不能互相替代。该主机还运行其他项目；没有 CPU affinity、独占磁盘或云资源费用隔离。30 分钟输入过程仍在累积 26 小时保留的键，不能因吞吐稳定声称状态内存已达到长期平台值。
