# 有界结果查询与版本一致性

`0.2.0` 将 `/v1/metrics`、`/v1/associations` 改为分页接口。字段 `metrics` / `associations` 和 `release_id` 保留，默认只返回 **100 条**，`limit` 范围 **1–500**。增加 `next_cursor`、`has_more`、`consistency`。原来假定一次请求返回全部结果的客户端必须循环翻页；全量对账保留完整验收语义；0.3.0 默认使用磁盘参考计算和流式数据库读取，旧内存路径仍可用于小样本对照。

```python
import requests

params = {"release": "replay-verified-20260906", "status": "matched", "limit": 500}
while True:
    response = requests.get("http://localhost:8080/v1/associations", params=params, timeout=15)
    response.raise_for_status()
    page = response.json()
    for record in page["associations"]:
        print(record["conversion_id"])
    if page["next_cursor"] is None:
        break
    params["cursor"] = page["next_cursor"]
```

指标筛选支持 `cohort=occurrence|impression|click`、`campaign`、`region`、`app_version`；关联筛选支持 `status=pending|matched|unmatched`。后续页必须保持相同筛选条件，`limit` 可以改变。金额 / CTR / CVR 继续遵守原 cohort 规则，数据质量提示保留。

## 顺序与正确性

每页先按 `release_id + output_key` 取 `argMax(payload, output_offset)`，随后筛选状态 / 维度，最后按 `output_key` 排序、取 `limit+1`。不能在求最新值前筛选 `status=pending`：这会让已被更新为 matched 的旧 pending 版本重新出现。`after` 仅作用于不可变业务键，因此可以推入 GROUP BY 之前。

所有值都用 ClickHouse 查询参数传递；字段名来自固定白名单。结果 topic 的单键分区不变及同 offset 内容一致规则仍由 sink 保证；分页没有改变交付语义。

首个请求未指定 release 时解析活动指针。下一页从签名游标读取同一个 release，原子推广 / 回退不会改变正在遍历的版本。未知 release 返回 404，未验证 / 失败 release 返回 409。`retired` 版本仍可追查。

- `immutable_release`：已验证、按发布流程保持不变的 replay。键集稳定，顺序翻页和同游标重试可复现。
- `live_keyset`：live release 持续接受新的完整值。只保证不跨 release；**不保证跨页 MVCC 快照**。在已过游标位置插入的新键可能不会出现在本次遍历，旧键的值也可能变化。要求完整复现时使用已验证 replay。

游标采用 HMAC-SHA256、固定 30 分钟生命周期、4 KiB 输入上限，绑定查询种类、筛选摘要、release 和最后一个业务键。后续页不延长到期时间。签名不是鉴权；本地治理 API 的访问控制边界未改变。Compose 使用明确的本地演示 key，共享部署应设置 `ADPULSE_CURSOR_SECRET`（至少 32 字节）并在所有副本保持一致；换 key 会让旧游标失效。未设置时独立进程生成临时 key，重启会失效。

旧事件契约部分字符串没有最大长度。超过游标容量的业务键若需要继续翻页，会明确返回 422，不能生成下一页无法使用的 token；应缩小筛选范围或用归档导出。没有静默丢弃超长键，也没有悄悄改变已发布事件契约。

## 资源和故障边界

服务查询默认限制：每页最多 500 行，最多读取 200 万物理行、128 MiB 查询内存、8 MiB 响应体、2 个 ClickHouse 线程、3 秒数据库执行预算；HTTP 连接超时 3 秒、读超时 6 秒。数据库计时 / 内存 / 行数检查发生在引擎检查点，并非实时调度或整个进程的硬资源隔离。每个 API 进程额外限制最多 4 个并发分页请求；超过时返回 `QUERY_OVERLOADED` / HTTP 503 / `Retry-After: 1`。多个 API 进程仍需分别规划资源，不是全局预算。

行数 LIMIT 只约束结果，不能代表底层只扫描这些行。扫描、内存和结果预算均使用 `throw`，`wait_end_of_query=1` 避免把服务器中途失败当作完整成功；HTTP 流式读取另有字节上限。超时返回 504，数据库或预算失败返回带稳定错误码的 503，不返回部分页。客户端应区分依赖恢复重试与需缩小范围的预算错误。

`receipt_updates` 是内部逐事件可见性载荷，在数据库响应中替换为空值、服务响应中移除；不改变内部审计存储。0.3.0 的 `/v1/reconcile`、`/v1/quality` 改为带检查时间的后台结果；`/v1/trace` 使用归档与质量索引，`/metrics` 读取后台快照。冷启动、失败和过期有明确状态，详见 [本机扩展性验证](scaling-validation.md)。内部完整快照及旧 replay 发布路径仍属于批量工具，未统一改成流式发布。

`adpulse_query_seconds{kind,outcome}`、`adpulse_query_rows_total{kind}` 只使用有限标签，没有 release、cursor、查询值或 receipt ID 标签。查询指标和新的 Grafana 面板、`AdPulseQueryFailures` 告警覆盖这两个有界结果端点。拒绝于参数 / 游标 / registry 阶段的请求不计入数据库查询直方图；准入失败以 `outcome="overloaded"` 和零执行耗时单独计数。

## 复现

```bash
make test
# 先通过真实链路并发布一个 immutable replay，然后运行：
make query-check RELEASE=replay-verified-20260906
make query-benchmark RELEASE=replay-verified-20260906
```

`query_acceptance.py` 在同一真实本地 PostgreSQL 内创建独立 schema，在 ClickHouse 创建独立的合成 release 记录，完成后删除自己创建的内容；不会切换正常服务的活动版本。故障场景是可控注入，不能称为生产事故。GitHub 托管 CI 执行真实完整栈验收；具体执行提交与结果见 [运维验证](operations-validation.md) 和 [本轮验证](scaling-validation.md)。
