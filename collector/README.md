`uvicorn adpulse.collector:app --host 0.0.0.0 --port 8088`。

POST `/v1/events`，请求含 `client_batch_id` 和 `events`（1–1000 条）。单请求最多 2 MiB。
Kafka 同一事务提交 raw 记录和完整接收清单之后才返回 202；失败返回 503。
客户端重试可能形成新的接收批次，业务 ID 保持相同，下游依据业务键去重。
坏业务 schema 仍持久接收，由清洗作业隔离并保留接收证据。
