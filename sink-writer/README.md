`python -m adpulse.worker sink`。只读 `read_committed` 数据。

完整值批量写入 ClickHouse，成功之后同步提交消费位点。写入成功／提交前退出会重放相同 offset。
`argMax(payload, output_offset)` 按 release_id + output_key 选取最新完整快照，再做业务聚合。
拒绝相同 topic/partition/offset 的内容冲突，以及同 release/key 跨 partition 写入。
