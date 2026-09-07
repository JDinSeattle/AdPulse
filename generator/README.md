业务生成器入口：`python -m adpulse.cli generate`。实现位于 `adpulse/generator.py`。

先保存未受传输错误影响的 `truth.jsonl` 和 `ground-truth.json`，再生成可重复 seed 的 `transport.jsonl`。
支持正常流量、重复、新 event_id 的业务重复、schema 异常、转化先到、乱序和热点活动。
