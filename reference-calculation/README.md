独立 Python 批量 oracle：`adpulse/oracle.py`，不导入 Java/Flink 实现。

按输入接收顺序选取同业务键第一条有效事件，再按完整事件时间进行曝光、点击、转化关联。
完整回放范围内持久去重；实时链路只承诺规则定义的 10 分钟。
CTR 使用曝光 cohort 的 clicks / impressions；CVR 使用点击 cohort 的 converted_clicks / clicks。
货币金额单列 conversion_value cohort，按 currency 分组，不跨币种相加。
