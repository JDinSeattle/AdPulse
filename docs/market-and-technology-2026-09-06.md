# 求职与技术更新依据 · 2026-09-06

## 项目定位与样本选择

本项目面向早期职业阶段的 Backend / Cloud Infrastructure 岗位，以及相关的数据基础设施岗位。样本优先选择 new grad / SDE I，不假定三年以上工作年限。岗位是否仍招聘或提供签证支持需申请时另行确认。

评估依据为当前源代码、测试、运行报告及本地经历库。初评时没有可用 Git 历史，因此历史证据采用文件 SHA-256；后续公开提交仅表示代码快照，不倒推历史作者身份。所有业务数据均为合成数据。该文记录初评决策，随后完成的运行时迁移与故障实验见 [运维验证](operations-validation.md)。

## 官方岗位样本

访问日期均为 **2026-09-06**。只把公司页面明确公开的日期称为发布日期；搜索抓取时间、毕业年份、职位编号均不是发布日期。公开页面仍有正文 / 申请入口，也不保证团队尚有名额。

| 样本 | 阶段、地点与日期 | 与本项目相关的明确要求 | 解释边界 |
| --- | --- | --- | --- |
| [Stripe Software Engineer, New Grad · 8128744](https://stripe.com/careers/listing/software-engineer-new-grad/8128744) | New York / Seattle / South San Francisco；官方 JSON-LD `datePosted=2026-09-01`；2027 夏前毕业，非实习专业经历不超过 18 个月 | 编程基础、理解陌生系统、代码评审、安全更新、独立完成小项目、审查和验证 AI 输出 | Java / Ruby / JS / Scala / Go 是该公司栈；页面强调可学习新语言，不能解释为必须全部掌握 |
| [AWS SDE I, ML Infra Services · 10464055](https://www.amazon.jobs/en/jobs/10464055/software-development-engineer-i-ml-infra-services-annapurna-labs) | Cupertino；SDE I；正文未公开可核实发布日期，列为当前可访问样本 | Java 加 Go / Python / TypeScript 之一，Git / CI；分布式与关系数据库、Linux profiling、指标和自动化；项目 / 实习可作为证据 | Neuron、Trainium、EKS 和编译器是团队偏好；AdPulse 只能支持数据基础设施 / 可靠性迁移能力，不是加速器项目 |
| [TikTok Data Engineer Graduate, Monetization Data · A247327](https://lifeattiktok.com/search/7668550561096665397) | San Jose；2027 start；正文未公开可核实发布日期 | 批流管道、SQL、Java / Python / Go、可靠数据模型、质量 / 延迟 / 成本 / 可观测性，以数据验证假设；接受相关项目经验 | 与广告数据项目最直接匹配；这是一条 DE 相邻路线，不把 AdPulse 改称模型训练 / 推理平台 |
| [ServiceNow / Moveworks Associate SWE, Core Infrastructure · JB0070812](https://careers.servicenow.com/jobs/744000107369741/associate-software-engineer-core-infrastructure-moveworks/) | Mountain View；官方日期 **2026-02-04**，早于优先 3–6 个月窗口 | 后端分布式系统、事件流、可观测性、性能度量、容器和云基础设施 | 仅作较早的补充样本，不能当近期新增职位；列举的许多技术不是每个初级岗位的共同门槛 |

排除了需要 PhD / 2026 到岗的 Google infrastructure 样本和明确要求 3+ 年经验的 AWS Transactional Services 样本。Datadog 早期职位在聚合页出现，但对应官方详情 / ATS 未成功核实，不据其正文作结论。

这是有选择的小样本，不能推出市场占比。可以观察到：可靠实现与验证出现在上述多个样本；Java / Python 和分布式数据能力在 AWS / TikTok 重复；可观测性与实测在 AWS / TikTok / 较早的 Moveworks 样本重复。分页、HMAC 或 ClickHouse 不是这些 JD 明确要求的关键词，而是解决当前代码问题、展示上述能力的工程选择。AI / Kubernetes 热度本身没有构成新增组件的理由。

## 与已有项目覆盖对照

| 已有覆盖 | AdPulse 选择 |
| --- | --- |
| 已有准入控制、有界任务与恢复项目 | 不重复添加任务队列或另一套通用 admission 层 |
| 已有事务消息、恢复和 KV 有界扫描项目 | 专注 **OLAP 多版本最新值查询**：不能把历史状态过滤放在 `argMax` 前，不能跨 release 翻页 |
| 已有模型服务、GPU 与内存测量项目 | 不添加模拟模型服务；保留广告测量 / 数据后端定位 |
| 已有容器、Helm / IaC 和可观测性项目 | 不为履历再堆 Kubernetes；增加当前查询路径的实际预算、错误指标和回归证据 |

## 技术生态与选型

以下版本 / 维护信息来自官方仓库、发布说明和文档；不是完整漏洞审计。部分原始 GitHub API 响应保存在本地 `artifacts/refresh/upstream-raw.json`，可分享的摘要在 [`upstream-review.json`](evidence/refresh/upstream-review.json)。

| 组件 | 核实信息 | 本次决定、收益与成本 |
| --- | --- | --- |
| [FastAPI 官方 release](https://github.com/fastapi/fastapi/releases/tag/0.141.1) / [依赖声明](https://github.com/fastapi/fastapi/blob/0.141.1/pyproject.toml) | 0.141.1 发布于 2026-07-29；要求 Python ≥3.10、Pydantic ≥2.9、Starlette ≥0.46 | 从 0.115.12 升至 0.141.1，继续使用现有 Python 3.12 / Pydantic 2.13.5。选择原因是兼容已维护 Starlette 与本项目简单 HTTP 路由；未使用新前端或 AI 特性。成本是 API / collector / OpenAPI 回归与镜像重建 |
| [Starlette 1.6.0](https://github.com/Kludex/starlette/releases/tag/1.6.0) / [官方路径处理公告](https://github.com/Kludex/starlette/security/advisories/GHSA-jp82-jpqv-5vv3) | 1.6.0 发布于 2026-08-08；上述公告修复线为 1.3.0，另有 1.3.1 表单限制修复；旧锁定 0.46.2 在受影响版本范围 | 固定 1.6.0，跨版本回归公共 API；保留 collector 显式 2 MiB 流式读取限制。没有重现本应用可利用的攻击链，不能写“修复了生产漏洞”。httpx TestClient 仍有弃用提示，未顺带迁移测试客户端 |
| [Flink downloads](https://flink.apache.org/downloads/) / [2.0 migration changes](https://flink.apache.org/2025/03/24/apache-flink-2.0.0-a-new-era-of-real-time-data-processing/) / [官方仓库](https://github.com/apache/flink) | 2.3.0（2026-06-25）是当前 stable；同 1.20 线已有 1.20.5（下载列表 2026-06-03；公告 06-08，见后续运维说明）。仓库未归档，2026-09-06 有更新 | 本次保留 1.20.3 及 Kafka connector 3.3.0-1.20。即使补丁更新也应做状态恢复验证；跨 2.x 涉及 API / connector / 配置变更。未把查询优化和双作业状态迁移合并，1.20.5 评估保留为待办，不能称整套栈已最新 |
| [Flink Kafka connector](https://github.com/apache/flink-connector-kafka) / [Kafka](https://github.com/apache/kafka) | 两仓库未归档，分别核实 2026-09-03 / 09-06 的更新；连接器需与 Flink API 配套 | 保留实际验证的 broker 3.9.0 和 connector；未执行 broker 升级或多副本迁移 |
| [ClickHouse release](https://github.com/ClickHouse/ClickHouse/releases/tag/v26.3.32.14-lts) / [官方版本策略](https://clickhouse.com/docs/resources/support-center/knowledge-base/setup-installation/production) | 26.3.32.14 LTS 于 2026-09-06 发布；官方 LTS 支持一年，原 24.8 已超该窗口 | 保持 24.8.14.39 作为此次同版本性能对照，**不作为新的生产版本推荐**。已增加真实 DB 查询验收，为后续隔离升级提供迁移门槛；本次没有改现有数据卷格式 |
| [Debezium releases](https://debezium.io/releases/) / [官方仓库](https://github.com/debezium/debezium) | GitHub `/releases/latest` 返回 404，不代表项目停止维护；版本以项目官网发布线为准 | 保留现有 3.0.8.Final；此次没有 CDC 迁移，因此不声明已升级或重新验证新发布线 |

除了 FastAPI / Starlette 和新增的必需 `annotated-doc`，其余 Python 锁定版本保持原值。镜像基础版本、Kafka / CDC / Flink / ClickHouse 更新仍是独立维护工作，不能因一次 HTTP 依赖更新声称整个环境达到生产安全基线。

## 实施优先级与验收

1. **查询预算及推入数据库的分页**：处理旧接口全量读取后筛选的问题。验收最新 offset 语义、分页完整性、真实预算失败及可复现内存 / 响应 / 延迟对照。
2. **版本固定和客户端重试**：游标携带 release、种类、位置、筛选摘要和固定到期时间，经 HMAC 校验。验收真实 PostgreSQL 并发切换、不混读、稳定重试和失效输入。
3. **有限依赖更新与观测**：升级公共 HTTP 依赖，增加查询耗时 / 行数指标、告警和面板。回归 collector、OpenAPI、原业务对账和完整值读取；明确本地、模拟故障及尚未验证的范围。

实际结果和复现命令见 [`refresh-validation.md`](refresh-validation.md)。
