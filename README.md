# market-data-feed：多数据源实时行情接入与分发服务

接入结构不同的行情来源，经归一化、连续性校验、断连恢复与背压分发后，
对外提供一致且可复现的行情视图。全部来源为本地可控模拟，不依赖外部
网络、真实凭据或付费服务。

## 运行

```bash
uv sync
uv run uvicorn main:app --reload   # HTTP 服务
uv run pytest -q                   # 测试（46 项）
```

## 支持的来源与归一化规则

| 维度 | source_a | source_b |
|---|---|---|
| 标的字段 | `sym` | `instrument` |
| 序号字段 | `seq` | `sequenceNumber` |
| 时间字段 | `ts_ms`（毫秒 epoch，UTC 语义） | `eventTime`（ISO 本地时间，无时区） |
| 时间处理 | 直接解释为 UTC | 按来源配置的本地时区（默认 UTC+8）解释后转 UTC，**绝不当作全局时间** |
| 价格字段 | `px`（字符串） | `price`（浮点） |
| 价格处理 | `Decimal(str)` | `Decimal(repr(f))`，杜绝浮点精度漂移 |
| 可空字段 | `qty` 可缺省 | `volume` 可缺省（None） |

统一模型 `NormalizedTick(source, symbol, sequence, event_time(UTC), price(Decimal), volume, raw_id)`。

**拒绝规则（不静默丢弃，原因可区分）**：`UNKNOWN_SOURCE` / `UNKNOWN_SYMBOL` /
`MALFORMED_RECORD` / `MISSING_FIELD` / `INVALID_PRICE`（含 NaN、Inf、负数、布尔）/
`INVALID_TIMESTAMP` / `INVALID_SEQUENCE` / `NOT_SUBSCRIBED`。
HTTP 层对拒绝返回 `422 + {reason, detail}`。

## 订阅语义

- 按 `(source, symbol)` 维护订阅；标的必须在本地注册表中存在。
- 重复订阅 → `ALREADY_SUBSCRIBED`（409）；取消未订阅 → `NOT_SUBSCRIBED`（409）；
  订阅不存在标的 → `UNKNOWN_SYMBOL`（409）。
- 所有非法操作显式拒绝且不破坏已有订阅（校验与变更在同一把锁内完成）。

## 连续性校验与恢复语义

每个 `(source, symbol)` 一个 `SequenceTracker` 状态机：

| 场景 | 判定 | 处置 |
|---|---|---|
| 序号 == 期望且时间不回退 | `ACCEPTED` | 更新最新价并分发 |
| 序号已处理过 | `DUPLICATE` | 丢弃，可追溯 |
| 序号 < 期望且未见过 | `OUT_OF_ORDER` | 仅补历史，**不作最新价** |
| 序号 > 期望（跳变） | `GAP_DETECTED` | 进入恢复态，本条及后续正常行情 `BUFFERED` |
| 时间早于最新已接受时间（超容忍度） | `STALE` | 序号被消费（防阻塞），但不更新最新价 |

**断连恢复**：`POST /recover/{source}/{symbol}` 提交回补数据。
- 恢复期间到达的正常行情被缓冲，**不与恢复数据混用**；
- 回补经 `apply_replay` 校验（必须在缺口区间内、不重复）；
- 恢复结束后缓冲行情重新判定并按序补推 —— **不重复推送、不漏推**；
- 缺口无法闭合时生成 `GapMarker`（`GET /gaps` 可查）**显式标记，而非静默跳过**；
  缺失序号若迟到到达，按 `OUT_OF_ORDER` 补历史。

## 背压与限速

- 每个订阅者有界队列（`maxsize`），杜绝无界内存占用；积压策略三选一：
  - `COALESCE_LATEST`：同标的旧行情被最新行情合并替换；
  - `DROP_OLDEST`：丢弃最旧；
  - `REJECT`：拒绝写入（背压上抛）。
- 每次合并/丢弃都生成 `DropRecord`（订阅者、被弃行情、策略、原因），
  `GET /drops` 可查，全程可追溯。
- 可选令牌桶限速（`Distributor(rate_per_sec=...)`），允许桶容量内突发，
  超出部分等待令牌，超时丢弃并记录。
- 单订阅者异常被隔离，不影响其他订阅者。

## 一致性保证

- 价格全程 `Decimal`，归一化与比较无精度丢失；跨来源同价数值相等。
- 时间统一 UTC 语义；来源本地时间按来源时区解释。
- 快照更新与连续性判定在同一把锁内完成，无半更新状态；
  最新价按 `(source, symbol)` 维度维护，杜绝串号/价格错配。

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/subscribe/{source}/{symbol}` | 订阅（201 / 409） |
| DELETE | `/subscribe/{source}/{symbol}` | 取消订阅 |
| GET | `/subscriptions` | 当前订阅快照 |
| POST | `/ingest/{source}` | 推送一条原始行情（200 / 422） |
| POST | `/recover/{source}/{symbol}` | 断连恢复（body 为回补记录数组） |
| GET | `/latest/{source}/{symbol}` | 最新价 |
| GET | `/gaps` | 不可恢复缺口标记 |
| POST | `/subscribers/{id}` | 注册分发订阅者（maxsize/policy） |
| GET | `/subscribers/{id}/poll` | 订阅者拉取一条 |
| GET | `/drops` | 被合并/丢弃记录 |

## 验证方法

```bash
uv run pytest -q          # 全部 46 项
uv run pytest tests/test_normalization.py -q   # 畸形记录/精度/时区
uv run pytest tests/test_sequencer.py -q       # 乱序/重复/跳变/过期/恢复
uv run pytest tests/test_distributor.py -q     # 背压/限速/隔离
uv run pytest tests/test_concurrency.py -q     # 多源并发一致性
uv run pytest tests/test_api.py -q             # HTTP 端到端
```

每个用例通过 `log_verdict` 在日志中打印 **输入（CASE/INPUT）与判定依据
（VERDICT）**，结论可区分且可复现（固定时钟基准 `T0`，无外部依赖）。
服务运行日志打印 `INGEST`（原始输入）、`CLASSIFY`（判定结论与原因）、
`GAP`/`GAP_UNRECOVERABLE`（缺口）、`DROP/COALESCE`（背压处置）。
