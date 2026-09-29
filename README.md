# Deterministic Multi-Source Market Data Feed

这是一个不依赖外部网络、真实凭据或付费服务的本地实时行情接入与分发服务。服务把结构不同的来源记录归一化为统一模型，对序号、时间、缺口恢复和订阅者背压进行确定性处理，并通过结构化日志打印原始输入、判定结果和依据。

## 1. 支持的行情来源

### Source A：`source_a`

JSON 对象，字段如下：

| 来源字段 | 统一字段 | 规则 |
| --- | --- | --- |
| `sym` | `symbol` | 非空字符串，去除首尾空白 |
| `seq` | `sequence` | 非负整数，必须为 JSON number 整数 |
| `ts_ms` | `event_time` | 非负整数，UTC 毫秒时间戳，转为 UTC `datetime` |
| `px` | `price` | 非空整数或十进制字符串；必须大于 0；拒绝二进制浮点 |
| `qty` | `quantity` | 非空整数或十进制字符串；允许 0；拒绝负数 |
| `trade_id` | `record_id` | 非空字符串 |

示例：

```json
{
  "trade_id": "a-AAA-12",
  "sym": "AAA",
  "seq": 12,
  "ts_ms": 1790035212000,
  "px": "100.2500",
  "qty": 3
}
```

### Source B：`source_b`

JSON 对象，字段命名、时间格式、价格表示和空值语义与 Source A 不同：

| 来源字段 | 统一字段 | 规则 |
| --- | --- | --- |
| `ticker` | `symbol` | 非空字符串 |
| `sequence` | `sequence` | 非负整数或纯数字字符串 |
| `event_time` | `event_time` | ISO-8601，必须显式携带 UTC offset；转换为 UTC |
| `price_scaled` + `price_scale` | `price` | 定点整数价格：`price_scaled × 10^-price_scale`，必须大于 0 |
| `size` | `quantity` | 十进制数；允许缺失或 `null`，统一为 `None` |
| `id` | `record_id` | 非空字符串 |

示例：

```json
{
  "id": "b-BBB-12",
  "ticker": "BBB",
  "sequence": "12",
  "event_time": "2026-09-22T10:00:12.250000+02:00",
  "price_scaled": 1002500,
  "price_scale": 4,
  "size": "2.50"
}
```

无 offset 的本地时间不会被猜测为 UTC 或任何全局时间，而是以 `invalid_timestamp` 拒绝。

## 2. 统一模型与拒绝原因

统一行情模型为 `market_feed.models.Quote`：

- `source`
- `symbol`
- `sequence`
- `event_time`：始终是带时区的 UTC 时间
- `received_time`：服务接收时间，始终是 UTC
- `price`：`Decimal`
- `quantity`：`Decimal | None`
- `record_id`
- `raw_record`

非法记录不会被静默丢弃，而会生成 `RejectedRecord` 并向该来源/标的的订阅者发布 `rejection` 事件。可区分原因包括：

- `malformed_record`
- `unknown_source`
- `unknown_symbol`
- `invalid_number`
- `invalid_timestamp`
- `non_utc_timestamp`（保留用于未携带 offset 等时间语义错误的扩展分类）
- `invalid_sequence`
- `illegal_price`
- `unsupported_record`

日志事件 `record_rejected` 包含原始记录、拒绝原因和详细说明。

## 3. 数值精度和时区语义

- 价格和数量使用 Python `Decimal`，归一化后仍保留十进制文本的精度和尾零，例如 `"12.3400"` 不会变成 `12.34` 的二进制浮点表示。
- Source A 明确拒绝 JSON 浮点价格（如 `1.25`），避免二进制浮点误差在合并和比较时造成错误结果。
- Source B 使用定点整数和显式 scale，从输入层面避免浮点价格。
- 所有比较都使用 `Decimal` 和 UTC `datetime`。
- `received_time` 只表示本地服务接收时刻；来源事件顺序以 `sequence` 为主、`event_time` 单调性为辅助约束。
- 来源本地时间必须携带 offset；裸本地时间不会被当成全局时间。

## 4. 订阅规则

订阅状态按“订阅者 + 来源 + 标的”维护，并在同一把锁内原子更新双向索引：

- 重复订阅：幂等成功，返回 `changed=false`，不产生重复投递。
- 取消未订阅标的：幂等成功，返回 `changed=false`。
- 未知来源或未知标的：明确拒绝，不修改已有订阅。
- 非法订阅者 ID：明确拒绝。
- 一个订阅者异常退订或处理失败，不会删除其他订阅者的状态。

默认已知标的为 `AAA`、`BBB`，可在构造 `MarketDataFeedService(known_symbols=...)` 时替换。

## 5. 序号、去重、乱序和过期处理

每个 `(source, symbol)` 拥有独立的 `SequenceTracker`。

1. **连续序号**：正常发布为 `quote`，可作为最新价。
2. **重复序号**：返回 `duplicate`，不重复发布。重复重放同一记录也是该结果。
3. **迟到/乱序序号**：序号小于已发布水位时返回 `out_of_order`，不会发布，也不会成为最新价。
4. **过期事件时间**：即使序号前进，只要 `event_time` 不新于当前水位，就返回 `stale` 且不发布。
5. **未来序号形成缺口**：缺口长度不超过 `max_recoverable_gap` 时，先发布 `gap(recovering)`，未来行情进入按序缓冲区，不作为最新价。
6. **缺口扩展**：恢复期间又收到更远的未来记录时，会再次发布 `gap(recovering)`，其中 `missing_sequences` 精确列出当前仍缺失的序号集合。
7. **缺口恢复**：缺失序号到达后，缓冲记录严格按序号释放，事件类型为 `recovered_quote`，与正常 `quote` 区分。
8. **恢复完成**：所有恢复行情发布后，再发布 `gap(normal)` 控制事件；其 `recovered_sequences` 只包含实际缺失后找回的序号，不包括一开始就提前到达并缓冲的未来行情。
9. **超大缺口**：返回并发布 `gap(unrecoverable)`，触发记录不会被混作正常行情；必须调用 `resume_after_gap(...)` 显式安装新水位。

## 6. 断连与重连语义

- `disconnect_source(source)` 后，新的注入以 `unsupported_record` 拒绝并记录，不会伪装成正常行情。
- `reconnect_source(source, replay_records)` 按顺序重放。
- 已发布过的重放记录得到 `duplicate`，不会产生第二次行情推送。
- 可恢复缺口通过同一序号状态机补发，保证按序、不重复。
- 无法恢复的缺口不会静默跳过，而是发布 `gap(unrecoverable)`，并阻塞后续发布直到显式 resume。
- 本地脚本来源 `ScriptedSource` 支持 `seek(...)`、`seek_sequence(...)`、`fetch_batch(...)`，可复现断线、游标回退和部分重放。

## 7. 分发、背压和限速

每个订阅者有独立的有界队列，队列容量由 `SubscriberOptions.queue_size` 控制，内存不会无界增长。

| 策略 | 行为 | 审计 |
| --- | --- | --- |
| `block` | 队列满时延迟发布者，直到有容量 | 无丢弃；队列长度可观测 |
| `drop_oldest` | 删除最旧事件，为新事件腾位 | `backpressure_decision` 记录被丢弃事件和序号 |
| `coalesce_latest` | 仅合并同一订阅者、来源、标的和投递类型的行情，保留较新序号 | 被合并事件以 `coalesced` 记录 |
| `reject` | 队列满时拒绝新事件 | 审计记录新事件、原因和序号 |

不同标的、不同来源、普通行情与恢复行情不会互相合并。合并、丢弃、拒绝均有结构化日志可追溯。

订阅者回调在独立协程中运行；单个回调抛出异常只记录 `subscriber_handler_error`，不会中断其他订阅者。

并发模型遵循 FastAPI/asyncio 的单事件循环语义：多个来源写入任务、多个订阅者读取任务可以并发运行；服务通过来源锁和标的锁保证同一来源/标的的状态转移原子且有序。若从工作线程接入外部组件，应由调用方把 `ingest(...)` 调度回服务所属事件循环，而不是在多个线程中各自创建事件循环共享同一 asyncio 队列。

## 8. HTTP 接口

服务入口为 `main:app`。

- `GET /health`
- `GET /state/subscriptions`
- `GET /state/queues`
- `GET /state/history`（历史窗口保留/淘汰状态，见第 9 节）
- `POST /subscriptions`（支持 `with_history`、`snapshot_limit`、`after_sequence`、`wait_for_gap`、`gap_wait_timeout`）
- `DELETE /subscriptions`
- `POST /sources/{source}/ingest`
- `POST /sources/{source}/disconnect`
- `POST /sources/{source}/reconnect`
- `GET /subscribers/{subscriber_id}/events`

订阅示例：

```bash
curl -X POST http://127.0.0.1:8000/subscriptions \
  -H 'content-type: application/json' \
  -d '{
    "subscriber_id": "s1",
    "source": "source_a",
    "symbol": "AAA",
    "queue_size": 4,
    "backpressure": "coalesce_latest"
  }'
```

注入示例：

```bash
curl -X POST http://127.0.0.1:8000/sources/source_a/ingest \
  -H 'content-type: application/json' \
  -d '{"record":{"trade_id":"a1","sym":"AAA","seq":1,"ts_ms":1790035201000,"px":"100.25","qty":1}}'
```

## 9. 中途接入：有界历史快照与断点续传

为“行情已经跑了一段时间后才启动”或“崩溃后只想从上次位置接着看”的消费者，服务在不改变原有订阅/序号/背压语义的前提下新增了一层**按 `(来源, 标的)` 隔离的有界历史窗口**，由 `market_feed.history.HistoryStore` 实现，并通过 `MarketDataFeedService.subscribe_with_history(...)` 暴露。

### 9.1 窗口里只放“已经确认”的行情

- 只有完整通过序号与 `event_time` 校验、并由 `SequenceTracker` 实际释放的行情才会进入窗口：正常行情（`accepted`）和缺口补齐后按序释放的恢复行情（`recovered`）。
- 乱序、过期、重复、仍在 `recovering` 缓冲区里等缺口的未来行情、以及落在不可恢复缺口之后的行情**都不会**进入窗口。
- 每个 `(source, symbol)` 拥有独立窗口，不同来源/标的绝不串位。
- 窗口写入与行情发布在同一把 per-key 条件锁内完成；生成快照只做一次内存拷贝，**不会长时间阻塞写入**，且等待某个标的补齐缺口时，其他来源/标的的写入完全不受影响。

### 9.2 保留范围、容量上限与可观测性

保留策略由 `HistoryRetention` 控制：

| 参数 | 含义 | 默认 |
| --- | --- | --- |
| `max_quotes` | 每个 `(来源, 标的)` 窗口的**硬性条数上限**，超出立即淘汰最旧行情 | `64` |
| `max_age_seconds` | 可选的时间上限，按最新行情的 `event_time` 衡量，超出的旧行情被淘汰 | `None`（不限时间） |

构造服务时传入：`MarketDataFeedService(history_retention=HistoryRetention(max_quotes=128, max_age_seconds=300))`。

淘汰是可观测的：

- Python：`service.history.stats(key)` / `service.history_stats()` 返回 `KeyHistoryStats`，包含 `retained`、`capacity`、`oldest_sequence`、`newest_sequence`、最早/最新 `event_time`、累计淘汰数 `total_evicted`、最后淘汰到的序号 `last_evicted_sequence`，以及 `gap_state` 和 `continuity_reset`。
- HTTP：`GET /state/history` 以 `source/symbol` 为键返回上述字段。
- 日志事件 `history_eviction` 记录每次被淘汰的序号集合与淘汰后保留数量。

### 9.3 中途订阅：先拿快照，再平滑进入实时流

```python
result = await service.subscribe_with_history("late", SourceId.SOURCE_A, "AAA")
result.status            # HistoryJoinStatus.SNAPSHOT_READY
result.quotes            # tuple[Quote]：最近一段已确认行情，按序号升序
result.last_sequence     # 快照水位：实时流严格从该序号之后开始
```

- 快照通过返回值**带外**交付（不占用订阅者队列容量），随后订阅者队列里只会收到一个 `history_boundary` 事件（`DeliveryKind.HISTORY_BOUNDARY`，负载为 `HistoryBoundary`），标记快照与实时流的交界。
- 边界之后订阅者只收到 `last_sequence` 之后的实时行情：快照里给过的不会再实时投一遍，边界之后的行情也不会漏。订阅者无需自行去重。
- 边界标记**只发给新加入的订阅者本人**，已存在的订阅者继续收到纯实时流。
- `snapshot_limit` 可进一步限制本次快照最多返回多少条（返回最新的 N 条）。
- 重复对同一 `(订阅者, 来源, 标的)` 调用返回 `already_subscribed`，不重发快照、不重复入队；需要重新对齐请先退订。

订阅时如果该标的**正在补可恢复缺口**：

- 默认 `wait_for_gap=True, gap_wait_timeout=5.0`：服务等待缺口补齐，随后给出从缺口前到补齐后的**前后一致**快照；
- 超时或显式 `wait_for_gap=False`：返回 `gap_pending`，`incomplete=True`，并附上“缺口之前”的部分窗口（其末尾不连续），同时**不会**注册订阅、不会制造一个看似正常实则缺一截的视图。调用方可稍后重试，或退回到普通 `subscribe(...)` 只收实时流。

若该标的横亘着**不可恢复缺口**（尚未 `resume_after_gap`）：返回 `unrecoverable_gap`，同样不注册订阅。显式 resume 会**清空缺口之前的整段历史**（记录为连续性重置），因此 resume 之后的快照绝不会把缺口两侧拼成一段连续视图。

### 9.4 断点续传：带“上次处理到哪”的进度标记重连

```python
marker = ProgressMarker(source=SourceId.SOURCE_A, symbol="AAA", sequence=12)
result = await service.subscribe_with_history(
    "resumer", SourceId.SOURCE_A, "AAA", checkpoint=marker,
)
```

- 只要 `sequence` 还在保留窗口内，服务返回 `resume_ready`，并把**严格位于该序号之后**的已确认行情作为补发窗口（`quotes`），已经处理过的不重发；之后经同一个 `history_boundary` 平滑接上实时流。
- `checkpoint == 最新确认序号`：补发为空，直接续实时流。不可恢复缺口 resume 之后用“基线序号”续传也属于这种情况。
- `checkpoint` 已落在保留范围之外（旧数据已被淘汰）：返回 `checkpoint_too_old`，`incomplete=True`，**不注册订阅、不静默跳过**，detail 明确提示只能重新拉一份快照从头对齐。
- `checkpoint` 比最新确认序号还大：返回 `checkpoint_ahead`。
- 续传路径上横着未解决的不可恢复缺口：返回 `unrecoverable_gap`。与快照一致，服务宁可显式拒绝也不会悄悄给出不连续的流。
- `checkpoint` 的来源/标的必须与订阅目标一致，否则请求被拒绝。

### 9.5 与缺口恢复、背压的配合

- 可恢复缺口期间：历史窗口停留在缺口前的连续水位，补齐后恢复行情一次性按序追加，等待中的订阅请求被唤醒并拿到完整快照。
- 不可恢复缺口：窗口不接纳缺口后的任何行情；`resume_after_gap(...)` 清空旧窗口并设置新基线，之后新行情重新累积。
- 快照/补发带外返回，不受订阅者有界队列的背压影响；交界标记作为单个事件进入订阅者队列，之后所有实时行情继续遵守该订阅者配置的 `queue_size` 与 `block / drop_oldest / coalesce_latest / reject` 策略（含既有丢弃/合并审计）。

### 9.6 HTTP 用法

`POST /subscriptions` 在原有字段上新增可选参数（不传时行为与以前完全一致）：

```bash
# 中途订阅，带最近 10 条已确认快照
curl -X POST http://127.0.0.1:8000/subscriptions \
  -H 'content-type: application/json' \
  -d '{
    "subscriber_id": "late", "source": "source_a", "symbol": "AAA",
    "with_history": true, "snapshot_limit": 10,
    "queue_size": 32, "backpressure": "block"
  }'
# 响应含 join_status / incomplete / last_sequence / quotes[]
# 之后 GET /subscribers/late/events 的第一个事件是 history_boundary

# 断点续传：只补发 12 之后的行情
curl -X POST http://127.0.0.1:8000/subscriptions \
  -H 'content-type: application/json' \
  -d '{"subscriber_id":"resumer","source":"source_a","symbol":"AAA","after_sequence":12}'
```

`join_status` 取值：`snapshot_ready`、`resume_ready`、`gap_pending`、`unrecoverable_gap`、`checkpoint_too_old`、`checkpoint_ahead`、`already_subscribed`、`rejected`。`GET /state/history` 查看各窗口保留/淘汰状态。

## 10. 本地验证

运行所有测试：

```bash
uv run pytest -q
```

测试覆盖：

- 两种来源的字段差异、定点价格、可空数量、UTC offset 和畸形记录分类；
- 重复订阅、取消未订阅、未知标的和已有订阅保护；
- 重复、乱序、过期、小缺口恢复和超大缺口显式不可恢复；
- 断连拒绝、重连重放去重和显式 resume；
- 四种背压策略、有界队列及丢弃/合并审计；
- 多来源并发写入、多订阅者读取、价格/标的不错配；
- 单个订阅者回调异常隔离；
- HTTP API 的订阅、注入、重复判定和 Decimal JSON 表示；
- **中途订阅先拿有界快照再经边界标记平滑进入实时流（不重不漏、无需消费端去重）；**
- **带进度标记重连只补发缺失部分；checkpoint 超范围/超前被显式拒绝；**
- **补缺口途中订阅会等待一致快照，超时明确返回 gap_pending；不可恢复缺口返回 unrecoverable_gap，resume 后旧历史被清空；**
- **快照生成不阻塞其他来源/标的写入；多个并发中途订阅者各自拿到独立且一致的快照；**
- **历史窗口的条数/时间上限淘汰与保留量、淘汰位置可观测。**

测试运行时会输出 JSON 结构化日志。关键日志包括：

- `ingest_input`：来源和原始输入；
- `sequence_decision`：序号、时间、状态和判定依据；
- `sequence_gap`：缺口范围、状态和恢复序号；
- `record_rejected`：原始记录和拒绝原因；
- `backpressure_decision`：丢弃、合并或拒绝审计；
- `history_eviction`：历史窗口淘汰的序号集合、淘汰后保留数量；
- `history_subscription`：快照/续传请求的 join 状态、快照条数与水位；
- `source_disconnected` / `source_reconnected` / `explicit_gap_resume`。

启动本地服务：

```bash
uv run uvicorn main:app --reload
```
