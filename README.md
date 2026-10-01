# Deterministic Multi-Source Market Data Feed

这是一个不依赖外部网络、真实凭据或付费服务的本地实时行情接入与分发服务。服务把结构不同的来源记录归一化为统一模型，对序号、时间、缺口恢复和订阅者背压进行确定性处理，并通过结构化日志打印原始输入、判定结果和依据。在实时分发之外，它还为“中途接入”的消费方提供**有界历史快照（snapshot）**和**断点续传（resume）**。

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

## 8. 有界历史快照与断点续传

为了支撑“行情已经跑了一段后才启动”和“崩了以后从上次处理位置接着看”的消费方，服务在实时状态机之外维护一份**每个 `(source, symbol)` 独立、容量有上限、只含已确认行情**的历史（`market_feed.history.HistoryStore`）。

### 8.1 什么数据会进历史

只有**完整通过序号与时间校验、由状态机按序释放**的行情才会写入历史：

- 正常连续行情（`accepted`）与缺口补齐后按序释放的行情（`recovered`）；
- 乱序（`out_of_order`）、过期（`stale`）、重复（`duplicate`）的行情**不进历史**；
- 还在缓冲区等补齐的未来行情（`buffered`）**不进历史**，它们尚未成为可用参考；
- 发生不可恢复缺口时，缺口之前的历史会被整体废弃并落一个**栅栏（barrier）**，快照绝不可能“看起来正常却跨过一个洞”；
- 调用 `resume_after_gap(...)` 显式安装新水位后，历史以该水位为排他下界重新开始。

### 8.2 保留范围、容量上限与可观测性

- 容量由构造参数 `MarketDataFeedService(history_capacity_per_key=N)` 控制（默认 `64`），按来源/标的各自独立计数；
- 超出容量的最旧行情被淘汰（evict），内存不会无界增长；
- 通过 `service.history_stats(source, symbol)`（HTTP：`GET /state/history/{source}/{symbol}`）可观测：
  - `retained_count`：当前保留条数；`capacity`：上限；
  - `oldest_sequence` / `newest_sequence`：保留窗口边界；
  - `evicted_total`：累计淘汰条数（“淘汰到哪”）；
  - `barrier_sequence`：不可恢复缺口/显式 resume 后有效区域的排他下界；
- 历史存储本身只用普通锁且全程不 `await`，生成快照只是拷贝一个有界切片，**不会长时间卡住正常写入**。

### 8.3 订阅时要一份快照

在 `subscribe(...)` 上传 `snapshot=<最多回看条数>`：

```python
result = await service.subscribe(
    "late-consumer", SourceId.SOURCE_A, "AAA",
    SubscriberOptions(queue_size=32),
    snapshot=10,
)
envelope = result.handoff          # HandoffEnvelope
envelope.quotes                    # 最近至多 10 条已确认行情，按序号升序
envelope.live_from_sequence        # 实时流的第一条序号 = 快照最后一条 + 1
```

服务先把快照作为一个 `handoff` 投递事件放进该订阅者队列的**最前面**，之后的实时行情排在其后。订阅者读到的顺序是：

1. 一个 `DeliveryKind.HANDOFF` 信封事件，`payload.quotes` 即历史视图；
2. 从 `live_from_sequence` 开始的正常实时 `quote` / `recovered_quote`。

交界处由服务在该标的的同一把锁内完成“选切片 + 注册订阅 + 入队历史”，因此**既不会重复投，也不会漏投**，订阅方不需要再做一轮去重。

### 8.4 补缺口期间订阅：纯实时照常，快照/续传等一致或明确拒绝

缺口状态只影响**依赖一致性初始视图**的请求，两类订阅语义不同：

| 订阅类型 | 可恢复缺口补齐中（`recovering`） | 不可恢复缺口/未重新对齐（`unrecoverable`） |
| --- | --- | --- |
| **纯实时订阅**（不传 `snapshot`、不传 `resume_from`） | 正常订阅成功；之后顺着实时流走——缺口补齐时收到 `recovered_quote`，补齐后收到正常 `quote`；不会被塞任何历史数据，也不会收到订阅前的行情 | 同样正常订阅成功；`resume_after_gap(...)` 对齐后自然收到新区域的实时行情 |
| **快照 / 续传订阅**（传 `snapshot` 或 `resume_from`） | 默认立即拒绝，返回 `kind=snapshot_incomplete`，`detail` 给出缺口范围；传 `wait_for_consistency=True`（可配 `consistency_timeout` 秒）则非阻塞轮询等缺口补齐后再给一致视图，超时仍返回 `snapshot_incomplete` | 返回 `kind=resume_barrier_present`；必须先 `resume_after_gap(...)` 重新对齐再带快照订阅 |

要点：

- 纯实时订阅既不要求初始视图一致、也不带进度标记，因此缺口状态**永远不挡**它——这与引入快照/续传功能之前的行为完全一致；
- 快照/续传被拒时是**干净拒绝**：不会注册订阅、不会创建队列，该订阅者在其他标的上的已有订阅也不受影响；调用方改成带快照或修正进度后重试即可正常成功；
- 纯实时订阅即使在缺口期间接入，也只收到**接入之后**释放的行情（补齐的是 `recovered_quote`），订阅之前已确认的行情不会被补投；
- `wait_for_consistency` 只对快照/续传请求生效，纯实时订阅传它没有作用（也不会等待）。

### 8.5 带进度标记的断点续传

消费端用 `ResumePosition(source, symbol, sequence)` 表示“`sequence`（含）之前我都处理过了”，在 `subscribe(..., resume_from=...)` 时传回：

- 只要该位置仍在保留窗口内且中间没有栅栏，服务就返回严格在该位置**之后**的保留行情，已经处理过的不重发，随后平滑接上实时流（同样以 `handoff` 信封 + 实时流交付）；
- 以下三种情况服务**绝不静默跳过**，而是明确返回错误并要求调用方“重新拉快照、从头对齐”（此时不创建订阅）：
  - `resume_position_evicted`：进度标记早于保留窗口，中间的行情已被淘汰；
  - `resume_position_ahead`：进度标记比保留的最新行情还新（例如连到了另一个实例）；
  - `resume_barrier_present`：标记与当前数据之间横着一个不可恢复缺口（或发生在显式 resume 之前的旧区域）；
- 进度标记与订阅的来源/标的不一致也会被拒绝。

边界语义：若保留窗口最旧序号是 `oldest`，则检查点 `oldest - 1` 仍然连续（补发正好从 `oldest` 开始）；再小才判定为淘汰不连续。

### 8.6 与缺口恢复、背压的配合

- 快照/续传历史通过 `Dispatcher.begin_handoff(...)` 进入订阅者队列，受同一个有界 `queue_size` 约束，不会因为历史回放让内存越过队列上限；
- 历史回放带“不可丢弃”标记：`block` 策略照常等待容量；`drop_oldest` 在交接期间只会丢实时旧事件、绝不丢历史切片，若队列里全是受保护的历史则退化为等待；`coalesce_latest` 在交接完成前暂停合并（历史不能被合并掉）；`reject` 也不会把历史切片切掉，因为那会交出一个不连续的视图；
- 交接期间入队的实时行情排在历史之后，交接完成前不会与历史发生合并或错序；
- 历史回放与实时流仍然是**每个来源/标的独立**的，不同来源、不同标的不会串位。

相关结构化日志：`handoff_delivered`（模式、条数、检查点、实时起点）、`history_barrier`（栅栏序号），交接期间的容量等待/降级仍记为 `backpressure_decision`（带 `handoff=true`）。

## 9. HTTP 接口

服务入口为 `main:app`。

- `GET /health`
- `GET /state/subscriptions`
- `GET /state/queues`
- `GET /state/history/{source}/{symbol}`：查看某个来源/标的的历史保留与淘汰情况
- `POST /subscriptions`（支持 `snapshot`、`resume_from_sequence`、`wait_for_consistency`、`consistency_timeout`）
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

中途接入、要最近 5 条已确认行情作为初始视图：

```bash
curl -X POST http://127.0.0.1:8000/subscriptions \
  -H 'content-type: application/json' \
  -d '{"subscriber_id":"late","source":"source_a","symbol":"AAA","snapshot":5}'
# 响应的 handoff.quotes 是历史切片，handoff.live_from_sequence 是实时起点；
# 随后 GET /subscribers/late/events 会先收到 kind=handoff 的信封事件。
```

同一标的正在补缺口（或还没用 `resume_after_gap` 重新对齐）时的区别：不带 `snapshot` / `resume_from_sequence` 的纯实时订阅**照常成功**，从当下开始收事件；带快照/续传的请求则返回 `ok=false`、`kind=snapshot_incomplete`（未对齐时为 `resume_barrier_present`），且不残留订阅，可稍后原样重试：

```bash
# 纯实时：缺口期间也 200/ok=true，之后收到 recovered_quote -> quote
curl -X POST http://127.0.0.1:8000/subscriptions \
  -H 'content-type: application/json' \
  -d '{"subscriber_id":"live","source":"source_a","symbol":"AAA"}'
```

崩后重启、从“已处理到序号 12”续传；若位置已被淘汰或横有不可恢复缺口，响应 `ok=false` 且 `kind` 为 `resume_position_evicted` / `resume_position_ahead` / `resume_barrier_present`，此时应改为带 `snapshot` 重新订阅对齐：

```bash
curl -X POST http://127.0.0.1:8000/subscriptions \
  -H 'content-type: application/json' \
  -d '{"subscriber_id":"reconnected","source":"source_a","symbol":"AAA","resume_from_sequence":12}'
```

注入示例：

```bash
curl -X POST http://127.0.0.1:8000/sources/source_a/ingest \
  -H 'content-type: application/json' \
  -d '{"record":{"trade_id":"a1","sym":"AAA","seq":1,"ts_ms":1790035201000,"px":"100.25","qty":1}}'
```

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
- 有界历史存储：容量淘汰、按来源/标的隔离、快照尾部、续传补发、栅栏与新基线；
- **中途订阅先拿快照、再平滑进入实时流（交界处不重不漏）**；
- **带进度标记重连只补发缺失部分；位置被淘汰/超前/跨越不可恢复缺口时被明确拒绝**；
- 补缺口中途订阅被明确告知不完整，或等待补齐后给出一致快照（含超时不留订阅）；
- **补缺口（含不可恢复缺口未对齐）期间，不带快照、不带进度的纯实时订阅照常成功，随后按序收到 `recovered_quote` 与正常实时 `quote`，且不被补投历史**；
- **快照/续传订阅在缺口期间被干净拒绝（注册表、队列均无残留，同订阅者的其他标的订阅不受影响），缺口合上或重新对齐后重试成功，视图与实时流前后连续**；
- **缺口并发补齐 + 等待型快照订阅 + 纯实时订阅并存时，交界处不重不漏、序列连续**；
- 不可恢复缺口使旧历史失效，显式 resume 后新区域才可快照；
- 交接在小容量 + `drop_oldest` 下也不丢历史；订阅与实时写入并发时边界原子、连续。

测试运行时会输出 JSON 结构化日志。关键日志包括：

- `ingest_input`：来源和原始输入；
- `sequence_decision`：序号、时间、状态和判定依据；
- `sequence_gap`：缺口范围、状态和恢复序号；
- `record_rejected`：原始记录和拒绝原因；
- `backpressure_decision`：丢弃、合并或拒绝审计；交接期间的容量等待也记此事件并带 `handoff=true`；
- `source_disconnected` / `source_reconnected` / `explicit_gap_resume`；
- `handoff_delivered`：快照/续传的模式、历史条数、检查点与实时起点；
- `history_barrier`：不可恢复缺口后历史失效的栅栏位置。

启动本地服务：

```bash
uv run uvicorn main:app --reload
```
