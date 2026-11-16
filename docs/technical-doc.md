# 订单通知的幂等消费与乱序处理 · 技术文档

> 版本：v1.0 · 2026-11-16
> 项目：`idempotent-order-notify`
> 仓库：https://github.com/songxufeng-Seanfin/idempotent-order-notify
> 运行环境：Python 3.13.14 · Django 5.2.19 · SQLite 3 · 可选 Redis Stream
> 代码规模：26 个文件、约 1,840 行（含模板与测试）、6 张业务表、19 个自动化测试

这份文档按「由外向内」的顺序写：先讲页面上看得见的东西，再讲请求怎么走、Django 怎么调度，最后拆到数据、状态机、并发与队列。每个专业术语第一次出现都配一句白话解释，不需要预先学过消息队列也能读下来。

---

## 1. 项目概览

### 1.1 要解决什么问题

电商/交易系统里，订单状态变化通常以「通知」（也叫**事件**，Event：一件事发生后发出的一条消息）的形式在系统之间传递，例如「订单已支付」「包裹已发货」。真实环境有两条几乎必然发生的干扰：

- **重复投递（at-least-once delivery，至少投递一次）**：消息队列为了保证消息不丢，网络抖动、消费者超时、未确认都会让同一条消息再投一次。如果消费端不做防护，用户会收到两条短信、积分会被加两次。
- **乱序到达（out-of-order）**：多个生产者、多个分区、重试队列都会让「发货」比「下单」先到。如果收到什么就写什么，订单状态会跳步甚至回退。

本项目用一个可交互的演示系统，把这两类异常**故意制造出来**，再验证系统在三个层面都不出错：订单状态正确、对外通知不重复、状态绝不回退。

### 1.2 做了什么

| 能力 | 说明 |
|---|---|
| 事件流水线 | 事件投递 → 队列认领 → 幂等闸门 → 状态机判断 → 状态推进 + 通知 → ACK 确认 |
| 幂等消费 | 全局唯一 `event_id` + 抢占式写入消费记录 + 副作用表唯一约束，三道防线 |
| 乱序处理 | 前置阶段位图校验；缺前置的事件**暂存**而不是丢弃；前置补齐后按业务序号**重放** |
| 状态机 | 5 个阶段用位图（bitmask）表示，只允许前进，禁止回退与非法跳转 |
| 双后端队列 | 默认用数据库表模拟 Stream；设置环境变量即切到真实 Redis Stream（XADD / XREADGROUP / XACK / XPENDING） |
| 可视化看板 | 点按钮投放三种场景，实时展示订单位图、消费时间线、通知日志、状态流水 |
| 回归验证 | 19 个自动化测试覆盖幂等、乱序、拒绝、ACK、Redis 重投、页面交互 |

### 1.3 技术栈（从外到内）

| 层 | 技术 | 白话解释 |
|---|---|---|
| 页面 | HTML + CSS（内联样式，零 CDN 依赖） | 页面骨架与长相；所有样式写在 `templates/base.html`，离线也能完整渲染 |
| Web 框架 | Django 5.2 | 别人盖好的楼：路由、表单防护、后台管理、数据库读写都自带，我们只填业务房间 |
| 语言 | Python 3.13 | 写业务逻辑的语言 |
| 存储 | SQLite | 一个 `db.sqlite3` 文件装下全部 6 张表 |
| 队列（默认） | 数据库模拟 Stream | 用 `delivered / claimed_by / claimed_at` 三个字段还原投递-认领-确认语义 |
| 队列（可选） | Redis Stream + redis-py | 真实消费组；`XADD` 投递、`XREADGROUP` 认领、`XACK` 确认、`XPENDING` 查未确认 |
| 测试 | Django `TestCase` | 模拟一次请求/一次消费，断言结果对不对 |

---

## 2. 第一层：从屏幕到服务器

打开 `http://127.0.0.1:8000/`，看到的是一个**演示控制台（Console）**。它只有两类交互：

1. **GET `/`** — 渲染看板。视图 `notify/views.py :: console()` 查 6 个统计数字、最近 12 笔订单、最近 40 次消费尝试、最近 12 条通知、最近 20 条状态流水，交给 `templates/console.html` 排版。
2. **POST `/run/<场景>/`** — 点击按钮触发。`notify/views.py :: run_scenario_view()` 调 `consumer.run_scenario()` 真的跑一遍流水线，然后用 **PRG（Post/Redirect/Get：提交后重定向，避免刷新重复提交）** 跳回看板，顶部显示一行汇总。

这里可以复习一条对所有 Web 系统都成立的规律：**页面不会自己变**。每点一次按钮，浏览器发一次请求，服务器跑一圈，再把新结果送回来画成页面。

`405 Method Not Allowed`（路找到了但请求方式不对）在这里也有实例：`/run/reset/` 只允许 POST，用 GET 直接访问会被 `@require_POST` 装饰器拒掉，测试 `ConsoleTests.test_get_not_allowed` 专门验证了这一条。

---

## 3. 第二层：Django 收到请求之后

框架（Framework）和工具包的区别值得再记一次：**工具是你叫它干活，框架是它来叫你**。Django 定好流程，命中网址后回来调我们写的函数。

一次请求依次经过：

| 站 | 谁负责 | 干什么 |
|---|---|---|
| 1 | 中间件（Middleware） | 固定安检：Session、CSRF（表单防伪凭证）、消息框架 |
| 2 | 路由（URLconf） | `config/urls.py` → `notify/urls.py`，把网址交给对应视图 |
| 3 | 视图（View） | `console()` 或 `run_scenario_view()`，真正干活的一小段 Python |
| 4 | 模型 / ORM | `models.py` 定义表；ORM（把 Python 翻成数据库指令的翻译官）负责读写 |
| 5 | 模板（Template） | 把数据排成 HTML |
| 6 | 响应 | 浏览器画出页面；POST 场景则返回 302 重定向 |

本项目没有登录态，Session/CSRF 仍然保留：CSRF 保证那五个按钮只能从本站页面点出来，这和上一个项目里手机登录 403 是同一套机制。

---

## 4. 第三层：目录与文件职责

```
idempotent-order-notify/
├── manage.py                         Django 命令行入口
├── requirements.txt                  Django 5.2 + 可选 redis-py
├── config/
│   ├── settings.py                   配置：数据库、队列后端、环境变量开关
│   ├── urls.py                       根路由（admin + notify）
│   └── wsgi.py                       部署入口
├── notify/                          业务应用
│   ├── models.py                     6 张表（数据长什么样）
│   ├── state_machine.py             位图与状态转移规则（纯函数，不碰数据库）
│   ├── queue.py                     两种队列后端 + 工厂（数据从哪来）
│   ├── consumer.py                  投递、消费、暂存重放、演示场景（数据怎么变）
│   ├── views.py                     控制台视图（数据怎么摆）
│   ├── urls.py                       "/" 与 "/run/<场景>/"
│   ├── admin.py                     后台管理注册
│   ├── tests.py                     19 个回归测试（含内存版 FakeRedis）
│   ├── migrations/0001_initial.py    建表脚本
│   └── management/commands/
│       ├── demo_notify.py            一键跑演示场景（命令行版）
│       └── consume_events.py        手动消费未 ACK 消息（worker 版）
├── templates/
│   ├── base.html                    全站外壳：设计系统、暗色适配、响应式
│   └── console.html                 演示看板
└── docs/                            本文档、博客、阅读版
```

一个刻意的设计：**状态机与队列是解耦的**。`state_machine.py` 是纯函数，不 import 任何模型；`queue.py` 只负责消息进出，不知道订单有几种状态。`consumer.py` 把两者粘起来。这样换队列（DB → Redis → Kafka）时，状态规则一行都不用改。

---

## 5. 第四层：数据模型（6 张表）

| 表 | 作用 | 关键约束 |
|---|---|---|
| `Order` | 订单当前状态 | `order_no` 唯一；`state_mask` 位图；`version` 状态版本 |
| `NotifyEvent` | 事件表 / 队列消息 | **`event_id` 唯一**（幂等的锚点）；`delivered`、`claimed_by`、`claimed_at` 模拟投递状态；`delivery_count` 记累计投递次数 |
| `ProcessedEvent` | 消费结果（幂等闸门） | `event_id` 唯一 + `source_event` 一对一；`duplicate_hits` 记重复被拦次数 |
| `DeliveryAttempt` | 每一次消费尝试 | 无唯一约束，重复被拦也留一条；看板时间线的数据源 |
| `NotificationLog` | 对外通知（副作用） | **`event_id` 唯一**，保证同一事件最多发一条 |
| `StateTransition` | 状态变更流水 | 每次位图前进记一条 `from_mask → to_mask` |

### 5.1 为什么需要两张「结果表」

这是本项目最容易混淆、也最值得记住的一点：

- `ProcessedEvent` **一个 event_id 只有一条**，它的存在本身就是「这条处理过了」的凭证，负责**正确性**。
- `DeliveryAttempt` **来几次记几条**，负责**可观测性**：重复投递被拦下、乱序暂存、重放了几条，都要看得见。

第一版只做了 `ProcessedEvent`，结果「重复拦下」在看板上永远是 0——被拦下的请求没有留痕。补上 `DeliveryAttempt` 之后，看板才能显示「1 次生效 + 3 次被拦」。这个坑记在本文第 13 章。

### 5.2 为什么唯一索引比 `if` 判断可靠

代码里「先查有没有、再插入」在并发下会失效：两个线程可能同时查到「没有」，然后都插入。数据库的 **唯一索引（Unique Constraint）** 由存储层保证，任何并发交错下都只会有一行成功。所以本项目把 `event_id` 唯一作为最后一道防线，代码里的判断只是为了让错误提示更友好。

---

## 6. 第五层：状态机与位图

### 6.1 位图怎么表示进度

定义在 `state_machine.py :: STAGES`：

| 阶段 | 事件类型 | 二进制位 | 十进制值 | 业务序号 |
|---|---|---|---|---|
| 已创建 | `created` | 第 0 位 | 1 | 1 |
| 已支付 | `paid` | 第 1 位 | 2 | 2 |
| 已发货 | `shipped` | 第 2 位 | 4 | 3 |
| 已完成 | `completed` | 第 3 位 | 8 | 4 |
| 已取消 | `cancelled` | 第 4 位 | 16 | — |

`Order.state_mask = 15` 即 `01111`：四个正常阶段全到了。若 `01011`，说明「已完成 + 已支付 + 已创建」到了，缺「已发货」——**缺哪一步用一个整数直接算出来**，不用存四个布尔字段、也不用查历史记录。

### 6.2 前置校验

`required_before(event_type)` 返回「这件事发生之前必须已经点亮的所有位」：

- `created` → 0（不需要前置）
- `paid` → 1
- `shipped` → 3（1|2）
- `completed` → 7（1|2|4）

`can_apply(event_type, mask)` 返回三元组 `(是否可推进, 原因, 是否属于乱序)`。关键分支：

- 该位已点亮 → `该阶段已到达（重复事件）`，**拒绝**
- 前置位不齐 → `前置阶段未齐：已创建、已支付…` 并标记 `out_of_order=True`，**暂存**
- 订单已完成还想取消 → `订单已完成，不允许取消`，**拒绝**
- 合法 → `按序推进`

`next_mask()` 用 `mask | bit` 做按位或。**或运算只能把 0 变 1，不能把 1 变 0**，这就是「状态只前进、绝不回退」在数学上的保证，比在代码里写一堆 `if` 更可靠。

---

## 7. 第六层：一次消费的完整链路

以「同一条 `paid` 消息被投了两次」为例，走 `notify/consumer.py`：

| 步 | 函数 | 动作 |
|---|---|---|
| 1 | `publish_event()` | `NotifyEvent.objects.get_or_create(event_id=...)`；重复投递不新建行，只累加 `delivery_count` |
| 2 | `queue.publish()` | 把 `event_id` 放进队列（DB 后端把 `delivered` 置回 False；Redis 后端 `XADD`） |
| 3 | `consume_pending()` | `claim()` 认领一批 → 逐条 `consume_one()` → 逐条 `ack()` |
| 4 | `_consume_one_inner()` | `transaction.atomic()` 内锁订单行；抢写 `ProcessedEvent` |
| 5 | 幂等闸门 | `IntegrityError`（唯一约束冲突）→ 返回 `DUPLICATE`，累加 `duplicate_hits`，**不产生任何副作用** |
| 6 | `can_apply()` | 合法才 `next_mask()`；缺前置则写 `BUFFERED` 返回 |
| 7 | `_apply_stage()` | 更新位图与版本、`NotificationLog.get_or_create(event_id=...)`、`StateTransition.create(...)` |
| 8 | `_drain_buffer()` | 按业务序号扫描暂存的事件，能推进的全部重放，循环直到一轮没有新增 |
| 9 | `consume_one()` 包装层 | 无论结果如何，写一条 `DeliveryAttempt` 审计记录 |
| 10 | `queue.ack()` | 标记 `delivered=True` / `XACK`，队列不再重投 |

**副作用只发一次**的根本原因不是「我们判断了重复」，而是 `NotificationLog.event_id` 有唯一约束：就算闸门被绕过，`get_or_create` 也不会插进第二条。

---

## 8. 双后端队列

`queue.py` 定义统一契约，业务逻辑只依赖四个方法：

```python
class EventQueue:
    def publish(self, event_id): ...       # 投递
    def claim(self, consumer, count): ...  # 认领（认领后未确认 = Pending）
    def ack(self, event_id): ...           # 确认
    def pending_count(self): ...           # 还有多少条没确认
```

| 语义 | `DatabaseQueue`（默认） | `RedisStreamQueue`（可选） |
|---|---|---|
| 投递 | `delivered=False`，`delivery_count + 1` | `XADD stream * event_id <id>` |
| 认领 | 取 `delivered=False` 的前 N 条，写 `claimed_by/claimed_at` | `XREADGROUP GROUP g c STREAMS key 0`（先读 Pending）再读 `>` |
| 确认 | `delivered=True` | `XACK stream group <msg-id>` |
| 未确认数 | `filter(delivered=False).count()` | `XPENDING` 返回 `pending` |
| 重投 | 重复 `publish` 把消息重新置为未确认 | 消费组未 ACK 的消息留在 Pending，换消费者重新读到 |

`get_queue()` 读 `NOTIFY_BACKEND` 环境变量，值为 `redis` 时尝试连接并 `PING`；**任何异常都自动回退到数据库队列**并在看板上标注原因，保证没有 Redis 的机器上演示不会中断。

`tests.py :: FakeRedis` 是一个内存版 Redis，只实现这六个命令，让 Stream 路径在没装 Redis 的环境里也能被真实测试覆盖（包括「消费者崩溃不 ACK → 消息重投 → 幂等拦下」这条经典场景）。

切换方式：

```bash
# Windows (PowerShell)
$env:NOTIFY_BACKEND="redis"; $env:NOTIFY_REDIS_URL="redis://127.0.0.1:6379/0"
python manage.py demo_notify --scenario mixed
```

---

## 9. 乱序处理：为什么是「暂存 + 重放」

面对乱序，常见三种做法：

| 做法 | 问题 |
|---|---|
| 收到就写 | 状态跳步，`completed` 先到会让订单「已完成但没创建过」 |
| 直接丢弃迟到的事件 | 消息永久丢失，最终状态不完整 |
| **暂存 + 补齐后重放**（本项目） | 既不丢也不硬写，最终状态与按序到达完全一致 |

`_drain_buffer()` 用 `while` 循环反复扫描 `BUFFERED` 记录，直到某一轮没有任何新增。这是必要的：`completed → shipped → paid → created` 全部乱序到达后，`created` 补齐会连锁解锁 `paid`、`shipped`、`completed` 三条，一次扫描不够。每条重放都会写状态流水并标记 `replayed=True`，看板上能直接看到「顺带重放 3 条暂存事件」。

`demo_notify --scenario outoforder` 的输出就是这套逻辑的实证：前三条都是 `BUFFERED`，最后一条 `created` 到达时位图从 `0 → 15`，通知恰好补齐到 4 条。

---

## 10. 并发与事务

- `consume_one()` 整体包在 `transaction.atomic()` 里，并对 `Order` 行用 `select_for_update()` 加行锁，避免两个消费者同时读写同一订单的位图。
- 抢占 `ProcessedEvent` 时再套一层 `transaction.atomic()` 作为**保存点（savepoint）**：捕获 `IntegrityError` 只回滚这一小段，外层事务不会变成 broken 状态。
- SQLite 下 `select_for_update` 是空操作，但唯一索引依然生效，所以正确性不依赖数据库是否支持行锁；换到 MySQL/PostgreSQL 时代码无需改动。
- `version` 字段每次状态前进 +1，是留给后续做**乐观锁（Optimistic Locking：更新时校验版本，版本变了就重试）**的接口。

---

## 11. 验证

### 11.1 自动化测试（19 个，全部通过）

| 分组 | 覆盖点 |
|---|---|
| `StateMachineTests` | 前置位计算、乱序识别、完成态拒绝回退、位图字符串 |
| `IdempotencyTests` | 正常流程 4 阶段、同一事件投 4 次只发 1 条通知、倒序到达最终收敛到 `01111`、状态不回退、完成后不可取消、唯一约束兜底、场景可复现 |
| `QueueTests` | 认领/ACK、重复投递后可再认领、Redis 不可用时回退、注入 FakeRedis 跑通 XADD/XREADGROUP/XACK、Pending 重投被幂等拦下 |
| `ConsoleTests` | 页面渲染、按钮 POST 生效、清空重来、GET 访问写接口返回 405 |

```bash
python manage.py test
# Ran 19 tests in 0.35s — OK
```

### 11.2 一键演示

```bash
python manage.py demo_notify --scenario mixed
```

输出摘要（真实运行结果）：生效 9 · 重复拦下 5 · 乱序暂存 3 · 暂存重放 3；三笔订单位图均为 `01111`、版本 4；对外通知共 12 条（3 订单 × 4 阶段），没有任何一条因重复投递而多发。

### 11.3 视觉验证

用 Django 测试客户端渲染真实页面后，以无头 Edge 在 1280px 与 500px 两种宽度截图核对：术语卡、按钮、六格统计、位图表、时间线、通知日志均正常；`document.scrollWidth (489) < window.innerWidth (504)`，手机端无横向溢出；深浅色主题都可读。

---

## 12. 运行方式与环境变量

```bash
pip install -r requirements.txt
python manage.py migrate
python manage.py demo_notify --scenario mixed
python manage.py runserver
# 浏览器打开 http://127.0.0.1:8000/
```

| 变量 | 默认值 | 说明 |
|---|---|---|
| `NOTIFY_BACKEND` | `db` | `db` = 数据库模拟 Stream；`redis` = 真实 Redis Stream |
| `NOTIFY_REDIS_URL` | `redis://127.0.0.1:6379/0` | Redis 连接串 |
| `NOTIFY_STREAM_KEY` | `order:events` | Stream 键名 |
| `NOTIFY_GROUP` | `order-consumer-group` | 消费组名 |
| `NOTIFY_CONSUMER` | `worker-1` | 消费者名 |
| `NOTIFY_SECRET_KEY` | 演示用固定值 | 生产环境必须覆盖 |
| `NOTIFY_DEBUG` | `1` | 设为 `0` 关闭 DEBUG |

`python manage.py consume_events --loop` 可以当作常驻 worker 消费未确认消息；`--scenario` / `--keep` 控制演示场景与是否清空历史数据。

---

## 13. 踩坑复盘（本轮真实发生）

| # | 现象 | 根因 | 怎么查 | 怎么防 |
|---|---|---|---|---|
| 1 | `manage.py check` 报 `models.E006` 字段冲突 | `OneToOneField(event)` 的属性名默认就是 `event_id`，与显式的字符串字段 `event_id` 撞名 | 系统检查直接指出 `ProcessedEvent.event_id clashes with field 'event'` | 外键改名 `source_event` 并指定 `db_column`，业务 ID 保持 `event_id` |
| 2 | 看板「重复拦下」永远显示 0 | 幂等闸门拦下重复消息时直接 `return`，没有任何写库动作 | 测试断言 `ProcessedEvent.filter(DUPLICATE).count() > 0` 失败 | 新增 `DeliveryAttempt` 表记录每次尝试；统计改查这张表；`ProcessedEvent.duplicate_hits` 累加次数 |
| 3 | 暗色主题下按钮文字几乎看不见 | `.btn` 背景硬编码 `#fff`，暗色下文字变成浅色，白底浅字 | 无头 Edge 截图肉眼比对 | 背景改用 `var(--surface)`，主按钮在暗色单独指定深蓝，虚线按钮文字用 `var(--ink)` |
| 4 | 本机没有 Redis / Docker，Redis 路径没法验证 | 环境限制，不是代码问题 | `redis-server`、`docker`、`importlib.util.find_spec` 逐个探测 | 抽象队列接口 + 自动回退；`FakeRedis` 注入测试覆盖 Stream 命令 |
| 5 | 手机端疑似横向溢出 | grid 子项默认 `min-width:auto`，表格 `white-space:nowrap` 会撑开列 | 注入脚本读 `scrollWidth` 与 `innerWidth` 对比（489 < 504） | `.card { min-width: 0 }`、`.table-wrap { max-width: 100% }` |

---

## 14. 术语速查表

| 术语 | 白话解释 |
|---|---|
| 事件 / Event | 一件事发生后发出的一条消息 |
| 幂等 / Idempotency | 同一个操作做 100 次和做 1 次效果一样 |
| at-least-once | 队列保证「至少送到一次」，所以一定会重复，必须靠消费端幂等 |
| 乱序 / Out-of-order | 先发生的事反而后到 |
| 位图 / Bitmask | 用一个整数的二进制位记多个开关状态 |
| 状态机 / State Machine | 规定状态只能按哪几条路往前走 |
| 消费组 / Consumer Group | 多个消费者分摊同一批消息，各自记录读到哪 |
| Pending / ACK | 已认领还没确认 / 确认处理完成 |
| XADD / XREADGROUP / XACK | Redis Stream 的投递 / 认领 / 确认命令 |
| ORM | 把 Python 翻成数据库指令的翻译官 |
| 迁移 / Migration | 改了模型之后，把表结构同步到数据库的脚本 |
| 事务 / Transaction | 一组操作要么全成功、要么全撤销 |
| 保存点 / Savepoint | 事务里的小回滚点，局部失败不拖垮整笔事务 |
| `select_for_update` | 给数据库行加锁，防止并发同时改 |
| 乐观锁 / Optimistic Locking | 更新时校验版本号，版本变了就重试 |
| PRG | 提交后重定向，刷新页面不会重复提交 |
| CSRF | 表单防伪凭证，确认请求真从本站页面发出 |
| 405 / 404 / 403 / 500 | 方式不对 / 找不着路 / 不让进 / 里面出错 |

---

## 15. 后续演进

1. 接真实 Redis 集群，`XINFO STREAM` 监控积压，`XAUTOCLAIM` 处理长期未 ACK 的消息。
2. 幂等键从随机 UUID 换成业务语义键（如 `order_no:event_type:version`），支持上游按业务重试。
3. `state_mask` 换成数据库原生位运算 + `version` 乐观锁，做多线程压测验证吞吐。
4. 引入死信队列（Dead Letter Queue）：重放超过 N 次仍失败的事件单独隔离告警。
5. 演示页接 WebSocket，消费结果实时推送，不用刷新页面。

---

## 附录 A：最短的入门切口

想读懂整个项目，只看一次「重复投递」即可，它把整条链路压缩成几行：

```
点击「重复投递」
  → views.run_scenario_view()            # 视图
  → consumer.run_scenario("duplicate")    # 场景
  → publish_event(event_id=同一个)       # 事件表 + 队列投递 ×3
  → consume_one(event_id)               # 幂等闸门
      第一次 → APPLIED   → _apply_stage() → 位图 0→1 + 1 条通知
      第二三次 → DUPLICATE → 只写 DeliveryAttempt，不改状态、不发通知
  → queue.ack()
  → 看板显示「生效 1 · 重复拦下 2 · 通知 1 条」
```

其余所有功能，都是这条链路上换了事件类型、换了队列后端、或加了暂存重放而已。

## 附录 B：场景对照

| 场景 | 投递顺序 | 期望结果 |
|---|---|---|
| `normal` | created → paid → shipped → completed | 4 条 APPLIED，位图 `01111`，4 条通知 |
| `duplicate` | 每个 event_id 投 2 次（首个投 3 次） | 5 条 DUPLICATE，通知仍按阶段各 1 条 |
| `outoforder` | completed → shipped → paid → created | 前 3 条 BUFFERED，最后 1 条 APPLIED 并重放 3 条，位图 `01111` |
| `mixed` | 以上三个场景连跑 | 生效 9 · 拦下 5 · 暂存 3 · 重放 3 · 通知 12 |
