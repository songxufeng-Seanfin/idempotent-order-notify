# 订单通知的幂等消费与乱序处理

> 事件表 + `event_id` 唯一索引 + 位图状态回滚，解决重复通知与乱序问题
> 状态：✅ 功能闭环已完成（19 个自动化测试全部通过）

一个**能亲手点出 bug 场景**的后端演示项目：把同一条订单通知故意投两次、把四个阶段的到达顺序打乱，看系统如何保证订单状态不乱、对外通知只发一条，并把每一次拦截、暂存、重放都留在看板上。

- 技术栈：Python 3.13 · Django 5.2 · SQLite · 可选 Redis Stream（redis-py）
- 仓库：https://github.com/songxufeng-Seanfin/idempotent-order-notify
- 关联简历：见 `项目工作区/01-两周行动计划.md` 项目 2

---

## 快速开始

```bash
pip install -r requirements.txt
python manage.py migrate
python manage.py demo_notify --scenario mixed   # 命令行跑一遍全部场景
python manage.py runserver                      # http://127.0.0.1:8000/
```

打开页面后点「一键跑全部场景」即可看到完整结果。**默认后端不需要安装 Redis**，零外部依赖开箱即跑。

```bash
python manage.py test                          # 19 个回归测试
python manage.py consume_events --loop          # 常驻 worker 消费未 ACK 消息
```

---

## 功能清单

| 模块 | 实现 |
|---|---|
| 事件流水线 | 投递 → 队列认领 → 幂等闸门 → 状态机判断 → 位图推进 + 发通知 → ACK |
| 幂等消费 | `event_id` 全局唯一、抢占式写 `ProcessedEvent`（唯一索引兜底）、`NotificationLog.event_id` 唯一保证副作用只发一次 |
| 乱序处理 | 前置位不齐的事件暂存 `BUFFERED`，前置补齐后按业务序号循环重放，最终状态与按序到达一致 |
| 状态机 | 5 个阶段用位图表示；`mask \| bit` 只能把 0 变 1，数学上保证状态不回退；非法转移直接拒绝 |
| 双后端队列 | `DatabaseQueue` 用表模拟 Stream（默认）；`RedisStreamQueue` 实现 XADD / XREADGROUP / XACK / XPENDING；不可用时自动回退 |
| 可视化看板 | 正常 / 重复 / 乱序 / 全部 / 清空五个按钮；六格统计、订单位图与进度条、消费时间线、通知日志、状态流水 |
| 可观测性 | `DeliveryAttempt` 记录每次投递尝试（含被拦下的重复消息）；`ProcessedEvent.duplicate_hits` 累计被拦次数 |
| 回归验证 | 19 个 `TestCase`：幂等、乱序收敛、状态不回退、唯一约束、ACK、Redis 重投（内存 FakeRedis）、页面交互 |

### 演示场景与期望结果

| 场景 | 投递方式 | 期望 |
|---|---|---|
| `normal` | created → paid → shipped → completed | 4 条生效，位图 `01111`，4 条通知 |
| `duplicate` | 每个 `event_id` 重复投递 | 5 条被拦下，通知仍按阶段各 1 条 |
| `outoforder` | completed → shipped → paid → created | 前 3 条暂存，最后 1 条生效并重放 3 条，位图 `01111` |
| `mixed` | 以上连跑 | 生效 9 · 拦下 5 · 暂存 3 · 重放 3 · 通知 12 |

---

## 切换到真实 Redis Stream

```bash
# Windows PowerShell
$env:NOTIFY_BACKEND="redis"
$env:NOTIFY_REDIS_URL="redis://127.0.0.1:6379/0"
python manage.py demo_notify --scenario mixed
```

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `NOTIFY_BACKEND` | `db` | `db` / `redis`；Redis 连不上自动回退 |
| `NOTIFY_REDIS_URL` | `redis://127.0.0.1:6379/0` | 连接串 |
| `NOTIFY_STREAM_KEY` | `order:events` | Stream 键 |
| `NOTIFY_GROUP` | `order-consumer-group` | 消费组 |
| `NOTIFY_CONSUMER` | `worker-1` | 消费者名 |
| `NOTIFY_SECRET_KEY` | 演示固定值 | 生产必须覆盖 |
| `NOTIFY_DEBUG` | `1` | `0` 关闭 DEBUG |

`tests.py` 内置 `FakeRedis`（内存实现 XADD / XGROUP_CREATE / XREADGROUP / XACK / XPENDING / PING），所以 Stream 路径在没有 Redis 服务的机器上也被完整测试覆盖。

---

## 目录结构

```
config/                  项目配置（settings / urls / wsgi）
notify/
  models.py              6 张表：Order / NotifyEvent / ProcessedEvent / DeliveryAttempt / NotificationLog / StateTransition
  state_machine.py       位图与状态转移规则（纯函数，不依赖数据库）
  queue.py               队列抽象 + 数据库后端 + Redis Stream 后端 + 工厂
  consumer.py            投递、消费、暂存重放、演示场景
  views.py / urls.py     演示控制台
  tests.py               19 个回归测试（含 FakeRedis）
  management/commands/   demo_notify / consume_events
templates/                base.html（设计系统）+ console.html（看板）
docs/                     技术文档、博客、离线阅读版与生成脚本
```

---

## 设计决策

1. **幂等交给数据库，不交给 `if`**：并发下「先查再写」会失效；用 `event_id` 唯一索引抢占式插入，冲突即重复。
2. **乱序用暂存 + 重放，不丢不硬写**：丢弃会永久缺步，硬写会跳步；重放循环扫描直到一轮无新增，才能连锁解锁多条。
3. **状态用位图 + 按位或**：一个整数表达多个阶段，缺哪步可直接算；或运算保证状态只前进不回退。
4. **正确性与可观测性分表**：`ProcessedEvent` 一个事件一条（负责幂等），`DeliveryAttempt` 来几次记几条（负责看得见）。
5. **队列与状态机解耦**：换队列不动状态规则；本机零依赖可跑，生产切 Redis 只需一个环境变量。
6. **副作用也加唯一约束**：`NotificationLog.event_id` 唯一，即使闸门被绕过也不会多发通知。

---

## 项目文档

| 文档 | 说明 |
|---|---|
| [技术文档](./docs/technical-doc.md) | 由外向内分层：技术栈 → 请求链路 → Django 六站 → 6 张表 → 状态机 → 双后端队列 → 并发事务 → 踩坑复盘 → 术语速查 |
| [博客文章](./docs/blog-post.md) | 第一人称故事线，讲清「为什么不能用 if」「为什么不能丢乱序消息」「没有 Redis 怎么办」 |
| [离线阅读版](./docs/reader.html) | 双击即可打开，双 Tab 切换 + 左侧目录 + 深浅色自适应，零依赖 |
| [生成脚本](./docs/build_reader.py) | 纯标准库 Markdown → HTML，改完 md 跑一次 `python docs/build_reader.py` 即同步 |

---

## 后续演进

- 接真实 Redis 集群，用 `XINFO STREAM` 监控积压、`XAUTOCLAIM` 处理长期未 ACK 消息
- 幂等键换成业务语义键（`order_no:event_type:version`），支持上游按业务重试
- 引入 `version` 乐观锁并做多线程压测
- 失败多次的事件进死信队列告警，看板接 WebSocket 实时推送
