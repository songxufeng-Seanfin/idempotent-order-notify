"""回归测试：python manage.py test

覆盖幂等、乱序、状态不回退、队列 ACK、Redis Stream 后端、演示页交互。
"""
from django.test import Client, TestCase, override_settings
from django.urls import reverse

from .consumer import (
    consume_one,
    consume_pending,
    publish_event,
    reset_demo,
    run_scenario,
)
from .models import (
    DeliveryAttempt,
    NotificationLog,
    NotifyEvent,
    Order,
    ProcessedEvent,
    StateTransition,
)
from .queue import DatabaseQueue, RedisStreamQueue, get_queue
from .state_machine import FULL_MASK, can_apply, mask_text, required_before


class FakeRedis:
    """内存版 Redis，用于在没有 Redis 服务的机器上测试 Stream 路径。

    只实现本项目用到的六个命令，语义与 redis-py 保持一致：
    XADD / XGROUP_CREATE / XREADGROUP / XACK / XPENDING / PING。
    """

    def __init__(self):
        self.messages = []
        self.groups = {}
        self.pending = {}
        self.last_id = 0

    def ping(self):
        return True

    def xgroup_create(self, stream, group, id="0", mkstream=False):
        if group in self.groups:
            raise Exception("BUSYGROUP Consumer Group name already exists")
        self.groups[group] = {"last": 0}

    def xadd(self, stream, fields):
        self.last_id += 1
        msg_id = f"{self.last_id}-0"
        self.messages.append((msg_id, dict(fields)))
        return msg_id

    def xreadgroup(self, group, consumer, streams, count=None):
        key = list(streams)[0]
        cursor = streams[key]
        if cursor == "0":
            items = list(self.pending.get(group, []))
        else:
            last = self.groups[group]["last"]
            items = list(self.messages[last:])
            self.groups[group]["last"] = len(self.messages)
            self.pending.setdefault(group, []).extend(items)
        if count:
            items = items[:count]
        return [(key, items)] if items else []

    def xack(self, stream, group, msg_id):
        self.pending[group] = [
            x for x in self.pending.get(group, []) if x[0] != msg_id
        ]
        return 1

    def xpending(self, stream, group):
        return {"pending": len(self.pending.get(group, []))}


@override_settings(NOTIFY_BACKEND="db")
class StateMachineTests(TestCase):
    def test_required_before(self):
        self.assertEqual(required_before("created"), 0)
        self.assertEqual(required_before("paid"), 1)
        self.assertEqual(required_before("completed"), 7)

    def test_out_of_order_detected(self):
        ok, reason, out_of_order = can_apply("paid", 0)
        self.assertFalse(ok)
        self.assertTrue(out_of_order)
        self.assertIn("前置阶段未齐", reason)

    def test_completed_then_paid_rejected_not_rollback(self):
        ok, _, _ = can_apply("paid", FULL_MASK)
        self.assertFalse(ok)

    def test_mask_text(self):
        self.assertEqual(mask_text(5), "00101")


@override_settings(NOTIFY_BACKEND="db")
class IdempotencyTests(TestCase):
    def setUp(self):
        reset_demo()
        self.queue = get_queue(force="db")

    def test_normal_flow(self):
        for stage in ("created", "paid", "shipped", "completed"):
            publish_event(order_no="ORD-A", event_type=stage, queue=self.queue)
        results = consume_pending(queue=self.queue)
        self.assertEqual(len(results), 4)
        self.assertTrue(all(r["result"] == "APPLIED" for r in results))
        order = Order.objects.get(order_no="ORD-A")
        self.assertEqual(order.state_mask, FULL_MASK)
        self.assertEqual(order.current_stage, "已完成")
        self.assertEqual(NotificationLog.objects.filter(order_no="ORD-A").count(), 4)
        self.assertEqual(StateTransition.objects.filter(order_no="ORD-A").count(), 4)

    def test_duplicate_event_blocked_and_notification_once(self):
        info = publish_event(
            order_no="ORD-B", event_type="created", event_id="evt-fixed-1", queue=self.queue
        )
        first = consume_one(info["event_id"])
        self.assertEqual(first.result, ProcessedEvent.APPLIED)
        self.assertTrue(first.notification_sent)

        for _ in range(3):
            publish_event(
                order_no="ORD-B",
                event_type="created",
                event_id="evt-fixed-1",
                queue=self.queue,
            )
            again = consume_one("evt-fixed-1")
            self.assertEqual(again.result, ProcessedEvent.DUPLICATE)

        self.assertEqual(NotificationLog.objects.filter(event_id="evt-fixed-1").count(), 1)
        self.assertEqual(ProcessedEvent.objects.filter(event_id="evt-fixed-1").count(), 1)
        self.assertEqual(
            NotifyEvent.objects.get(event_id="evt-fixed-1").delivery_count, 4
        )
        # 每次尝试都留痕：1 次生效 + 3 次被拦
        self.assertEqual(DeliveryAttempt.objects.filter(event_id="evt-fixed-1").count(), 4)
        self.assertEqual(
            DeliveryAttempt.objects.filter(
                event_id="evt-fixed-1", result=ProcessedEvent.DUPLICATE
            ).count(),
            3,
        )
        self.assertEqual(
            ProcessedEvent.objects.get(event_id="evt-fixed-1").duplicate_hits, 3
        )

    def test_out_of_order_converges_to_completed(self):
        # 故意倒序，且每条立刻消费，确保「后发的先到」真实发生
        for stage in ("completed", "shipped", "paid", "created"):
            info = publish_event(order_no="ORD-C", event_type=stage, queue=self.queue)
            r = consume_one(info["event_id"])
            self.queue.ack(info["event_id"])
            if stage != "created":
                self.assertEqual(r.result, ProcessedEvent.BUFFERED)

        order = Order.objects.get(order_no="ORD-C")
        self.assertEqual(order.state_mask, FULL_MASK)
        self.assertEqual(order.current_stage, "已完成")
        self.assertEqual(
            ProcessedEvent.objects.filter(result=ProcessedEvent.BUFFERED).count(), 0,
            "暂存事件应在前置补齐后全部重放",
        )
        replayed = ProcessedEvent.objects.filter(replayed=True).count()
        self.assertEqual(replayed, 3)
        self.assertEqual(NotificationLog.objects.filter(order_no="ORD-C").count(), 4)

    def test_state_never_rolls_back(self):
        run_scenario("normal")
        order = Order.objects.first()
        mask = order.state_mask
        info = publish_event(
            order_no=order.order_no, event_type="paid", queue=self.queue
        )
        r = consume_one(info["event_id"])
        self.assertEqual(r.result, ProcessedEvent.REJECTED)
        order.refresh_from_db()
        self.assertEqual(order.state_mask, mask)

    def test_cancelled_after_completed_rejected(self):
        run_scenario("normal")
        order = Order.objects.first()
        info = publish_event(
            order_no=order.order_no, event_type="cancelled", queue=self.queue
        )
        r = consume_one(info["event_id"])
        self.assertEqual(r.result, ProcessedEvent.REJECTED)
        self.assertIn("不允许取消", r.detail)

    def test_unique_constraint_is_last_line_of_defense(self):
        from django.db import IntegrityError, transaction

        info = publish_event(order_no="ORD-D", event_type="created", queue=self.queue)
        event = NotifyEvent.objects.get(event_id=info["event_id"])
        ProcessedEvent.objects.create(
            source_event=event,
            event_id=event.event_id,
            order_no=event.order_no,
            result=ProcessedEvent.APPLIED,
        )
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                ProcessedEvent.objects.create(
                    source_event=event,
                    event_id=event.event_id,
                    order_no=event.order_no,
                    result=ProcessedEvent.PROCESSING,
                )

    def test_scenarios_reproducible(self):
        for name in ("normal", "duplicate", "outoforder"):
            reset_demo()
            result = run_scenario(name)
            self.assertTrue(result["results"])
        reset_demo()
        mixed = run_scenario("mixed")
        self.assertEqual(Order.objects.count(), 3)


@override_settings(NOTIFY_BACKEND="db")
class QueueTests(TestCase):
    def setUp(self):
        reset_demo()

    def test_db_queue_claim_and_ack(self):
        q = DatabaseQueue()
        publish_event(order_no="ORD-Q", event_type="created", queue=q)
        self.assertEqual(q.pending_count(), 1)
        ids = q.claim("worker-1")
        self.assertEqual(len(ids), 1)
        q.ack(ids[0])
        self.assertEqual(q.pending_count(), 0)
        self.assertEqual(q.claim("worker-1"), [])

    def test_redelivery_makes_message_claimable_again(self):
        q = DatabaseQueue()
        info = publish_event(order_no="ORD-Q2", event_type="paid", queue=q)
        first = q.claim("worker-1")
        q.ack(first[0])
        publish_event(
            order_no="ORD-Q2", event_type="paid", event_id=info["event_id"], queue=q
        )
        self.assertEqual(len(q.claim("worker-1")), 1, "重复投递后应能再次认领")

    def test_redis_backend_falls_back_without_server(self):
        q = get_queue(force="redis")
        self.assertIsInstance(q, DatabaseQueue)
        self.assertIn("回退", q.describe()["note"])

    def test_redis_stream_backend_with_injected_client(self):
        fake = FakeRedis()
        q = RedisStreamQueue(client=fake, stream="test:events", group="g1")
        publish_event(
            order_no="ORD-REDIS", event_type="created", event_id="ev-redis-1", queue=q
        )
        ids = q.claim("worker-1", count=10)
        self.assertEqual(ids, ["ev-redis-1"])
        r = consume_one(ids[0])
        self.assertEqual(r.result, ProcessedEvent.APPLIED)
        q.ack(ids[0])
        self.assertEqual(fake.xpending("test:events", "g1")["pending"], 0)
        self.assertEqual(NotifyEvent.objects.get(event_id="ev-redis-1").delivered, True)

    def test_redis_pending_redelivery_is_idempotent(self):
        fake = FakeRedis()
        q = RedisStreamQueue(client=fake, stream="test:events", group="g1")
        publish_event(
            order_no="ORD-REDIS2", event_type="created", event_id="ev-redis-2", queue=q
        )
        first = q.claim("worker-1", count=10)
        self.assertEqual(consume_one(first[0]).result, ProcessedEvent.APPLIED)
        # 不 ACK，模拟消费者崩溃后消息重投
        again = q.claim("worker-2", count=10)
        self.assertEqual(again, ["ev-redis-2"])
        self.assertEqual(consume_one(again[0]).result, ProcessedEvent.DUPLICATE)
        q.ack(again[0])
        self.assertEqual(NotificationLog.objects.filter(event_id="ev-redis-2").count(), 1)


@override_settings(NOTIFY_BACKEND="db")
class ConsoleTests(TestCase):
    def setUp(self):
        reset_demo()
        self.client = Client()

    def test_page_renders(self):
        r = self.client.get(reverse("console"))
        self.assertEqual(r.status_code, 200)
        self.assertContains(r, "幂等")
        self.assertContains(r, "投放事件")

    def test_post_actions(self):
        r = self.client.post(reverse("run_scenario", args=["duplicate"]))
        self.assertEqual(r.status_code, 302)
        page = self.client.get(reverse("console"))
        self.assertEqual(page.status_code, 200)
        self.assertGreater(DeliveryAttempt.objects.filter(result="DUPLICATE").count(), 0)
        r = self.client.post(reverse("run_scenario", args=["reset"]))
        self.assertEqual(r.status_code, 302)
        self.assertEqual(Order.objects.count(), 0)
        self.assertEqual(DeliveryAttempt.objects.count(), 0)

    def test_get_not_allowed(self):
        r = self.client.get(reverse("run_scenario", args=["reset"]))
        self.assertEqual(r.status_code, 405)
