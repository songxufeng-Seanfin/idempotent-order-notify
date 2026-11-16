"""事件队列：两种后端，同一套接口。

接口只有四个动作，对应消息队列最基本的语义：
    publish 投递一条消息
    claim   消费者认领一批消息（认领后未确认 = Pending）
    ack     确认处理完成
    pending_count  还有多少条没确认

默认 DatabaseQueue 用一张表模拟 Redis Stream 的投递/认领/确认，
本机零依赖即可运行；设置 NOTIFY_BACKEND=redis 时切到真实 Redis Stream。
"""
import logging

from django.conf import settings
from django.db.models import F
from django.utils import timezone

from .models import NotifyEvent

logger = logging.getLogger(__name__)


class EventQueue:
    """抽象基类：定义队列契约，业务逻辑只依赖这四个方法。"""

    name = "abstract"

    def publish(self, event_id: str):
        raise NotImplementedError

    def claim(self, consumer: str, count: int = 10):
        raise NotImplementedError

    def ack(self, event_id: str):
        raise NotImplementedError

    def pending_count(self):
        raise NotImplementedError

    def describe(self):
        return {"name": self.name, "label": self.name, "note": ""}


class DatabaseQueue(EventQueue):
    """用 NotifyEvent 表模拟 Stream。

    delivered=False 的消息就是「已投递但未 ACK」；重复 publish 会把它
    重新置为未确认，从而被再次认领 —— 这正是 at-least-once 重投语义。
    """

    name = "db"

    def __init__(self, fallback_reason: str = ""):
        self.fallback_reason = fallback_reason

    def publish(self, event_id: str):
        NotifyEvent.objects.filter(event_id=event_id).update(
            delivered=False,
            claimed_by="",
            claimed_at=None,
            delivery_count=F("delivery_count") + 1,
        )

    def claim(self, consumer: str, count: int = 10):
        ids = list(
            NotifyEvent.objects.filter(delivered=False)
            .order_by("id")
            .values_list("event_id", flat=True)[:count]
        )
        if ids:
            NotifyEvent.objects.filter(event_id__in=ids).update(
                claimed_by=consumer, claimed_at=timezone.now()
            )
        return ids

    def ack(self, event_id: str):
        NotifyEvent.objects.filter(event_id=event_id).update(delivered=True)

    def pending_count(self):
        return NotifyEvent.objects.filter(delivered=False).count()

    def describe(self):
        note = "用数据库表模拟 Stream 语义，零外部依赖"
        if self.fallback_reason:
            note = f"Redis 不可用，已自动回退：{self.fallback_reason[:80]}"
        return {
            "name": "db",
            "label": "数据库队列（默认）",
            "note": note,
        }


class RedisStreamQueue(EventQueue):
    """真实 Redis Stream 后端：XADD / XREADGROUP / XACK / XPENDING。

    claim 先读 Pending（历史上认领过但没 ACK 的消息），再读 ">" 新消息，
    这就是消费组保证「至少投递一次」的来源，也是幂等必须存在的原因。
    """

    name = "redis"

    def __init__(self, client=None, stream=None, group=None):
        if client is None:
            import redis  # 延迟导入：不装 redis 包时，默认后端照样能跑

            client = redis.Redis.from_url(
                settings.NOTIFY_REDIS_URL, decode_responses=True
            )
        self.client = client
        self.stream = stream or settings.NOTIFY_STREAM_KEY
        self.group = group or settings.NOTIFY_GROUP
        self._msg_ids = {}
        self._ensure_group()

    def _ensure_group(self):
        try:
            self.client.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except Exception as exc:  # BUSYGROUP 表示消费组已存在，属于正常情况
            if "BUSYGROUP" not in str(exc):
                raise

    def publish(self, event_id: str):
        msg_id = self.client.xadd(self.stream, {"event_id": event_id})
        self._msg_ids[event_id] = msg_id
        NotifyEvent.objects.filter(event_id=event_id).update(
            stream_msg_id=str(msg_id),
            delivery_count=F("delivery_count") + 1,
        )
        return msg_id

    def claim(self, consumer: str, count: int = 10):
        ids = []

        # 1) Pending：之前认领但未 ACK 的消息（重投）
        try:
            pending = self.client.xreadgroup(
                self.group, consumer, {self.stream: "0"}, count=count
            )
            for _, messages in pending or []:
                for msg_id, fields in messages:
                    eid = fields.get("event_id")
                    if eid:
                        self._msg_ids[eid] = msg_id
                        ids.append(eid)
        except Exception as exc:
            logger.warning("读取 Redis Pending 失败：%s", exc)

        # 2) 新消息：">" 表示只取本消费组从未投递过的消息
        remain = count - len(ids)
        if remain > 0:
            fresh = self.client.xreadgroup(
                self.group, consumer, {self.stream: ">"}, count=remain
            )
            for _, messages in fresh or []:
                for msg_id, fields in messages:
                    eid = fields.get("event_id")
                    if eid:
                        self._msg_ids[eid] = msg_id
                        ids.append(eid)

        # 同一条消息可能同时出现在两批里，去重但保持顺序
        seen = set()
        return [x for x in ids if not (x in seen or seen.add(x))]

    def ack(self, event_id: str):
        msg_id = self._msg_ids.get(event_id)
        if not msg_id:
            msg_id = (
                NotifyEvent.objects.filter(event_id=event_id)
                .values_list("stream_msg_id", flat=True)
                .first()
            )
        if msg_id:
            self.client.xack(self.stream, self.group, msg_id)
        NotifyEvent.objects.filter(event_id=event_id).update(delivered=True)

    def pending_count(self):
        try:
            info = self.client.xpending(self.stream, self.group)
            return int(info.get("pending", 0)) if isinstance(info, dict) else 0
        except Exception:
            return NotifyEvent.objects.filter(delivered=False).count()

    def describe(self):
        return {
            "name": "redis",
            "label": "Redis Stream（真实消费组）",
            "note": f"{self.stream} · 消费组 {self.group} · XADD/XREADGROUP/XACK",
        }


def get_queue(force: str = None) -> EventQueue:
    """按配置返回队列实例，Redis 不可用则自动回退，保证演示不会中断。"""
    backend = (force or settings.NOTIFY_BACKEND or "db").lower()
    if backend == "redis":
        try:
            queue = RedisStreamQueue()
            queue.client.ping()
            return queue
        except Exception as exc:
            logger.warning("Redis 后端不可用，回退数据库队列：%s", exc)
            return DatabaseQueue(fallback_reason=f"{type(exc).__name__}: {exc}")
    return DatabaseQueue()
