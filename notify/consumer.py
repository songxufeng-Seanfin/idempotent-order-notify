"""消费者：幂等闸门 + 乱序暂存 + 状态机推进。

一次消费的完整顺序（也是技术文档里的那条主线）：
    1. 从队列 claim 到 event_id
    2. 抢先写 ProcessedEvent（event_id 唯一）—— 抢不到就是重复消息
    3. 用状态机判断此刻能不能推进；前置阶段没齐 ⇒ 暂存，不丢也不硬写
    4. 推进状态位图 + 写一条对外通知 + 记一条状态流水
    5. 前置补齐后，把暂存的事件按业务序号重放
    6. ack 消息
"""
import uuid
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation

from django.conf import settings
from django.db import IntegrityError, transaction
from django.db.models import F

from .models import (
    DeliveryAttempt,
    NotificationLog,
    NotifyEvent,
    Order,
    ProcessedEvent,
    StateTransition,
)
from .queue import get_queue
from .state_machine import (
    STAGE_BY_KEY,
    can_apply,
    current_label,
    next_mask,
)


@dataclass
class ConsumeResult:
    event_id: str
    result: str
    detail: str
    mask_before: int = 0
    mask_after: int = 0
    notification_sent: bool = False
    replayed: int = 0

    def as_dict(self):
        return asdict(self)


# ---------------------------------------------------------------------------
# 投递
# ---------------------------------------------------------------------------
def publish_event(
    *,
    order_no,
    event_type,
    event_id=None,
    seq=None,
    payload=None,
    buyer="演示用户",
    amount="99.00",
    queue=None,
):
    """投递一条订单事件；传入相同 event_id 即模拟队列重复投递。"""
    event_id = event_id or uuid.uuid4().hex[:16]
    body = dict(payload or {})
    body.setdefault("buyer", buyer)
    body.setdefault("amount", str(amount))
    body.setdefault("event_type", event_type)
    if seq is None:
        seq = STAGE_BY_KEY.get(event_type, {}).get("seq", 5)

    event, created = NotifyEvent.objects.get_or_create(
        event_id=event_id,
        defaults={
            "order_no": order_no,
            "event_type": event_type,
            "seq": seq,
            "payload": body,
            "delivery_count": 0,
        },
    )
    q = queue or get_queue()
    q.publish(event_id)
    return {"event_id": event_id, "redelivery": not created, "queue": q.name}


# ---------------------------------------------------------------------------
# 内部：真正推进状态 + 产生副作用
# ---------------------------------------------------------------------------
def _to_decimal(value):
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _apply_stage(order, event, note=""):
    """把事件应用到订单：位图只前进不回退，通知按 event_id 去重。"""
    mask_before = order.state_mask
    mask_after = next_mask(event.event_type, mask_before)
    if mask_after == mask_before:
        return False

    label = current_label(mask_after)
    order.state_mask = mask_after
    order.current_stage = label
    order.version += 1
    order.save(update_fields=["state_mask", "current_stage", "version", "updated_at"])

    # 副作用：同一 event_id 最多一条通知（数据库唯一约束兜底）
    _, sent = NotificationLog.objects.get_or_create(
        event_id=event.event_id,
        defaults={
            "order_no": order.order_no,
            "content": f"订单 {order.order_no} 状态更新：{label}",
        },
    )
    StateTransition.objects.create(
        order_no=order.order_no,
        event_id=event.event_id,
        from_mask=mask_before,
        to_mask=mask_after,
        action=f"{event.get_event_type_display()} → {label}",
        note=note,
    )
    return sent


def _drain_buffer(order):
    """前置阶段补齐后，按业务序号重放被暂存的事件。

    循环扫描直到一轮没有任何新增，才能处理「先收到 completed 再收到
    created」这种一次补齐后连锁解锁多条的情况。
    """
    replayed = 0
    while True:
        progressed = False
        buffered = list(
            ProcessedEvent.objects.select_related("source_event")
            .filter(result=ProcessedEvent.BUFFERED, order_no=order.order_no)
            .order_by("source_event__seq", "id")
        )
        for record in buffered:
            event = record.source_event
            ok, reason, _ = can_apply(event.event_type, order.state_mask)
            if not ok:
                continue
            sent = _apply_stage(order, event, note="暂存重放补齐")
            order.refresh_from_db()
            record.result = ProcessedEvent.APPLIED
            record.detail = f"前置补齐后重放：{reason}"
            record.mask_after = order.state_mask
            record.notification_sent = sent
            record.replayed = True
            record.save()
            replayed += 1
            progressed = True
        if not progressed:
            break
    return replayed


# ---------------------------------------------------------------------------
# 消费
# ---------------------------------------------------------------------------
def consume_one(event_id):
    """消费一条事件，并把这次尝试写进 DeliveryAttempt（重复被拦也留痕）。"""
    result = _consume_one_inner(event_id)
    source = NotifyEvent.objects.filter(event_id=event_id).first()
    DeliveryAttempt.objects.create(
        source_event=source,
        event_id=result.event_id,
        order_no=source.order_no if source else "",
        result=result.result,
        detail=result.detail,
        mask_before=result.mask_before,
        mask_after=result.mask_after,
        notification_sent=result.notification_sent,
        replayed_count=result.replayed,
    )
    return result


def _consume_one_inner(event_id):
    """真正的消费逻辑；重复事件会在这里被幂等闸门拦下。"""
    with transaction.atomic():
        event = (
            NotifyEvent.objects.select_for_update()
            .filter(event_id=event_id)
            .first()
        )
        if not event:
            return ConsumeResult(event_id, "REJECTED", "事件不存在")

        # ② 幂等闸门：唯一约束抢占，抢不到说明这条已经处理过
        try:
            with transaction.atomic():
                record = ProcessedEvent.objects.create(
                    source_event=event,
                    event_id=event_id,
                    order_no=event.order_no,
                    result=ProcessedEvent.PROCESSING,
                    mask_before=0,
                )
        except IntegrityError:
            ProcessedEvent.objects.filter(event_id=event_id).update(
                duplicate_hits=F("duplicate_hits") + 1
            )
            return ConsumeResult(
                event_id,
                ProcessedEvent.DUPLICATE,
                "event_id 已有消费记录，重复投递被拦下，通知不会重发",
            )

        order, _ = Order.objects.select_for_update().get_or_create(
            order_no=event.order_no,
            defaults={
                "buyer": event.payload.get("buyer", "演示用户"),
                "amount": _to_decimal(event.payload.get("amount", "0")),
            },
        )
        mask_before = order.state_mask

        # ③ 状态机判断
        ok, reason, out_of_order = can_apply(event.event_type, mask_before)
        if not ok and out_of_order:
            record.result = ProcessedEvent.BUFFERED
            record.detail = f"乱序：{reason}，暂存等待前置阶段"
            record.mask_before = mask_before
            record.mask_after = mask_before
            record.save()
            return ConsumeResult(
                event_id,
                ProcessedEvent.BUFFERED,
                record.detail,
                mask_before,
                mask_before,
            )

        if not ok:
            record.result = ProcessedEvent.REJECTED
            record.detail = reason
            record.mask_before = mask_before
            record.mask_after = mask_before
            record.save()
            return ConsumeResult(
                event_id, ProcessedEvent.REJECTED, reason, mask_before, mask_before
            )

        # ④ 推进 + ⑤ 重放暂存
        sent = _apply_stage(order, event)
        replayed = _drain_buffer(order)
        order.refresh_from_db()
        record.result = ProcessedEvent.APPLIED
        record.detail = reason
        record.mask_before = mask_before
        record.mask_after = order.state_mask
        record.notification_sent = sent
        record.save()

        return ConsumeResult(
            event_id,
            ProcessedEvent.APPLIED,
            (reason + (f"；顺带重放 {replayed} 条暂存事件" if replayed else "")),
            mask_before,
            order.state_mask,
            sent,
            replayed,
        )


def consume_pending(consumer=None, limit=50, queue=None):
    """认领一批消息、逐条消费、逐条 ACK。"""
    q = queue or get_queue()
    consumer = consumer or settings.NOTIFY_CONSUMER
    ids = q.claim(consumer, count=limit)
    results = []
    for event_id in ids:
        result = consume_one(event_id)
        q.ack(event_id)
        results.append(result.as_dict())
    return results


# ---------------------------------------------------------------------------
# 演示场景
# ---------------------------------------------------------------------------
def _next_order_no(prefix):
    count = Order.objects.filter(order_no__startswith=f"ORD-{prefix}-").count()
    return f"ORD-{prefix}-{count + 1:03d}"


def reset_demo():
    """清空演示数据（保留表结构），每个场景都从干净状态开始。"""
    DeliveryAttempt.objects.all().delete()
    StateTransition.objects.all().delete()
    NotificationLog.objects.all().delete()
    ProcessedEvent.objects.all().delete()
    NotifyEvent.objects.all().delete()
    Order.objects.all().delete()


def run_scenario(name):
    """跑一个可复现的场景，返回摘要。页面按钮和命令行演示共用这里。"""
    q = get_queue()
    results = []

    def feed(order_no, event_type, event_id=None, seq=None, immediate=False):
        info = publish_event(
            order_no=order_no,
            event_type=event_type,
            event_id=event_id,
            seq=seq,
            queue=q,
        )
        if immediate:
            results.append(consume_one(info["event_id"]).as_dict())
            q.ack(info["event_id"])
        return info

    if name == "normal":
        order_no = _next_order_no("NORMAL")
        for stage in ("created", "paid", "shipped", "completed"):
            feed(order_no, stage)
        results = consume_pending(queue=q)

    elif name == "duplicate":
        order_no = _next_order_no("DUPLICATE")
        first = feed(order_no, "created", immediate=True)
        for _ in range(2):
            feed(order_no, "created", event_id=first["event_id"], immediate=True)
        # 另外三条也各自重复投一次，让看板上重复拦下更直观
        for stage in ("paid", "shipped", "completed"):
            eid = publish_event(order_no=order_no, event_type=stage, queue=q)["event_id"]
            results.append(consume_one(eid).as_dict())
            q.ack(eid)
            publish_event(
                order_no=order_no, event_type=stage, event_id=eid, queue=q
            )
            results.append(consume_one(eid).as_dict())
            q.ack(eid)

    elif name == "outoforder":
        order_no = _next_order_no("OUTOFORDER")
        # 故意倒序投递，且每条立刻消费 —— 保证真的「后发的先到」
        for stage in ("completed", "shipped", "paid", "created"):
            feed(order_no, stage, immediate=True)

    elif name == "mixed":
        for sub in ("normal", "duplicate", "outoforder"):
            results.extend(run_scenario(sub)["results"])
    else:
        raise ValueError(f"未知场景：{name}")

    return {
        "name": name,
        "queue": q.name,
        "results": results,
        "stats": {
            "applied": sum(1 for r in results if r["result"] == "APPLIED"),
            "duplicate": sum(1 for r in results if r["result"] == "DUPLICATE"),
            "buffered": sum(1 for r in results if r["result"] == "BUFFERED"),
            "replayed": sum(r["replayed"] for r in results),
        },
    }
