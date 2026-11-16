"""五张表，对应整条流水线的五个「记忆点」：

Order            订单当前状态（位图 + 当前阶段）
NotifyEvent     事件表 / 队列里的消息（event_id 全局唯一，是幂等的锚点）
ProcessedEvent  消费记录（一个 event_id 只允许一条，这就是幂等闸门）
NotificationLog 对外通知（副作用；event_id 唯一，保证只发一次）
StateTransition 状态变更流水（看板时间线的数据来源）
"""
from django.db import models

from .state_machine import EVENT_TYPES, current_label, mask_text, reached_labels


class Order(models.Model):
    order_no = models.CharField("订单号", max_length=40, unique=True)
    buyer = models.CharField("买家", max_length=40, default="演示用户")
    amount = models.DecimalField("金额", max_digits=10, decimal_places=2, default=0)
    state_mask = models.PositiveIntegerField("状态位图", default=0)
    current_stage = models.CharField("当前阶段", max_length=20, default="待创建")
    version = models.PositiveIntegerField("状态版本", default=0)
    created_at = models.DateTimeField("创建时间", auto_now_add=True)
    updated_at = models.DateTimeField("更新时间", auto_now=True)

    class Meta:
        verbose_name = "订单"
        verbose_name_plural = "订单"
        ordering = ["-id"]

    def __str__(self):
        return f"{self.order_no} · {self.current_stage}"

    @property
    def mask_binary(self):
        return mask_text(self.state_mask)

    @property
    def reached(self):
        return reached_labels(self.state_mask)

    def refresh_stage(self):
        self.current_stage = current_label(self.state_mask)


class NotifyEvent(models.Model):
    """事件表。重复投递同一个 event_id 不会新建行，只累加投递次数。"""

    event_id = models.CharField("事件ID", max_length=64, unique=True)
    order_no = models.CharField("订单号", max_length=40, db_index=True)
    event_type = models.CharField("事件类型", max_length=20, choices=EVENT_TYPES)
    seq = models.PositiveIntegerField("业务序号", default=0)
    payload = models.JSONField("消息体", default=dict, blank=True)

    # 数据库队列（模拟 Redis Stream）的投递元信息
    delivered = models.BooleanField("已确认ACK", default=False)
    claimed_by = models.CharField("认领消费者", max_length=64, blank=True)
    claimed_at = models.DateTimeField("认领时间", null=True, blank=True)
    delivery_count = models.PositiveIntegerField("累计投递次数", default=0)
    stream_msg_id = models.CharField("Redis Stream 消息ID", max_length=64, blank=True)
    created_at = models.DateTimeField("首次投递时间", auto_now_add=True)

    class Meta:
        verbose_name = "订单事件"
        verbose_name_plural = "订单事件"
        ordering = ["id"]
        indexes = [
            models.Index(fields=["delivered", "id"]),
            models.Index(fields=["order_no", "seq"]),
        ]

    def __str__(self):
        return f"{self.event_id} · {self.get_event_type_display()}"


class ProcessedEvent(models.Model):
    """消费结果记录。event_id 唯一 = 同一事件最多被真正处理一次。"""

    APPLIED = "APPLIED"
    BUFFERED = "BUFFERED"
    DUPLICATE = "DUPLICATE"
    REJECTED = "REJECTED"
    PROCESSING = "PROCESSING"
    RESULT_CHOICES = [
        (APPLIED, "已生效"),
        (BUFFERED, "乱序暂存"),
        (DUPLICATE, "重复拦下"),
        (REJECTED, "非法拒绝"),
        (PROCESSING, "处理中"),
    ]

    source_event = models.OneToOneField(
        NotifyEvent, on_delete=models.CASCADE, related_name="process_record",
        verbose_name="来源事件", db_column="source_event_pk",
    )
    # 字符串形式的业务 ID（幂等键），与外键分开命名避免 Django 字段冲突
    event_id = models.CharField("事件ID", max_length=64, unique=True)
    order_no = models.CharField("订单号", max_length=40, db_index=True)
    result = models.CharField("处理结果", max_length=20, choices=RESULT_CHOICES)
    detail = models.CharField("说明", max_length=200, blank=True)
    mask_before = models.PositiveIntegerField("处理前位图", default=0)
    mask_after = models.PositiveIntegerField("处理后位图", default=0)
    notification_sent = models.BooleanField("是否发出通知", default=False)
    replayed = models.BooleanField("是否由暂存重放补上", default=False)
    duplicate_hits = models.PositiveIntegerField("重复被拦次数", default=0)
    processed_at = models.DateTimeField("处理时间", auto_now_add=True)

    class Meta:
        verbose_name = "消费记录"
        verbose_name_plural = "消费记录"
        ordering = ["-id"]

    def __str__(self):
        return f"{self.event_id} · {self.get_result_display()}"


class DeliveryAttempt(models.Model):
    """每一次消费尝试都留一条，包括被幂等闸门拦下的重复投递。

    ProcessedEvent 一个 event_id 只有一条（负责幂等），
    DeliveryAttempt 则记录「来过几次、每次结果如何」，是看板时间线的数据源。
    """

    source_event = models.ForeignKey(
        NotifyEvent, on_delete=models.CASCADE, related_name="attempts",
        verbose_name="来源事件", null=True, blank=True,
    )
    event_id = models.CharField("事件ID", max_length=64, db_index=True)
    order_no = models.CharField("订单号", max_length=40, db_index=True)
    result = models.CharField("处理结果", max_length=20, choices=ProcessedEvent.RESULT_CHOICES)
    detail = models.CharField("说明", max_length=200, blank=True)
    mask_before = models.PositiveIntegerField("处理前位图", default=0)
    mask_after = models.PositiveIntegerField("处理后位图", default=0)
    notification_sent = models.BooleanField("是否发出通知", default=False)
    replayed_count = models.PositiveIntegerField("本次重放条数", default=0)
    created_at = models.DateTimeField("尝试时间", auto_now_add=True)

    class Meta:
        verbose_name = "投递尝试"
        verbose_name_plural = "投递尝试"
        ordering = ["-id"]

    def __str__(self):
        return f"{self.event_id} · {self.get_result_display()}"


class NotificationLog(models.Model):
    """外部副作用（短信/站内信）。event_id 唯一，重复消费不会多发一条。"""

    event_id = models.CharField("事件ID", max_length=64, unique=True)
    order_no = models.CharField("订单号", max_length=40, db_index=True)
    channel = models.CharField("通道", max_length=20, default="站内信")
    content = models.CharField("通知内容", max_length=200)
    created_at = models.DateTimeField("发送时间", auto_now_add=True)

    class Meta:
        verbose_name = "通知日志"
        verbose_name_plural = "通知日志"
        ordering = ["-id"]

    def __str__(self):
        return f"{self.event_id} · {self.content}"


class StateTransition(models.Model):
    """每次状态真正前进都留一条流水，看板时间线据此展示。"""

    order_no = models.CharField("订单号", max_length=40, db_index=True)
    event_id = models.CharField("事件ID", max_length=64)
    from_mask = models.PositiveIntegerField("变更前位图", default=0)
    to_mask = models.PositiveIntegerField("变更后位图", default=0)
    action = models.CharField("动作", max_length=40)
    note = models.CharField("备注", max_length=120, blank=True)
    created_at = models.DateTimeField("发生时间", auto_now_add=True)

    class Meta:
        verbose_name = "状态变更流水"
        verbose_name_plural = "状态变更流水"
        ordering = ["-id"]

    def __str__(self):
        return f"{self.order_no} · {self.action}"
