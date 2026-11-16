"""演示控制台：把抽象的「幂等 / 乱序」变成点一下就能看见的结果。"""
from django.contrib import messages
from django.shortcuts import redirect, render
from django.views.decorators.http import require_POST

from .consumer import reset_demo, run_scenario
from .models import (
    DeliveryAttempt,
    NotificationLog,
    NotifyEvent,
    Order,
    ProcessedEvent,
    StateTransition,
)
from .queue import get_queue
from .state_machine import CANCELLED_BIT, STAGES, progress_percent

RESULT_CSS = {
    ProcessedEvent.APPLIED: "ok",
    ProcessedEvent.BUFFERED: "warn",
    ProcessedEvent.DUPLICATE: "dup",
    ProcessedEvent.REJECTED: "bad",
    ProcessedEvent.PROCESSING: "muted",
}


def console(request):
    queue = get_queue()
    orders = list(Order.objects.all()[:12])
    for order in orders:
        order.stage_flags = [
            {"label": s["label"], "on": bool(order.state_mask & s["bit"])}
            for s in STAGES
        ]
        order.cancelled = bool(order.state_mask & CANCELLED_BIT)
        order.progress = progress_percent(order.state_mask)
        order.mask_bin = order.mask_binary

    records = list(
        DeliveryAttempt.objects.select_related("source_event").all()[:40]
    )
    for record in records:
        record.css = RESULT_CSS.get(record.result, "muted")
        record.mask_before_bin = format(record.mask_before, "05b")
        record.mask_after_bin = format(record.mask_after, "05b")

    transitions = list(StateTransition.objects.all()[:20])
    for t in transitions:
        t.from_bin = format(t.from_mask, "05b")
        t.to_bin = format(t.to_mask, "05b")

    context = {
        "queue": queue.describe(),
        "stats": {
            "orders": Order.objects.count(),
            "events": NotifyEvent.objects.count(),
            "applied": ProcessedEvent.objects.filter(result=ProcessedEvent.APPLIED).count(),
            "duplicate": DeliveryAttempt.objects.filter(result=ProcessedEvent.DUPLICATE).count(),
            "attempts": DeliveryAttempt.objects.count(),
            "buffered": ProcessedEvent.objects.filter(result=ProcessedEvent.BUFFERED).count(),
            "notifications": NotificationLog.objects.count(),
            "redeliveries": NotifyEvent.objects.filter(delivery_count__gt=1).count(),
            "pending": queue.pending_count(),
        },
        "orders": orders,
        "records": records,
        "transitions": transitions,
        "notifications": NotificationLog.objects.all()[:12],
        "stages": STAGES,
    }
    return render(request, "console.html", context)


@require_POST
def run_scenario_view(request, name):
    if name == "reset":
        reset_demo()
        messages.success(request, "演示数据已清空，可以重新投放事件了")
        return redirect("console")

    if name not in {"normal", "duplicate", "outoforder", "mixed"}:
        messages.error(request, "未知场景")
        return redirect("console")

    result = run_scenario(name)
    s = result["stats"]
    label = {
        "normal": "正常流程",
        "duplicate": "重复投递",
        "outoforder": "乱序投递",
        "mixed": "全部场景",
    }[name]
    messages.success(
        request,
        f"「{label}」跑完：生效 {s['applied']} 条 · 重复拦下 {s['duplicate']} 条 · "
        f"乱序暂存 {s['buffered']} 条 · 暂存重放 {s['replayed']} 条",
    )
    return redirect("console")
