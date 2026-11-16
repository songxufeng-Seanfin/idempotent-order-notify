"""一键演示：python manage.py demo_notify --scenario mixed"""
from django.core.management.base import BaseCommand

from notify.consumer import reset_demo, run_scenario
from notify.models import NotificationLog, Order
from notify.queue import get_queue
from notify.state_machine import FULL_MASK


class Command(BaseCommand):
    help = "投放订单事件并消费，直观复现重复投递与乱序到达"

    def add_arguments(self, parser):
        parser.add_argument(
            "--scenario",
            default="mixed",
            choices=["normal", "duplicate", "outoforder", "mixed"],
            help="要跑的场景（默认 mixed 全部）",
        )
        parser.add_argument(
            "--keep", action="store_true", help="保留已有演示数据，不清空"
        )

    def handle(self, *args, **options):
        if not options["keep"]:
            reset_demo()
            self.stdout.write("已清空历史演示数据")
        queue = get_queue()
        self.stdout.write(self.style.SUCCESS(f"当前队列后端：{queue.describe()['label']}"))
        self.stdout.write(f"        {queue.describe()['note']}\n")

        result = run_scenario(options["scenario"])
        self.stdout.write(f"场景：{result['name']}")
        self.stdout.write("-" * 78)
        self.stdout.write(f"{'事件ID':<18}{'结果':<12}{'位图':<16}说明")
        self.stdout.write("-" * 78)
        for r in result["results"]:
            mask = f"{r['mask_before']:>2} → {r['mask_after']:<2}"
            self.stdout.write(
                f"{r['event_id'][:16]:<18}{r['result']:<12}{mask:<16}{r['detail']}"
            )
        self.stdout.write("-" * 78)
        s = result["stats"]
        self.stdout.write(
            self.style.SUCCESS(
                f"汇总：生效 {s['applied']} · 重复拦下 {s['duplicate']} · "
                f"乱序暂存 {s['buffered']} · 暂存重放 {s['replayed']}\n"
            )
        )

        self.stdout.write("订单最终状态：")
        for order in Order.objects.all():
            flag = "✓ 状态完整" if order.state_mask == FULL_MASK else "… 未完成"
            self.stdout.write(
                f"  {order.order_no:<24} {order.current_stage:<8} "
                f"位图 {order.mask_binary} 版本 {order.version}  {flag}"
            )
        self.stdout.write(
            self.style.SUCCESS(
                f"\n对外通知共 {NotificationLog.objects.count()} 条"
                "（同一 event_id 只会出现一条，重复消费不会重发）"
            )
        )
