"""手动消费队列里未确认的事件：python manage.py consume_events --limit 50"""
from django.core.management.base import BaseCommand

from notify.consumer import consume_pending
from notify.queue import get_queue


class Command(BaseCommand):
    help = "认领并消费队列中未 ACK 的订单事件（可用于常驻 worker）"

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=50)
        parser.add_argument("--consumer", default=None)
        parser.add_argument(
            "--loop", action="store_true", help="持续消费直到队列为空（演示用）"
        )

    def handle(self, *args, **options):
        queue = get_queue()
        self.stdout.write(f"后端：{queue.describe()['label']}")
        total = 0
        while True:
            results = consume_pending(
                consumer=options["consumer"], limit=options["limit"], queue=queue
            )
            if not results:
                break
            for r in results:
                self.stdout.write(f"  {r['event_id'][:16]}  {r['result']:<10} {r['detail']}")
            total += len(results)
            if not options["loop"]:
                break
        self.stdout.write(self.style.SUCCESS(f"本次处理 {total} 条事件"))
