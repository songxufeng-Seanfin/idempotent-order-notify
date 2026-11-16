from django.contrib import admin

from .models import (
    DeliveryAttempt,
    NotificationLog,
    NotifyEvent,
    Order,
    ProcessedEvent,
    StateTransition,
)


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = ("order_no", "buyer", "current_stage", "state_mask", "version", "updated_at")
    list_filter = ("current_stage",)
    search_fields = ("order_no",)


@admin.register(NotifyEvent)
class NotifyEventAdmin(admin.ModelAdmin):
    list_display = ("event_id", "order_no", "event_type", "seq", "delivered", "delivery_count")
    list_filter = ("event_type", "delivered")
    search_fields = ("event_id", "order_no")


@admin.register(ProcessedEvent)
class ProcessedEventAdmin(admin.ModelAdmin):
    list_display = ("event_id", "order_no", "result", "detail", "replayed", "processed_at")
    list_filter = ("result",)


@admin.register(DeliveryAttempt)
class DeliveryAttemptAdmin(admin.ModelAdmin):
    list_display = ("event_id", "order_no", "result", "detail", "created_at")
    list_filter = ("result",)


@admin.register(NotificationLog)
class NotificationLogAdmin(admin.ModelAdmin):
    list_display = ("event_id", "order_no", "channel", "content", "created_at")


@admin.register(StateTransition)
class StateTransitionAdmin(admin.ModelAdmin):
    list_display = ("order_no", "action", "from_mask", "to_mask", "created_at")
