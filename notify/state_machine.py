"""订单状态机：用位图（bitmask）记录订单走过哪些阶段。

位图就是一个整数的二进制位：第 0 位代表「已创建」，第 1 位代表「已支付」……
例如 0011（十进制 3）= 已创建 + 已支付，缺「已发货」。
这样「缺哪一步」「有没有回退」都能用一个数字直接算出来。
"""

# 阶段定义：顺序即业务发生顺序
STAGES = [
    {"key": "created", "bit": 1 << 0, "label": "已创建", "seq": 1},
    {"key": "paid", "bit": 1 << 1, "label": "已支付", "seq": 2},
    {"key": "shipped", "bit": 1 << 2, "label": "已发货", "seq": 3},
    {"key": "completed", "bit": 1 << 3, "label": "已完成", "seq": 4},
]

CANCELLED_BIT = 1 << 4
FULL_MASK = sum(s["bit"] for s in STAGES)  # 15 == 0b01111

STAGE_BY_KEY = {s["key"]: s for s in STAGES}
EVENT_TYPES = [(s["key"], s["label"]) for s in STAGES] + [("cancelled", "已取消")]


def bit_of(event_type: str) -> int:
    """事件类型对应的位；cancelled 单独用取消位。"""
    if event_type == "cancelled":
        return CANCELLED_BIT
    stage = STAGE_BY_KEY.get(event_type)
    return stage["bit"] if stage else 0


def required_before(event_type: str) -> int:
    """该事件发生前，必须已经到达的所有阶段的位或值。

    paid 之前必须有 created（1），shipped 之前必须有 1|2=3，
    这就是判断「乱序」的依据。
    """
    stage = STAGE_BY_KEY.get(event_type)
    if not stage:
        return 0
    need = 0
    for s in STAGES:
        if s["seq"] < stage["seq"]:
            need |= s["bit"]
    return need


def mask_text(mask: int, width: int = 5) -> str:
    """位图可视化：5 -> 00101（从左到右为高到低）。"""
    return format(mask, f"0{width}b")


def reached_labels(mask: int) -> list:
    return [s["label"] for s in STAGES if mask & s["bit"]]


def current_label(mask: int) -> str:
    if mask & CANCELLED_BIT:
        return "已取消"
    reached = reached_labels(mask)
    return reached[-1] if reached else "待创建"


def can_apply(event_type: str, mask: int):
    """判断这条事件此刻能不能推进状态。

    返回 (是否可推进, 原因, 是否属于乱序)。
    乱序单独标记出来，消费者就会把它暂存而不是丢弃。
    """
    if event_type not in STAGE_BY_KEY and event_type != "cancelled":
        return False, "未知事件类型", False

    if mask & CANCELLED_BIT:
        return False, "订单已取消，不接受后续事件", False

    if event_type == "cancelled":
        if mask & STAGE_BY_KEY["completed"]["bit"]:
            return False, "订单已完成，不允许取消", False
        return True, "取消订单", False

    bit = bit_of(event_type)
    if mask & bit:
        return False, "该阶段已到达（重复事件）", False

    need = required_before(event_type)
    if need and (mask & need) != need:
        missing = [s["label"] for s in STAGES if s["bit"] & need and not mask & s["bit"]]
        return False, "前置阶段未齐：" + "、".join(missing), True

    return True, "按序推进", False


def next_mask(event_type: str, mask: int) -> int:
    """应用事件后的新位图。状态只前进、绝不回退。"""
    return mask | bit_of(event_type)


def progress_percent(mask: int) -> int:
    reached = sum(1 for s in STAGES if mask & s["bit"])
    return int(reached / len(STAGES) * 100)
