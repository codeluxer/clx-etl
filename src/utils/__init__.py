from datetime import UTC, datetime
from decimal import Decimal


def precision(x):
    if x is None:
        return None
    s = str(x).rstrip("0")
    return len(s.split(".")[-1]) if "." in s else 0


def to_decimal_str(precision: int) -> str:
    """
    将数值转换为固定小数位字符串，避免科学计数法。
    """
    d = Decimal(1) / (Decimal(10) ** precision)
    return f"{d:.{precision}f}"


def align_to_5m(ms: int | str) -> datetime:
    """
    将毫秒时间戳对齐到整 5 分钟
    如 13:07 → 13:05；13:04 → 13:00
    返回 UTC datetime
    """
    dt = datetime.fromtimestamp(int(ms) / 1000, tz=UTC)
    minute = dt.minute - (dt.minute % 5)
    aligned = dt.replace(minute=minute, second=0, microsecond=0)
    return aligned
