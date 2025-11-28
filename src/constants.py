from enum import IntEnum


class SymbolStatus(IntEnum):
    ACTIVE = 0  # 交易中
    HALTED = 1  # 暂停
    PENDING = 2  # 待上线
    CLOSED = 3  # 已下线


class InstType(IntEnum):
    SPOT = 0
    PERP = 1


INTERVAL_TO_SECONDS = {
    "1m": 60,
    "1h": 3600,
    "1d": 86400,
}
