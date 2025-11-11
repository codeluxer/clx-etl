import datetime
from typing import Any

from clickhouse_sqlalchemy.types.common import DateTime, Float64, LowCardinality, String, UInt64
from sqlalchemy import text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Kline1h(Base):
    __tablename__ = "kline_1h"

    symbol: Mapped[Any] = mapped_column(LowCardinality(String()), primary_key=True, comment="交易对符号，例如 BTCUSDT")
    timestamp: Mapped[datetime.datetime] = mapped_column(DateTime, primary_key=True, comment="K线起始时间（UTC）")
    datetime: Mapped[str] = mapped_column(String, nullable=False, comment="人类可读时间（例如 2025-11-11 10:00:00）")
    open: Mapped[float] = mapped_column(Float64, nullable=False, comment="开盘价")
    high: Mapped[float] = mapped_column(Float64, nullable=False, comment="最高价")
    low: Mapped[float] = mapped_column(Float64, nullable=False, comment="最低价")
    close: Mapped[float] = mapped_column(Float64, nullable=False, comment="收盘价")
    volume: Mapped[float] = mapped_column(Float64, nullable=False, comment="成交量（Base资产数量）")
    quote_volume: Mapped[float] = mapped_column(Float64, nullable=False, comment="成交额（Quote资产数量）")
    count: Mapped[int] = mapped_column(UInt64, nullable=False, server_default=text("0"), comment="成交笔数（可选）")
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("now()"), comment="数据更新时间"
    )


class Kline1m(Base):
    __tablename__ = "kline_1m"

    symbol: Mapped[Any] = mapped_column(LowCardinality(String()), primary_key=True, comment="交易对符号，例如 BTCUSDT")
    timestamp: Mapped[datetime.datetime] = mapped_column(DateTime, primary_key=True, comment="K线起始时间（UTC）")
    datetime: Mapped[str] = mapped_column(String, nullable=False, comment="人类可读时间（例如 2025-11-11 10:00:00）")
    open: Mapped[float] = mapped_column(Float64, nullable=False, comment="开盘价")
    high: Mapped[float] = mapped_column(Float64, nullable=False, comment="最高价")
    low: Mapped[float] = mapped_column(Float64, nullable=False, comment="最低价")
    close: Mapped[float] = mapped_column(Float64, nullable=False, comment="收盘价")
    volume: Mapped[float] = mapped_column(Float64, nullable=False, comment="成交量（Base资产数量）")
    quote_volume: Mapped[float] = mapped_column(Float64, nullable=False, comment="成交额（Quote资产数量）")
    count: Mapped[int] = mapped_column(UInt64, nullable=False, server_default=text("0"), comment="成交笔数（可选）")
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime, nullable=False, server_default=text("now()"), comment="数据更新时间"
    )
