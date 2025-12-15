import asyncio
import traceback
from typing import Literal

from prefect import flow, get_run_logger, task
from sqlalchemy import select
from sqlalchemy.orm import Session

from databases.doris import get_doris
from databases.mysql import sync_engine
from databases.mysql.models import ClxSymbol, ExchangeInfo, ExchangeSymbol
from exchanges._base_ import BaseClient
from exchanges.aster import AsterPerpClient
from exchanges.binance import BinancePerpClient, BinanceSpotClient
from exchanges.bitget import BitgetPerpClient, BitgetSpotClient
from exchanges.bitmart import BitmartPerpClient, BitmartSpotClient
from exchanges.bybit import BybitPerpClient, BybitSpotClient
from exchanges.gate import GateSpotClient
from exchanges.kraken import KrakenSpotClient
from exchanges.mexc import MexcPerpClient, MexcSpotClient
from exchanges.okx import OkxPerpClient, OkxSpotClient
from exchanges.woox import WooxPerpClient, WooxSpotClient


def get_active_symbols():
    with Session(sync_engine) as conn:
        stmt = (
            select(ExchangeSymbol)
            .join(ClxSymbol, ExchangeSymbol.id == ClxSymbol.symbol_id)
            .where(ClxSymbol.is_active == 1)
        )

        symbols = conn.execute(stmt).scalars().all()

    return symbols


def get_exchanges_map():
    with Session(sync_engine) as conn:
        stmt = select(ExchangeInfo)
        exchanges = conn.execute(stmt).scalars().all()
    return {e.id: e.name for e in exchanges}


async def get_last_kline_timestamp(interval: Literal["1m", "1h", "1d"], symbol: ExchangeSymbol):
    doris = get_doris()
    data = await doris.query(
        f"""
        SELECT
            dt
        FROM kline_{interval}
        WHERE exchange_id = {symbol.exchange_id}
            AND inst_type = '{symbol.inst_type}'
            AND symbol = '{symbol.symbol}'
        ORDER BY dt DESC LIMIT 1;
        """
    )
    if not data:
        return None
    return data[0][0]


HANDLE_CLIENT = [
    AsterPerpClient,
    BinancePerpClient,
    BitgetPerpClient,
    BitmartPerpClient,
    BybitPerpClient,
    MexcPerpClient,
    OkxPerpClient,
    WooxPerpClient,
    BinanceSpotClient,
    BitgetSpotClient,
    BitmartSpotClient,
    BybitSpotClient,
    GateSpotClient,
    KrakenSpotClient,
    MexcSpotClient,
    OkxSpotClient,
    WooxSpotClient,
]

CLIENT_MAP = {(client.exchange_name, client.inst_type): client for client in HANDLE_CLIENT}


@task(name="update-kline-task", retries=2, retry_delay_seconds=3)
async def update_kline(client: BaseClient, symbols: [ExchangeSymbol], interval: Literal["1m", "1h", "1d"]):
    logger = get_run_logger()
    for i in symbols:
        try:
            logger.info(f"Start update kline {interval} for {client.exchange_name} {i}")
            last_ts = await get_last_kline_timestamp(interval, i)
            last_ts = last_ts or 1735689600000
            await client.update_kline(i.symbol, interval, last_ts)
        except Exception as e:
            logger.error(f"Failed to update kline for {client.exchange_name} {i}: {e}")
            traceback.print_exc()
            await asyncio.sleep(1)


async def sync_klines(interval):
    logger = get_run_logger()
    symbols = get_active_symbols()
    exchange_map = get_exchanges_map()
    symbols_map = {}
    for s in symbols:
        symbols_map.setdefault((exchange_map[s.exchange_id], s.inst_type), []).append(s)

    tasks = []
    for key, symbols in symbols_map.items():
        logger.info(f"Start sync klines {interval} for {key}: {[i.symbol for i in symbols]}")

        client = CLIENT_MAP[key](logger)
        tasks.append(update_kline(client, symbols, interval))

    await asyncio.gather(*tasks)


@flow(name="sync-klines-1m")
async def sync_klines_1m():
    await sync_klines("1m")


@flow(name="sync-klines-1h")
async def sync_klines_1h():
    await sync_klines("1h")


@flow(name="sync-klines-1d")
async def sync_klines_1d():
    await sync_klines("1d")


if __name__ == "__main__":
    asyncio.run(sync_klines_1m())
