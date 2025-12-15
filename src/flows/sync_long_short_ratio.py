import asyncio
import traceback
from typing import Literal

from constants import InstType
from prefect import flow, get_run_logger, task

from exchanges._base_ import BaseClient
from exchanges.binance import BinancePerpClient
from exchanges.bitget import BitgetPerpClient
from exchanges.bybit import BybitPerpClient
from exchanges.okx import OkxPerpClient

from .constants import COINS
from .utils import get_symbols

ALL_CLIENTS: dict[str, BaseClient] = {
    "binance": BinancePerpClient,
    "bitget": BitgetPerpClient,
    "bybit": BybitPerpClient,
    "okx": OkxPerpClient,
}


@task(
    name="update-long-short-ratio-task",
)
async def update_long_short_ratio(client_name: str, interval: Literal["5m", "1h", "1d"], coins: list[str]):
    logger = get_run_logger()
    logger.info(f"[{client_name}] Start")
    try:
        # 动态加载 client
        client = ALL_CLIENTS[client_name](logger)

        symbols = await get_symbols(client_name, coins, "USDT", InstType.PERP)

        for sym in symbols:
            try:
                if interval == "5m":
                    await client.update_long_short_ratio_5m(sym)
                elif interval == "1h":
                    await client.update_long_short_ratio_1h(sym)
                elif interval == "1d":
                    await client.update_long_short_ratio_1d(sym)
            except Exception as e:
                logger.error(f"[{client_name}] Failed {sym}: {e}")
                traceback.print_exc()
                await asyncio.sleep(1)

    except Exception as e:
        logger.error(f"[{client_name}] Failed overall: {e}")
        traceback.print_exc()
        await asyncio.sleep(1)


def get_client_names() -> list[str]:
    return ["binance", "bitget", "bybit", "okx"]


async def submit_tasks(interval: str):
    tasks = [update_long_short_ratio(name, interval, COINS) for name in get_client_names()]

    await asyncio.gather(*tasks)


@flow(name="sync-long-short-ratio-5m")
async def sync_long_short_ratio_5m():
    logger = get_run_logger()
    logger.info("Start sync long short ratio 5m")
    await submit_tasks("5m")


@flow(name="sync-long-short-ratio-1h")
async def sync_long_short_ratio_1h():
    await submit_tasks("1h")


@flow(name="sync-long-short-ratio-1d")
async def sync_long_short_ratio_1d():
    await submit_tasks("1d")


if __name__ == "__main__":
    asyncio.run(sync_long_short_ratio_1h())
