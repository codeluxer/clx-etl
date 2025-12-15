import asyncio
import traceback

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE

from exchanges._base_ import BaseClient
from exchanges.binance import BinancePerpClient
from exchanges.bitget import BitgetPerpClient
from exchanges.bybit import BybitPerpClient
from exchanges.okx import OkxPerpClient

ALL_CLIENTS: dict[str, BaseClient] = {
    "binance": BinancePerpClient,
    "bitget": BitgetPerpClient,
    "bybit": BybitPerpClient,
    "okx": OkxPerpClient,
}


@task(name="update-funding-rate", cache_policy=NO_CACHE)
async def update_funding_rate_task(client_name: str):
    logger = get_run_logger()
    logger.info(f"Start update funding rate for {client_name}")
    try:
        await ALL_CLIENTS[client_name](logger).update_funding_rate()
        logger.info(f"Update funding rate for {client_name} ok")
    except Exception as e:
        logger.error(f"[{client_name}] Failed: {e}")
        traceback.print_exc()
        await asyncio.sleep(1)


@flow(name="sync-funding-rate")
async def sync_funding_rate():
    tasks = []

    for name in ALL_CLIENTS.keys():
        tasks.append(update_funding_rate_task(client_name=name))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(sync_funding_rate())
