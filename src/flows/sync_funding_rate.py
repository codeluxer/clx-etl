import asyncio
import traceback

from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from exchanges._base_ import BaseClient
from exchanges.binance import BinancePerpClient
from exchanges.bitget import BitgetPerpClient
from exchanges.bybit import BybitPerpClient
from exchanges.okx import OkxPerpClient
from utils.logger import logger as _logger


@task(name="update-funding-rate", cache_policy=NO_CACHE)
async def update_funding_rate_task(client_name: str, client: BaseClient):
    try:
        await client.update_funding_rate()
        return f"{client_name} ok"
    except Exception as e:
        _logger.error(f"[{client_name}] Failed: {e}")
        traceback.print_exc()
        await asyncio.sleep(1)
        return f"{client_name} failed"


@flow(name="sync-funding-rate")
async def sync_funding_rate():
    logger = _logger.bind(job_id="FUNDING_RATE")

    clients: dict[str, BaseClient] = {
        "binance": BinancePerpClient(logger),
        "bitget": BitgetPerpClient(logger),
        "bybit": BybitPerpClient(logger),
        "okx": OkxPerpClient(logger),
    }
    tasks = []

    for name, client in clients.items():
        tasks.append(update_funding_rate_task(client_name=name, client=client))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(sync_funding_rate())
