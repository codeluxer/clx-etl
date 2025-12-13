import asyncio

from macro_markets.kalshi import KalshiClient
from prefect import flow

from utils.logger import logger as _logger


@flow(name="sync-kalshi")
async def sync_kalshi_flow():
    logger = _logger.bind(job_id="KALSHI")
    client = KalshiClient(logger)
    await client.sync_market_meta()


if __name__ == "__main__":
    asyncio.run(sync_kalshi_flow())
