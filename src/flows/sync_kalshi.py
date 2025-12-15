import asyncio

from macro_markets.kalshi import KalshiClient
from prefect import flow, get_run_logger


@flow(name="sync-kalshi")
async def sync_kalshi_flow():
    logger = get_run_logger()
    logger.info("Start sync kalshi market meta")
    client = KalshiClient(logger)
    await client.sync_market_meta()


if __name__ == "__main__":
    asyncio.run(sync_kalshi_flow())
