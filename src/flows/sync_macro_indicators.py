from macro_markets.macro_indicators import get_macro_klines
from prefect import flow, get_run_logger

from databases.doris import get_stream_loader


@flow(name="sync-macro-indicators")
async def sync_macro_indicators():
    logger = get_run_logger()
    logger.info("Starting sync_macro_indicators...")
    results = await get_macro_klines(logger)
    await get_stream_loader().send_rows(results, "macro_kline_raw_1m")


if __name__ == "__main__":
    import asyncio

    asyncio.run(sync_macro_indicators())
