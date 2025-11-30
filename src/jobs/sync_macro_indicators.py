from macro_markets.macro_indicators import get_macro_klines

from databases.doris import get_stream_loader


async def sync_macro_indicators():
    results = await get_macro_klines()
    await get_stream_loader().send_rows(results, "macro_kline_raw_1m")
