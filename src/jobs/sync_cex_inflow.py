from macro_markets.oklink.fetcher import OklinkOnchainInfo
from sqlalchemy import select
from sqlalchemy.orm import Session

from databases.doris import get_stream_loader
from databases.mysql import sync_engine
from databases.mysql.models import ExchangeInfo

exchanges = ["binance", "okx", "bybit", "bitget", "kraken"]


def get_exchange_info():
    with Session(sync_engine) as conn:
        results = select(ExchangeInfo).where(ExchangeInfo.name.in_(exchanges))
        exchange_info = conn.execute(results).scalars().all()
    return exchange_info


async def sync_cex_inflow():
    exchange_info = get_exchange_info()

    stream_loader = get_stream_loader()
    oklink_onchain_info = OklinkOnchainInfo()
    for exchange in exchange_info:
        result = await oklink_onchain_info.get_inflow(exchange)
        await stream_loader.send_rows(result, "cex_inflow_hourly")
