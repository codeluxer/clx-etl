import asyncio
import traceback

from macro_markets.oklink.fetcher import OklinkOnchainInfo
from prefect import flow, task
from sqlalchemy import select
from sqlalchemy.orm import Session

from databases.doris import get_stream_loader
from databases.mysql import sync_engine
from databases.mysql.models import ExchangeInfo

exchange_names = ["binance", "okx", "bybit", "bitget", "kraken"]


def get_exchange_info(exchange_name: str):
    with Session(sync_engine) as conn:
        stmt = select(ExchangeInfo).where(ExchangeInfo.name == exchange_name)
        return conn.execute(stmt).scalar_one_or_none()


@task(name="sync-cex-inflow-task", retries=2, retry_delay_seconds=3)
async def sync_one_cex_inflow(exchange_name: str):
    stream_loader = get_stream_loader()
    oklink_onchain_info = OklinkOnchainInfo()

    exchange_info = get_exchange_info(exchange_name)

    try:
        inflow_rows = await oklink_onchain_info.get_inflow(exchange_info)
        await stream_loader.send_rows(inflow_rows, "cex_inflow_hourly")
        return f"{exchange_name} inflow ok"
    except Exception as e:
        traceback.print_exc()
        return f"{exchange_name} inflow failed: {e}"


@flow(name="sync-cex-inflow")
async def sync_cex_inflow():
    tasks = []
    for name in exchange_names:
        tasks.append(sync_one_cex_inflow(name))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(sync_cex_inflow())
