import asyncio

from prefect import flow, task

from exchanges.aster import AsterPerpClient, AsterSpotClient
from exchanges.binance import BinancePerpClient, BinanceSpotClient
from exchanges.bitget import BitgetPerpClient, BitgetSpotClient
from exchanges.bitmart import BitmartPerpClient, BitmartSpotClient
from exchanges.bybit import BybitPerpClient, BybitSpotClient
from exchanges.coinbase import CoinbaseSpotClient
from exchanges.gate import GatePerpClient, GateSpotClient
from exchanges.kraken import KrakenSpotClient
from exchanges.mexc import MexcPerpClient, MexcSpotClient
from exchanges.okx import OkxPerpClient, OkxSpotClient
from exchanges.woox import WooxPerpClient, WooxSpotClient
from utils.logger import logger as _logger

CLIENT_REGISTRY = {
    "aster_spot": AsterSpotClient,
    "aster_perp": AsterPerpClient,
    "binance_spot": BinanceSpotClient,
    "binance_perp": BinancePerpClient,
    "bitget_spot": BitgetSpotClient,
    "bitget_perp": BitgetPerpClient,
    "bitmart_spot": BitmartSpotClient,
    "bitmart_perp": BitmartPerpClient,
    "bybit_spot": BybitSpotClient,
    "bybit_perp": BybitPerpClient,
    "coinbase_spot": CoinbaseSpotClient,
    "gate_spot": GateSpotClient,
    "gate_perp": GatePerpClient,
    "kraken_spot": KrakenSpotClient,
    "mexc_spot": MexcSpotClient,
    "mexc_perp": MexcPerpClient,
    "okx_spot": OkxSpotClient,
    "okx_perp": OkxPerpClient,
    "woox_spot": WooxSpotClient,
    "woox_perp": WooxPerpClient,
}


@task(name="update-symbols-task", retries=2, retry_delay_seconds=3)
async def update_symbols_task(client_name: str):
    logger = _logger.bind(job_id=f"SYMBOLS-{client_name}")

    client_class = CLIENT_REGISTRY[client_name]
    client = client_class(logger)

    await client.update_all_symbols()
    return f"{client_name} symbols ok"


@flow(name="sync-symbols", timeout_seconds=1800)
async def sync_symbols():
    tasks = []
    for client_name in CLIENT_REGISTRY:
        tasks.append(update_symbols_task(client_name))
    await asyncio.gather(*tasks)


if __name__ == "__main__":

    async def sync_symbols_test():
        client_name = "binance_spot"
        await update_symbols_task(client_name)

    asyncio.run(sync_symbols_test())
