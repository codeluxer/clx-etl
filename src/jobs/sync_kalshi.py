from macro_markets.kalshi import KalshiClient

from utils.logger import logger as _logger


async def sync_kalshi():
    logger = _logger.bind(job_id="KALSHI")
    await KalshiClient(logger).sync_market_meta()
