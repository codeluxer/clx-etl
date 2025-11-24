from typing import ClassVar

from constants import InstType, SymbolStatus
from utils import precision

from exchanges._base_ import BaseClient


class BinanceSpotClient(BaseClient):
    """https://developers.binance.com/docs/binance-spot-api-docs"""

    exchange_name = "binance"
    inst_type = InstType.SPOT
    base_url = "https://api.binance.com"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "TRADING": SymbolStatus.ACTIVE,
        "END_OF_DAY": SymbolStatus.CLOSED,
        "HALT": SymbolStatus.HALTED,
        "BREAK": SymbolStatus.HALTED,
    }

    async def get_exchange_info(self):
        """
        https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-endpoints#exchange-information
        """
        return await self.send_request("GET", "/api/v3/exchangeInfo")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data["symbols"]:
            tick = step = None
            for f in sym["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    tick = f.get("tickSize")
                elif f["filterType"] == "LOT_SIZE":
                    step = f.get("stepSize")
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseAsset"],
                    "quote_asset": sym["quoteAsset"],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": tick.rstrip("0"),
                    "step_size": step.rstrip("0"),
                    "price_precision": precision(tick),
                    "quantity_precision": precision(step),
                }
            )
        return rows

    async def get_kline(
        self,
        symbol: str,
        interval: str = "1m",
        start_ms: int | None = None,
        end_ms: int | None = None,
        sleep_ms: int = 100,
    ):
        """
        https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data

        [
            [
                1499040000000,      // Kline open time
                "0.01634790",       // Open price
                "0.80000000",       // High price
                "0.01575800",       // Low price
                "0.01577100",       // Close price
                "148976.11427815",  // Volume
                1499644799999,      // Kline Close time
                "2434.19055334",    // Quote asset volume
                308,                // Number of trades
                "1756.87402397",    // Taker buy base asset volume
                "28.46694368",      // Taker buy quote asset volume
                "0"                 // Unused field, ignore.
            ]
        ]
        """
        limit = 1000
        async for results in self._get_kline(
            url="/api/v3/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            get_data=lambda d: d,
            format_item=lambda d: {
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type,
                "symbol": symbol,
                "timestamp": d[0],
                "open": d[1],
                "high": d[2],
                "low": d[3],
                "close": d[4],
                "volume": d[5],
                "quote_volume": d[7],
                "count": d[8],
            },
            start_time_key="startTime",
            end_time_key="endTime",
            limit=limit,
            time_unit="ms",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results
