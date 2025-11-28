from typing import ClassVar

from constants import InstType, SymbolStatus

from exchanges._base_ import BaseClient


class MexcSpotClient(BaseClient):
    """https://www.mexc.com/api-docs/spot-v3/introduction"""

    exchange_name = "mexc"
    inst_type = InstType.SPOT
    base_url = "https://api.mexc.com"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "1": SymbolStatus.ACTIVE,
        "2": SymbolStatus.HALTED,
        "3": SymbolStatus.CLOSED,
    }

    async def get_exchange_info(self):
        """
        https://www.mexc.com/api-docs/spot-v3/market-data-endpoints#exchange-information
        """
        return await self.send_request("GET", "/api/v3/exchangeInfo")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data["symbols"]:
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseAsset"],
                    "quote_asset": sym["quoteAsset"],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["quoteAmountPrecision"],
                    "step_size": sym["baseSizePrecision"],
                    "price_precision": sym["quoteAssetPrecision"],
                    "quantity_precision": sym["baseAssetPrecision"],
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
        https://www.mexc.com/api-docs/spot-v3/market-data-endpoints#klinecandlestick-data
        [
            [
                1640804880000, // Open time
                "47482.36", // Open
                "47482.36", // High
                "47416.57", // Low
                "47436.1", // Close
                "3.550717", // Volume
                1640804940000, // Close time
                "168387.3" // Quote asset volume
            ]
        ]
        """
        interval_map = {
            "1m": "1m",
            "1h": "60m",
            "1d": "1d",
        }
        limit = 1000
        async for results in self._get_kline(
            url="/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": interval_map.get(interval),
                "limit": limit,
            },
            get_data=lambda d: d,
            format_item=lambda d: {
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type,
                "symbol": symbol,
                "timestamp": int(d[0]),
                "open": d[1],
                "high": d[2],
                "low": d[3],
                "close": d[4],
                "volume": d[5],
                "quote_volume": d[7],
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
