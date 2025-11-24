from typing import ClassVar

from constants import InstType, SymbolStatus
from utils import precision

from exchanges._base_ import BaseClient


class BitgetSpotClient(BaseClient):
    """https://www.bitget.com/api-doc/spot/intro"""

    exchange_name = "bitget"
    inst_type = InstType.SPOT
    base_url = "https://api.bitget.com"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "online": SymbolStatus.ACTIVE,
        "halt": SymbolStatus.HALTED,
        "gray": SymbolStatus.PENDING,
        "offline": SymbolStatus.CLOSED,
    }

    async def get_exchange_info(self):
        """
        https://www.bitget.com/api-doc/spot/market/Get-Symbols
        """
        return await self.send_request("GET", "/api/v2/spot/public/symbols")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data["data"]:
            price_precision = int(sym["pricePrecision"])
            quantity_precision = int(sym["quantityPrecision"])

            tick_size = f"{10 ** (-price_precision):.{price_precision}f}"
            step_size = f"{10 ** (-quantity_precision):.{quantity_precision}f}"

            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseCoin"],
                    "quote_asset": sym["quoteCoin"],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": tick_size,
                    "step_size": step_size,
                    "price_precision": precision(tick_size),
                    "quantity_precision": precision(step_size),
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
        https://www.bitget.com/api-doc/spot/market/Get-Candle-Data

        {
            "code": "00000",
            "msg": "success",
            "requestTime": 1695800278693,
            "data": [
                [
                    "1656604800000",  // System timestamp, Unix millisecond timestamp
                    "37834.5",        // Open price
                    "37849.5",        // High price
                    "37773.5",        // Low price
                    "37773.5",        // Close price
                    "428.3462",       // Volume
                    "16198849.1079",  // USDT volume
                    "16198849.1079"   // Quote volume
                ],
            ]
        }
        """
        limit = 1000
        interval_map = {
            "1m": "1min",
            "1h": "1h",
            "1d": "1day",
        }
        async for results in self._get_kline(
            url="/api/v2/spot/market/candles",
            params={"symbol": symbol, "granularity": interval_map.get(interval), "limit": limit},
            get_data=lambda d: d["data"],
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
