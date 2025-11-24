from typing import ClassVar

from constants import InstType, SymbolStatus
from utils import precision

from exchanges._base_ import BaseClient


class BybitSpotClient(BaseClient):
    """https://bybit-exchange.github.io/docs/v5/intro"""

    exchange_name = "bybit"
    inst_type = InstType.SPOT
    base_url = "https://api.bybit.com"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "Trading": SymbolStatus.ACTIVE,
    }

    async def get_exchange_info(self):
        """
        https://bybit-exchange.github.io/docs/v5/market/instrument
        """
        return await self.send_request("GET", "/v5/market/instruments-info?category=spot")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data["result"]["list"]:
            tick_size = str(sym["priceFilter"]["tickSize"])
            step_size = str(sym["lotSizeFilter"]["basePrecision"])
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
        https://bybit-exchange.github.io/docs/v5/market/kline

        {
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "symbol": "BTCUSD",
                "category": "inverse",
                "list": [
                    [
                        "1670608800000",  // startTime
                        "17071", // open
                        "17073", // high
                        "17027", // low
                        "17055.5", // close
                        "268611", // volume
                        "15.74462667" // Turnover
                    ],
                ]
            },
            "retExtInfo": {},
            "time": 1672025956592
        }
        """
        interval_map = {
            "1m": "1",
            "1h": "60",
            "1d": "D",
        }
        limit = 1000
        async for results in self._get_kline(
            url="/v5/market/kline",
            params={
                "category": "spot",
                "symbol": symbol,
                "interval": interval_map.get(interval),
                "limit": limit,
            },
            get_data=lambda d: d["result"]["list"],
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
                "quote_volume": d[6],
            },
            start_time_key="start",
            end_time_key="end",
            limit=limit,
            time_unit="ms",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results
