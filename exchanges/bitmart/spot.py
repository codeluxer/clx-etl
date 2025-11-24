from typing import ClassVar

from constants import InstType, SymbolStatus
from utils import precision, to_decimal_str

from exchanges._base_ import BaseClient


class BitmartSpotClient(BaseClient):
    """https://developer-pro.bitmart.com/en/spot/#public-market-data"""

    exchange_name = "bitmart"
    inst_type = InstType.SPOT
    base_url = "https://api-cloud.bitmart.com/spot"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "trading": SymbolStatus.ACTIVE,
        "pre-trade": SymbolStatus.PENDING,
    }

    async def get_exchange_info(self):
        """
        https://developer-pro.bitmart.com/en/spot/#get-trading-pairs-list-v1
        """
        return await self.send_request("GET", "/v1/symbols/details")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data["data"]["symbols"]:
            price_precision = int(sym["price_max_precision"])
            tick_size = to_decimal_str(price_precision)
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["base_currency"],
                    "quote_asset": sym["quote_currency"],
                    "status": self.status_map.get(sym["trade_status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": tick_size,
                    "step_size": sym["base_min_size"],
                    "price_precision": price_precision,
                    "quantity_precision": precision(sym["base_min_size"]),
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
        https://developer-pro.bitmart.com/en/spot/#get-history-k-line-v3

        {
            "code":1000,
            "trace":"886fb6ae-456b-4654-b4e0-1231",
            "message": "success",
            "data":[
                [
                    "1689736680",  // t
                    "3.721",  // o
                    "3.743",  // h
                    "3.677",  // l
                    "3.708",  // c
                    "22698348.04828491",  // v
                    "12698348.04828491"  // qv
                ],
            ]
        }
        """
        interval_map = {
            "1m": "1",
            "1h": "60",
            "1d": "1440",
        }
        limit = 200

        def get_data(data):
            if isinstance(data.get("data"), list):
                return data["data"]
            if data.get("message") == "no data":
                return []
            raise Exception(f"Unknown data format: {data}")

        async for results in self._get_kline(
            url="/quotation/v3/klines",
            params={
                "symbol": symbol,
                "step": interval_map.get(interval),
                "limit": limit,
            },
            get_data=get_data,
            format_item=lambda d: {
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type,
                "symbol": symbol,
                "timestamp": int(d[0]) * 1000,
                "open": d[1],
                "high": d[2],
                "low": d[3],
                "close": d[4],
                "volume": d[5],
                "quote_volume": d[6],
            },
            start_time_key="after",
            end_time_key="before",
            limit=limit,
            time_unit="s",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results
