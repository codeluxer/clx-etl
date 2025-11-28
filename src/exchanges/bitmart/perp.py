from decimal import Decimal
from typing import ClassVar

from constants import InstType, SymbolStatus
from utils import precision

from exchanges._base_ import BaseClient


class BitmartPerpClient(BaseClient):
    """https://developer-pro.bitmart.com/en/futuresv2/"""

    exchange_name = "bitmart"
    inst_type = InstType.PERP
    base_url = "https://api-cloud-v2.bitmart.com"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "Trading": SymbolStatus.ACTIVE,
        "Delisted": SymbolStatus.PENDING,
    }

    async def get_exchange_info(self):
        """
        https://developer-pro.bitmart.com/en/futuresv2/#get-contract-details
        """
        return await self.send_request("GET", "/contract/public/details")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["data"]["symbols"]:
            actual_step_size = Decimal(sym["vol_precision"]) * Decimal(sym["contract_size"])

            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["base_currency"],
                    "quote_asset": sym["quote_currency"],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["price_precision"],
                    "step_size": actual_step_size,
                    "price_precision": precision(sym["price_precision"]),
                    "quantity_precision": precision(actual_step_size),
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
        https://developer-pro.bitmart.com/en/futuresv2/#get-k-line

        {
            "code":1000,
            "trace":"886fb6ae-456b-4654-b4e0-1231",
            "message": "Ok",
            "data":[
                {
                    "timestamp": 1662518160,
                    "open_price": "100",
                    "close_price": "120",
                    "high_price": "130",
                    "low_price": "90",
                    "volume": "941008"
                },
            ]
        }
        """
        interval_map = {
            "1m": "1",
            "1h": "60",
            "1d": "1440",
        }
        limit = 200
        async for results in self._get_kline(
            url="/contract/public/kline",
            params={
                "symbol": symbol,
                "step": interval_map.get(interval),
                "limit": limit,
            },
            get_data=lambda d: d["data"],
            format_item=lambda d: {
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type,
                "symbol": symbol,
                "timestamp": d["timestamp"] * 1000,
                "open": d["open_price"],
                "high": d["high_price"],
                "low": d["low_price"],
                "close": d["close_price"],
                "volume": d["volume"],
            },
            start_time_key="start_time",
            end_time_key="end_time",
            limit=limit,
            time_unit="s",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results
