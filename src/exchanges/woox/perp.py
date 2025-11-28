from typing import ClassVar

from constants import InstType, SymbolStatus

from exchanges._base_ import BaseClient
from utils import precision


class WooxPerpClient(BaseClient):
    """https://docs.woox.io/#general-information"""

    exchange_name = "woox"
    inst_type = InstType.PERP
    base_url = ""

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "TRADING": SymbolStatus.ACTIVE,
        "SUSPENDED": SymbolStatus.HALTED,
    }

    async def get_exchange_info(self):
        """
        https://docs.woox.io/#available-symbols-public
        """
        return await self.send_request("GET", "https://api.woox.io/v1/public/info")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["rows"]:
            symbol = sym["symbol"]
            inst_type, base, quote = symbol.split("_")
            if inst_type == "PERP":
                rows.append(
                    {
                        "symbol": symbol,
                        "base_asset": base,
                        "quote_asset": quote,
                        "status": self.status_map.get(sym["status"]),
                        "exchange_id": self.exchange_id,
                        "inst_type": self.inst_type,
                        "tick_size": sym["quote_tick"],
                        "step_size": sym["base_tick"],
                        "price_precision": precision(sym["quote_tick"]),
                        "quantity_precision": precision(sym["base_tick"]),
                        "onboard_time": float(sym["listing_time"]) * 1000,
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
        https://docs.woox.io/#kline-historical-data-public
        {
            "success": true,
            "data": {
                "rows": [
                    {
                        "open": 66166.23,
                        "close": 66124.56,
                        "low": 66038.06,
                        "high": 66176.97,
                        "volume": 23.45528526,
                        "amount": 1550436.21725288,
                        "symbol": "SPOT_BTC_USDT",
                        "type": "1m",
                        "start_timestamp": 1636388220000, // Unix epoch time in milliseconds
                        "end_timestamp": 1636388280000
                    }
                ],
                "meta":{
                    "total":67377,
                    "records_per_page":100,
                    "current_page":1
                }
            },
            "timestamp": 1636388280000
        }
        """
        limit = 1000
        async for results in self._get_kline(
            url="https://api-pub.woox.io/v1/hist/kline",
            params={
                "symbol": symbol,
                "type": interval,
                "size": limit,
            },
            get_data=lambda d: d["data"]["rows"],
            format_item=lambda d: {
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type,
                "symbol": symbol,
                "timestamp": int(d["start_timestamp"]),
                "open": d["open"],
                "high": d["high"],
                "low": d["low"],
                "close": d["close"],
                "volume": d["volume"],
                "quote_volume": d["amount"],
            },
            start_time_key="start_time",
            end_time_key="end_time",
            limit=limit,
            time_unit="ms",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results
