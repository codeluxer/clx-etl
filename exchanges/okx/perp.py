from typing import ClassVar

from constants import InstType, SymbolStatus
from utils import precision

from exchanges._base_ import BaseClient


class OkxPerpClient(BaseClient):
    """https://www.okx.com/docs-v5/en/#public-data"""

    exchange_name = "okx"
    inst_type = InstType.PERP
    base_url = "https://www.okx.com/api"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "live": SymbolStatus.ACTIVE,
        "suspend": SymbolStatus.HALTED,
        "preopen": SymbolStatus.PENDING,
        "test": SymbolStatus.PENDING,
    }

    async def get_exchange_info(self):
        """
        https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-instruments
        """
        return await self.send_request("GET", "/v5/public/instruments?instType=SWAP")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["data"]:
            inst_family = sym["instFamily"]
            base, quote = inst_family.split("-")
            rows.append(
                {
                    "symbol": sym["instId"],
                    "base_asset": base,
                    "quote_asset": quote,
                    "status": self.status_map.get(sym["state"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["tickSz"],
                    "step_size": sym["lotSz"],
                    "price_precision": precision(sym["tickSz"]),
                    "quantity_precision": precision(sym["lotSz"]),
                    "onboard_time": sym["listTime"],
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
        https://www.okx.com/docs-v5/en/#public-data-rest-api-get-mark-price-candlesticks-history
        {
            "code":"0",
            "msg":"",
            "data":[
                [
                    "1597026383085",  // open time
                    "3.721",  // open
                    "3.743",  // high
                    "3.677",  // low
                    "3.708",  // close
                    "1"  // confirm
                ]
            ]
        }

        """
        interval_map = {
            "1m": "1m",
            "1h": "1H",
        }
        limit = 1000
        async for results in self._get_kline(
            url="/v5/market/history-mark-price-candles",
            params={
                "instId": symbol,
                "bar": interval_map.get(interval),
                "limit": limit,
            },
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
            },
            start_time_key="after",
            end_time_key="before",
            limit=limit,
            time_unit="ms",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results
