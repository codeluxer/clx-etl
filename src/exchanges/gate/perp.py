from typing import ClassVar

from constants import InstType, SymbolStatus
from utils import precision

from exchanges._base_ import BaseClient


class GatePerpClient(BaseClient):
    """https://www.gate.com/docs/developers/apiv4/zh_CN/#futures"""

    exchange_name = "gate"
    inst_type = InstType.PERP
    base_url = "https://api.gateio.ws/api/v4"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "prelaunch": SymbolStatus.PENDING,
        "trading": SymbolStatus.ACTIVE,
        "delisting": SymbolStatus.HALTED,
        "delisted": SymbolStatus.CLOSED,
        "circuit_breaker": SymbolStatus.HALTED,
    }

    async def get_exchange_info(self):
        """
        https://www.gate.com/docs/developers/apiv4/zh_CN/#%E6%9F%A5%E8%AF%A2%E6%89%80%E6%9C%89%E7%9A%84%E5%90%88%E7%BA%A6%E4%BF%A1%E6%81%AF
        """
        return await self.send_request("GET", "/futures/usdt/contracts")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data:
            name = sym["name"]
            rows.append(
                {
                    "symbol": name,
                    "base_asset": name.split("_")[0],
                    "quote_asset": name.split("_")[1],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["order_price_round"],
                    "step_size": 1,
                    "price_precision": precision(sym["order_price_round"]),
                    "quantity_precision": 0,
                    "onboard_time": sym["launch_time"] * 1000,
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
        https://www.gate.com/docs/developers/apiv4/zh_CN/#%E5%90%88%E7%BA%A6%E5%B8%82%E5%9C%BA-k-%E7%BA%BF%E5%9B%BE
        [
            {
                "t": 1539852480, # 秒(s)精度的 Unix 时间戳
                "v": 97151, # 成交量
                "c": "1.032", # 收盘价
                "h": "1.032", # 最高价
                "l": "1.032", # 最低价
                "o": "1.032", # 开盘价
                "sum": "3580" # 计价货币交易额
            }
        ]
        """
        limit = 1000
        async for results in self._get_kline(
            url="/futures/usdt/candlesticks",
            params={
                "contract": symbol,
                "interval": interval,
                "limit": limit,
            },
            get_data=lambda d: d,
            format_item=lambda d: {
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type,
                "symbol": symbol,
                "timestamp": int(d["t"]) * 1000,
                "open": d["o"],
                "high": d["h"],
                "low": d["l"],
                "close": d["c"],
                "volume": d["v"],
                "quote_volume": d["sum"],
            },
            start_time_key="from",
            # end_time_key="to",
            limit=limit,
            time_unit="s",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results
