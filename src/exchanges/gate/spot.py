from typing import ClassVar

from constants import InstType, SymbolStatus
from utils import to_decimal_str

from exchanges._base_ import BaseClient


class GateSpotClient(BaseClient):
    """https://www.gate.com/docs/developers/apiv4/zh_CN/"""

    exchange_name = "gate"
    inst_type = InstType.SPOT
    base_url = "https://api.gateio.ws/api/v4"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "untradable": SymbolStatus.CLOSED,
        "buyable": SymbolStatus.ACTIVE,
        "sellable": SymbolStatus.ACTIVE,
        "tradable": SymbolStatus.ACTIVE,
    }

    async def get_exchange_info(self):
        """
        https://www.gate.com/docs/developers/apiv4/zh_CN/#%E6%9F%A5%E8%AF%A2%E6%89%80%E6%9C%89%E5%B8%81%E7%A7%8D%E4%BF%A1%E6%81%AF
        """
        return await self.send_request("GET", "/spot/currency_pairs")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data:
            rows.append(
                {
                    "symbol": sym["id"],
                    "base_asset": sym["base"],
                    "quote_asset": sym["quote"],
                    "status": self.status_map.get(sym["trade_status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": to_decimal_str(sym["precision"]),
                    "step_size": to_decimal_str(sym["amount_precision"]),
                    "price_precision": sym["precision"],
                    "quantity_precision": sym["amount_precision"],
                    "onboard_time": min(sym["sell_start"], sym["buy_start"]) * 1000,
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
        https://www.gate.com/docs/developers/apiv4/zh_CN/#%E5%B8%82%E5%9C%BA-k-%E7%BA%BF%E5%9B%BE
        [
            [
                "1539852480", # 秒(s)精度的 Unix 时间戳
                "971519.677", # 计价货币交易额
                "0.0021724", # 收盘价
                "0.0021922", # 最高价
                "0.0021724", # 最低价
                "0.0021737", # 开盘价
                "true", # 窗口是否关闭
            ]
        ]
        """
        limit = 1000

        def get_data(data):
            if "message" not in data or "Candlestick too long ago" not in data["message"]:
                return data
            return []

        async for results in self._get_kline(
            url="/spot/candlesticks",
            params={
                "currency_pair": symbol,
                "interval": interval,
                "limit": limit,
            },
            get_data=get_data,
            format_item=lambda d: {
                "exchange_id": self.exchange_id,
                "inst_type": self.inst_type,
                "symbol": symbol,
                "timestamp": int(d[0]) * 1000,
                "open": d[5],
                "high": d[3],
                "low": d[4],
                "close": d[2],
                "quote_volume": d[1],
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
