from typing import ClassVar

from constants import InstType, SymbolStatus

from exchanges._base_ import BaseClient


class MexcPerpClient(BaseClient):
    """https://www.mexc.com/api-docs/futures/update-log"""

    exchange_name = "mexc"
    inst_type = InstType.PERP
    base_url = "https://contract.mexc.com/api"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        0: SymbolStatus.ACTIVE,
        1: SymbolStatus.HALTED,
        2: SymbolStatus.CLOSED,
        3: SymbolStatus.CLOSED,
        4: SymbolStatus.HALTED,
    }

    async def get_exchange_info(self):
        """
        https://www.gate.com/docs/developers/apiv4/zh_CN/#%E6%9F%A5%E8%AF%A2%E6%89%80%E6%9C%89%E7%9A%84%E5%90%88%E7%BA%A6%E4%BF%A1%E6%81%AF
        """
        return await self.send_request("GET", "/v1/contract/detail")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["data"]:
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseCoin"],
                    "quote_asset": sym["quoteCoin"],
                    "status": self.status_map.get(sym["state"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["priceUnit"],
                    "step_size": sym["volUnit"],
                    "price_precision": sym["priceScale"],
                    "quantity_precision": sym["amountScale"],
                    "onboard_time": sym["openingTime"] * 1000,
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
        https://www.mexc.com/api-docs/futures/market-endpoints#get-candlestick-data
        {
            "success": true,
            "code": 0,
            "data": {
                "time": [
                    1761876000,
                    1761876900
                ],
                "open": [
                    109573.9,
                    109006.4
                ],
                "close": [
                    109006.4,
                    109301.5
                ],
                "high": [
                    109628.1,
                    109426.2
                ],
                "low": [
                    108953.3,
                    109006.4
                ],
                "vol": [
                    5587051.0,
                    5739575.0
                ],
                "amount": [
                    6.106243567181E7,
                    6.270099147368E7
                ],
                "realOpen": [
                    109574.0,
                    109010.0
                ],
                "realClose": [
                    109006.4,
                    109301.5
                ],
                "realHigh": [
                    109628.1,
                    109426.2
                ],
                "realLow": [
                    108953.3,
                    109010.0
                ]
            }
        }
        """
        interval_map = {
            "1m": "1m",
            "1h": "60m",
            "1d": "1d",
        }
        limit = 2000
        async for results in self._get_kline(
            url=f"https://contract.mexc.com/api/v1/contract/kline/{symbol}",
            params={
                "interval": interval_map.get(interval),
                "limit": limit,
            },
            get_data=lambda d: zip(
                d["data"]["time"],
                d["data"]["open"],
                d["data"]["high"],
                d["data"]["low"],
                d["data"]["close"],
                d["data"]["vol"],
                d["data"]["amount"],
                strict=False,
            ),
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
            start_time_key="start",
            end_time_key="end",
            limit=limit,
            time_unit="s",
            symbol=symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            sleep_ms=sleep_ms,
        ):
            yield results
