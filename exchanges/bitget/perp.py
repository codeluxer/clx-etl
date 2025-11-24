from typing import ClassVar

from constants import InstType, SymbolStatus

from exchanges._base_ import BaseClient


class BitgetPerpClient(BaseClient):
    """https://www.bitget.com/api-doc/contract/intro"""

    exchange_name = "bitget"
    inst_type = InstType.PERP
    base_url = "https://api.bitget.com"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "normal": SymbolStatus.ACTIVE,
        "listed": SymbolStatus.PENDING,
        "maintain": SymbolStatus.HALTED,
        "limit_open": SymbolStatus.HALTED,
        "restrictedAPI": SymbolStatus.HALTED,
        "off": SymbolStatus.CLOSED,
    }

    async def get_exchange_info(self):
        """
        https://www.bitget.com/api-doc/contract/market/Get-All-Symbols-Contracts
        """
        return await self.send_request("GET", "/api/mix/v1/market/contracts?productType=umcbl")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["data"]:
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseCoin"],
                    "quote_asset": sym["quoteCoin"],
                    "status": self.status_map.get(sym["symbolStatus"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": f"{10 ** (-int(sym['pricePlace'])):.{int(sym['pricePlace'])}f}",
                    "step_size": sym["sizeMultiplier"],
                    "price_precision": sym["pricePlace"],
                    "quantity_precision": sym["volumePlace"],
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
        https://www.bitget.com/api-doc/contract/market/Get-Candle-Data

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
                    "16198849.1079"   // Quote volume
                ],
            ]
        }
        """
        interval_map = {
            "1m": "1m",
            "1h": "1H",
            "1d": "1D",
        }
        limit = 1000
        async for results in self._get_kline(
            url="/api/v2/mix/market/candles",
            params={
                "symbol": symbol,
                "productType": "usdt-futures",
                "granularity": interval_map.get(interval),
                "kLineType": "MARKET",
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
                "volume": d[5],
                "quote_volume": d[6],
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
