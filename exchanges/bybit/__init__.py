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


class BybitPerpClient(BaseClient):
    """https://bybit-exchange.github.io/docs/v5/intro"""

    exchange_name = "bybit"
    inst_type = InstType.PERP
    base_url = "https://api.bybit.com"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "Trading": SymbolStatus.ACTIVE,
    }

    async def get_exchange_info(self):
        """
        https://bybit-exchange.github.io/docs/v5/market/instrument
        """
        return await self.send_request("GET", "/v5/market/instruments-info?category=linear")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data["result"]["list"]:
            rows.append(
                {
                    "symbol": sym["symbol"],
                    "base_asset": sym["baseCoin"],
                    "quote_asset": sym["quoteCoin"],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["priceFilter"]["tickSize"],
                    "step_size": sym["lotSizeFilter"]["qtyStep"],
                    "price_precision": int(sym.get("priceScale", precision(sym["priceFilter"]["tickSize"]))),
                    "quantity_precision": precision(sym["lotSizeFilter"]["qtyStep"]),
                }
            )
        return rows
