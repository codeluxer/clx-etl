from decimal import Decimal
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
