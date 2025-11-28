from typing import ClassVar

from constants import InstType, SymbolStatus

from exchanges._base_ import BaseClient
from utils import precision


class WeexPerpClient(BaseClient):
    """https://www.weex.com/api-doc/contract/log/changelog"""

    exchange_name = "weex"
    inst_type = InstType.PERP
    base_url = "https://pro-openapi.weex.tech"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "TRADING": SymbolStatus.ACTIVE,
        "SUSPENDED": SymbolStatus.HALTED,
    }

    async def get_exchange_info(self):
        """
        https://www.weex.com/api-doc/contract/Market_API/GetContractInfo
        """
        return await self.send_request("GET", "/capi/v2/market/contracts")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()
        rows = []
        for sym in data:
            symbol = sym["symbol"]

            rows.append(
                {
                    "symbol": symbol,
                    "base_asset": symbol["underlying_index"],
                    "quote_asset": symbol["quote_currency"],
                    "status": self.status_map.get(sym["status"]),
                    "exchange_id": self.exchange_id,
                    "inst_type": self.inst_type,
                    "tick_size": sym["tick_size"],
                    "step_size": sym["size_increment"],
                    "price_precision": precision(sym["tick_size"]),
                    "quantity_precision": precision(sym["minOrderSize"]),
                }
            )
        return rows
