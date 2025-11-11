from typing import ClassVar

from constants import InstType, SymbolStatus
from utils import precision

from exchanges._base_ import BaseClient


class BitgetSpotClient(BaseClient):
    """https://www.bitget.com/api-doc/spot/intro"""

    exchange_name = "bitget"
    inst_type = InstType.SPOT
    base_url = "https://api.bitget.com/api"

    status_map: ClassVar[dict[str, SymbolStatus]] = {
        "online": SymbolStatus.ACTIVE,
        "halt": SymbolStatus.HALTED,
        "gray": SymbolStatus.PENDING,
        "offline": SymbolStatus.CLOSED,
    }

    async def get_exchange_info(self):
        """
        https://www.bitget.com/api-doc/spot/market/Get-Symbols
        """
        return await self.send_request("GET", "/v2/spot/public/symbols")

    async def get_all_symbols(self):
        data = await self.get_exchange_info()

        rows = []
        for sym in data["data"]:
            pricePrecision = int(sym["pricePrecision"])
            quantityPrecision = int(sym["quantityPrecision"])

            tick_size = f"{10 ** (-pricePrecision):.{pricePrecision}f}"
            step_size = f"{10 ** (-quantityPrecision):.{quantityPrecision}f}"

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


class BitgetPerpClient(BaseClient):
    """https://www.bitget.com/api-doc/contract/intro"""

    exchange_name = "bitget"
    inst_type = InstType.PERP
    base_url = "https://api.bitget.com/api"

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
        return await self.send_request("GET", "/mix/v1/market/contracts?productType=umcbl")

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
