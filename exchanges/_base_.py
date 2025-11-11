from abc import ABC, abstractmethod
from typing import Literal

from aiohttp import ClientSession
from databases.mysql import ExchangeSymbol, async_upsert, sync_engine
from sqlalchemy import text


class BaseClient(ABC):
    def __init__(self):
        self._exchange_id = None

    @abstractmethod
    def base_url(self):
        raise NotImplementedError

    @abstractmethod
    def exchange_name(self) -> str:
        raise NotImplementedError

    @property
    def exchange_id(self):
        if self._exchange_id:
            return self._exchange_id
        with sync_engine.begin() as conn:
            result = conn.execute(text("SELECT id FROM exchange_info WHERE name = :name"), {"name": self.exchange_name})
            row = result.scalar_one_or_none()
            return row

    @abstractmethod
    def inst_type(self):
        raise NotImplementedError

    async def send_request(self, method: Literal["GET", "POST"], endpoint: str, params=None, headers=None) -> dict:
        url = f"{self.base_url}{endpoint}"
        async with ClientSession() as session:
            if method == "GET":
                response = await session.get(url, params=params, headers=headers)
            elif method == "POST":
                response = await session.post(url, json=params, headers=headers)
            response.raise_for_status()
            return await response.json()

    @abstractmethod
    async def get_all_symbols(self):
        raise NotImplementedError

    async def update_all_symbols(self):
        values = await self.get_all_symbols()
        await async_upsert(
            values,
            ExchangeSymbol,
            [
                "tick_size",
                "step_size",
                "price_precision",
                "quantity_precision",
                "status",
            ],
        )
