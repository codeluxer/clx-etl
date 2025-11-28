from sqlalchemy import text

from constants import InstType
from databases.mysql import async_engine


async def get_symbols(exchange: str, base_asset: [str], quote_asset: str, inst_type: InstType):
    async with async_engine.begin() as conn:
        result = await conn.execute(
            text(f"""
        SELECT s.symbol FROM exchange_symbol s
        LEFT JOIN exchange_info i ON s.exchange_id = i.id
        WHERE i.name = '{exchange}' AND s.base_asset IN {str(tuple(base_asset)).replace(",)", ")")} AND s.quote_asset = '{quote_asset}' AND s.inst_type = {inst_type.value}
        """),
        )
        symbols = [i[0] for i in result.all()]
    return symbols
