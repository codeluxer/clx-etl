from datetime import date, datetime, timedelta

from dotenv import load_dotenv
from prefect import flow, get_run_logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from databases.doris import DorisAsyncDB
from databases.mysql import sync_engine
from databases.mysql.models import ClxSymbol, ExchangeSymbol

from .restore_market_snapshot_from_s3 import restore_from_s3

load_dotenv()


# =====================
# MySQL: active symbols
# =====================


def get_active_symbols(symbol_filter=None):
    with Session(sync_engine) as conn:
        stmt = (
            select(
                ExchangeSymbol.symbol,
                ExchangeSymbol.exchange_id,
                ExchangeSymbol.inst_type,
            )
            .join(ClxSymbol, ExchangeSymbol.id == ClxSymbol.symbol_id)
            .where(ClxSymbol.is_active == 1)
        )

        if symbol_filter:
            stmt = stmt.where(ExchangeSymbol.symbol == symbol_filter)

        return conn.execute(stmt).all()


# =====================
# Doris helpers
# =====================


async def check_hour(conn, symbol, exchange_id, inst_type, start, end):
    sql = """
        SELECT COUNT(*) AS cnt
        FROM market_snapshot
        WHERE symbol = :symbol
          AND exchange_id = :exchange_id
          AND inst_type = :inst_type
          AND dt >= :start
          AND dt < :end
    """

    res = await conn.query(
        sql,
        {
            "symbol": symbol,
            "exchange_id": exchange_id,
            "inst_type": inst_type,
            "start": start,
            "end": end,
        },
    )
    return res[0][0] or 0


doris = DorisAsyncDB()


@flow
async def check_market_snapshot_integrity(
    lookback_days=7,
    only_empty=False,
    only_partial=False,
    symbol_filter=None,
):
    logger = get_run_logger()
    active_symbols = get_active_symbols(symbol_filter)

    today = date.today()
    results = []

    for d in range(lookback_days):
        day = today - timedelta(days=d + 1)  # 不查今天
        day_start = datetime.combine(day, datetime.min.time())

        logger.info(f"\n📅 Checking day {day}")

        for symbol, exchange_id, inst_type in active_symbols:
            for h in range(24):
                start = day_start + timedelta(hours=h)
                end = start + timedelta(hours=1)

                cnt = await check_hour(doris, symbol, exchange_id, inst_type, start, end)
                should_restore = cnt == 0 or cnt < 3600

                if cnt == 0:
                    if not only_partial:
                        logger.info(f"❌ EMPTY  {symbol} ex={exchange_id} inst={inst_type} {start:%Y-%m-%d %H}:00")
                        results.append(("EMPTY", symbol, exchange_id, inst_type, start, cnt))

                elif cnt < 3600:
                    if not only_empty:
                        logger.info(
                            f"⚠️ PARTIAL {symbol} ex={exchange_id} inst={inst_type} {start:%Y-%m-%d %H}:00 rows={cnt}"
                        )
                        results.append(("PARTIAL", symbol, exchange_id, inst_type, start, cnt))

                if should_restore:
                    logger.info(f"Restoring {symbol} ex={exchange_id} inst={inst_type} {start:%Y-%m-%d %H}:00")
                    await restore_from_s3(symbol, exchange_id, inst_type, day, h, logger)

    return results
