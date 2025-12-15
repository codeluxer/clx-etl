from datetime import UTC, datetime
import os

from flows.sync_cex_inflow import sync_cex_inflow
from flows.sync_funding_rate import sync_funding_rate
from flows.sync_kalshi import sync_kalshi_flow
from flows.sync_klines import sync_klines_1m
from flows.sync_long_short_ratio import (
    sync_long_short_ratio_1d,
    sync_long_short_ratio_1h,
    sync_long_short_ratio_5m,
)
from flows.sync_macro_indicators import sync_macro_indicators
from flows.sync_onchain_tx import sync_onchain_large_transfer
from flows.sync_symbols import sync_symbols
from prefect import deploy
from prefect.client.schemas.schedules import CronSchedule, IntervalSchedule, RRuleSchedule
from prefect.types.entrypoint import EntrypointType

ENV = os.getenv("ENV")
IS_PROD = ENV == "production"
IMAGE_URL = os.getenv("REGISTRY") + "/" + os.getenv("IMAGE_NAME") + ":" + os.getenv("VERSION")

POOL_MAP = {
    "staging": "clx-stg",
    "production": "coinluxer",
}


# -------------------------------------------------------------------
# Helper: Expand cron fields (支持 *, */5, 5, 5,10)
# -------------------------------------------------------------------
def expand_cron_field(value: str, max_value: int):
    if value == "*" or value == "":
        return list(range(0, max_value + 1))

    if value.startswith("*/"):
        step = int(value[2:])
        return list(range(0, max_value + 1, step))

    return [int(x) for x in value.split(",")]


# -------------------------------------------------------------------
# Helper: 秒级 cron → Prefect RRuleSchedule（完全合法 RFC5545）
# -------------------------------------------------------------------
def cron_seconds_schedule(seconds: list[int], minutes="*", hours="*"):
    minutes_list = expand_cron_field(minutes, 59)
    hours_list = expand_cron_field(hours, 23)
    seconds_list = sorted(seconds)

    minutes_str = ",".join(map(str, minutes_list))
    hours_str = ",".join(map(str, hours_list))
    seconds_str = ",".join(map(str, seconds_list))

    # RFC5545 RRule 格式
    dtstart = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    rrule = f"DTSTART:{dtstart}\nRRULE:FREQ=MINUTELY;BYSECOND={seconds_str};BYMINUTE={minutes_str};BYHOUR={hours_str}"

    return RRuleSchedule(rrule=rrule)


# -------------------------------------------------------------------
# Deployments
# -------------------------------------------------------------------
if __name__ == "__main__":
    deployments = [
        # sync_symbols: 每天一次
        sync_symbols.to_deployment(
            name=f"{ENV}-sync-symbols",
            tags=[ENV],
            description="同步交易所交易对",
            cron="0 0 * * *" if IS_PROD else None,
            entrypoint_type=EntrypointType.MODULE_PATH,
            concurrency_limit=1,
        ),
        # 每 5 分钟，第 5 秒执行
        sync_long_short_ratio_5m.to_deployment(
            name=f"{ENV}-sync-long-short-ratio-5m",
            tags=[ENV],
            description="同步交易所多空比[5min]",
            schedule=cron_seconds_schedule([5], minutes="*/5") if IS_PROD else None,
            entrypoint_type=EntrypointType.MODULE_PATH,
            concurrency_limit=1,
        ),
        # 每小时 0 分，第 5 和 30 秒执行
        sync_long_short_ratio_1h.to_deployment(
            name=f"{ENV}-sync-long-short-ratio-1h",
            tags=[ENV],
            description="同步交易所多空比[1h]",
            schedule=cron_seconds_schedule([5, 30], minutes="0") if IS_PROD else None,
            entrypoint_type=EntrypointType.MODULE_PATH,
            concurrency_limit=1,
        ),
        # 每天 00:00 的第 5 和 30 秒执行
        sync_long_short_ratio_1d.to_deployment(
            name=f"{ENV}-sync-long-short-ratio-1d",
            tags=[ENV],
            description="同步交易所多空比[1d]",
            schedule=cron_seconds_schedule([5, 30], minutes="0", hours="0") if IS_PROD else None,
            entrypoint_type=EntrypointType.MODULE_PATH,
            concurrency_limit=1,
        ),
        # 每分钟 0,1,5,30 分钟，第 5 秒执行
        sync_funding_rate.to_deployment(
            name=f"{ENV}-sync-funding-rate",
            tags=[ENV],
            description="同步交易所资金费率",
            schedule=cron_seconds_schedule([5], minutes="0,1,5,30") if IS_PROD else None,
            entrypoint_type=EntrypointType.MODULE_PATH,
            concurrency_limit=1,
        ),
        # onchain_large_transfer: 每 30 秒执行
        sync_onchain_large_transfer.to_deployment(
            name=f"{ENV}-sync-onchain-large-transfer",
            tags=[ENV],
            description="同步链上大额转出",
            schedule=IntervalSchedule(interval=30) if IS_PROD else None,
            entrypoint_type=EntrypointType.MODULE_PATH,
            concurrency_limit=1,
        ),
        # cex inflow: 每小时 0 分，第 5 和 30 秒执行
        sync_cex_inflow.to_deployment(
            name=f"{ENV}-sync-cex-inflow",
            tags=[ENV],
            description="同步CEX资金流入",
            schedule=cron_seconds_schedule([5, 30], minutes="0") if IS_PROD else None,
            entrypoint_type=EntrypointType.MODULE_PATH,
            concurrency_limit=1,
        ),
        sync_macro_indicators.to_deployment(
            name=f"{ENV}-sync-macro-indicators",
            tags=[ENV],
            description="同步宏观指标",
            schedule=IntervalSchedule(interval=30) if IS_PROD else None,
            entrypoint_type=EntrypointType.MODULE_PATH,
            concurrency_limit=1,
        ),
        sync_kalshi_flow.to_deployment(
            name=f"{ENV}-sync-kalshi",
            tags=[ENV],
            description="同步 Kalshi 数据",
            schedule=IntervalSchedule(interval=60) if IS_PROD else None,
            entrypoint_type=EntrypointType.MODULE_PATH,
            concurrency_limit=1,
        ),
        sync_klines_1m.to_deployment(
            name=f"{ENV}-sync-klines-1m",
            tags=[ENV],
            description="同步交易所 Kline[1m]",
            schedule=CronSchedule(cron="1 * * * *") if IS_PROD else None,
            entrypoint_type=EntrypointType.MODULE_PATH,
            concurrency_limit=1,
        ),
    ]

    deploy(
        *deployments,
        image=IMAGE_URL,
        build=False,
        work_pool_name=POOL_MAP[ENV],
    )
