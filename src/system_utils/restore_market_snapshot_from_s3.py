from datetime import date, datetime, timedelta
import hashlib
import os
from pathlib import Path
import sqlite3
import tarfile

import boto3

from databases.doris import DorisStreamLoader

# =====================
# Config
# =====================

S3_BUCKET = "coinluxer"
S3_PREFIX = "sqlite/production/2025/12"

WORKDIR = "/tmp/restore_sqlite"
BATCH_SIZE = 5000

# Doris
DORIS = {
    "host": "127.0.0.1",
    "port": 9030,
    "user": "root",
    "password": "",
    "db": "clx_stg",
    "table": "market_snapshot",
}

# =====================
# Helpers
# =====================


def sha256sum(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def download_from_s3(s3, key, local_path):
    print(f"⬇️  Downloading s3://{S3_BUCKET}/{key}")
    s3.download_file(S3_BUCKET, key, local_path)


def extract_tar(tar_path, target_dir):
    print(f"📦 Extracting {tar_path}")
    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(target_dir)


def iter_sqlite_rows(sqlite_path):
    conn = sqlite3.connect(sqlite_path)
    cur = conn.cursor()

    sql = """
    SELECT
      ts, symbol, exchange_id, inst_type, dt,
      mark_price, index_price, last_price,
      funding_rate, next_funding_time,
      open_interest, volume, quote_volume, trades,
      taker_buy_vol, taker_sell_vol,
      taker_buy_notional, taker_sell_notional,
      long_liquidation_volume, long_liquidation_notional, long_liquidation_count,
      short_liquidation_volume, short_liquidation_notional, short_liquidation_count,
      max_long_liquidation_notional, max_short_liquidation_notional,
      min_liquidation_price, max_liquidation_price,
      bid_p100, bid_p99, bid_p98, bid_p95, bid_p90,
      bid_p75, bid_p50, bid_p25, bid_p10, bid_p5, bid_p2, bid_p1, bid_p0,
      ask_p0, ask_p1, ask_p2, ask_p5, ask_p10, ask_p25, ask_p50, ask_p75, ask_p100,
      bid_total_qty, top_10bids_level,
      ask_total_qty, top_10asks_level,
      depth_bid_1bps, depth_bid_3bps, depth_bid_5bps, depth_bid_10bps, depth_bid_20bps,
      depth_ask_1bps, depth_ask_3bps, depth_ask_5bps, depth_ask_10bps, depth_ask_20bps,
      curvature_short_bid, curvature_long_bid,
      curvature_short_ask, curvature_long_ask,
      worker_id, version
    FROM market_snapshot
    """

    for row in cur.execute(sql):
        yield row

    conn.close()


def insert_batch(cur, batch):
    placeholders = ",".join(["%s"] * len(batch[0]))
    sql = f"""
    INSERT INTO {DORIS["table"]} VALUES ({placeholders})
    """
    cur.executemany(sql, batch)


# =====================
# Main restore logic
# =====================


async def restore_from_s3(symbol, exchange_id, inst_type, day, hour, logger):
    """
    day: datetime.date
    hour: int (0-23)
    """
    stream_loader = DorisStreamLoader()

    year = day.year
    month = f"{day.month:02d}"
    day_str = day.strftime("%Y-%m-%d")

    s3_prefix = f"sqlite/production/{year}/{month}"

    tar_name = f"sqlite_{day_str}_aws.tar.gz"
    sha_name = tar_name + ".sha256"

    os.makedirs(WORKDIR, exist_ok=True)

    tar_path = f"{WORKDIR}/{tar_name}"
    sha_path = f"{WORKDIR}/{sha_name}"
    extract_dir = f"{WORKDIR}/{day_str}"

    s3 = boto3.client("s3")

    # 1️⃣ 下载
    if not os.path.exists(tar_path):
        logger.info(f"⬇️  Download {tar_name}")
        s3.download_file(S3_BUCKET, f"{s3_prefix}/{tar_name}", tar_path)
        s3.download_file(S3_BUCKET, f"{s3_prefix}/{sha_name}", sha_path)

        expected = Path(sha_path).read_text().split()[0]
        actual = sha256sum(tar_path)
        if expected != actual:
            raise RuntimeError(f"SHA256 mismatch for {tar_name}")

    # 2️⃣ 解压（只解一次）
    if not os.path.exists(extract_dir):
        logger.info(f"📦 Extract {tar_name}")
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(extract_dir)

    sqlite_files = list(Path(extract_dir).rglob("*.db"))
    if not sqlite_files:
        raise RuntimeError("No sqlite file found")

    sqlite_path = sqlite_files[0]

    # 3️⃣ 计算小时窗口
    hour_start = datetime.combine(day, datetime.min.time()) + timedelta(hours=hour)
    hour_end = hour_start + timedelta(hours=1)

    logger.info(f"🚑 Restoring {symbol} ex={exchange_id} inst={inst_type} {hour_start:%Y-%m-%d %H}:00")

    # 5️⃣ 从 SQLite 读取“只属于这一小时 + 这个 symbol”
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_cur = sqlite_conn.cursor()

    sql = """
    SELECT *
    FROM market_snapshot
    WHERE symbol = ?
      AND exchange_id = ?
      AND inst_type = ?
      AND dt >= ?
      AND dt < ?
    """

    batch = []
    for row in sqlite_cur.execute(
        sql,
        (
            symbol,
            exchange_id,
            inst_type,
            hour_start,
            hour_end,
        ),
    ):
        batch.append(row)

    if batch:
        logger.info("Sending rows..." + str(len(batch)))
        res = await stream_loader.send_rows(
            batch, "market_snapshot", column_names=[i[0] for i in sqlite_cur.description]
        )
        if res["Status"] != "Success":
            raise RuntimeError(f"Failed to restore {symbol} {hour_start:%Y-%m-%d %H}:00")
    sqlite_conn.close()

    logger.info(f"✅ Restore finished for {symbol} {hour_start:%Y-%m-%d %H}:00")


if __name__ == "__main__":
    restore_from_s3("BTCUSDT", "binance", "future", date(2025, 12, 16), 16, DORIS)
