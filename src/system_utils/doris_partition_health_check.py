import os

from dotenv import load_dotenv
from prefect import flow
import pymysql

load_dotenv()

# =====================
# Doris connection
# =====================
DORIS_HOST = os.getenv("DORIS_HOST")
DORIS_PORT = os.getenv("DORIS_PORT")
DORIS_USER = os.getenv("DORIS_USER")
DORIS_PASSWORD = os.getenv("DORIS_PASSWORD")
DATABASE = os.getenv("DATABASE")

# =====================
# Error keywords that indicate corruption
# =====================
CORRUPTION_KEYWORDS = [
    "tablet",
    "segment",
    "checksum",
    "file not exist",
    "io error",
    "meta not found",
    "fail to find path in version_graph",
]


def is_partitioned_table(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(f"SHOW CREATE TABLE {DATABASE}.{table}")
        ddl = cur.fetchone()[1]
        return "PARTITION BY" in ddl.upper()


def is_corruption_error(err: str) -> bool:
    err = err.lower()
    return any(k in err for k in CORRUPTION_KEYWORDS)


def get_tables(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(f"SHOW TABLES FROM {DATABASE}")
        return [row[0] for row in cur.fetchall()]


def get_partitions(conn, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(f"SHOW PARTITIONS FROM {DATABASE}.{table}")
        return [row[1] for row in cur.fetchall()]  # PartitionName


def check_partition(conn, table: str, partition: str) -> bool:
    sql = f"SELECT 1 FROM {DATABASE}.{table} PARTITION({partition}) LIMIT 1"
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        return True
    except Exception as e:
        msg = str(e)
        print(f"❌ Query failed: {table}.{partition}")
        print(f"   Error: {msg}")
        return not is_corruption_error(msg)


def drop_partition(conn, table: str, partition: str):
    if not is_partitioned_table(conn, table):
        print(f"⏭️  Skip drop (not partitioned): {table}")
        return

    sql = f"ALTER TABLE {DATABASE}.{table} DROP PARTITION {partition} FORCE;"
    with conn.cursor() as cur:
        cur.execute(sql)
    print(f"🔥 DROPPED {table}.{partition}")


@flow
def doris_partition_health_check(drop: bool):
    conn = pymysql.connect(
        host=DORIS_HOST,
        port=DORIS_PORT,
        user=DORIS_USER,
        password=DORIS_PASSWORD,
        autocommit=True,
    )

    bad_partitions = []

    tables = get_tables(conn)
    print(f"🔍 Found {len(tables)} tables")

    for table in tables:
        print(f"\n📦 Checking table: {table}")
        try:
            partitions = get_partitions(conn, table)
        except Exception as e:
            print(f"❌ Cannot list partitions for {table}: {e}")
            continue

        for p in partitions:
            ok = check_partition(conn, table, p)
            if not ok:
                bad_partitions.append((table, p))

    print("\n========================")
    print("❗ Corrupted partitions")
    print("========================")

    for t, p in bad_partitions:
        print(f" - {t}.{p}")

    if drop and bad_partitions:
        print("\n🔥 Dropping corrupted partitions...")
        for t, p in bad_partitions:
            drop_partition(conn, t, p)

    if not bad_partitions:
        print("✅ No corrupted partitions found")

    conn.close()
