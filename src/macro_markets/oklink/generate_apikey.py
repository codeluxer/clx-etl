import base64
import random
import time

S = 1111111111111
ORIGINAL_API_KEY = "a2c903cc-b31e-4547-9299-b6d07b7631ab"


def encrypt_api_key(api_key: str) -> str:
    arr = list(api_key)
    prefix = arr[:8]
    rest = arr[8:]
    return "".join(rest + prefix)


def encrypt_time(ts: int) -> str:
    base = list(str(ts + S))
    extra = [str(random.randint(0, 9)) for _ in range(3)]
    return "".join(base + extra)


def comb(api_key_enc: str, ts_enc: str) -> str:
    raw = f"{api_key_enc}|{ts_enc}"
    return base64.b64encode(raw.encode()).decode()


def get_api_key() -> str:
    ts = int(time.time() * 1000)
    key_enc = encrypt_api_key(ORIGINAL_API_KEY)
    ts_enc = encrypt_time(ts)
    return comb(key_enc, ts_enc), ts
