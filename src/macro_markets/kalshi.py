import asyncio
from datetime import datetime
import time
from typing import Literal

from databases.doris import get_doris, get_stream_loader
from utils.http_session import get_session

OI_THRESHOLDS = {
    # ===== Fed / Rates =====
    "KXFEDDECISION": 50000,
    "KXFEDHIKE": 30000,
    "KXRATEHIKE": 25000,
    "KXRATECUT": 30000,
    "KXRATECUTS": 20000,
    "KXTERMINALRATE": 15000,
    "KXDOTPLOT": 12000,
    "KXFEDFACILITY": 8000,
    "KXZERORATE": 8000,
    # ===== CPI / Inflation =====
    "KXCPI": 20000,
    "KXCPICOREYOY": 15000,
    # ===== Employment =====
    "KXPAYROLLS": 15000,
    "KXNFPDELAY": 8000,
    # ===== Recession / GDP =====
    "KXNBERRECESSQ": 10000,
    "KXRECSSNBER": 12000,
    "KXGDP": 10000,
    # ===== Liquidity / FX / Macro =====
    "KXDOLLARFED": 10000,
    "KXUSDEBT": 10000,
    "KXWTI": 9000,
    "KXUSDJPYEU": 7000,
    "KXEURUSD": 7000,
    "KXGBPUSD": 7000,
    "KXCNY": 5000,
    "KXCHINAUSGDP": 5000,
    # ===== Fed Communication =====
    "KXFEDCOMBO": 8000,
    "KXFEDGOVMENTION": 5000,
    "KXFEDEMPLOYEES": 4000,
    "KXFEDMEET": 4000,
    "KXECB": 6000,
    # ===== Crypto-native (降为非常低 避免全被过滤) =====
    "KXBTC": 1000,
    "KXBTCE": 1000,
    "KXBTCMAX100": 1000,
    "KXBTCMAXW": 1000,
    "KXBTC2025100": 1000,
    "KXBTC2026200": 1000,
    "KXBTCD": 1000,
    "KXBTCRESERVE": 1000,
}

STATUS_MAP = {
    "initialized": 0,
    "active": 1,
    "inactive": 2,
    "closed": 3,
    "finalized": 4,
}

HEADERS = {"Accept": "application/json", "User-Agent": "CoinLuxer-PM-ETL/1.0"}


class KalshiClient:
    def __init__(self, logger=None):
        self.logger = logger
        self._session = None

    async def get_session(self):
        if self._session is None:
            self._session = await get_session()
        return self._session

    @staticmethod
    def normalize_prob(market: dict):
        """
        Input: market details
        Output: (yes_prob, no_prob) in [0,1]
        """
        yes_bid = market.get("yes_bid")
        no_bid = market.get("no_bid")

        if yes_bid is None or no_bid is None:
            return None, None

        yes = yes_bid / 100
        no = no_bid / 100

        s = yes + no
        if s == 0:
            return None, None

        yes_norm = yes / s
        no_norm = no / s
        return yes_norm, no_norm

    async def send_request(self, method: Literal["GET", "POST"], url: str, body: dict | None = None):
        session = await self.get_session()
        resp = await session.request(
            method,
            url,
            headers=HEADERS,
            json=body,
        )
        return await resp.json()

    async def fetch_series_list(self):
        series = await self.send_request("GET", "https://api.elections.kalshi.com/trade-api/v2/series")
        return [s for s in series["series"] if s.get("ticker") in OI_THRESHOLDS]

    async def fetch_markets_by_series(self, series_ticker):
        cursor = ""
        result = []
        for _ in range(20):
            resp = await self.send_request(
                "GET",
                f"https://api.elections.kalshi.com/trade-api/v2/markets?series_ticker={series_ticker}&cursor={cursor}",
            )
            markets = resp.get("markets", [])
            if not markets:
                break
            for market in markets:
                if market.get("volume"):
                    result.append(
                        {
                            "updated_ts": int(time.time() * 1000),
                            "event_ticker": market["event_ticker"],
                            "ticker": market["ticker"],
                            "status": STATUS_MAP.get(market.get("status")),
                            "last_price": market.get("last_price"),
                            "yes_bid": market.get("yes_bid"),
                            "yes_ask": market.get("yes_ask"),
                            "no_bid": market.get("no_bid"),
                            "no_ask": market.get("no_ask"),
                            "liquidity": market.get("liquidity"),
                            "volume": market.get("volume"),
                            "open_interest": market.get("open_interest"),
                            "custom_strike": market.get("custom_strike"),
                            "rules_primary": market.get("rules_primary"),
                            "close_time": market.get("close_time"),
                            "expiration_time": market.get("expiration_time"),
                        }
                    )
            if len(result) > 100:
                break
            cursor = resp.get("cursor", "")
            if not cursor:
                break
        return result

    async def sync_market_meta(self):
        doris = get_doris()
        stream_loader = get_stream_loader()
        data = await doris.query("SELECT ticker FROM kalshi_market_meta WHERE status = 4;")
        tickers = {i[0] for i in data}

        series = await self.fetch_series_list()
        markets = []
        for i in series:
            markets.extend(await self.fetch_markets_by_series(i["ticker"]))
        non_finalized_markets = [i for i in markets if i["ticker"] not in tickers]

        await stream_loader.send_rows(
            non_finalized_markets,
            "kalshi_market_meta",
        )

        snapshot = []
        for i in markets:
            threshold = 300
            for k, v in OI_THRESHOLDS.items():
                if i.get("event_ticker").startswith(f"{k}-"):
                    threshold = v
                    break
            if i.get("status") == 1 and i.get("open_interest", 0) > threshold:
                snapshot.append(
                    {
                        "ts": i["updated_ts"],
                        "event_ticker": i["event_ticker"],
                        "ticker": i["ticker"],
                        "dt": datetime.fromtimestamp(i["updated_ts"] / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                        "last_price": i.get("last_price"),
                        "yes_bid": i.get("yes_bid"),
                        "yes_ask": i.get("yes_ask"),
                        "no_bid": i.get("no_bid"),
                        "no_ask": i.get("no_ask"),
                        "liquidity": i.get("liquidity"),
                        "volume": i.get("volume"),
                        "open_interest": i.get("open_interest"),
                    }
                )
        await stream_loader.send_rows(snapshot, "kalshi_market_snapshot")


if __name__ == "__main__":

    async def main():
        client = KalshiClient()
        await client.sync_market_meta()

    asyncio.run(main())
