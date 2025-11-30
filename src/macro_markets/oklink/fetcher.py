import asyncio
from collections import defaultdict
from datetime import UTC, datetime
from typing import Literal
from uuid import uuid4

from databases.doris import get_stream_loader
from databases.mysql.models import ExchangeInfo
from utils.http_session import get_session

from .decrypt_post import decrypt_oklink_response
from .generate_apikey import get_api_key

ENTITY_RULES = {
    "CEX_BINANCE": ["Binance"],
    "CEX_GATE": ["Gate.io"],
    "CEX_USER": ["User"],
    "CEX_OKX": ["OKX"],
    "CEX_KUCOIN": ["KuCoin"],
    "CEX_BITGET": ["Bitget"],
    "CEX_KRAKEN": ["Kraken"],
    "CEX_BYBIT": ["Bybit"],
    "CEX_COINBASE": ["Coinbase"],
    "CEX_BITFINEX": ["Bitfinex"],
    "CEX_BITSTAMP": ["Bitstamp"],
    "CEX_HTX": ["HTX", "Huobi"],
    "LENDING": ["Morpho", "Lending", "Aave"],
    "DEX_LP": ["Uniswap", "LP(", "Liquidity Pool"],
    "DEFI": ["Balancer", "Curve", "DeFi"],
}


def classify_entity(entity_text):
    if not entity_text:
        return None
    text = entity_text.lower()

    for label, keywords in ENTITY_RULES.items():
        for kw in keywords:
            if kw.lower() in text:
                return label

    return None


class OklinkOnchainInfo:
    def __init__(self):
        self.api_key = None
        self.ts = None
        self.session = None
        self.device_id = str(uuid4())

    async def _get_session(self):
        if self.session is None:
            self.session = await get_session()
        return self.session

    async def send_request(
        self, method: Literal["GET", "POST"], url: str, body: dict | None = None, decrypt: bool = False
    ):
        if not self.api_key:
            self.api_key, self.ts = get_api_key()

        headers = {
            "accept": "application/json",
            "accept-language": "zh-CN,zh;q=0.9",
            "app-type": "web",
            "devid": self.device_id,
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
            "x-apikey": self.api_key,
        }

        params = {
            "t": self.ts,
        }

        session = await self._get_session()

        response = await session.request(
            method,
            url,
            params=params,
            headers=headers,
            json=body,
        )

        data = await response.json()
        if decrypt:
            data = decrypt_oklink_response(data, self.ts)
        return data

    async def get_inflow(self, exchange: ExchangeInfo):
        url = f"https://www.oklink.com/api/explorer/v2/por/{exchange.name}/inflowHistory"
        data = await self.send_request("POST", url, body={"unit": "hour"})
        if data and data.get("code") == 0:
            result = []
            for i in data.get("data", []):
                result.append(
                    {
                        "ts": i["timestamp"],
                        "exchange_id": exchange.id,
                        "dt": datetime.fromtimestamp(i["timestamp"] / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S"),
                        "netflow": i["totalValue"],
                    }
                )
            return result
        else:
            self.logger.error(f"Failed to get inflow history for [{url}]: {data}")
        return None

    @staticmethod
    def extract_address_entity_map(api_result):
        """
        输入 OKLink 返回的完整 JSON
        输出 dict[address] = entityTag(或best guess)
        """
        result = {}

        data = api_result.get("data", {})
        for _, addr_map in data.items():
            for addr, info in addr_map.items():
                tag = info.get("entityTag")

                if not tag:
                    tag = info.get("hoverEntityTag")

                if not tag:
                    tag = info.get("tokenTag")

                if not tag:
                    ent_list = info.get("entityTags")
                    if isinstance(ent_list, list) and len(ent_list) > 0:
                        tag = ent_list[0]

                if not tag:
                    continue

                result[addr] = tag

        return result

    async def large_tranfer_monitor(self):
        txs = await self.send_request(
            "POST",
            "https://www.oklink.com/api/explorer/v2/chain-data-broadcast/data/v2",
            body={
                "offset": 0,
                "chainList": ["BTC", "ETH", "POLYGON", "X1", "BSC", "ARBITRUM", "OPTIMISM"],
                "minUsdValue": 5_000_000,
                "limit": 50,
                "needBigField": True,
            },
        )

        addresses = defaultdict(set)
        for tx in txs["data"]["hits"]:
            addresses[tx["chain"]].add(tx["fromAddress"])
            addresses[tx["chain"]].add(tx["toAddress"])
        tags = await self.send_request(
            "POST",
            "https://www.oklink.com/api/explorer/v2/all/address-tags/support",
            body={
                "addressTagMoreListDto": [
                    {
                        "chain": chain,
                        "address": list(adds),
                        "type": ["entity", "ens", "dns", "property", "risk", "domain", "contract", "program", "token"],
                    }
                    for chain, adds in addresses.items()
                ],
            },
            decrypt=True,
        )
        tags = self.extract_address_entity_map(tags)

        result = []
        for tx in txs["data"]["hits"]:
            from_tag = tags.get(tx["fromAddress"])
            to_tag = tags.get(tx["toAddress"])

            result.append(
                {
                    "chain": tx["chain"],
                    "ts": tx["timestamp"],
                    "dt": datetime.fromtimestamp(tx["timestamp"] / 1000, tz=UTC).strftime("%Y-%m-%d %H:%M:%S"),
                    "tx_hash": tx["txHash"],
                    "from_address": tx["fromAddress"],
                    "from_tag": from_tag,
                    "to_address": tx["toAddress"],
                    "to_tag": to_tag,
                    "token": tx.get("tokenSymbol"),
                    "token_contract": tx.get("tokenContractAddress"),
                    "value": tx.get("value"),
                    "price": tx.get("price"),
                    "value_usd": tx.get("valueUsd"),
                }
            )
        return result


async def main():
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    from databases.mysql import sync_engine
    from databases.mysql.models import ExchangeInfo

    exchanges = ["binance", "okx", "bybit", "bitget", "kraken"]

    with Session(sync_engine) as conn:
        results = select(ExchangeInfo).where(ExchangeInfo.name.in_(exchanges))
        exchange_info = conn.execute(results).scalars().all()

    stream_loader = get_stream_loader()
    oklink_onchain_info = OklinkOnchainInfo()
    # for exchange in exchange_info:
    #     result = await oklink_onchain_info.get_inflow(exchange)
    #     await stream_loader.send_rows(result, "cex_inflow_hourly")

    result = await oklink_onchain_info.large_tranfer_monitor()
    await stream_loader.send_rows(result, "large_transfer")


if __name__ == "__main__":
    asyncio.run(main())
