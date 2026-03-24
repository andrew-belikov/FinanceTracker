from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlparse


@dataclass(frozen=True)
class VlessRealityLink:
    address: str
    port: int
    user_id: str
    server_name: str
    public_key: str
    short_id: str
    fingerprint: str
    flow: str | None
    spider_x: str
    network: str

    def masked_summary(self) -> str:
        flow = self.flow or "none"
        return (
            f"remote={self.address}:{self.port} "
            f"server_name={self.server_name} "
            f"network={self.network} "
            f"fingerprint={self.fingerprint} "
            f"flow={flow}"
        )


def _last_query_value(query: dict[str, list[str]], *names: str) -> str:
    for name in names:
        values = query.get(name)
        if values:
            return values[-1].strip()
    return ""


def parse_vless_reality_url(raw_url: str) -> VlessRealityLink:
    vless_url = raw_url.strip()
    parsed = urlparse(vless_url)
    if parsed.scheme != "vless":
        raise ValueError("BOT_VLESS_URL must start with vless://")

    user_id = unquote(parsed.username or "").strip()
    if not user_id:
        raise ValueError("VLESS url must include user id")

    if not parsed.hostname:
        raise ValueError("VLESS url must include server hostname")

    if parsed.port is None:
        raise ValueError("VLESS url must include server port")

    query = parse_qs(parsed.query, keep_blank_values=True)
    encryption = _last_query_value(query, "encryption").lower() or "none"
    security = _last_query_value(query, "security").lower()
    network = (_last_query_value(query, "type", "network").lower() or "tcp")

    if encryption != "none":
        raise ValueError("Only VLESS links with encryption=none are supported")

    if security != "reality":
        raise ValueError("Only VLESS + Reality links are supported")

    if network not in {"tcp", "raw"}:
        raise ValueError(f"Unsupported VLESS network: {network}")

    server_name = _last_query_value(query, "sni", "serverName") or parsed.hostname
    public_key = _last_query_value(query, "pbk", "publicKey")
    short_id = _last_query_value(query, "sid", "shortId")
    fingerprint = _last_query_value(query, "fp", "fingerprint") or "chrome"
    flow = _last_query_value(query, "flow") or None
    spider_x = unquote(_last_query_value(query, "spx", "spiderX") or "/")

    if not public_key:
        raise ValueError("Reality link must include pbk/publicKey")

    return VlessRealityLink(
        address=parsed.hostname,
        port=parsed.port,
        user_id=user_id,
        server_name=server_name,
        public_key=public_key,
        short_id=short_id,
        fingerprint=fingerprint,
        flow=flow,
        spider_x=spider_x,
        network=network,
    )


def build_config(raw_url: str, listen_host: str = "0.0.0.0", listen_port: int = 3128) -> tuple[dict, VlessRealityLink]:
    link = parse_vless_reality_url(raw_url)

    user: dict[str, str] = {
        "id": link.user_id,
        "encryption": "none",
    }
    if link.flow:
        user["flow"] = link.flow

    config = {
        "log": {
            "loglevel": "info",
        },
        "inbounds": [
            {
                "tag": "bot-http-proxy",
                "listen": listen_host,
                "port": listen_port,
                "protocol": "http",
                "settings": {},
            }
        ],
        "outbounds": [
            {
                "tag": "proxy",
                "protocol": "vless",
                "settings": {
                    "vnext": [
                        {
                            "address": link.address,
                            "port": link.port,
                            "users": [user],
                        }
                    ]
                },
                "streamSettings": {
                    "network": link.network,
                    "security": "reality",
                    "realitySettings": {
                        "show": False,
                        "fingerprint": link.fingerprint,
                        "serverName": link.server_name,
                        "publicKey": link.public_key,
                        "shortId": link.short_id,
                        "spiderX": link.spider_x,
                    },
                },
            },
            {
                "tag": "direct",
                "protocol": "freedom",
            },
        ],
        "routing": {
            "domainStrategy": "AsIs",
            "rules": [
                {
                    "type": "field",
                    "inboundTag": ["bot-http-proxy"],
                    "outboundTag": "proxy",
                }
            ],
        },
    }
    return config, link


def main() -> int:
    import os
    import sys

    config, _ = build_config(
        raw_url=os.environ["BOT_VLESS_URL"],
        listen_host=os.getenv("XRAY_LOCAL_PROXY_HOST", "0.0.0.0"),
        listen_port=int(os.getenv("XRAY_LOCAL_PROXY_PORT", "3128")),
    )
    json.dump(config, sys.stdout, ensure_ascii=True, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
