from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import parse_qs, unquote, urlparse


@dataclass(frozen=True)
class VlessLink:
    address: str
    port: int
    user_id: str
    encryption: str
    security: str
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
            f"security={self.security} "
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


def parse_vless_url(raw_url: str) -> VlessLink:
    vless_url = raw_url.strip()
    parsed = urlparse(vless_url)
    if parsed.scheme != "vless":
        raise ValueError("VLESS URL must start with vless://")

    user_id = unquote(parsed.username or "").strip()
    if not user_id:
        raise ValueError("VLESS url must include user id")

    if not parsed.hostname:
        raise ValueError("VLESS url must include server hostname")

    if parsed.port is None:
        raise ValueError("VLESS url must include server port")

    query = parse_qs(parsed.query, keep_blank_values=True)
    encryption = _last_query_value(query, "encryption") or "none"
    security = _last_query_value(query, "security").lower()
    network = (_last_query_value(query, "type", "network").lower() or "tcp")

    if security not in {"none", "reality"}:
        raise ValueError(f"Unsupported VLESS transport security: {security}")

    if network not in {"tcp", "raw", "kcp"}:
        raise ValueError(f"Unsupported VLESS network: {network}")

    server_name = _last_query_value(query, "sni", "serverName") or parsed.hostname
    public_key = _last_query_value(query, "pbk", "publicKey")
    short_id = _last_query_value(query, "sid", "shortId")
    fingerprint = _last_query_value(query, "fp", "fingerprint") or "chrome"
    flow = _last_query_value(query, "flow") or None
    spider_x = unquote(_last_query_value(query, "spx", "spiderX") or "/")

    if security == "reality" and not public_key:
        raise ValueError("Reality link must include pbk/publicKey")

    return VlessLink(
        address=parsed.hostname,
        port=parsed.port,
        user_id=user_id,
        encryption=encryption,
        security=security,
        server_name=server_name,
        public_key=public_key,
        short_id=short_id,
        fingerprint=fingerprint,
        flow=flow,
        spider_x=spider_x,
        network=network,
    )


def normalize_stream_network(network: str) -> str:
    if network in {"tcp", "raw"}:
        return "tcp"
    return network


def build_config(
    raw_url: str,
    listen_host: str = "0.0.0.0",
    listen_port: int = 1080,
    log_level: str = "warning",
) -> tuple[dict, VlessLink]:
    link = parse_vless_url(raw_url)

    user: dict[str, str] = {
        "id": link.user_id,
        "encryption": link.encryption,
    }
    if link.flow:
        user["flow"] = link.flow

    normalized_network = normalize_stream_network(link.network)
    stream_settings: dict = {
        "network": normalized_network,
        "security": link.security,
    }
    if normalized_network == "tcp":
        stream_settings["sockopt"] = {
            "domainStrategy": "UseIP",
        }
        stream_settings["tcpSettings"] = {
            "header": {
                "type": "none",
            }
        }
    if normalized_network == "kcp":
        stream_settings["kcpSettings"] = {}
    if link.security == "reality":
        stream_settings["realitySettings"] = {
            "show": False,
            "fingerprint": link.fingerprint,
            "serverName": link.server_name,
            "publicKey": link.public_key,
            "shortId": link.short_id,
            "spiderX": link.spider_x,
        }

    config = {
        "log": {
            "loglevel": log_level,
        },
        "inbounds": [
            {
                "tag": "bot-socks",
                "listen": listen_host,
                "port": listen_port,
                "protocol": "socks",
                "settings": {
                    "auth": "noauth",
                    "udp": False,
                },
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
                "streamSettings": stream_settings,
            },
            {
                "tag": "direct",
                "protocol": "freedom",
            },
            {
                "tag": "blocked",
                "protocol": "blackhole",
            },
        ],
        "routing": {
            "domainStrategy": "IPIfNonMatch",
            "rules": [],
        },
    }
    return config, link


def main() -> int:
    import os
    import sys

    config, _ = build_config(
        raw_url=os.environ["BOT_VLESS_URL"],
        listen_host=os.getenv("XRAY_LOCAL_PROXY_HOST", "0.0.0.0"),
        listen_port=int(os.getenv("XRAY_LOCAL_PROXY_PORT", "1080")),
        log_level=os.getenv("XRAY_LOG_LEVEL", "warning").strip() or "warning",
    )
    json.dump(config, sys.stdout, ensure_ascii=True, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
