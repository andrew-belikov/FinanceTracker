import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from xray_client.entrypoint import iter_vless_candidates
from xray_client.healthcheck import build_proxy_check_command
from xray_client.render_config import build_config


TEST_VLESS_URL = (
    "vless://8ba56e85-f6c5-45c1-af4a-988073947c45@144.124.228.63:443"
    "?encryption=none&flow=xtls-rprx-vision&security=reality&sni=www.apple.com"
    "&fp=chrome&pbk=TKApDiSXjLexhtMTrKNOlsdDOI96HPLjDnGpzMw8Ux4"
    "&sid=3959d570c542874d&type=tcp#homeserver"
)
TEST_KCP_VLESS_URL = (
    "vless://b5d2fa88-211b-411d-a671-4b1ac45538e3@77.246.106.198:8443"
    "?encryption=mlkem768x25519plus.native.0rtt.3VgZ4Sao-er_QQpT8rWtzH2QIsFado-wjv7_LL6Eymk"
    "&security=none&type=kcp#homeserver-financetracker-udp"
)


class XrayProxyConfigTests(unittest.TestCase):
    def test_build_config_renders_socks_inbound_and_reality_transport_options(self):
        config, _link = build_config(TEST_VLESS_URL, listen_port=1080)

        inbound = config["inbounds"][0]
        self.assertEqual(inbound["tag"], "bot-socks")
        self.assertEqual(inbound["port"], 1080)
        self.assertEqual(inbound["protocol"], "socks")
        self.assertEqual(
            inbound["settings"],
            {
                "auth": "noauth",
                "udp": False,
            },
        )

        proxy_outbound = next(item for item in config["outbounds"] if item["tag"] == "proxy")
        self.assertEqual(proxy_outbound["streamSettings"]["network"], "tcp")
        self.assertEqual(proxy_outbound["streamSettings"]["sockopt"]["domainStrategy"], "UseIP")
        self.assertEqual(proxy_outbound["streamSettings"]["tcpSettings"]["header"]["type"], "none")
        self.assertEqual(config["routing"]["domainStrategy"], "IPIfNonMatch")
        self.assertEqual(config["routing"]["rules"], [])

    def test_healthcheck_command_uses_socks5_hostname_mode(self):
        command = build_proxy_check_command(
            1080,
            "https://api.ipify.org",
            proxy_scheme="socks5h",
        )

        self.assertEqual(
            command,
            [
                "curl",
                "--max-time",
                "10",
                "--socks5-hostname",
                "127.0.0.1:1080",
                "--fail",
                "--silent",
                "--show-error",
                "--output",
                "/dev/null",
                "https://api.ipify.org",
            ],
        )

    def test_healthcheck_command_rejects_unsupported_proxy_scheme(self):
        with self.assertRaises(ValueError):
            build_proxy_check_command(1080, "https://api.ipify.org", proxy_scheme="http")

    def test_build_config_supports_vless_kcp_fallback_links(self):
        config, _link = build_config(TEST_KCP_VLESS_URL, listen_port=1080)

        proxy_outbound = next(item for item in config["outbounds"] if item["tag"] == "proxy")
        user = proxy_outbound["settings"]["vnext"][0]["users"][0]
        stream_settings = proxy_outbound["streamSettings"]

        self.assertEqual(
            user["encryption"],
            "mlkem768x25519plus.native.0rtt.3VgZ4Sao-er_QQpT8rWtzH2QIsFado-wjv7_LL6Eymk",
        )
        self.assertEqual(stream_settings["network"], "kcp")
        self.assertEqual(stream_settings["security"], "none")
        self.assertIn("kcpSettings", stream_settings)
        self.assertNotIn("realitySettings", stream_settings)
        self.assertNotIn("tcpSettings", stream_settings)

    def test_iter_vless_candidates_prefers_primary_then_fallback(self):
        self.assertEqual(
            iter_vless_candidates("vless://primary", "vless://fallback"),
            [("primary", "vless://primary"), ("fallback", "vless://fallback")],
        )

    def test_iter_vless_candidates_uses_fallback_when_primary_missing(self):
        self.assertEqual(
            iter_vless_candidates("", "vless://fallback"),
            [("fallback", "vless://fallback")],
        )

    def test_iter_vless_candidates_deduplicates_identical_urls(self):
        self.assertEqual(
            iter_vless_candidates("vless://shared", "vless://shared"),
            [("primary", "vless://shared")],
        )


if __name__ == "__main__":
    unittest.main()
