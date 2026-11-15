from __future__ import annotations

import os
from urllib.parse import urlsplit


def validate_proxy(proxy_url: str | None) -> None:
    if proxy_url is None:
        return
    if not isinstance(proxy_url, str):
        raise ValueError("network.proxy_url must be an HTTP proxy URL")
    try:
        parsed = urlsplit(proxy_url)
        valid = (parsed.scheme in ("http", "https") and parsed.hostname and parsed.port
                 and not parsed.username and not parsed.password and not parsed.query
                 and not parsed.fragment and parsed.path in ("", "/"))
    except ValueError:
        valid = False
    if not valid:
        raise ValueError("network.proxy_url must be an HTTP(S) host:port without credentials or URL parameters")


def configure_proxy(proxy_url: str | None) -> None:
    if proxy_url is None:
        return
    validate_proxy(proxy_url)
    # Covers SDK async HTTP, synchronous nonce reads, native HTTP and notifications.
    # Only the current process and its children inherit these values.
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "WS_PROXY", "WSS_PROXY"):
        os.environ[key] = proxy_url
        os.environ[key.lower()] = proxy_url
    for key in ("NO_PROXY", "no_proxy"):
        os.environ[key] = "localhost,127.0.0.1,::1"
