"""Proxy provider abstractions for routing HTTP requests via Oxylabs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from altdata.settings import Settings


class ProxyProvider(ABC):
    """Abstract proxy provider interface."""

    @abstractmethod
    def get_proxy_url(self) -> str:
        """Return the proxy URL string.

        Returns:
            A fully-qualified proxy URL including credentials if required.
        """

    @abstractmethod
    def httpx_proxies(self) -> dict[str, Any]:
        """Return a proxies dict suitable for ``httpx.AsyncClient(proxies=...)``.

        Returns:
            Dict mapping URL schemes to proxy URLs.
        """


class OxylabsProxyProvider(ProxyProvider):
    """Proxy provider backed by Oxylabs residential proxies.

    Reads credentials from the application settings and constructs the
    proxy URL on every call, allowing for credential rotation at the
    settings layer if needed.

    Args:
        settings: Application settings instance containing Oxylabs credentials.
    """

    def __init__(self, settings: Settings) -> None:
        self._username = settings.oxylabs_username
        self._password = settings.oxylabs_password
        self._endpoint = settings.oxylabs_endpoint

    def get_proxy_url(self) -> str:
        """Return the Oxylabs HTTPS proxy URL with embedded credentials.

        Returns:
            Proxy URL in the form ``https://user:pass@host:port``.
        """
        return f"https://{self._username}:{self._password}@{self._endpoint}"

    def httpx_proxies(self) -> dict[str, Any]:
        """Return proxy configuration dict for httpx.

        Returns:
            Dict with HTTP and HTTPS entries pointing to the Oxylabs endpoint.
        """
        proxy_url = self.get_proxy_url()
        return {
            "http://": proxy_url,
            "https://": proxy_url,
        }


class NullProxyProvider(ProxyProvider):
    """No-op proxy provider for sources that do not require proxying."""

    def get_proxy_url(self) -> str:
        """Return an empty string (no proxy).

        Returns:
            Empty string.
        """
        return ""

    def httpx_proxies(self) -> dict[str, Any]:
        """Return an empty dict (no proxy configuration).

        Returns:
            Empty dict.
        """
        return {}
