"""Core abstractions for the altdata framework."""

from altdata.core.base_source import BaseSource, FetchResult
from altdata.core.http_client import HttpClient, MaxRetriesExceeded
from altdata.core.job_runner import JobRunner, RunSummary
from altdata.core.proxy import NullProxyProvider, OxylabsProxyProvider, ProxyProvider
from altdata.core.raw_store import DiskRawStore, RawStore, get_raw_store

__all__ = [
    "BaseSource",
    "FetchResult",
    "HttpClient",
    "MaxRetriesExceeded",
    "JobRunner",
    "RunSummary",
    "NullProxyProvider",
    "OxylabsProxyProvider",
    "ProxyProvider",
    "DiskRawStore",
    "RawStore",
    "get_raw_store",
]
