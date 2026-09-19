"""Local stand-in for the pinned Hermes agent/memory_provider.py contract.

Used only to run tests/test_memory.py::UpstreamContractTests on a dev box; the
authoritative check still runs against the real pinned upstream file host-side.
"""
from collections import namedtuple


class MemoryProvider:
    pass


RecallStatus = namedtuple("RecallStatus", ("provider_label", "count"))
