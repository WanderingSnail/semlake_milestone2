"""S3 request prices and the analytical latency model used by the harness."""

from __future__ import annotations

# AWS S3 Standard, us-east-1, USD per request.
# PUT/POST/LIST are $0.005 per 1,000; GET is $0.0004 per 1,000.
PRICE_PUT_USD = 0.005 / 1000
PRICE_GET_USD = 0.0004 / 1000
PRICE_LIST_USD = 0.005 / 1000

# Per-request round-trip times added to the measured CPU time. The harness
# never sleeps; these numbers only feed estimated_latency_s. They are rough
# S3 first-byte latencies.
RTT_PUT_S = 0.100
RTT_GET_S = 0.030
RTT_LIST_S = 0.050
BANDWIDTH_BPS = 100 * 1024 * 1024


def cost_usd(counters) -> float:
    return (
        counters.put * PRICE_PUT_USD
        + counters.get * PRICE_GET_USD
        + counters.list * PRICE_LIST_USD
    )


def estimated_latency_s(cpu_s: float, counters) -> float:
    return (
        cpu_s
        + counters.put * RTT_PUT_S
        + counters.get * RTT_GET_S
        + counters.list * RTT_LIST_S
        + counters.put_bytes / BANDWIDTH_BPS
    )


def pricing_doc() -> dict:
    return {
        "price_put_usd": PRICE_PUT_USD,
        "price_get_usd": PRICE_GET_USD,
        "price_list_usd": PRICE_LIST_USD,
        "rtt_put_s": RTT_PUT_S,
        "rtt_get_s": RTT_GET_S,
        "rtt_list_s": RTT_LIST_S,
        "bandwidth_bps": BANDWIDTH_BPS,
        "latency_formula": "cpu + put*rtt_put + get*rtt_get + list*rtt_list + put_bytes/bandwidth",
    }
