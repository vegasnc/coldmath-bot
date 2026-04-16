"""
core/metrics.py

Tracks daily trading metrics per domain.
Fed into the rotation engine at end of day — this is the
feedback loop that enables automatic domain switching.
"""

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from core.rotation_engine import Domain

log = logging.getLogger("metrics")


@dataclass
class DomainDailyMetrics:
    tx_count:      int   = 0
    usdc_deployed: float = 0.0
    usdc_returned: float = 0.0
    opps_found:    int   = 0
    opps_taken:    int   = 0

    @property
    def net_margin_per_100(self) -> float:
        if self.usdc_deployed == 0:
            return 0.0
        return ((self.usdc_returned - self.usdc_deployed)
                / self.usdc_deployed * 100)


class MetricsTracker:
    """Accumulates intraday metrics, reports to engine at EOD."""

    def __init__(self):
        self._data: dict[Domain, DomainDailyMetrics] = defaultdict(
            DomainDailyMetrics
        )

    def record_scan(self, domain: Domain, found: int, taken: int):
        self._data[domain].opps_found += found
        self._data[domain].opps_taken += taken

    def record_trade(self, domain: Domain, deployed: float, no_size: float = 0, yes_size: float = 0, market: str = ""):
        self._data[domain].tx_count      += 1
        self._data[domain].usdc_deployed += deployed

    def record_resolution(self, domain: Domain, returned: float):
        """Call when a position resolves and USDC comes back."""
        self._data[domain].usdc_returned += returned

    def daily_summary(self) -> dict[Domain, dict]:
        """
        Returns metrics in the format expected by DomainRotationEngine.daily_update()
        """
        summary = {}
        for domain, m in self._data.items():
            summary[domain] = {
                "session_volume":    m.tx_count,
                "net_margin":        m.net_margin_per_100,
                "opportunity_count": m.opps_found,
            }
            log.info(
                f"Daily metrics | {domain.value}: "
                f"txs={m.tx_count} "
                f"deployed=${m.usdc_deployed:.0f} "
                f"returned=${m.usdc_returned:.0f} "
                f"margin={m.net_margin_per_100:+.2f}¢/$100 "
                f"opps={m.opps_found}/{m.opps_taken}"
            )
        return summary

    def reset(self):
        self._data.clear()
