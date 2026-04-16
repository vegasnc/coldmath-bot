"""
core/rotation_engine.py

v4: Cycling domain added, defaults reflect April 5 state.
"""

import json, logging, pathlib
from datetime import datetime, timezone
from dataclasses import dataclass, field
from collections import deque
from enum import Enum
from typing import Optional

log = logging.getLogger("rotation_engine")


class Domain(Enum):
    WEATHER   = "weather"
    SOCCER    = "soccer"
    FINANCIAL = "financial"
    CYCLING   = "cycling"


class DomainStatus(Enum):
    INACTIVE  = "inactive"
    TESTING   = "testing"
    ACTIVE    = "active"
    DECAYING  = "decaying"
    STANDBY   = "standby"


SEASONAL_WEIGHTS = {
    1:  dict(weather=1.00,soccer=0.70,financial=0.50,cycling=0.05),
    2:  dict(weather=1.00,soccer=0.80,financial=0.50,cycling=0.05),
    3:  dict(weather=0.60,soccer=0.90,financial=0.60,cycling=0.20),
    4:  dict(weather=0.30,soccer=1.00,financial=0.70,cycling=0.90),
    5:  dict(weather=0.30,soccer=0.80,financial=0.80,cycling=0.80),
    6:  dict(weather=0.50,soccer=0.30,financial=1.00,cycling=0.40),
    7:  dict(weather=0.60,soccer=0.20,financial=1.00,cycling=0.95),
    8:  dict(weather=0.70,soccer=0.30,financial=0.90,cycling=0.60),
    9:  dict(weather=0.80,soccer=0.70,financial=0.80,cycling=0.30),
    10: dict(weather=0.90,soccer=1.00,financial=0.60,cycling=0.05),
    11: dict(weather=1.00,soccer=0.90,financial=0.50,cycling=0.05),
    12: dict(weather=1.00,soccer=0.80,financial=0.50,cycling=0.05),
}


@dataclass
class DomainSignals:
    domain: Domain
    session_volume:    deque = field(default_factory=lambda: deque(maxlen=14))
    net_margin:        deque = field(default_factory=lambda: deque(maxlen=14))
    opportunity_count: deque = field(default_factory=lambda: deque(maxlen=14))
    health: float = 1.0

    def record(self, session_volume, net_margin, opportunity_count):
        now = datetime.now(tz=timezone.utc)
        self.session_volume.append((now, session_volume))
        self.net_margin.append((now, net_margin))
        self.opportunity_count.append((now, opportunity_count))
        self.health = self._compute_health()

    def _ratio(self, series):
        vals = [v for _,v in series]
        if len(vals) < 4: return 1.0
        r = sum(vals[-3:])/3
        b = sum(vals[:-3])/max(len(vals)-3,1)
        return max(0.0, r/b) if b > 0 else (1.0 if r >= 0 else 0.0)

    def _compute_health(self):
        h = (0.25*min(self._ratio(self.session_volume),1.0) +
             0.35*min(self._ratio(self.net_margin),1.0) +
             0.40*min(self._ratio(self.opportunity_count),1.0))
        return max(0.0, min(1.0, h))

    @property
    def is_decaying(self): return self.health < 0.65
    @property
    def is_critical(self):  return self.health < 0.35
    @property
    def latest_margin(self): return self.net_margin[-1][1] if self.net_margin else 0.0
    @property
    def latest_opportunities(self): return int(self.opportunity_count[-1][1]) if self.opportunity_count else 0


@dataclass
class DomainState:
    domain:          Domain
    status:          DomainStatus       = DomainStatus.INACTIVE
    signals:         DomainSignals      = field(default=None)
    test_start:      Optional[datetime] = None
    active_start:    Optional[datetime] = None
    allocation:      float              = 0.0
    model_validated: bool               = False

    def __post_init__(self):
        if self.signals is None:
            self.signals = DomainSignals(self.domain)

    def days_in_testing(self):
        if not self.test_start: return 0
        return (datetime.now(tz=timezone.utc)-self.test_start).days

    def is_ready_to_scale(self):
        return (self.model_validated and
                self.days_in_testing() >= 14 and
                self.signals.health >= 0.70)


class DomainRotationEngine:
    MIN_TEST_ALLOCATION = 0.05
    MAX_SINGLE_DOMAIN   = 0.80
    MAX_DAILY_ROTATION  = 0.05
    PREEMPTIVE_LEAD_WKS = 5

    def __init__(self, total_budget_usdc, state_file="engine_state.json"):
        self.total_budget = total_budget_usdc
        self.state_file   = pathlib.Path(state_file)
        self.month        = datetime.now(tz=timezone.utc).month
        self.domains      = {d: DomainState(domain=d) for d in Domain}
        if self.state_file.exists():
            self._load_state()
        else:
            self._initialize_defaults()

    def _initialize_defaults(self):
        self.domains[Domain.WEATHER].status     = DomainStatus.ACTIVE
        self.domains[Domain.WEATHER].allocation  = 0.58
        self.domains[Domain.WEATHER].model_validated = True
        self.domains[Domain.SOCCER].status      = DomainStatus.ACTIVE
        self.domains[Domain.SOCCER].allocation   = 0.37
        self.domains[Domain.SOCCER].model_validated = True
        self.domains[Domain.CYCLING].status     = DomainStatus.TESTING
        self.domains[Domain.CYCLING].allocation  = 0.05
        self.domains[Domain.CYCLING].test_start  = datetime.now(tz=timezone.utc)
        self.domains[Domain.FINANCIAL].status   = DomainStatus.INACTIVE
        self.domains[Domain.FINANCIAL].allocation = 0.00

    def daily_update(self, metrics):
        self.month = datetime.now(tz=timezone.utc).month
        for domain, m in metrics.items():
            s = self.domains.get(domain)
            if s and s.status != DomainStatus.INACTIVE:
                s.signals.record(m.get("session_volume",0),
                                 m.get("net_margin",0),
                                 m.get("opportunity_count",0))
        self._check_preemptive_testing()
        self._update_statuses()
        target = self._compute_targets()
        self._smooth_rotate(target)
        self.save_state()
        self._log_state()
        return {d: s.allocation for d,s in self.domains.items()}

    def _check_preemptive_testing(self):
        primary_decaying = any(s.signals.is_decaying for s in self.domains.values()
                               if s.status == DomainStatus.ACTIVE)
        fm = (self.month + self.PREEMPTIVE_LEAD_WKS//4) % 12 + 1
        for domain, state in self.domains.items():
            if state.status != DomainStatus.INACTIVE: continue
            fw = SEASONAL_WEIGHTS[fm].get(domain.value, 0)
            cw = SEASONAL_WEIGHTS[self.month].get(domain.value, 0)
            if (primary_decaying and fw >= 0.60) or cw >= 0.70:
                state.status = DomainStatus.TESTING
                state.test_start = datetime.now(tz=timezone.utc)
                state.allocation = self.MIN_TEST_ALLOCATION

    def _update_statuses(self):
        for domain, state in self.domains.items():
            sw = SEASONAL_WEIGHTS[self.month].get(domain.value, 0)
            sig = state.signals
            if state.status == DomainStatus.ACTIVE:
                if sig.is_critical:
                    state.status = DomainStatus.DECAYING
            elif state.status == DomainStatus.DECAYING:
                if sw < 0.25: state.status = DomainStatus.STANDBY
                elif sig.health >= 0.70: state.status = DomainStatus.ACTIVE
            elif state.status == DomainStatus.TESTING:
                if state.is_ready_to_scale():
                    state.status = DomainStatus.ACTIVE
                    state.active_start = datetime.now(tz=timezone.utc)
            elif state.status == DomainStatus.STANDBY:
                if sw >= 0.50:
                    state.status = DomainStatus.TESTING
                    state.test_start = datetime.now(tz=timezone.utc)

    def _compute_targets(self):
        targets, total = {}, 0.0
        for domain, state in self.domains.items():
            sw = SEASONAL_WEIGHTS[self.month].get(domain.value, 0)
            if   state.status == DomainStatus.INACTIVE:  raw = 0.0
            elif state.status == DomainStatus.TESTING:   raw = self.MIN_TEST_ALLOCATION
            elif state.status == DomainStatus.STANDBY:   raw = self.MIN_TEST_ALLOCATION/2
            else:                                          raw = sw * state.signals.health
            targets[domain] = raw; total += raw
        if total > 0:
            for d in targets: targets[d] /= total
        for d in targets:
            if self.domains[d].status == DomainStatus.TESTING:
                targets[d] = max(targets[d], self.MIN_TEST_ALLOCATION)
            targets[d] = min(targets[d], self.MAX_SINGLE_DOMAIN)
        t = sum(targets.values())
        if t > 0:
            for d in targets: targets[d] /= t
        return targets

    def _smooth_rotate(self, target):
        for domain, state in self.domains.items():
            delta = max(-self.MAX_DAILY_ROTATION,
                        min(self.MAX_DAILY_ROTATION, target.get(domain,0)-state.allocation))
            state.allocation = max(0.0, state.allocation + delta)
        total = sum(s.allocation for s in self.domains.values())
        if total > 0:
            for s in self.domains.values(): s.allocation /= total

    def get_budget_for_domain(self, domain):
        return self.total_budget * self.domains[domain].allocation

    def mark_model_validated(self, domain):
        self.domains[domain].model_validated = True
        self.save_state()

    def get_status_report(self):
        return {
            "date": datetime.now(tz=timezone.utc).isoformat(),
            "total_budget": self.total_budget, "month": self.month,
            "domains": {
                d.value: {"status": s.status.value, "health": round(s.signals.health,3),
                          "allocation": round(s.allocation,4),
                          "budget": round(self.get_budget_for_domain(d),2),
                          "validated": s.model_validated,
                          "days_testing": s.days_in_testing()}
                for d,s in self.domains.items()
            }
        }

    def save_state(self):
        self.state_file.write_text(json.dumps(self.get_status_report(), indent=2))

    def _load_state(self):
        try:
            saved = json.loads(self.state_file.read_text())
            for domain in Domain:
                info = saved.get("domains",{}).get(domain.value)
                if not info: continue
                s = self.domains[domain]
                try: s.status = DomainStatus(info["status"])
                except: s.status = DomainStatus.INACTIVE
                s.allocation = info.get("allocation",0.0)
                s.model_validated = info.get("validated",False)
        except Exception as e:
            log.warning(f"Load state failed: {e}")
            self._initialize_defaults()

    def _log_state(self):
        for d,s in self.domains.items():
            if s.status != DomainStatus.INACTIVE:
                log.info(f"  {d.value:12} {s.status.value:10} "
                         f"h={s.signals.health:.2f} "
                         f"{s.allocation*100:5.1f}% "
                         f"${self.get_budget_for_domain(d):,.0f}")
