"""
core/bot.py

v4 CHANGES:
  - 6 sessions: S0(06z) S1(07z) S4(13z) S2(15z) S3(17z) S5(21z)
  - Session 3 start moved 18:00 → 17:00 UTC (Bundesliga confirmed Apr 1)
  - Early sell loop added (confirmed Apr 5: 16 sells at avg 99.74c)
  - Cycling domain integrated
  - Soccer as co-primary (70% S3 affinity)
"""

import asyncio
import logging
from datetime import datetime, timezone, date

from core.rotation_engine import DomainRotationEngine, Domain, DomainStatus
from domains.weather   import WeatherScanner
from domains.soccer    import SoccerScanner
from domains.financial import FinancialScanner
from domains.cycling   import CyclingScanner
from core.orders       import OrderManager
from core.metrics      import MetricsTracker

log = logging.getLogger("bot")

SESSION_AFFINITY = {
    "S0": {Domain.WEATHER: 0.50, Domain.SOCCER: 0.10, Domain.FINANCIAL: 0.10, Domain.CYCLING: 0.30},
    "S1": {Domain.WEATHER: 0.45, Domain.SOCCER: 0.25, Domain.FINANCIAL: 0.20, Domain.CYCLING: 0.10},
    "S4": {Domain.WEATHER: 0.40, Domain.SOCCER: 0.20, Domain.FINANCIAL: 0.30, Domain.CYCLING: 0.10},
    "S2": {Domain.WEATHER: 0.55, Domain.SOCCER: 0.05, Domain.FINANCIAL: 0.35, Domain.CYCLING: 0.05},
    "S3": {Domain.WEATHER: 0.15, Domain.SOCCER: 0.70, Domain.FINANCIAL: 0.10, Domain.CYCLING: 0.05},
    "S5": {Domain.WEATHER: 0.40, Domain.SOCCER: 0.45, Domain.FINANCIAL: 0.05, Domain.CYCLING: 0.10},
}

MAX_BOOK_FRACTION = 0.30


class IntegratedBot:

    def __init__(self, config: dict):
        self.config  = config
        self.engine  = DomainRotationEngine(config["total_budget_usdc"])
        self.paper_trade = config.get("paper_trade", True)
        self.metrics = MetricsTracker()
        self.orders  = OrderManager(config)
        self.today   = date.today()
        self.scanners = {
            Domain.WEATHER:   WeatherScanner(config),
            Domain.SOCCER:    SoccerScanner(config),
            Domain.FINANCIAL: FinancialScanner(config),
            Domain.CYCLING:   CyclingScanner(config),
        }
        log.info("Bot v4: 6 sessions, 4 domains, early sell enabled")

    async def run(self):
        while True:
            try:
                if date.today() != self.today:
                    await self._end_of_day()
                now      = datetime.now(tz=timezone.utc)
                sessions = self._active_sessions(now)
                if self.paper_trade:
                    await self._run_sessions(sessions)
                    await asyncio.sleep(30)
                # if sessions:
                #     if self.config.get("early_sell_enabled"):
                #         await self._early_sell_loop()
                #     await self._run_sessions(sessions)
                # else:
                #    await asyncio.sleep(30)
            except Exception as e:
                log.error(f"Bot loop error: {e}", exc_info=True)
                await asyncio.sleep(60)

    def _active_sessions(self, now: datetime) -> list[str]:
        h, m, wday = now.hour, now.minute, now.weekday()
        mins = h * 60 + m
        cfg  = self.config

        def chk(sk, ek, wd_only=False):
            s = cfg[sk][0]*60 + cfg[sk][1]
            e = cfg[ek][0]*60 + cfg[ek][1]
            log.info(f"Checking session {sk} to {ek} at {mins} minutes")
            if wd_only and wday >= 5:
                return False
            return s <= mins <= e

        sessions = []

        if self.paper_trade:
            sessions = ["S0"]
        else:
            if chk("session_0_start", "session_0_end"):             sessions.append("S0")
            if chk("session_1_start", "session_1_end"):             sessions.append("S1")
            if chk("session_4_start", "session_4_end"):             sessions.append("S4")
            if chk("session_2_start", "session_2_end",
                   cfg.get("session_2_weekdays_only", True)):       sessions.append("S2")
            if chk("session_3_start", "session_3_end"):             sessions.append("S3")
            if chk("session_5_start", "session_5_end"):             sessions.append("S5")

        log.info(f"Active sessions: {sessions}")
        return sessions

    async def _early_sell_loop(self):
        """
        Runs at the start of every session.
        Sells NO positions priced >= early_sell_threshold immediately.

        Confirmed Apr 5 behavior:
          Miami NO $1,626 @ 99.9c  |  Moscow NO $502 @ 99.7c
          Munich NO $461 @ 99.6c   |  Seoul NO $448 @ 99.7c
          Wellington NO $282 @ 99.8c — all sold, capital recycled same session.

        DEVELOPER: implement orders.get_open_positions() and orders.sell_position()
        """
        if self.paper_trade:
            log.info("Running early sell loop in paper trade mode")

        threshold = self.config.get("early_sell_threshold", 0.990)
        try:
            open_positions = await self.orders.get_open_positions()
            log.info(f"Open positions: {open_positions}")
        except Exception as e:
            log.debug(f"Early sell skipped: {e}")
            return

        sold = 0
        total = 0.0
        for pos in open_positions:
            if pos.get("outcome", "").upper() != "NO":
                continue
            cur = float(pos.get("currentPrice", 0))
            if cur < threshold:
                continue
            size  = float(pos.get("size", 0))
            result = await self.orders.sell_position(
                token_id=pos["asset"], size=size, min_price=threshold)
            if result.success:
                sold  += 1
                total += size * cur
                log.info(f"[SELL] {pos.get('title','')[:45]} @ {cur:.4f} ${size*cur:.2f}")

        if sold:
            log.info(f"Early sell: {sold} positions, ${total:,.2f} recycled")

    async def _run_sessions(self, sessions: list[str]):
        # if self.paper_trade:
        #     log.info("Running sessions in paper trade mode")

        for session in sessions:
            tasks = []

            for domain, state in self.engine.domains.items():
                if state.status not in (DomainStatus.ACTIVE, DomainStatus.TESTING):
                    continue

                budget = self._session_budget(domain, session)

                # if self.paper_trade:
                #     log.info(f"Running domains in paper trade mode: budget: {budget:.0f}")

                if budget >= 5.0:
                    tasks.append(self._run_domain(domain, budget, session))

                # if self.paper_trade:
                #     log.info(f"Running domains in paper trade mode: {tasks}")

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(300)

    def _session_budget(self, domain: Domain, session: str) -> float:
        base     = self.engine.get_budget_for_domain(domain)
        affinity = SESSION_AFFINITY.get(session, {}).get(domain, 0.25)
        return base * affinity * 6   # 6 sessions per day

    async def _run_domain(self, domain: Domain, budget: float, session: str):
        scanner = self.scanners.get(domain)
        
        # if self.paper_trade:
        #     log.info(f"Running domain in paper trade mode: domain : session : {session} \n budget : {budget:.0f} \n scanner: {scanner}")

        if not scanner:
            return
            
        log.info(f"-----[{session}] {domain.value} budget=${budget:.0f}------")

        # if self.paper_trade:
        #     log.info(f"---------------START {domain.value} SCAN---------------")

        try:
            opps = await scanner.scan()
        except Exception as e:
            log.error(f"[{session}] {domain.value} scan: {e}")
            return

        valid = [o for o in opps if o.edge >= self.config["min_edge"] and o.confidence >= self.config["min_confidence"]]
        
        # if self.paper_trade:
        #     log.info(f"Running domain in paper trade mode: valid: {valid}\n\n")
        
        self.metrics.record_scan(domain, len(opps), len(valid))

        spent = 0.0
        for opp in valid:
            
            # if self.paper_trade:
            #     log.info(f"Running domain in paper trade mode: opp: {opp}")

            if spent >= budget:
                break
            no_sz  = self._position_size(domain, opp)
            yes_sz = no_sz * self.config["yes_fraction"]

            if self.paper_trade:
                log.info(f"Running domain in paper trade mode: budget: {budget:.0f} \n spent: {spent:.0f} \n no_sz: {no_sz:.0f} \n yes_sz: {yes_sz:.0f}")

            if no_sz <= 0 or spent + no_sz + yes_sz > budget:
                continue
            result = await self.orders.execute(opp, no_sz, yes_sz)

            # if self.paper_trade:
            #     log.info(f"Running domain in paper trade mode: result: {result}")
            
            if result.success:
                spent += no_sz + yes_sz
                self.metrics.record_trade(domain, no_sz + yes_sz, no_sz, yes_sz, opp.slug)
                log.info(f"\n [{session}] \n {domain.value} \n {opp.slug[:35]} \n " f"NO ${no_sz:.0f} @ {opp.no_price:.3f} edge={opp.edge:.3f}\n\n")

    def _position_size(self, domain: Domain, opp) -> float:
        state     = self.engine.domains[domain]
        base      = self.config["base_no_size"].get(domain.value, 10.0)
        max_alloc = self.config.get("max_single_domain", 0.80)

        if state.status == DomainStatus.TESTING:
            return base * 0.20

        alloc_scale = min(1.0, state.allocation / max_alloc)
        edge_scale  = min(1.5, 1.0 + (opp.edge - self.config["min_edge"]) * 4)

        conf = opp.confidence
        if   conf >= 0.90: conf_scale = 1.00
        elif conf >= 0.75: conf_scale = 0.75
        elif conf >= 0.65: conf_scale = 0.50
        else:              return 0.0

        raw = base * alloc_scale * edge_scale * conf_scale
        avail = getattr(opp, "available_liquidity", None)
        if avail and avail > 0:
            return min(raw, avail * MAX_BOOK_FRACTION)
        return raw

    async def _end_of_day(self):
        log.info("EOD — rotating domains")
        allocs = self.engine.daily_update(self.metrics.daily_summary())
        for d, a in allocs.items():
            s = self.engine.domains[d]
            # if s.status != DomainStatus.INACTIVE:
            #     log.info(f"  {d.value:12} {s.status.value:10} " f"health={s.signals.health:.2f} " f"{a*100:.1f}% ${self.engine.  (d):,.0f}")
        self.metrics.reset()
        self.today = date.today()
