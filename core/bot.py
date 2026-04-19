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
from core.monitor_hub  import emit as mon_emit
from core.monitor_hub  import is_enabled as mon_enabled

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
        self.scanners[Domain.WEATHER].set_ws_trade_executor(self._weather_ws_trade)
        log.info(
            "Bot v4: 6 sessions, 4 domains, early_sell=%s",
            self.config.get("early_sell_enabled", False),
        )

    async def run(self):
        """
        Main loop (dev guide): early sell → session scans → sleep.
        In paper mode, early sell also runs after scans so virtual positions from this tick can recycle.
        """
        active_sleep = float(self.config.get("loop_sleep_active_sec", 120))
        idle_sleep = float(self.config.get("loop_sleep_idle_sec", 300))
        while True:
            try:
                if date.today() != self.today:
                    await self._end_of_day()
                now = datetime.now(tz=timezone.utc)
                sessions = self._active_sessions(now)

                if mon_enabled(self.config):
                    mon_emit(
                        "engine",
                        phase="tick",
                        utc=now.strftime("%Y-%m-%d %H:%M UTC"),
                        sessions=sessions,
                        paper_trade=self.paper_trade,
                    )

                if self.config.get("early_sell_enabled") and not self.paper_trade:
                    await self._early_sell_loop()

                if sessions:
                    await self._run_sessions(sessions)
                    if self.config.get("early_sell_enabled") and self.paper_trade:
                        await self._early_sell_loop()
                    await asyncio.sleep(active_sleep)
                else:
                    log.info(
                        "Idle — no session window (UTC %s). Next check in %ds",
                        now.strftime("%H:%M"),
                        int(idle_sleep),
                    )
                    if mon_enabled(self.config):
                        mon_emit(
                            "engine",
                            phase="idle",
                            utc=now.strftime("%Y-%m-%d %H:%M UTC"),
                            next_check_sec=int(idle_sleep),
                        )
                    await asyncio.sleep(idle_sleep)
            except Exception as e:
                log.error(f"Bot loop error: {e}", exc_info=True)
                if mon_enabled(self.config):
                    mon_emit("error", where="bot_loop", message=str(e))
                await asyncio.sleep(60)

    def _active_sessions(self, now: datetime) -> list[str]:
        h, m, wday = now.hour, now.minute, now.weekday()
        mins = h * 60 + m
        cfg  = self.config

        def chk(sk, ek, wd_only=False):
            s = cfg[sk][0] * 60 + cfg[sk][1]
            e = cfg[ek][0] * 60 + cfg[ek][1]
            log.debug("Session gate %s->%s at %d min (wd_only=%s)", sk, ek, mins, wd_only)
            if wd_only and wday >= 5:
                return False
            return s <= mins <= e

        if self.paper_trade and cfg.get("paper_dev_fast_loop", False):
            log.info("Active sessions: ['S0'] (paper_dev_fast_loop)")
            return ["S0"]

        sessions: list[str] = []
        if chk("session_0_start", "session_0_end"):
            sessions.append("S0")
        if chk("session_1_start", "session_1_end"):
            sessions.append("S1")
        if chk("session_4_start", "session_4_end"):
            sessions.append("S4")
        if chk("session_2_start", "session_2_end", cfg.get("session_2_weekdays_only", True)):
            sessions.append("S2")
        if chk("session_3_start", "session_3_end"):
            sessions.append("S3")
        if chk("session_5_start", "session_5_end"):
            sessions.append("S5")

        log.info("Active sessions: %s (paper=%s)", sessions, self.paper_trade)
        if mon_enabled(self.config):
            mon_emit("session", active=sessions, paper_trade=self.paper_trade)
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
            if open_positions:
                log.info("Early sell: %d open position(s) from API", len(open_positions))
            else:
                log.debug("Early sell: no open positions")
        except Exception as e:
            log.debug("Early sell skipped: %s", e)
            return

        sold = 0
        total = 0.0
        for pos in open_positions:
            if pos.get("outcome", "").upper() != "NO":
                continue
            raw_px = pos.get("currentPrice", pos.get("curPrice", 0))
            try:
                cur = float(raw_px or 0)
            except (TypeError, ValueError):
                cur = 0.0
            if cur < threshold:
                continue
            size = float(pos.get("size", 0) or 0)
            token_id = pos.get("asset") or pos.get("tokenId") or pos.get("token_id") or ""
            if not token_id or size <= 0:
                continue
            desc = (
                pos.get("description")
                or pos.get("question")
                or pos.get("eventTitle")
                or pos.get("slug")
                or ""
            )
            result = await self.orders.sell_position(
                token_id=str(token_id),
                size=size,
                min_price=threshold,
                title=str(pos.get("title") or ""),
                description=str(desc),
                slug=str(pos.get("slug") or pos.get("eventSlug") or ""),
                outcome=str(pos.get("outcome") or "NO"),
                mark_price=cur,
            )
            if result.success:
                sold += 1
                total += size * cur

        if sold:
            log.info(f"Early sell: {sold} positions, ${total:,.2f} recycled")
            if mon_enabled(self.config):
                mon_emit(
                    "early_sell",
                    sold_count=sold,
                    recycled_usd=round(total, 2),
                )

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

    def _session_budget(self, domain: Domain, session: str) -> float:
        base     = self.engine.get_budget_for_domain(domain)
        affinity = SESSION_AFFINITY.get(session, {}).get(domain, 0.25)
        return base * affinity * 6   # 6 sessions per day

    async def _weather_ws_trade(self, opp) -> None:
        """
        WS-driven weather: always log a virtual trade plan; optionally execute (paper/live).
        """
        domain = Domain.WEATHER
        if opp.edge < self.config["min_edge"] or opp.confidence < self.config["min_confidence"]:
            return
        no_sz = self._position_size(domain, opp)
        yes_sz = no_sz * self.config["yes_fraction"]
        preview_note = None
        if no_sz <= 0 or yes_sz <= 0:
            preview_note = "NO/YES size is zero — model sizing skipped this leg"
        if not self.config.get("weather_ws_auto_execute", True):
            preview_note = (preview_note + "; " if preview_note else "") + "weather_ws_auto_execute=false (log-only)"

        self.orders.log_virtual_trade_plan(
            opp,
            no_sz,
            yes_sz,
            session="WS",
            domain=domain.value,
            source="WS",
            note=preview_note,
        )
        if not self.config.get("weather_ws_auto_execute", True):
            return
        if no_sz <= 0 or yes_sz <= 0:
            return

        result = await self.orders.execute(opp, no_sz, yes_sz, quiet_paper=True)
        if result.success:
            self.metrics.record_trade(domain, no_sz + yes_sz, no_sz, yes_sz, opp.slug)
            log.info(
                "[WS] outcome slug=%s paper=%s merged=%s filled_NO=%.2f filled_YES=%.2f",
                (opp.slug or "")[:48],
                result.paper,
                result.merged,
                result.filled_no,
                result.filled_yes,
            )
            if mon_enabled(self.config):
                mon_emit(
                    "execute",
                    source="WS",
                    domain=domain.value,
                    session="WS",
                    slug=(opp.slug or "")[:120],
                    title=(getattr(opp, "title", None) or "")[:200],
                    success=True,
                    paper=result.paper,
                    merged=result.merged,
                    filled_no=result.filled_no,
                    filled_yes=result.filled_yes,
                )
        else:
            log.warning(
                "[WS] execute failed slug=%s err=%s",
                (opp.slug or "")[:48],
                getattr(result, "error", None),
            )
            if mon_enabled(self.config):
                mon_emit(
                    "execute",
                    source="WS",
                    domain=domain.value,
                    session="WS",
                    slug=(opp.slug or "")[:120],
                    success=False,
                    error=str(getattr(result, "error", None) or ""),
                )

    async def _run_domain(self, domain: Domain, budget: float, session: str):
        scanner = self.scanners.get(domain)
        
        # if self.paper_trade:
        #     log.info(f"Running domain in paper trade mode: domain : session : {session} \n budget : {budget:.0f} \n scanner: {scanner}")

        if not scanner:
            return
            
        log.info(f"-----[{session}] {domain.value} budget=${budget:.0f}------")
        if mon_enabled(self.config):
            mon_emit(
                "domain_scan",
                phase="start",
                domain=domain.value,
                session=session,
                budget_usd=round(budget, 2),
            )

        # if self.paper_trade:
        #     log.info(f"---------------START {domain.value} SCAN---------------")

        try:
            opps = await scanner.scan()
        except Exception as e:
            log.error(f"[{session}] {domain.value} scan: {e}")
            if mon_enabled(self.config):
                mon_emit(
                    "error",
                    where="domain_scan",
                    domain=domain.value,
                    session=session,
                    message=str(e),
                )
            return

        if self.paper_trade and not opps:
            log.info(
                "[%s] Paper: scan returned 0 opportunities (nothing to buy virtually this round).",
                domain.value,
            )

        strict = [o for o in opps if o.edge >= self.config["min_edge"] and o.confidence >= self.config["min_confidence"]]
        valid = strict
        forced_paper = False
        if (
            self.paper_trade
            and self.config.get("paper_simulate_trades_without_threshold", True)
            and not strict
            and opps
        ):
            valid = [max(opps, key=lambda x: float(x.edge or 0.0))]
            forced_paper = True
            log.info(
                "[%s] Paper virtual mode: %d scan row(s), none passed live gates; simulating best edge row for BUY/SELL logs.",
                domain.value,
                len(opps),
            )

        self.metrics.record_scan(domain, len(opps), len(strict))
        if mon_enabled(self.config):
            mon_emit(
                "domain_scan",
                phase="complete",
                domain=domain.value,
                session=session,
                opportunities=len(opps),
                passed_gates=len(strict),
                forced_paper_virtual=forced_paper,
            )

        spent = 0.0
        for opp in valid:
            no_sz = self._position_size(domain, opp)
            yes_sz = no_sz * self.config["yes_fraction"]
            if forced_paper and (no_sz <= 0 or yes_sz <= 0):
                no_sz = float(self.config.get("paper_virtual_min_no_size", 25.0))
                yes_sz = no_sz * self.config["yes_fraction"]
            remaining = budget - spent
            can_execute = (
                no_sz > 0
                and yes_sz > 0
                and spent < budget
                and (no_sz + yes_sz) <= remaining
            )
            note = None
            if spent >= budget:
                note = f"session budget already used (${spent:,.0f} of ${budget:,.0f})"
            elif no_sz <= 0:
                note = "NO leg size is zero at this confidence/allocation"
            elif (no_sz + yes_sz) > remaining:
                note = (
                    f"not enough budget left (${remaining:,.0f} left, "
                    f"need ${no_sz + yes_sz:,.0f} for NO+YES)"
                )

            vnote = note
            if forced_paper and can_execute:
                vnote = (
                    "Paper virtual: trade is for logs/portfolio only (scan row did not meet live min_edge/min_confidence)."
                )
                if note:
                    vnote = vnote + " " + note
            self.orders.log_virtual_trade_plan(
                opp,
                no_sz,
                yes_sz,
                session=session,
                domain=domain.value,
                source="SESSION",
                note=vnote if not can_execute or forced_paper else None,
            )
            if mon_enabled(self.config):
                opp_extras: dict = {}
                if not can_execute:
                    opp_extras = {
                        "reject_reason": "session_gate",
                        "ticket_detail": (note or "Execute blocked for this session.").strip(),
                    }
                elif forced_paper and can_execute:
                    opp_extras = {
                        "reject_reason": "",
                        "ticket_detail": (vnote or "").strip(),
                    }
                mon_emit(
                    "opportunity",
                    domain=domain.value,
                    session=session,
                    slug=(opp.slug or "")[:120],
                    title=(getattr(opp, "title", None) or "")[:200],
                    edge=float(getattr(opp, "edge", 0) or 0),
                    confidence=float(getattr(opp, "confidence", 0) or 0),
                    no_price=float(getattr(opp, "no_price", 0) or 0),
                    yes_price=float(getattr(opp, "yes_price", 0) or 0),
                    can_execute=can_execute,
                    forced_paper_virtual=forced_paper,
                    no_size=no_sz,
                    yes_size=yes_sz,
                    **opp_extras,
                )

            if not can_execute:
                continue

            result = await self.orders.execute(opp, no_sz, yes_sz, quiet_paper=True)
            if result.success:
                spent += no_sz + yes_sz
                self.metrics.record_trade(domain, no_sz + yes_sz, no_sz, yes_sz, opp.slug)
                log.info(
                    "[SESSION] outcome domain=%s session=%s slug=%s paper=%s merged=%s "
                    "filled_NO=%.2f filled_YES=%.2f",
                    domain.value,
                    session,
                    (opp.slug or "")[:48],
                    result.paper,
                    result.merged,
                    result.filled_no,
                    result.filled_yes,
                )
                if mon_enabled(self.config):
                    mon_emit(
                        "execute",
                        source="SESSION",
                        domain=domain.value,
                        session=session,
                        slug=(opp.slug or "")[:120],
                        title=(getattr(opp, "title", None) or "")[:200],
                        success=True,
                        paper=result.paper,
                        merged=result.merged,
                        filled_no=result.filled_no,
                        filled_yes=result.filled_yes,
                    )
            else:
                log.warning(
                    "[SESSION] execute failed domain=%s slug=%s err=%s",
                    domain.value,
                    (opp.slug or "")[:48],
                    getattr(result, "error", None),
                )
                if mon_enabled(self.config):
                    err_t = str(getattr(result, "error", None) or "")
                    mon_emit(
                        "execute",
                        source="SESSION",
                        domain=domain.value,
                        session=session,
                        slug=(opp.slug or "")[:120],
                        title=(getattr(opp, "title", None) or "")[:200],
                        success=False,
                        error=err_t,
                        reject_reason="execute_failed",
                        ticket_detail=err_t or "Order path returned success=False with no error text.",
                    )

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
