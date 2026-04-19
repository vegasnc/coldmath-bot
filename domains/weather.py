"""
domains/weather.py

Weather temperature market scanner.
Primary domain — proven $27,697 March 2026 profit.

Data source: NOAA GFS ensemble (free, public)
  - 00z run available ~03:30 UTC → Session 1 (07:00-09:30)
  - 12z run available ~15:30 UTC → Session 2 (15:00-16:45)

Model: Ensemble probability distribution
  - Count members above/below threshold
  - Confidence = inverse of ensemble spread
  - Skip if spread > 8°F (spring transition)
"""

import asyncio
import logging
import re
import json
import time
from dataclasses import replace
from typing import Any, Awaitable, Callable, Optional

import aiohttp

from core.opportunity import Opportunity
from core.polymarket_market_ws import get_shared_market_ws_feed
from datetime import date, datetime, timedelta, timezone

log = logging.getLogger("weather_scanner")


def _short_condition_slug(cid: str) -> str:
    c = (cid or "").replace("0x", "").strip()
    if not c:
        return "unknown"
    if len(c) <= 18:
        return c
    return f"{c[:10]}...{c[-4:]}"


_TITLE_MONTH_TO_NUM = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


# Polymarket titles often use abbreviations that are not substrings of our canonical names.
_CITY_TITLE_ALIASES: tuple[tuple[str, str], ...] = (
    (r"\bnyc\b", "new york"),
    (r"\bla\b", "los angeles"),
    (r"\bsf\b", "san francisco"),
    (r"\bhk\b", "hong kong"),
)


def _normalize_title_for_parse(title: str) -> str:
    """Lowercase + fold common unicode degree symbols for regex matching."""
    t = str(title).lower().strip()
    t = t.replace("\u00b0", "°").replace("\u2103", "°c").replace("\u2109", "°f")
    for pat, repl in _CITY_TITLE_ALIASES:
        t = re.sub(pat, repl, t, flags=re.I)
    return t


def _parse_event_date_from_title(title: str) -> Optional[date]:
    """
    Extract the market's resolution day from Polymarket-style English titles.

    Examples:
      "... on April 19?"           → date(year, 4, 19) (year inferred if omitted)
      "... on April 19, 2026?"     → 2026-04-19
      "... on 2026-04-19"          → ISO form after \"on\"
    """
    if not title or not str(title).strip():
        return None
    low = str(title).lower().strip()

    m_iso = re.search(r"\bon\s+(\d{4})-(\d{2})-(\d{2})\b", low)
    if m_iso:
        try:
            y, mo, d = int(m_iso.group(1)), int(m_iso.group(2)), int(m_iso.group(3))
            return date(y, mo, d)
        except ValueError:
            return None

    months = "|".join(sorted(_TITLE_MONTH_TO_NUM.keys(), key=len, reverse=True))
    m = re.search(
        rf"\bon\s+({months})\s+(\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s*(\d{{4}}))?\b",
        low,
    )
    if not m:
        return None

    mon_word = m.group(1).lower()
    mo = _TITLE_MONTH_TO_NUM.get(mon_word)
    if not mo:
        return None
    try:
        d = int(m.group(2))
    except (TypeError, ValueError):
        return None
    if d < 1 or d > 31:
        return None

    today = datetime.now(timezone.utc).date()
    y_s = m.group(3)
    if y_s:
        try:
            return date(int(y_s), mo, d)
        except ValueError:
            return None

    y = today.year
    try:
        cand = date(y, mo, d)
    except ValueError:
        return None
    if cand < today - timedelta(days=14):
        y += 1
        try:
            cand = date(y, mo, d)
        except ValueError:
            return None
    elif cand > today + timedelta(days=330):
        y -= 1
        try:
            cand = date(y, mo, d)
        except ValueError:
            return None
    return cand


class WeatherModel:
    """
    GFS ensemble → P(temperature outcome).
    DEVELOPER: Implement _fetch_gfs_ensemble() with NOAA NOMADS API.
    """

    GFS_NOMADS_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p50.pl"

    # City → (lat, lon) for GFS grid lookup
    CITY_COORDS = {
        "Dallas":        (32.78, -96.80),
        "Houston":       (29.76, -95.37),
        "Miami":         (25.76, -80.19),
        "Atlanta":       (33.75, -84.39),
        "Chicago":       (41.88, -87.63),
        "New York":      (40.71, -74.01),
        "Austin":        (30.27, -97.74),
        "Los Angeles":   (34.05, -118.24),
        "San Francisco": (37.77, -122.42),
        "Seattle":       (47.61, -122.33),
        "Toronto":       (43.65, -79.38),
        "London":        (51.51, -0.13),
        "Madrid":        (40.42, -3.70),
        "Ankara":        (39.93, 32.86),
        "Seoul":         (37.57, 126.98),
        "Tokyo":         (35.69, 139.69),
        "Beijing":       (39.91, 116.39),
        "Singapore":     (1.35, 103.82),
        "Shanghai":      (31.23, 121.47),
        "Wellington":    (-41.29, 174.78),
        "Sao Paulo":     (-23.55, -46.63),
        "Buenos Aires":  (-34.61, -58.38),
        "Lucknow":       (26.85, 80.95),
        "Mexico City":   (19.43, -99.13),
        "Moscow":        (55.76, 37.62),
        "Munich":        (48.14, 11.58),
        "Helsinki":      (60.17, 24.94),
        "Denver":        (39.74, -104.99),
        "Chongqing":     (29.56, 106.55),
        "Shenzhen":      (22.54, 114.06),
        "Busan":         (35.18, 129.08),
        "Kuala Lumpur":  (3.14, 101.69),
        "Paris":         (48.86, 2.35),
        "Hong Kong":     (22.32, 114.17),
        "Amsterdam":     (52.37, 4.90),
        "Guangzhou":     (23.13, 113.26),
        "Panama City":   (8.98, -79.52),
    }

    def get_probability(
        self,
        city: str,
        threshold: float,
        direction: str,
        *,
        event_date: Optional[date] = None,
    ) -> tuple[float, float]:
        """
        Returns (prob_no, confidence) synchronously.
        Direction: 'above' | 'below' | 'exact'

        DEVELOPER: Call async version from async context.
        """
        ensemble = self._get_ensemble_sync(city, event_date=event_date)
        if not ensemble or len(ensemble) < 5:
            return 0.5, 0.0

        return self._calculate(ensemble, threshold, direction)

    async def get_probability_async(
        self,
        city: str,
        threshold: float,
        direction: str,
        *,
        event_date: Optional[date] = None,
    ) -> tuple[float, float]:
        ensemble = await self._fetch_gfs_ensemble(city, event_date=event_date)

        if not ensemble or len(ensemble) < 5:
            return 0.5, 0.0
        return self._calculate(ensemble, threshold, direction)

    def _calculate(self, ensemble: list[float], threshold: float,
                   direction: str) -> tuple[float, float]:
        n = len(ensemble)

        if direction == "above":
            hits = sum(1 for t in ensemble if t >= threshold)
        elif direction == "below":
            hits = sum(1 for t in ensemble if t <= threshold)
        else:  # exact band (±0.5°)
            hits = sum(1 for t in ensemble if abs(t - threshold) <= 0.5)

        prob_yes = hits / n
        prob_no  = 1.0 - prob_yes

        spread = max(ensemble) - min(ensemble)

        if spread <= 4:   confidence = 1.0
        elif spread <= 6: confidence = 0.80
        elif spread <= 8: confidence = 0.60
        else:             confidence = 0.30  # spring chaos — skip

        return prob_no, confidence

    async def _fetch_gfs_ensemble(
        self, city: str, *, event_date: Optional[date] = None
    ) -> Optional[list[float]]:
        """
        Fetch GFS-like ensemble distribution of daily high temperatures (F).
        Chosen approach: Open-Meteo Ensemble API (Option B).
        - Option A (NOAA GRIB2): more control, but much heavier parser infra.
        - Option C (ECMWF raw): strong quality, but operationally heavier.

        When ``event_date`` is set, hourly rows for that **UTC calendar day** are
        used to form each member's daily high (title dates are parsed without TZ;
        local-resolution-day refinement would need per-city time zones).
        """
        coords = self.CITY_COORDS.get(city)
        if not coords:
            log.debug(f"Unknown city for ensemble fetch: {city}")
            return None
        lat, lon = coords
        url = "https://ensemble-api.open-meteo.com/v1/ensemble"
        eval_hours = int(self.__dict__.get("eval_hours", 24))
        base_days = max(1, min(35, int(self.__dict__.get("forecast_days", 1))))

        today_utc = datetime.now(timezone.utc).date()
        forecast_days = base_days
        if event_date is not None:
            span = (event_date - today_utc).days
            if span < 0:
                log.debug("ensemble skip: event_date %s before today %s (%s)", event_date, today_utc, city)
                return None
            forecast_days = max(1, min(35, span + 2))

        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m",
            "models": "gfs_seamless",
            "forecast_days": forecast_days,
            "temperature_unit": "fahrenheit",
            "timezone": "UTC",
        }
        try:
            timeout = aiohttp.ClientTimeout(total=20)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        log.warning(f"Ensemble API HTTP {resp.status}: {text[:160]}")
                        return None
                    data = await resp.json()
        except Exception as e:
            log.warning(f"Ensemble fetch failed for {city}: {e}")
            return None
        hourly = data.get("hourly") or {}

        if not isinstance(hourly, dict):
            return None

        times = hourly.get("time")
        good_idx: Optional[list[int]] = None
        if event_date is not None:
            if not isinstance(times, list) or not times:
                log.debug("ensemble skip: no hourly times for event_date=%s (%s)", event_date, city)
                return None
            idxs: list[int] = []
            for i, ts in enumerate(times):
                if ts is None:
                    continue
                try:
                    s = str(ts)
                    if len(s) < 10 or s[4] != "-" or s[7] != "-":
                        continue
                    d0 = date.fromisoformat(s[:10])
                except (ValueError, TypeError):
                    continue
                if d0 == event_date:
                    idxs.append(i)
            if idxs:
                good_idx = idxs
            else:
                try:
                    s0 = str(times[0])
                    start_d = date.fromisoformat(s0[:10])
                    off = (event_date - start_d).days
                    n_times = len(times)
                    if 0 <= off and off * 24 < n_times:
                        lo = off * 24
                        hi = min(lo + 24, n_times)
                        good_idx = list(range(lo, hi))
                except (ValueError, TypeError):
                    good_idx = None
            if not good_idx:
                log.debug("ensemble skip: no hourly window for event_date=%s (%s)", event_date, city)
                return None

        member_keys = sorted(
            k for k in hourly.keys() if k.startswith("temperature_2m_member")
        )
        window = max(1, eval_hours)

        def _series_daily_high(series: list[Any]) -> Optional[float]:
            try:
                if good_idx is not None:
                    vals = []
                    for i in good_idx:
                        if i < len(series) and series[i] is not None:
                            vals.append(float(series[i]))
                else:
                    vals = [
                        float(x)
                        for x in series[: min(window, len(series))]
                        if x is not None
                    ]
            except (TypeError, ValueError):
                return None
            if not vals:
                return None
            return max(vals)

        if not member_keys:
            base = hourly.get("temperature_2m")
            if isinstance(base, list) and base:
                dh = _series_daily_high(base)
                if dh is not None and -120.0 <= dh <= 140.0:
                    return [dh]
            return None

        ensemble_highs: list[float] = []
        for key in member_keys:
            series = hourly.get(key)
            if not isinstance(series, list) or not series:
                continue
            daily_high = _series_daily_high(series)
            if daily_high is None:
                continue
            if -120.0 <= daily_high <= 140.0:
                ensemble_highs.append(daily_high)
        if len(ensemble_highs) < 5:
            return None
        return ensemble_highs


    def _get_ensemble_sync(self, city: str, *, event_date: Optional[date] = None) -> Optional[list[float]]:
        """Sync wrapper — runs in thread pool for sync callers."""
        import asyncio
        try:
            loop = asyncio.get_event_loop()

            data_gfs_ensemble = self._fetch_gfs_ensemble(city, event_date=event_date)
            res = loop.run_until_complete(data_gfs_ensemble)

            return res
        except RuntimeError:
            return None

    def backtest(self, days: int = 30) -> dict:
        """
        Back-test model accuracy against historical Polymarket resolutions.
        DEVELOPER: Implement using historical GFS data + known outcomes.
        """
        return {
            "days":              days,
            "total_positions":   0,
            "accuracy":          0.0,
            "avg_edge":          0.0,
            "no_win_rate":       0.0,
            "simulated_pnl":     0.0,
        }


class WeatherScanner:
    """
    Scans Polymarket for temperature markets and finds opportunities.
    """

    # GAMMA_URL = "https://gamma-api.polymarket.com/markets"
    GAMMA_URL = "https://gamma-api.polymarket.com/public-search"

    # Keywords that identify weather/temperature markets
    KEYWORDS = [
        "highest temperature",
        "will the temperature",
        "temperature in",
    ]

    def __init__(self, config: dict):
        self.config = config
        self.paper_trade = config.get("paper_trade", True)
        self.model  = WeatherModel()
        self._ws_feed = None  # Polymarket CLOB Market channel (optional)
        self._market_token_cache: dict[str, tuple[str, str]] = {}
        self._ws_opp_cache: dict[str, Opportunity] = {}
        self._ws_trade_executor: Optional[Callable[[Opportunity], Awaitable[None]]] = None
        self._ws_last_trade_mono: dict[str, float] = {}
        # CLOB token_id → last scan gate card (mirrored on YES and NO id for the UI price table).
        self._ticket_evidence: dict[str, dict[str, Any]] = {}

    async def _collect_ws_asset_ids(self, events: list[dict]) -> list[str]:
        """Gather YES/NO clob token IDs from events for WebSocket subscription."""
        ids: list[str] = []
        skipped = 0
        for event in events:
            if not isinstance(event, dict):
                continue
            for m in event.get("markets", []) or []:
                if not isinstance(m, dict) or not self._is_open_event(m):
                    continue
                y = self._get_token_id(m, "yes")
                n = self._get_token_id(m, "no")
                # Fallback: many public-search payloads omit token IDs; hydrate by slug.
                if (not y or not n) and m.get("slug"):
                    fy, fn = await self._fetch_token_ids_by_slug(str(m.get("slug")))
                    if not y:
                        y = fy
                    if not n:
                        n = fn
                if y:
                    ids.append(y)
                else:
                    skipped += 1
                if n:
                    ids.append(n)
                else:
                    skipped += 1
        # Keep first-seen order, de-duplicate IDs.
        unique = list(dict.fromkeys(ids))
        log.info(
            "Weather WS asset-id collection: events=%d ids=%d unique=%d skipped=%d",
            len(events),
            len(ids),
            len(unique),
            skipped,
        )
        return unique

    def set_ws_trade_executor(self, fn: Optional[Callable[[Opportunity], Awaitable[None]]]) -> None:
        """Bot wires OrderManager path here for WS-triggered trades (same execute() as main loop)."""
        self._ws_trade_executor = fn

    async def _merge_ws_pending_markets(self, events: list[dict], feed: Any) -> None:
        """Append Gamma-hydrated events for weather-shaped WS `new_market` slugs (main workflow discovery)."""
        pending = feed.drain_weather_pending_slugs() if feed else []
        if not pending:
            return
        seen = {str(e.get("slug") or "") for e in events if isinstance(e, dict)}
        for slug in pending:
            if not slug or slug in seen:
                continue
            payload = await self._fetch_market_payload_by_slug(slug)
            if not isinstance(payload, dict):
                continue
            shell = self._event_shell_from_market(payload)
            if self._is_open_event(shell) and self._is_weather_event(shell):
                events.append(shell)
                seen.add(slug)
                log.info(
                    "Weather WS pending merged into scan title=%r slug=%r",
                    (shell.get("title") or "")[:120],
                    slug,
                )

    async def _fetch_market_payload_by_slug(self, slug: str) -> Optional[dict]:
        if not slug:
            return None
        gamma = self.config.get("polymarket_gamma_url", "https://gamma-api.polymarket.com")
        url = f"{gamma}/markets/slug/{slug}"
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return None
                    return await resp.json()
        except Exception as e:
            log.debug("Gamma slug fetch failed slug=%s err=%s", slug, e)
            return None

    def _event_shell_from_market(self, m: dict) -> dict:
        """Wrap a single Gamma market dict into an event-shaped container for _evaluate."""
        q = m.get("question") or m.get("title") or ""
        slug = m.get("slug") or ""
        return {
            "title": q,
            "slug": slug,
            "active": m.get("active", True),
            "closed": m.get("closed", False),
            "archived": m.get("archived", False),
            "ended": m.get("ended", False),
            "endDate": m.get("endDate") or "",
            "conditionId": m.get("conditionId", ""),
            "markets": [m],
        }

    def _register_ws_asset_displays(self, feed: Any, events: list[dict]) -> None:
        if not feed:
            return
        for event in events:
            if not isinstance(event, dict):
                continue
            etitle = str(event.get("title") or "")
            eslug = str(event.get("slug") or "")
            for m in event.get("markets", []) or []:
                if not isinstance(m, dict):
                    continue
                title = str(m.get("question") or m.get("title") or etitle).strip()
                slug = str(m.get("slug") or eslug).strip()
                cid = str(m.get("conditionId") or m.get("id") or "").strip()
                if not slug and cid:
                    slug = f"cond-{_short_condition_slug(cid)}"
                if not title:
                    title = slug or "Weather market"
                if not slug:
                    slug = "weather-market"
                y = self._get_token_id(m, "yes")
                n = self._get_token_id(m, "no")
                if y:
                    feed.set_asset_display(y, title=title, slug=slug, outcome="yes")
                if n:
                    feed.set_asset_display(n, title=title, slug=slug, outcome="no")

    def _emit_monitor_weather_discovery(self, events: list[dict]) -> None:
        """Push Gamma/public-search weather events to the web dashboard (titles, slugs, counts)."""
        try:
            from core.monitor_hub import emit as mon_emit
            from core.monitor_hub import is_enabled
        except Exception:
            return
        if not is_enabled(self.config):
            return
        rows: list[dict[str, Any]] = []
        for ev in events[:120]:
            if not isinstance(ev, dict):
                continue
            mkts = ev.get("markets") or []
            n_m = len(mkts) if isinstance(mkts, list) else 0
            rows.append(
                {
                    "title": str(ev.get("title") or "")[:240],
                    "slug": str(ev.get("slug") or "")[:160],
                    "market_count": n_m,
                    "active": bool(ev.get("active", True)),
                    "endDate": str(ev.get("endDate") or "")[:32],
                }
            )
        mon_emit("weather_discovery", event_count=len(events), events=rows)

    async def _ws_on_quote_batch(self, asset_ids: set[str]) -> None:
        """Realtime: re-check cached opportunities when WS quotes move; optional execute."""
        feed = self._ws_feed
        if not feed or not asset_ids:
            return
        min_edge = float(self.config.get("min_edge", 0.0))
        min_conf = float(self.config.get("min_confidence", 0.0))
        cooldown = float(self.config.get("weather_ws_trade_cooldown_sec", 4.0))
        now = time.monotonic()
        touched: set[str] = set()
        for aid in asset_ids:
            for key, opp in self._ws_opp_cache.items():
                if aid in (opp.yes_token_id, opp.no_token_id):
                    touched.add(key)
        for key in touched:
            opp0 = self._ws_opp_cache.get(key)
            if not opp0:
                continue
            if now - self._ws_last_trade_mono.get(key, 0.0) < cooldown:
                continue
            y_ask, n_ask = feed.get_buy_asks(opp0.yes_token_id, opp0.no_token_id)
            no_price = n_ask if n_ask is not None else opp0.no_price
            yes_price = y_ask if y_ask is not None else opp0.yes_price
            if no_price is None or yes_price is None:
                continue
            edge = float(opp0.our_prob_no) - float(no_price)
            updated = replace(opp0, no_price=float(no_price), yes_price=float(yes_price), edge=edge)
            if edge < min_edge or float(opp0.confidence) < min_conf:
                continue
            log.info(
                "Weather WS edge OK title=%r slug=%r edge=%.4f conf=%.2f no_ask=%.4f yes_ask=%.4f",
                (updated.title or "")[:120],
                updated.slug,
                edge,
                float(opp0.confidence),
                float(no_price),
                float(yes_price),
            )
            if self._ws_trade_executor:
                self._ws_last_trade_mono[key] = now
                await self._ws_trade_executor(updated)

    def _pair_ticket_evidence(self, yes_tid: str, no_tid: str, card: dict[str, Any]) -> None:
        """Attach the same gate/evidence dict to both outcome token ids (monitor price rows)."""
        yt, nt = str(yes_tid or "").strip(), str(no_tid or "").strip()
        if not yt and not nt:
            return
        safe = {str(k): v for k, v in card.items() if v is not None}
        if yt:
            self._ticket_evidence[yt] = safe
        if nt:
            self._ticket_evidence[nt] = safe

    async def scan(self) -> list[Opportunity]:
        """Returns all valid weather opportunities."""
        self._ticket_evidence = {}
        if self.paper_trade:
            log.info(f"********* Weather: fetching markets in paper trade mode *********")
        events = await self._fetch_markets()

        if self.paper_trade:
            log.info(f"\n\nWeather: fetched {len(events)} weather events\n\n")

        self._emit_monitor_weather_discovery(events)

        self._ws_feed = None
        if self.config.get("polymarket_ws_enabled", True):
            try:
                self._ws_feed = await get_shared_market_ws_feed(self.config)
                await self._merge_ws_pending_markets(events, self._ws_feed)
                self._ws_feed.apply_ticket_evidence_map({})
                asset_ids = await self._collect_ws_asset_ids(events)
                log.info(
                    "Weather WS enabled=%s feed_running=%s asset_ids=%d events=%d",
                    self.config.get("polymarket_ws_enabled", True),
                    bool(getattr(self._ws_feed, "running", False)),
                    len(asset_ids),
                    len(events),
                )
                # Labels must exist BEFORE subscribe so WS book/tick handlers can emit to the dashboard.
                self._register_ws_asset_displays(self._ws_feed, events)
                if asset_ids:
                    log.info("Weather WS subscribing (sample ids): %s", asset_ids[:6])
                    await self._ws_feed.subscribe(asset_ids)
                    # Brief wait for initial book / best_bid_ask before snapshotting the dashboard.
                    await asyncio.sleep(
                        float(self.config.get("polymarket_ws_after_subscribe_sleep", 0.25)) + 0.4
                    )
                else:
                    log.warning("Weather WS: no valid asset IDs found; realtime price logs will be empty.")
            except Exception as e:
                log.warning("Weather WS feed unavailable, using REST prices only: %s", e)
                self._ws_feed = None

        opps: list[Opportunity] = []
        new_cache: dict[str, Opportunity] = {}
        for event in events:
            opp = await self._evaluate(event)
            if opp:
                opps.append(opp)
                ck = str(opp.slug or opp.condition_id or "")
                if ck:
                    new_cache[ck] = opp

        feed = self._ws_feed
        try:
            from core.monitor_hub import is_enabled as _mon_on
        except Exception:
            _mon_on = lambda _c: False
        if feed and _mon_on(self.config):
            feed.apply_ticket_evidence_map(self._ticket_evidence)
            feed.emit_dashboard_price_snapshot()

        self._ws_opp_cache = new_cache
        if feed and self.config.get("weather_ws_monitor", True):
            feed.set_quote_batch_handler(self._ws_on_quote_batch)
        elif feed:
            feed.set_quote_batch_handler(None)

        opps.sort(key=lambda o: o.edge, reverse=True)
        return opps

    def _is_open_event(self, obj: dict) -> bool:
        return (
            bool(obj.get("active", True))
            and not bool(obj.get("closed", False))
            and not bool(obj.get("archived", False))
            and not bool(obj.get("ended", False))
        )

    def _is_weather_event(self, e: dict) -> bool:
        text = f"{e.get('title', '')} {e.get('slug', '')}".lower()
        return any(kw in text for kw in self.KEYWORDS)

    def _extract_prices(self, m: dict) -> tuple[Optional[float], Optional[float]]:
        # Primary shape: tokens=[{"outcome":"Yes","price":...}, {"outcome":"No","price":...}]
        tokens = m.get("tokens", [])
        if isinstance(tokens, list) and tokens and isinstance(tokens[0], dict):
            yes_p = no_p = None
            for t in tokens:
                outcome = str(t.get("outcome", "")).lower()
                price = t.get("price")
                if price is None:
                    continue
                try:
                    p = float(price)
                except (TypeError, ValueError):
                    continue
                if outcome == "yes":
                    yes_p = p
                elif outcome == "no":
                    no_p = p
            if yes_p is not None and no_p is not None:
                return no_p, yes_p
        # Fallback shape in your sample: outcomes=["Yes","No"], outcomePrices=["0.31","0.69"]
        outcomes = m.get("outcomes", [])
        prices = m.get("outcomePrices", [])
        if (
            isinstance(outcomes, list)
            and isinstance(prices, list)
            and len(outcomes) == len(prices)
            and len(outcomes) >= 2
        ):
            yes_p = no_p = None
            for i, out in enumerate(outcomes):
                try:
                    p = float(prices[i])
                except (TypeError, ValueError):
                    continue
                o = str(out).lower()
                if o == "yes":
                    yes_p = p
                elif o == "no":
                    no_p = p
            if yes_p is not None and no_p is not None:
                return no_p, yes_p
        # Last fallback: assume [Yes, No] order if present
        if isinstance(prices, list) and len(prices) >= 2:
            try:
                yes_p = float(prices[0])
                no_p = float(prices[1])
                return no_p, yes_p
            except (TypeError, ValueError):
                pass
        return None, None


    async def _fetch_markets(self) -> list[dict]:
        """Fetch active weather markets from Polymarket (public-search with pagination).

        Without ``limit_per_type``, Gamma returns only a handful of rows (~6), which makes
        the scanner look \"blind\" while the website lists dozens of temperature events.
        """
        q = str(self.config.get("weather_search_query", "temperature"))
        limit = max(10, min(500, int(self.config.get("weather_search_limit_per_type", 100))))
        max_pages = max(1, min(50, int(self.config.get("weather_search_max_pages", 20))))

        collected: list[dict] = []
        seen: set[str] = set()
        pages_done = 0

        try:
            async with aiohttp.ClientSession() as session:
                for page in range(max_pages):
                    pages_done = page + 1
                    params = {
                        "q": q,
                        "optimized": "true",
                        "limit_per_type": str(limit),
                        "page": str(page),
                    }
                    async with session.get(
                        self.GAMMA_URL,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as resp:
                        if resp.status != 200:
                            log.error("Weather market fetch error: HTTP %s", resp.status)
                            break
                        data = await resp.json()

                    if isinstance(data, dict) and isinstance(data.get("events"), list):
                        batch = data["events"]
                    elif isinstance(data, list):
                        batch = data
                    else:
                        break

                    if not batch:
                        break

                    for e in batch:
                        if not isinstance(e, dict):
                            continue
                        if not self._is_open_event(e) or not self._is_weather_event(e):
                            continue
                        key = str(e.get("id") or "") or str(e.get("slug") or "")
                        if not key:
                            key = str(hash(json.dumps(e.get("title", ""), sort_keys=True)))
                        if key in seen:
                            continue
                        seen.add(key)
                        collected.append(e)

                    if not (isinstance(data, dict) and data.get("hasMore")):
                        break

            log.info(
                "Weather discovery: q=%r pages_fetched=%d limit_per_type=%d -> events=%d",
                q,
                pages_done,
                limit,
                len(collected),
            )
            return collected

        except aiohttp.ClientError as e:
            log.warning("Weather Gamma network error (DNS/TLS/HTTP): %s", e)
            return []
        except Exception as e:
            log.error("Weather market fetch error: %s", e)
            return []

    def _parse_token_pair(self, raw: object) -> tuple[str, str]:
        """
        Parse yes/no token IDs from multiple common shapes:
        - ["yes_id", "no_id"]
        - "[\"yes_id\",\"no_id\"]"
        - "yes_id,no_id"
        """
        data = raw
        if isinstance(data, str):
            s = data.strip()
            if not s:
                return "", ""
            if s.startswith("["):
                try:
                    data = json.loads(s)
                except Exception:
                    data = s
            if isinstance(data, str):
                parts = [p.strip().strip("\"'") for p in data.split(",") if p.strip()]
                if len(parts) >= 2:
                    return str(parts[0]), str(parts[1])
                return "", ""
        if isinstance(data, list) and len(data) >= 2:
            return str(data[0]), str(data[1])
        return "", ""

    async def _fetch_token_ids_by_slug(self, slug: str) -> tuple[str, str]:
        if not slug:
            return "", ""
        cached = self._market_token_cache.get(slug)
        if cached:
            return cached

        gamma = self.config.get("polymarket_gamma_url", "https://gamma-api.polymarket.com")
        url = f"{gamma}/markets/slug/{slug}"
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        return "", ""
                    data = await resp.json()
        except Exception:
            return "", ""

        yes, no = self._parse_token_pair(data.get("clobTokenIds"))
        if not yes or not no:
            y2, n2 = self._parse_token_pair(data.get("clob_token_ids"))
            yes = yes or y2
            no = no or n2
        if not yes or not no:
            y3, n3 = self._parse_token_pair(data.get("assets_ids"))
            yes = yes or y3
            no = no or n3

        if yes and no:
            self._market_token_cache[slug] = (yes, no)
            return yes, no
        return "", ""

    async def _evaluate(self, event: dict) -> Optional[Opportunity]:
        """
        Strict evaluator:
        - Event and child market must be truly open (strict booleans, no permissive defaults)
        - Must not be expired by endDate
        - Must look tradable (prices + token ids + non-zero book signal)
        - Must pass model confidence/edge thresholds
        Returns the best child market (highest edge) as one Opportunity, else None.
        """

        def _parse_dt_utc(s: str) -> Optional[datetime]:
            if not s:
                return None
            try:
                return datetime.fromisoformat(str(s).replace("Z", "+00:00")).astimezone(timezone.utc)
            except Exception:
                return None

        def _not_expired(obj: dict) -> bool:
            end_s = obj.get("endDate")
            if not end_s:
                # Gamma often omits endDate on nested markets; parent carries resolution time.
                return True
            end_dt = _parse_dt_utc(end_s)
            if end_dt is None:
                return True
            return end_dt > datetime.now(timezone.utc)

        def _has_live_quote(m: dict) -> bool:
            # Prefer explicit best bid/ask if present
            try:
                ba = m.get("bestAsk")
                bb = m.get("bestBid")
                if ba is not None and float(ba) > 0:
                    return True
                if bb is not None and float(bb) > 0:
                    return True
            except Exception:
                pass
            # Fallback: outcomePrices imply live quote if any side strictly between 0 and 1
            prices = m.get("outcomePrices", [])
            if isinstance(prices, list):
                for p in prices:
                    try:
                        fp = float(p)
                        if 0.0 < fp < 1.0:
                            return True
                    except Exception:
                        continue
            return False

        # ---------- Event-level strict gates ----------
        if not isinstance(event, dict):
            return None
        if not self._is_open_event(event):
            return None
            
        event_title = event.get("title") or ""
        event_slug = event.get("slug") or ""
        text = f"{event_title} {event_slug}".lower()
        if not any(kw in text for kw in self.KEYWORDS):
            return None
        # Event may have endDate; if present, enforce not expired
        ev_end = event.get("endDate")
        
        if ev_end and not _not_expired(event):
            return None
        # ---------- Child market scan ----------
        best_opp: Optional[Opportunity] = None
        best_edge = float("-inf")
        markets = event.get("markets", [])
        if not isinstance(markets, list):
            return None
        min_conf_gate = float(self.config["min_confidence"])
        min_edge_gate = float(self.config["min_edge"])

        for m in markets:
            if not isinstance(m, dict):
                continue
            # Strict open + not expired at child level
            if not self._is_open_event(m):
                continue
            if not _not_expired(m if m.get("endDate") else event):
                continue
            title = m.get("question") or m.get("title") or ""
            group_t = str(m.get("groupItemTitle") or "")
            if not title and not group_t:
                continue
            blob_kw = f"{event_title} {title} {group_t}".lower()
            if not any(kw in blob_kw for kw in self.KEYWORDS):
                continue

            no_token_id = self._get_token_id(m, "no")
            yes_token_id = self._get_token_id(m, "yes")
            if not no_token_id or not yes_token_id:
                continue

            parsed = self._parse_title(f"{title} {group_t}".strip())
            if not parsed:
                self._pair_ticket_evidence(
                    yes_token_id,
                    no_token_id,
                    {
                        "ticket_status": "rejected",
                        "reject_reason": "parse_mismatch",
                        "ticket_detail": (
                            "Question + groupItemTitle did not resolve to a known city "
                            "and a °F/°C threshold (model cannot price this row)."
                        ),
                    },
                )
                continue

            city, threshold, direction, unit = parsed

            # Price extraction + sanity checks (prefer CLOB WebSocket best ask)
            no_price, yes_price = self._extract_prices(m)
            ws_feed = getattr(self, "_ws_feed", None)
            if ws_feed:
                y_ask, n_ask = ws_feed.get_buy_asks(yes_token_id, no_token_id)
                if n_ask is not None:
                    no_price = n_ask
                if y_ask is not None:
                    yes_price = y_ask
            if no_price is None or yes_price is None:
                self._pair_ticket_evidence(
                    yes_token_id,
                    no_token_id,
                    {
                        "ticket_status": "rejected",
                        "reject_reason": "missing_quotes",
                        "ticket_detail": "NO or YES price missing from REST/WS — cannot compare model to market.",
                        "no_ask": no_price,
                        "yes_ask": yes_price,
                        "city": city,
                    },
                )
                continue
            if not (0.0 < no_price < 1.0 and 0.0 < yes_price < 1.0):
                self._pair_ticket_evidence(
                    yes_token_id,
                    no_token_id,
                    {
                        "ticket_status": "rejected",
                        "reject_reason": "bad_prices",
                        "ticket_detail": (
                            f"Prices must be strictly between 0 and 1 for CLOB math; "
                            f"got NO={no_price}, YES={yes_price}."
                        ),
                        "no_ask": no_price,
                        "yes_ask": yes_price,
                        "city": city,
                    },
                )
                continue

            if not _has_live_quote(m):
                self._pair_ticket_evidence(
                    yes_token_id,
                    no_token_id,
                    {
                        "ticket_status": "rejected",
                        "reject_reason": "no_live_quote",
                        "ticket_detail": (
                            "No usable bid/ask or in-range outcomePrices — treated as not actively tradable."
                        ),
                        "no_ask": no_price,
                        "yes_ask": yes_price,
                        "city": city,
                    },
                )
                continue

            # Model inference (optional calendar day from title, e.g. "... on April 19?")
            event_date = _parse_event_date_from_title(f"{title} {group_t}".strip())
            threshold_f = threshold * 9 / 5 + 32 if unit == "C" else threshold
            prob_no, confidence = await self.model.get_probability_async(
                city, threshold_f, direction, event_date=event_date
            )
            if confidence < min_conf_gate:
                self._pair_ticket_evidence(
                    yes_token_id,
                    no_token_id,
                    {
                        "ticket_status": "rejected",
                        "reject_reason": "confidence",
                        "ticket_detail": (
                            f"Ensemble confidence {confidence:.3f} < min_confidence {min_conf_gate:.3f} "
                            f"(wide spread / low model trust this run)."
                        ),
                        "no_ask": no_price,
                        "yes_ask": yes_price,
                        "our_prob_no": prob_no,
                        "model_confidence": confidence,
                        "min_confidence": min_conf_gate,
                        "city": city,
                    },
                )
                continue

            edge = prob_no - no_price
            if edge < min_edge_gate:
                self._pair_ticket_evidence(
                    yes_token_id,
                    no_token_id,
                    {
                        "ticket_status": "rejected",
                        "reject_reason": "edge",
                        "ticket_detail": (
                            f"Edge P(model NO)−NO_ask = {edge:+.4f} < min_edge {min_edge_gate:.4f}. "
                            f"Model P(NO)={prob_no:.4f}, market NO ask={no_price:.4f}."
                        ),
                        "no_ask": no_price,
                        "yes_ask": yes_price,
                        "our_prob_no": prob_no,
                        "model_confidence": confidence,
                        "edge": edge,
                        "min_edge": min_edge_gate,
                        "city": city,
                    },
                )
                continue

            liquidity = await self._get_order_book_depth(no_token_id)
            opp = Opportunity(
                domain="weather",
                slug=m.get("slug", event.get("slug", "")),
                title=title,
                condition_id=m.get("conditionId", event.get("conditionId", "")),
                no_token_id=no_token_id,
                yes_token_id=yes_token_id,
                no_price=no_price,
                yes_price=yes_price,
                our_prob_no=prob_no,
                edge=edge,
                confidence=confidence,
                end_date=m.get("endDate", event.get("endDate", "")),
                domain_meta={
                    "city": city,
                    "threshold": threshold,
                    "direction": direction,
                    "unit": unit,
                    "event_date": event_date.isoformat() if event_date else None,
                    "event_id": event.get("id"),
                    "group_item_title": m.get("groupItemTitle"),
                    "bestAsk": m.get("bestAsk"),
                    "bestBid": m.get("bestBid"),
                },
                available_liquidity=liquidity,
            )
            if edge > best_edge:
                if best_opp is not None:
                    prev_e = float(best_opp.edge)
                    self._pair_ticket_evidence(
                        best_opp.yes_token_id,
                        best_opp.no_token_id,
                        {
                            "ticket_status": "rejected",
                            "reject_reason": "not_best_in_event",
                            "ticket_detail": (
                                f"Passed gates but another bracket beat it this scan "
                                f"(edge was {prev_e:.4f}; higher edge exists in the same event)."
                            ),
                            "no_ask": float(best_opp.no_price),
                            "yes_ask": float(best_opp.yes_price),
                            "our_prob_no": float(best_opp.our_prob_no),
                            "model_confidence": float(best_opp.confidence),
                            "edge": prev_e,
                        },
                    )
                best_edge = edge
                best_opp = opp
            else:
                self._pair_ticket_evidence(
                    yes_token_id,
                    no_token_id,
                    {
                        "ticket_status": "rejected",
                        "reject_reason": "not_best_in_event",
                        "ticket_detail": (
                            f"Passed gates but edge {edge:.4f} is not the best in this event "
                            f"(best edge here {best_edge:.4f})."
                        ),
                        "no_ask": no_price,
                        "yes_ask": yes_price,
                        "our_prob_no": prob_no,
                        "model_confidence": confidence,
                        "edge": edge,
                        "city": city,
                    },
                )

        if best_opp is not None:
            self._pair_ticket_evidence(
                best_opp.yes_token_id,
                best_opp.no_token_id,
                {
                    "ticket_status": "selected",
                    "reject_reason": "",
                    "ticket_detail": (
                        "Best weather ticket in this Gamma event for this scan "
                        f"(edge {float(best_opp.edge):.4f}, conf {float(best_opp.confidence):.3f})."
                    ),
                    "no_ask": float(best_opp.no_price),
                    "yes_ask": float(best_opp.yes_price),
                    "our_prob_no": float(best_opp.our_prob_no),
                    "model_confidence": float(best_opp.confidence),
                    "edge": float(best_opp.edge),
                    "min_edge": min_edge_gate,
                    "min_confidence": min_conf_gate,
                    "city": str((best_opp.domain_meta or {}).get("city") or ""),
                },
            )
        return best_opp

    async def _get_order_book_depth(self, token_id: str) -> Optional[float]:
        """
        Return immediately-tradable depth (in shares) for BUYing this token.

        We use ASK side depth because BUY market orders consume asks.
        Conservative approach: sum only first N ask levels.
        """
        if not token_id:
            return None

        # Lazy-create a public (L0) CLOB client once
        if not hasattr(self, "_clob_public_client"):
            from py_clob_client.client import ClobClient
            self._clob_public_client = ClobClient(
                host=self.config["polymarket_clob_url"]
            )

        try:
            # py-clob-client is sync; run in thread so scanner remains non-blocking
            book = await asyncio.to_thread(
                self._clob_public_client.get_order_book,
                token_id,
            )
        except Exception as e:
            log.debug(f"Order book fetch failed for {token_id[:12]}...: {e}")
            return None

        asks = getattr(book, "asks", None) or []
        if not asks:
            return 0.0

        max_levels = int(self.config.get("weather_depth_levels", 5))
        depth = 0.0

        for level in asks[:max_levels]:
            try:
                sz = float(level.size)
                if sz > 0:
                    depth += sz
            except (TypeError, ValueError):
                continue

        return depth

    def _parse_title(self, title: str) -> Optional[tuple]:
        """
        Parses market title to extract:
        (city, threshold, direction, unit)

        Examples:
          "Will the highest temperature in Dallas be 68°F or higher on March 28?"
          → ("Dallas", 68.0, "above", "F")

          "Will the highest temperature in Ankara be 10°C or higher on March 1?"
          → ("Ankara", 10.0, "above", "C")

          "Will the highest temperature in Seoul be 14°C on March 28?"
          → ("Seoul", 14.0, "exact", "C")

          "Will the highest temperature in NYC be 68°F or higher on April 18?"
          → ("New York", 68.0, "above", "F")   # via alias
        """
        t = _normalize_title_for_parse(title)

        # Extract city (longest name first so \"Hong Kong\" beats \"Kong\" if ever ambiguous)
        city = None
        for c in sorted(self.model.CITY_COORDS.keys(), key=len, reverse=True):
            if c.lower() in t:
                city = c
                break
        if not city:
            return None

        # Extract threshold and unit (degree sign optional — API often sends plain \"68f\")
        f_match = re.search(r'(\d+(?:\.\d+)?)\s*°?\s*f', t)
        c_match = re.search(r'(-?\d+(?:\.\d+)?)\s*°?\s*c', t)

        if f_match:
            threshold, unit = float(f_match.group(1)), "F"
        elif c_match:
            threshold, unit = float(c_match.group(1)), "C"
        else:
            return None

        # Extract direction
        if "or higher" in t or "or above" in t:
            direction = "above"
        elif "or lower" in t or "or below" in t:
            direction = "below"
        else:
            direction = "exact"

        return city, threshold, direction, unit

    def _get_token_price(self, market: dict, side: str) -> Optional[float]:
        tokens = market.get("tokens", []) or market.get("outcomes", [])
        for token in tokens:
            outcome = (token.get("outcome") or "").lower()
            if side == "no" and outcome == "no":
                return float(token.get("price", 0))
            if side == "yes" and outcome == "yes":
                return float(token.get("price", 0))
        # Fallback: outright_price fields
        if side == "no":
            return market.get("outcomePrices", [None, None])[1]
        return market.get("outcomePrices", [None, None])[0]

    def _get_token_id(self, market: dict, side: str) -> str:
        # Preferred shapes for Polymarket CLOB IDs.
        yes, no = self._parse_token_pair(market.get("clobTokenIds"))
        if yes and no:
            return no if side == "no" else yes
        yes, no = self._parse_token_pair(market.get("clob_token_ids"))
        if yes and no:
            return no if side == "no" else yes
        yes, no = self._parse_token_pair(market.get("assets_ids"))
        if yes and no:
            return no if side == "no" else yes

        # Fallback shape:
        # tokens: [{"outcome":"Yes", "token_id"/"asset_id"/"id": ...}, ...]
        tokens = market.get("tokens", [])
        if isinstance(tokens, list) and tokens and isinstance(tokens[0], dict):
            wanted = "no" if side == "no" else "yes"
            for t in tokens:
                outcome = str(t.get("outcome", "")).lower()
                if outcome != wanted:
                    continue
                tid = (
                    t.get("token_id")
                    or t.get("asset_id")
                    or t.get("clobTokenId")
                    or t.get("id")
                )
                if tid is not None:
                    return str(tid)

        return ""
