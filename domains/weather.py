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
from typing import Optional

import aiohttp

from core.opportunity import Opportunity
from datetime import datetime, timezone

log = logging.getLogger("weather_scanner")


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
    }

    def get_probability(self, city: str, threshold: float,
                         direction: str) -> tuple[float, float]:
        """
        Returns (prob_no, confidence) synchronously.
        Direction: 'above' | 'below' | 'exact'

        DEVELOPER: Call async version from async context.
        """
        ensemble = self._get_ensemble_sync(city)
        if not ensemble or len(ensemble) < 5:
            return 0.5, 0.0

        return self._calculate(ensemble, threshold, direction)

    async def get_probability_async(self, city: str, threshold: float,
                                     direction: str) -> tuple[float, float]:
        ensemble = await self._fetch_gfs_ensemble(city)

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

    async def _fetch_gfs_ensemble(self, city: str) -> Optional[list[float]]:
        """
        Fetch GFS-like ensemble distribution of daily high temperatures (F).
        Chosen approach: Open-Meteo Ensemble API (Option B).
        - Option A (NOAA GRIB2): more control, but much heavier parser infra.
        - Option C (ECMWF raw): strong quality, but operationally heavier.
        """
        coords = self.CITY_COORDS.get(city)
        if not coords:
            log.debug(f"Unknown city for ensemble fetch: {city}")
            return None
        lat, lon = coords
        url = "https://ensemble-api.open-meteo.com/v1/ensemble"
        # Tunables (optional; safe defaults)
        forecast_days = int(self.__dict__.get("forecast_days", 1))   # 1-35 supported
        eval_hours = int(self.__dict__.get("eval_hours", 24))        # window for "daily high"

        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m",
            "models": "gfs_seamless",          # GFS ensemble members
            "forecast_days": 1,
            "temperature_unit": "fahrenheit",  # model expects F in your scanner flow
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
        # Open-Meteo ensemble response keys look like:
        # temperature_2m_member01 ... temperature_2m_member30
        member_keys = sorted(
            k for k in hourly.keys() if k.startswith("temperature_2m_member")
        )
        if not member_keys:
            # Fallback: no ensemble members returned
            # (do not fake ensemble; better to skip trading than inject synthetic uncertainty)
            base = hourly.get("temperature_2m")
            if isinstance(base, list) and base:
                try:
                    vals = [float(x) for x in base[:max(1, eval_hours)] if x is not None]
                    return [max(vals)] if vals else None
                except Exception:
                    return None
            return None

        ensemble_highs: list[float] = []
        window = max(1, eval_hours)
        for key in member_keys:
            series = hourly.get(key)
            if not isinstance(series, list) or not series:
                continue
            try:
                vals = [float(x) for x in series[:window] if x is not None]
            except (TypeError, ValueError):
                continue
            if not vals:
                continue
            daily_high = max(vals)
            # Basic sanity filter for corrupted values
            if -120.0 <= daily_high <= 140.0:
                ensemble_highs.append(daily_high)
        # Need enough members to form a meaningful probability
        if len(ensemble_highs) < 5:
            return None
        return ensemble_highs


    def _get_ensemble_sync(self, city: str) -> Optional[list[float]]:
        """Sync wrapper — runs in thread pool for sync callers."""
        import asyncio
        try:
            loop = asyncio.get_event_loop()

            data_gfs_ensemble = self._fetch_gfs_ensemble(city)
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

    async def scan(self) -> list[Opportunity]:
        """Returns all valid weather opportunities."""
        if self.paper_trade:
            log.info(f"********* Weather: fetching markets in paper trade mode *********")
        events = await self._fetch_markets()

        if self.paper_trade:
            log.info(f"\n\nWeather: fetched {len(events)} weather events\n\n")

        opps: list[Opportunity] = []
        for event in events:
            opp = await self._evaluate(event)
            if opp:
                opps.append(opp)

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
        """Fetch active weather markets from Polymarket."""
        # params = {
        #     "active":       "true",
        #     "closed":       "false",
        #     "limit":        "500",
        #     "slug":     "weather",
        # }

        params = {
            "q":            "temperature",
            "optimized":    "true"
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.GAMMA_URL, 
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:

                    if resp.status != 200:
                        log.error(f"Weather market fetch error: {resp.status}")
                        return []

                    data = await resp.json()
                    
                    # Expected shape (your sample): {"events": [...]}
                    if isinstance(data, dict) and isinstance(data.get("events"), list):
                        events = data["events"]
                    # Some endpoints return list directly
                    elif isinstance(data, list):
                        events = data
                    else:
                        return []

                    # Only weather + active + unclosed events
                    return [e for e in events if self._is_open_event(e) and self._is_weather_event(e)]

        except Exception as e:
            log.error(f"Weather market fetch error: {e}")
        return []

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
            end_dt = _parse_dt_utc(end_s)
            if end_dt is None:
                # If endDate is missing/unparseable, be conservative and reject
                return False
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
        for m in markets:
            if not isinstance(m, dict):
                continue
            # Strict open + not expired at child level
            if not self._is_open_event(m):
                continue
            if not _not_expired(m if m.get("endDate") else event):
                continue
            title = m.get("question") or m.get("title") or ""
            if not title:
                continue
            if not any(kw in title.lower() for kw in self.KEYWORDS):
                continue
            # Parse weather semantics
            parsed = self._parse_title(title)
            if not parsed:
                continue

            city, threshold, direction, unit = parsed
            # Token IDs required for real tradability/depth checks
            no_token_id = self._get_token_id(m, "no")
            yes_token_id = self._get_token_id(m, "yes")
            if not no_token_id or not yes_token_id:
                continue

            # Price extraction + sanity checks
            no_price, yes_price = self._extract_prices(m)
            if no_price is None or yes_price is None:
                continue
            if not (0.0 < no_price < 1.0 and 0.0 < yes_price < 1.0):
                continue

            # Must look actually tradable now
            if not _has_live_quote(m):
                continue

            # Model inference
            threshold_f = threshold * 9 / 5 + 32 if unit == "C" else threshold
            prob_no, confidence = await self.model.get_probability_async(city, threshold_f, direction)
            if confidence < self.config["min_confidence"]:
                continue

            edge = prob_no - no_price
            if edge < self.config["min_edge"]:
                continue

            # Optional liquidity cap input (None means "unknown")
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
                    "event_id": event.get("id"),
                    "group_item_title": m.get("groupItemTitle"),
                    "bestAsk": m.get("bestAsk"),
                    "bestBid": m.get("bestBid"),
                },
                available_liquidity=liquidity,
            )
            if edge > best_edge:
                best_edge = edge
                best_opp = opp
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
        """
        t = title.lower()

        # Extract city
        city = None
        for c in self.model.CITY_COORDS:
            if c.lower() in t:
                city = c
                break
        if not city:
            return None

        # Extract threshold and unit
        f_match = re.search(r'(\d+(?:\.\d+)?)\s*°?f', t)
        c_match = re.search(r'(-?\d+(?:\.\d+)?)\s*°?c', t)

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
        tokens = market.get("tokens", []) or market.get("clobTokenIds", [])
        if isinstance(tokens, list) and len(tokens) >= 2:
            return str(tokens[1] if side == "no" else tokens[0])
        return ""
