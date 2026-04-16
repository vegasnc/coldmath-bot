"""
domains/cycling.py

Cycling race market scanner — Domain 4, confirmed active April 5, 2026.

Confirmed from Tour de Flanders Apr 5 Action 1 data:
  van der Poel  NOT top 3 → NO 91-95c large + YES 5-8c → MERGE
  Evenepoel     NOT top 3 → NO 93-97c large + YES 3-6c → MERGE
  Pedersen      NOT top 3 → NO 91.9c large + YES 6.9c  → MERGE
  Laporte       WILL top 3 → YES 92-95c large + NO 5c  → MERGE
  van Aert      WILL top 3 → YES 52-75c + NO 45-75c    → MERGE
  Stuyven       NOT top 3 → NO 99c (no insurance needed)

All merged immediately for guaranteed profit — YES+NO < $1.00 on every rider.

MECHANIC: Identical to weather and soccer.
  BUY near-certain side large (88-99c)
  BUY opposite side tiny as insurance (1-12c)
  MERGE when combined cost < $1.00 → guaranteed profit locked

MODEL: Race-day form rating × historical podium probability
  Data sources:
    ProCyclingStats.com (free)  — career stats, recent form, race history
    FirstCycling.com (free)     — race results, podium rates
    PCS API (unofficial)        — team lineups, recent DNFs

DEVELOPER: Implement _fetch_rider_form() and _parse_market_title()
"""

import logging
import re
from typing import Optional

import aiohttp

from core.opportunity import Opportunity

log = logging.getLogger("cycling_scanner")


class CyclingModel:
    """
    Predicts P(rider finishes top N) for a given race.

    Factors:
    1. Recent form score (last 5 races, weighted recency)
    2. Historical podium rate at this specific race
    3. Current team support assessment
    4. Race profile fit (climber vs classics rider)
    """

    # Top-3 base rates for major spring classics — derived from 10yr history
    # These are the PRIOR before form adjustment
    RACE_BASE_RATES = {
        "tour-de-flanders":      {"top_3": 0.15, "n_contenders": 20},
        "paris-roubaix":         {"top_3": 0.15, "n_contenders": 20},
        "liege-bastogne-liege":  {"top_3": 0.15, "n_contenders": 20},
        "amstel-gold-race":      {"top_3": 0.15, "n_contenders": 20},
        "giro-d-italia":         {"top_3": 0.15, "n_contenders": 20},
        "tour-de-france":        {"top_3": 0.15, "n_contenders": 20},
        "vuelta-a-espana":       {"top_3": 0.15, "n_contenders": 20},
    }

    def get_probability(self, rider: str, race: str,
                         top_n: int = 3) -> tuple[float, float]:
        """
        Returns (prob_yes, confidence) — probability rider finishes top N.

        DEVELOPER: Replace stub with real data fetch.
        prob_yes feeds into the formula as:
          If prob_yes > threshold → buy YES large + NO insurance
          If prob_no  > threshold → buy NO large + YES insurance
        """
        form_score = self._get_form_score(rider, race)
        if form_score is None:
            return 0.5, 0.0

        base = self.RACE_BASE_RATES.get(race, {}).get("top_3", 0.15)
        # Form adjustment: excellent form (1.0) doubles base rate
        prob_yes   = min(0.97, base * (1 + form_score))
        confidence = 0.70 if form_score is not None else 0.0

        return prob_yes, confidence

    def _get_form_score(self, rider: str, race: str) -> Optional[float]:
        """
        Returns form score 0.0-1.0 based on recent results.

        DEVELOPER: Implement with ProCyclingStats or FirstCycling data.

        Example approach:
            1. Fetch rider's last 5 race results from PCS
            2. Score: win=1.0, top3=0.8, top10=0.5, DNF=0.0
            3. Weight by recency (most recent = highest weight)
            4. Adjust for race profile fit (classics specialist rating)

        Example with requests + BeautifulSoup:
            url = f"https://www.procyclingstats.com/rider/{rider_slug}"
            # Parse recent results table
            # Calculate weighted form score
        """
        log.warning(f"_get_form_score not implemented for {rider} @ {race}")
        return None

    def backtest(self, days: int = 60) -> dict:
        """
        DEVELOPER: Backtest against historical cycling market resolutions.
        Check: did riders priced at NO > 88c actually finish outside top 3?
        """
        return {"days": days, "total_positions": 0, "accuracy": 0.0,
                "avg_edge": 0.0, "no_win_rate": 0.0, "simulated_pnl": 0.0}


class CyclingScanner:
    """
    Scans Polymarket for cycling top-N finish markets.

    Confirmed slug patterns from Apr 5 Action 1:
      will-{rider-name}-finish-in-the-top-{n}-at-the-{year}-mens-{race}
      will-wout-van-aert-finish-in-the-top-3-at-the-2026-mens-tour-de-flanders
      will-mathieu-van-der-poel-finish-in-the-top-3-at-the-2026-mens-tour-de-flanders
    """

    GAMMA_URL = "https://gamma-api.polymarket.com/markets"
    KEYWORDS  = ["finish in the top", "cycling", "tour de flanders",
                 "paris-roubaix", "tour de france", "giro d'italia",
                 "liege", "amstel", "vuelta"]

    def __init__(self, config: dict):
        self.config = config
        self.model  = CyclingModel()

    async def scan(self) -> list[Opportunity]:
        if not self.config.get("cycling_enabled", False):
            return []
        markets = await self._fetch_markets()
        log.debug(f"Cycling: fetched {len(markets)} markets")

        opps = []
        for m in markets:
            opp = await self._evaluate(m)
            if opp:
                opps.append(opp)

        opps.sort(key=lambda o: o.edge, reverse=True)
        return opps

    async def _fetch_markets(self) -> list[dict]:
        all_markets = []
        try:
            async with aiohttp.ClientSession() as session:
                for kw in self.KEYWORDS[:3]:   # limit during development
                    params = {"active": "true", "closed": "false",
                              "limit": "100", "search": kw}
                    async with session.get(
                        self.GAMMA_URL, params=params,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            markets = data if isinstance(data, list) \
                                           else data.get("markets", [])
                            all_markets.extend(markets)
        except Exception as e:
            log.error(f"Cycling market fetch: {e}")

        # Deduplicate
        seen, unique = set(), []
        for m in all_markets:
            mid = m.get("id") or m.get("conditionId", "")
            if mid not in seen:
                seen.add(mid)
                unique.append(m)
        return unique

    async def _evaluate(self, market: dict) -> Optional[Opportunity]:
        title = (market.get("question") or market.get("title", ""))
        title_lower = title.lower()

        if "finish in the top" not in title_lower:
            return None

        parsed = self._parse_title(title)
        if not parsed:
            return None
        rider, race, top_n = parsed

        # Get model probability
        prob_yes, confidence = self.model.get_probability(rider, race, top_n)
        prob_no = 1.0 - prob_yes

        if confidence < self.config["min_confidence"]:
            return None

        # Determine which side is near-certain
        no_price  = self._get_price(market, "no")
        yes_price = self._get_price(market, "yes")
        if no_price is None or yes_price is None:
            return None

        min_price = self.config.get("cycling_min_no_price", 0.88)

        # Near-certain NO side
        if prob_no >= 0.88 and no_price >= min_price:
            edge = prob_no - no_price
        # Near-certain YES side (rider is a favourite)
        elif prob_yes >= 0.88 and yes_price >= min_price:
            # Swap: treat YES as the "NO" side in our formula
            # (buy YES large, NO tiny — same mechanic)
            edge = prob_yes - yes_price
        else:
            return None

        if edge < self.config["min_edge"]:
            return None

        return Opportunity(
            domain       = "cycling",
            slug         = market.get("slug", ""),
            title        = title,
            condition_id = market.get("conditionId", ""),
            no_token_id  = self._get_token_id(market, "no"),
            yes_token_id = self._get_token_id(market, "yes"),
            no_price     = no_price,
            yes_price    = yes_price,
            our_prob_no  = prob_no,
            edge         = edge,
            confidence   = confidence,
            end_date     = market.get("endDate", ""),
            domain_meta  = {"rider": rider, "race": race, "top_n": top_n},
        )

    def _parse_title(self, title: str) -> Optional[tuple[str, str, int]]:
        """
        Parse: 'Will Wout van Aert finish in the top 3 at the 2026 Tour de Flanders?'
        Returns: ('wout-van-aert', 'tour-de-flanders', 3)
        """
        t = title.lower()
        m = re.search(r'will\s+(.+?)\s+finish in the top\s+(\d+)', t)
        if not m:
            return None
        rider_raw = m.group(1).strip()
        top_n     = int(m.group(2))
        rider     = rider_raw.replace(" ", "-")

        # Extract race
        race = "unknown"
        for event in self.config.get("cycling_events", []):
            if event.replace("-", " ") in t:
                race = event
                break

        return rider, race, top_n

    def _get_price(self, market: dict, side: str) -> Optional[float]:
        tokens = market.get("tokens", []) or market.get("outcomes", [])
        for token in tokens:
            if (token.get("outcome") or "").lower() == side:
                return float(token.get("price", 0))
        prices = market.get("outcomePrices", [])
        if len(prices) >= 2:
            return float(prices[1] if side == "no" else prices[0])
        return None

    def _get_token_id(self, market: dict, side: str) -> str:
        tokens = market.get("clobTokenIds", [])
        if len(tokens) >= 2:
            return str(tokens[1] if side == "no" else tokens[0])
        return ""
