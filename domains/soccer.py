"""
domains/soccer.py

Soccer BTTS + spread market scanner.
Secondary domain — confirmed active March 2026.

Leagues confirmed from Action 1 live data:
  - Turkish Super Lig (tur-*)
  - MLS (mls-*)
  - J2 Japan (j2100-*)
  - Norwegian Eliteserien (nor-*)

Market types confirmed:
  - BTTS (Both Teams to Score) — YES/NO
  - Spread handicaps (-1.5, -2.5 goal lines)

Model: Poisson Expected Goals (xG)
  P(team scores) = 1 - e^(-xG)   [Poisson zero probability]
  P(BTTS) = P(home scores) × P(away scores)

Data source: FBref / Understat / football-data.org (all free)
"""

import math
import logging
import re
from typing import Optional

import aiohttp

from core.opportunity import Opportunity

log = logging.getLogger("soccer_scanner")


class SoccerEdgeDetector:
    """
    Detects when soccer edge is decaying.
    Edge decays when:
    1. League season has fewer than 10 games played (early season)
    2. Final 4 games of season (motivation factors)
    3. Major cup competitions overlapping (squad rotation)
    """

    LEAGUE_TOTAL_GAMES = {
        "Turkish Super Lig":     34,
        "MLS":                   34,
        "J2 Japan":              42,
        "Norwegian Eliteserien": 30,
    }

    def __init__(self):
        self.games_played: dict[str, int] = {}

    def get_confidence(self, league: str) -> float:
        played = self.games_played.get(league, 0)
        total  = self.LEAGUE_TOTAL_GAMES.get(league, 34)
        remaining = total - played

        if played < 6:   return 0.30   # too early — insufficient sample
        if played < 10:  return 0.60   # warming up
        if remaining < 4: return 0.25  # end of season — motivation chaos
        return 1.00

    def update(self, league: str, games: int):
        self.games_played[league] = games


class XGModel:
    """
    Poisson xG model for BTTS and spread probability.
    DEVELOPER: Implement _fetch_team_xg() with FBref/Understat.
    """

    def btts_probability(self, home_xg: float, away_xg: float) -> float:
        """P(both teams score) using independent Poisson distributions."""
        p_home_scores = 1.0 - math.exp(-home_xg)
        p_away_scores = 1.0 - math.exp(-away_xg)
        return p_home_scores * p_away_scores

    def spread_probability(self, fav_xg: float, und_xg: float,
                            goals: float) -> float:
        """
        P(favorite wins by more than 'goals' goals).
        Uses Poisson distribution to enumerate score matrices.
        """
        max_goals = 10
        prob_fav_covers = 0.0

        for fav_g in range(max_goals + 1):
            for und_g in range(max_goals + 1):
                p_fav = self._poisson_pmf(fav_xg, fav_g)
                p_und = self._poisson_pmf(und_xg, und_g)
                if (fav_g - und_g) > goals:
                    prob_fav_covers += p_fav * p_und

        return prob_fav_covers

    def _poisson_pmf(self, lam: float, k: int) -> float:
        """P(X=k) for Poisson(lambda)."""
        return (math.exp(-lam) * lam**k) / math.factorial(k)

    async def get_team_xg(self, team: str, league: str,
                           n_games: int = 10) -> Optional[float]:
        """
        DEVELOPER: Fetch rolling xG for a team from FBref or Understat.

        FBref (free, requires scraping):
            URL: https://fbref.com/en/comps/{league_id}/stats/
            Extract: xG per game for last n_games

        Understat (free JSON API — top 7 European leagues):
            URL: https://understat.com/team/{team_name}/{season}

        football-data.org (free API with key — covers many leagues):
            URL: https://api.football-data.org/v4/competitions/{code}/matches
            Note: Does not provide xG directly — compute from goals as proxy

        Sofascore (scraping required):
            Provides xG for Turkish, Norwegian, MLS, J2

        For Turkish Super Lig / Norwegian / J2 (not in Understat):
            Use Sofascore or Fotmob scraping.
            See: https://github.com/Felixmil/sofascore-api (unofficial)

        Example implementation:
            async with aiohttp.ClientSession() as session:
                url = f"https://understat.com/team/{team}/2025"
                async with session.get(url) as resp:
                    html = await resp.text()
                    # Parse JavaScript data embedded in page
                    match = re.search(r"datesData\\s*=\\s*JSON.parse\('(.+?)'\)", html)
                    dates_data = json.loads(match.group(1).encode().decode('unicode_escape'))
                    recent = dates_data[-n_games:]
                    return sum(float(g['xG']) for g in recent) / len(recent)
        """
        log.warning(f"get_team_xg not implemented for {team}")
        return None

    def backtest(self, days: int = 30) -> dict:
        return {
            "days":            days,
            "total_positions": 0,
            "accuracy":        0.0,
            "avg_edge":        0.0,
            "no_win_rate":     0.0,
            "simulated_pnl":   0.0,
        }


class SoccerScanner:
    """
    Scans Polymarket for soccer BTTS and spread markets.
    Confirmed market slug patterns from Action 1:
      tur-{home}-{away}-{date}-btts
      mls-{home}-{away}-{date}-btts
      j2100-{home}-{away}-{date}-btts
      nor-{home}-{away}-{date}-btts
      *-spread-home-{line}
      *-spread-away-{line}
    """

    GAMMA_URL = "https://gamma-api.polymarket.com/markets"

    LEAGUE_SLUGS = {
        "Turkish Super Lig":     "tur-",
        "MLS":                   "mls-",
        "J2 Japan":              "j2100-",
        "Norwegian Eliteserien": "nor-",
    }

    def __init__(self, config: dict):
        self.config    = config
        self.model     = XGModel()
        self.detector  = SoccerEdgeDetector()

    async def scan(self) -> list[Opportunity]:
        markets = await self._fetch_soccer_markets()
        log.debug(f"Soccer: fetched {len(markets)} markets")

        opps = []
        for m in markets:
            opp = await self._evaluate(m)
            if opp:
                opps.append(opp)

        opps.sort(key=lambda o: o.edge, reverse=True)
        return opps

    async def _fetch_soccer_markets(self) -> list[dict]:
        """Fetch BTTS + spread markets for configured leagues."""
        all_markets = []
        market_types = self.config.get("soccer_market_types", ["btts", "spread"])

        for mtype in market_types:
            try:
                async with aiohttp.ClientSession() as session:
                    params = {
                        "active":   "true",
                        "closed":   "false",
                        "limit":    "200",
                        "slug":     mtype,  # filter by slug keyword
                    }
                    async with session.get(
                        self.GAMMA_URL, params=params,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            markets = data if isinstance(data, list) else data.get("markets", [])
                            # Filter to configured leagues
                            for m in markets:
                                slug = m.get("slug", "")
                                if any(slug.startswith(prefix)
                                       for prefix in self.LEAGUE_SLUGS.values()):
                                    all_markets.append(m)
            except Exception as e:
                log.error(f"Soccer market fetch error ({mtype}): {e}")

        return all_markets

    async def _evaluate(self, market: dict) -> Optional[Opportunity]:
        slug  = market.get("slug", "")
        title = market.get("question") or market.get("title", "")

        # Determine market type and league
        market_type = "btts" if "btts" in slug else "spread"
        league      = self._detect_league(slug)
        if not league:
            return None

        # Check league season confidence
        league_conf = self.detector.get_confidence(league)
        if league_conf < 0.50:
            log.debug(f"Skipping {slug} — league confidence {league_conf:.2f}")
            return None

        # Parse teams
        home, away = self._parse_teams(slug, league)
        if not home or not away:
            return None

        # Get xG data
        home_xg = await self.model.get_team_xg(home, league)
        away_xg = await self.model.get_team_xg(away, league)

        if home_xg is None or away_xg is None:
            return None

        # Calculate model probability
        if market_type == "btts":
            prob_yes = self.model.btts_probability(home_xg, away_xg)
        else:
            spread_line = self._parse_spread_line(slug)
            if spread_line is None:
                return None
            # Determine which side is favored
            if "spread-home" in slug:
                prob_yes = self.model.spread_probability(home_xg, away_xg, spread_line)
            else:
                prob_yes = self.model.spread_probability(away_xg, home_xg, spread_line)

        prob_no = 1.0 - prob_yes

        # Get market prices
        no_price  = self._get_price(market, "no")
        yes_price = self._get_price(market, "yes")

        if no_price is None or yes_price is None:
            return None

        # Apply ColdMath's confirmed price thresholds
        # He only buys NO when priced ≥ 88¢ (near-certain)
        min_no = self.config.get("soccer_min_no_price", 0.88)
        if no_price < min_no:
            return None

        edge = prob_no - no_price

        if edge < self.config["min_edge"]:
            return None

        # Combined confidence
        confidence = league_conf * min(1.0, edge * 10 + 0.5)

        if confidence < self.config["min_confidence"]:
            return None

        return Opportunity(
            domain       = "soccer",
            slug         = slug,
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
            domain_meta  = {
                "league":      league,
                "market_type": market_type,
                "home":        home,
                "away":        away,
                "home_xg":     home_xg,
                "away_xg":     away_xg,
            },
        )

    def _detect_league(self, slug: str) -> Optional[str]:
        for league, prefix in self.LEAGUE_SLUGS.items():
            if slug.startswith(prefix):
                return league
        return None

    def _parse_teams(self, slug: str, league: str) -> tuple[Optional[str], Optional[str]]:
        """
        Parse team names from slug.
        Example: "tur-gen-goz-2026-04-04-btts" → ("gen", "goz")
        """
        prefix = self.LEAGUE_SLUGS.get(league, "")
        remainder = slug[len(prefix):]
        # Remove date and type suffix
        parts = remainder.split("-")
        if len(parts) >= 2:
            # Teams are before the date (YYYY-MM-DD pattern)
            date_idx = next(
                (i for i, p in enumerate(parts) if re.match(r'\d{4}', p)),
                len(parts)
            )
            team_parts = parts[:date_idx]
            if len(team_parts) >= 2:
                mid = len(team_parts) // 2
                return "-".join(team_parts[:mid]), "-".join(team_parts[mid:])
        return None, None

    def _parse_spread_line(self, slug: str) -> Optional[float]:
        """Extract spread line from slug. E.g. 'spread-home-1pt5' → 1.5"""
        match = re.search(r'(\d+)pt(\d+)', slug)
        if match:
            return float(f"{match.group(1)}.{match.group(2)}")
        match = re.search(r'spread.*?(\d+)$', slug)
        if match:
            return float(match.group(1))
        return None

    def _get_price(self, market: dict, side: str) -> Optional[float]:
        tokens = market.get("tokens", []) or market.get("outcomes", [])
        for token in tokens:
            outcome = (token.get("outcome") or "").lower()
            if outcome == side:
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
