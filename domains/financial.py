"""
domains/financial.py

Financial daily close market scanner.
NEXT domain after soccer — predicted based on ColdMath's expansion pattern.

Markets: S&P 500, BTC, ETH daily close bands on Polymarket
Model: Black-Scholes implied probability distribution

This domain fills the Jun-Aug gap when:
  - Weather edge is low (spring/summer transition)
  - Soccer is in European off-season

DEVELOPER: This module is structured but needs data feeds implemented.
Start with small positions ($10) to validate before scaling.
"""

import math
import logging
from typing import Optional

import aiohttp

from core.opportunity import Opportunity

log = logging.getLogger("financial_scanner")


class FinancialModel:
    """
    Black-Scholes implied distribution → P(price outcome).

    The options market already prices in the probability distribution
    of an asset's closing price. We use that to find cases where
    Polymarket's crowd pricing diverges from the options market.

    Key insight: The d2 term from Black-Scholes IS the probability.
      P(S_T > K) = N(d2)
      where d2 = [ln(S/K) - 0.5σ²t] / (σ√t)
    """

    def get_probability(self, current_price: float, target: float,
                         impl_vol: float, days_to_expiry: int,
                         direction: str) -> tuple[float, float]:
        """
        Returns (prob_no, confidence).
        direction: 'above' | 'below'
        """
        if days_to_expiry <= 0 or impl_vol <= 0 or current_price <= 0:
            return 0.5, 0.0

        t = days_to_expiry / 252.0  # trading days → years

        # Black-Scholes d2
        try:
            d2 = (math.log(current_price / target) - 0.5 * impl_vol**2 * t) \
                 / (impl_vol * math.sqrt(t))
        except (ValueError, ZeroDivisionError):
            return 0.5, 0.0

        prob_above = self._norm_cdf(d2)
        prob_below = 1.0 - prob_above

        if direction == "above":
            prob_yes = prob_above
        else:
            prob_yes = prob_below

        prob_no = 1.0 - prob_yes

        # Confidence: lower IV = tighter distribution = more confidence
        # Above 35% IV = crypto territory = skip
        if impl_vol < 0.15:   confidence = 0.95
        elif impl_vol < 0.25: confidence = 0.80
        elif impl_vol < 0.35: confidence = 0.60
        else:                  confidence = 0.30

        return prob_no, confidence

    def _norm_cdf(self, x: float) -> float:
        """Abramowitz & Stegun approximation of N(x). Accurate to 7.5e-8."""
        t = 1.0 / (1.0 + 0.2316419 * abs(x))
        poly = t * (0.319381530 +
               t * (-0.356563782 +
               t * (1.781477937 +
               t * (-1.821255978 +
               t * 1.330274429))))
        density = math.exp(-0.5 * x**2) / math.sqrt(2 * math.pi)
        cdf = 1.0 - density * poly
        return cdf if x >= 0 else 1.0 - cdf

    async def get_market_data(self, ticker: str) -> tuple[Optional[float], Optional[float]]:
        """
        Returns (spot_price, implied_vol).

        DEVELOPER: Implement per ticker:

        For S&P 500 (SPX):
            Spot: Yahoo Finance / yfinance Python library
                import yfinance as yf
                spot = yf.Ticker("^GSPC").fast_info["last_price"]

            IV: CBOE VIX (free) as ATM IV proxy
                VIX/100 × sqrt(1/12) ≈ 1-month IV
                Or fetch SPX options chain from Tradier (free tier):
                https://api.tradier.com/v1/markets/options/chains

        For BTC/ETH:
            Spot: Binance API (free)
                https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT

            IV: Deribit options API (free)
                https://www.deribit.com/api/v2/public/get_order_book
                ?instrument_name=BTC-{date}-{strike}-C

            Or use Deribit DVOL index (BTC/ETH implied vol index, free):
                https://www.deribit.com/api/v2/public/get_index_price?index_name=btc_usd

        Example for BTC:
            async with aiohttp.ClientSession() as session:
                # Spot price
                async with session.get(
                    "https://api.binance.com/api/v3/ticker/price",
                    params={"symbol": "BTCUSDT"}
                ) as resp:
                    data = await resp.json()
                    spot = float(data["price"])

                # Implied vol from Deribit DVOL
                async with session.get(
                    "https://www.deribit.com/api/v2/public/get_index_price",
                    params={"index_name": "dvol_btc_usd"}
                ) as resp:
                    data = await resp.json()
                    dvol = data["result"]["index_price"]  # annualised vol in %
                    impl_vol = dvol / 100.0

                return spot, impl_vol
        """
        log.warning(f"get_market_data not implemented for {ticker}")
        return None, None

    def backtest(self, days: int = 30) -> dict:
        """
        DEVELOPER: Back-test against historical Polymarket financial markets.
        Compare our Black-Scholes prob vs actual market outcomes.
        """
        return {
            "days":            days,
            "total_positions": 0,
            "accuracy":        0.0,
            "avg_edge":        0.0,
            "no_win_rate":     0.0,
            "simulated_pnl":   0.0,
        }


class FinancialScanner:
    """
    Scans Polymarket for financial close markets.

    Confirmed market types on Polymarket:
      - "Will BTC be above $X on [date]?"
      - "Will the S&P 500 close above X on [date]?"
      - "Will ETH be above $X on [date]?"

    Same YES/NO/MERGE mechanic as weather.
    Only trade when NO is priced ≥ 88¢ (near-certain outcomes).
    """

    GAMMA_URL = "https://gamma-api.polymarket.com/markets"

    TICKERS = {
        "btc":      ("BTC",  "binance"),
        "bitcoin":  ("BTC",  "binance"),
        "s&p":      ("SPX",  "yahoo"),
        "sp500":    ("SPX",  "yahoo"),
        "eth":      ("ETH",  "binance"),
        "ethereum": ("ETH",  "binance"),
    }

    def __init__(self, config: dict):
        self.config = config
        self.model  = FinancialModel()

    async def scan(self) -> list[Opportunity]:
        markets = await self._fetch_markets()
        log.debug(f"Financial: fetched {len(markets)} markets")

        opps = []
        for m in markets:
            opp = await self._evaluate(m)
            if opp:
                opps.append(opp)

        opps.sort(key=lambda o: o.edge, reverse=True)
        return opps

    async def _fetch_markets(self) -> list[dict]:
        """Fetch financial close markets."""
        keywords = ["will btc", "will eth", "s&p 500", "bitcoin close",
                    "ethereum close", "will the s&p"]
        all_markets = []

        for kw in keywords[:3]:  # limit to avoid rate limiting during dev
            try:
                async with aiohttp.ClientSession() as session:
                    params = {
                        "active":     "true",
                        "closed":     "false",
                        "limit":      "100",
                        "search":     kw,
                    }
                    async with session.get(
                        self.GAMMA_URL, params=params,
                        timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            markets = data if isinstance(data, list) else \
                                      data.get("markets", [])
                            all_markets.extend(markets)
            except Exception as e:
                log.error(f"Financial market fetch error ({kw}): {e}")

        # Deduplicate
        seen = set()
        unique = []
        for m in all_markets:
            mid = m.get("id") or m.get("conditionId", "")
            if mid not in seen:
                seen.add(mid)
                unique.append(m)
        return unique

    async def _evaluate(self, market: dict) -> Optional[Opportunity]:
        title = (market.get("question") or market.get("title", "")).lower()

        # Detect ticker
        ticker = None
        for kw, (sym, source) in self.TICKERS.items():
            if kw in title:
                ticker = sym
                break
        if not ticker:
            return None

        # Parse target price and direction
        parsed = self._parse_title(title)
        if not parsed:
            return None
        target, direction = parsed

        # Check expiry
        end_date = market.get("endDate", "")
        days_to_expiry = self._days_to_expiry(end_date)
        max_days = self.config.get("financial_max_days", 7)
        min_days = self.config.get("financial_min_days", 1)
        if not (min_days <= days_to_expiry <= max_days):
            return None

        # Get market data
        spot, impl_vol = await self.model.get_market_data(ticker)
        if spot is None or impl_vol is None:
            return None

        # Check IV threshold
        max_iv = self.config.get("financial_max_iv", 0.35)
        if impl_vol > max_iv:
            log.debug(f"Skipping {ticker} — IV {impl_vol:.2f} > {max_iv}")
            return None

        # Get probability
        prob_no, confidence = self.model.get_probability(
            spot, target, impl_vol, days_to_expiry, direction
        )

        # Get prices
        no_price  = self._get_price(market, "no")
        yes_price = self._get_price(market, "yes")
        if no_price is None or yes_price is None:
            return None

        # Only near-certain NO positions
        if no_price < self.config.get("soccer_min_no_price", 0.88):
            return None

        edge = prob_no - no_price
        if edge < self.config["min_edge"]:
            return None

        if confidence < self.config["min_confidence"]:
            return None

        return Opportunity(
            domain       = "financial",
            slug         = market.get("slug", ""),
            title        = market.get("question") or market.get("title", ""),
            condition_id = market.get("conditionId", ""),
            no_token_id  = self._get_token_id(market, "no"),
            yes_token_id = self._get_token_id(market, "yes"),
            no_price     = no_price,
            yes_price    = yes_price,
            our_prob_no  = prob_no,
            edge         = edge,
            confidence   = confidence,
            end_date     = end_date,
            domain_meta  = {
                "ticker":         ticker,
                "target":         target,
                "direction":      direction,
                "spot":           spot,
                "impl_vol":       impl_vol,
                "days_to_expiry": days_to_expiry,
            },
        )

    def _parse_title(self, title: str) -> Optional[tuple[float, str]]:
        """
        Extracts (target_price, direction) from market title.
        "Will BTC be above $85,000 on April 4?" → (85000.0, "above")
        """
        import re
        title = title.replace(",", "")

        amount_match = re.search(r'\$?([\d]+(?:\.[\d]+)?)[k]?', title)
        if not amount_match:
            return None

        amount = float(amount_match.group(1))
        if "k" in title[amount_match.start():amount_match.end()+1].lower():
            amount *= 1000

        if any(w in title for w in ["above", "higher", "over", "more than"]):
            direction = "above"
        elif any(w in title for w in ["below", "lower", "under", "less than"]):
            direction = "below"
        else:
            return None

        return amount, direction

    def _days_to_expiry(self, date_str: str) -> int:
        if not date_str:
            return 0
        from datetime import datetime, timezone
        try:
            target = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            now    = datetime.now(tz=timezone.utc)
            return max(0, (target - now).days)
        except Exception:
            return 0

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
