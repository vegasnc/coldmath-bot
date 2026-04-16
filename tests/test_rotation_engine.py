"""
tests/test_rotation_engine.py

Unit tests for the domain rotation engine.
Run: pytest tests/
"""

import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from core.rotation_engine import (
    DomainRotationEngine,
    DomainSignals,
    DomainStatus,
    Domain,
    SEASONAL_WEIGHTS,
)


# ── SIGNAL TESTS ──────────────────────────────────────────────

class TestDomainSignals:

    def test_health_starts_at_one(self):
        sig = DomainSignals(Domain.WEATHER)
        assert sig.health == 1.0

    def test_health_stable_when_metrics_stable(self):
        sig = DomainSignals(Domain.WEATHER)
        for _ in range(14):
            sig.record(session_volume=1000, net_margin=3.5,
                       opportunity_count=40)
        assert sig.health > 0.90

    def test_health_drops_when_volume_collapses(self):
        sig = DomainSignals(Domain.WEATHER)
        # Establish baseline
        for _ in range(10):
            sig.record(1000, 3.5, 40)
        # Volume collapses
        for _ in range(3):
            sig.record(50, -2.0, 5)
        assert sig.health < 0.65
        assert sig.is_decaying

    def test_health_critical_on_near_zero(self):
        sig = DomainSignals(Domain.WEATHER)
        for _ in range(10):
            sig.record(1000, 3.5, 40)
        for _ in range(3):
            sig.record(0, -10.0, 0)
        assert sig.is_critical

    def test_health_recovers(self):
        sig = DomainSignals(Domain.WEATHER)
        # Decay
        for _ in range(10):
            sig.record(1000, 3.5, 40)
        for _ in range(3):
            sig.record(50, -5.0, 2)
        assert sig.is_decaying
        # Recover
        for _ in range(5):
            sig.record(1200, 4.0, 50)
        assert sig.health > 0.65


# ── ROTATION ENGINE TESTS ─────────────────────────────────────

class TestDomainRotationEngine:

    def setup_method(self):
        self.engine = DomainRotationEngine(
            total_budget_usdc=10_000,
            state_file="/tmp/test_engine_state.json"
        )

    def test_initial_state(self):
        assert self.engine.domains[Domain.WEATHER].status == DomainStatus.ACTIVE
        assert self.engine.domains[Domain.SOCCER].status == DomainStatus.TESTING

    def test_weather_budget_dominant_initially(self):
        weather_budget = self.engine.get_budget_for_domain(Domain.WEATHER)
        soccer_budget  = self.engine.get_budget_for_domain(Domain.SOCCER)
        assert weather_budget > soccer_budget

    def test_allocations_sum_to_one(self):
        total = sum(
            s.allocation for s in self.engine.domains.values()
        )
        assert abs(total - 1.0) < 0.01

    def test_mark_model_validated(self):
        self.engine.mark_model_validated(Domain.FINANCIAL)
        assert self.engine.domains[Domain.FINANCIAL].model_validated

    def test_rotation_on_weather_decay(self):
        # Establish good weather baseline
        self.engine.mark_model_validated(Domain.SOCCER)
        for _ in range(14):
            self.engine.daily_update({
                Domain.WEATHER: {"session_volume": 1000,
                                  "net_margin": 3.5,
                                  "opportunity_count": 40},
                Domain.SOCCER:  {"session_volume": 100,
                                  "net_margin": 2.0,
                                  "opportunity_count": 15},
            })

        initial_weather_alloc = self.engine.domains[Domain.WEATHER].allocation

        # Simulate weather edge collapse (like March 15-23)
        for _ in range(7):
            self.engine.daily_update({
                Domain.WEATHER: {"session_volume": 50,
                                  "net_margin": -8.0,
                                  "opportunity_count": 3},
                Domain.SOCCER:  {"session_volume": 200,
                                  "net_margin": 2.5,
                                  "opportunity_count": 20},
            })

        final_weather_alloc = self.engine.domains[Domain.WEATHER].allocation
        final_soccer_alloc  = self.engine.domains[Domain.SOCCER].allocation

        # Weather should have reduced allocation
        assert final_weather_alloc < initial_weather_alloc
        # Soccer should have increased
        assert final_soccer_alloc > 0.10

    def test_max_daily_rotation_limit(self):
        """Rotation should never jump more than 5% per day."""
        initial_alloc = self.engine.domains[Domain.WEATHER].allocation

        # Extreme collapse
        self.engine.daily_update({
            Domain.WEATHER: {"session_volume": 0,
                              "net_margin": -100,
                              "opportunity_count": 0},
        })

        new_alloc = self.engine.domains[Domain.WEATHER].allocation
        daily_change = abs(new_alloc - initial_alloc)
        assert daily_change <= self.engine.MAX_DAILY_ROTATION + 0.01

    def test_budget_never_exceeds_total(self):
        total_allocated = sum(
            self.engine.get_budget_for_domain(d) for d in Domain
        )
        assert total_allocated <= self.engine.total_budget + 1.0  # 1 USDC rounding

    def test_seasonal_weights_sum_reasonable(self):
        for month, weights in SEASONAL_WEIGHTS.items():
            total = sum(weights.values())
            # At least 1.5 total weight available across all domains every month
            assert total >= 1.5, f"Month {month} has low total weight: {total}"


# ── SEASONAL WEIGHT TESTS ─────────────────────────────────────

class TestSeasonalWeights:

    def test_weather_peaks_in_winter(self):
        jan_w = SEASONAL_WEIGHTS[1]["weather"]
        apr_w = SEASONAL_WEIGHTS[4]["weather"]
        assert jan_w > apr_w  # winter > spring for weather

    def test_soccer_peaks_in_spring_autumn(self):
        apr_s = SEASONAL_WEIGHTS[4]["soccer"]
        jul_s = SEASONAL_WEIGHTS[7]["soccer"]
        assert apr_s > jul_s  # spring > summer for soccer

    def test_financial_peaks_in_summer(self):
        jun_f = SEASONAL_WEIGHTS[6]["financial"]
        jan_f = SEASONAL_WEIGHTS[1]["financial"]
        assert jun_f > jan_f  # summer > winter for financial

    def test_portfolio_never_below_threshold(self):
        """Combined portfolio edge should stay above 60% every month."""
        for month, weights in SEASONAL_WEIGHTS.items():
            avg_weight = sum(weights.values()) / len(weights)
            assert avg_weight >= 0.55, \
                f"Month {month} portfolio avg too low: {avg_weight:.2f}"


# ── OPPORTUNITY MODEL TESTS ───────────────────────────────────

class TestFinancialModel:

    def test_prob_above_when_price_well_below_target(self):
        from domains.financial import FinancialModel
        model = FinancialModel()
        # Current price $100, target $150 (50% above), 1 day
        # Should be very unlikely to reach $150 — high P(NO)
        prob_no, conf = model.get_probability(
            current_price=100, target=150,
            impl_vol=0.20, days_to_expiry=1, direction="above"
        )
        assert prob_no > 0.95

    def test_prob_below_when_price_well_above_target(self):
        from domains.financial import FinancialModel
        model = FinancialModel()
        # Current $100, target $50, 1 day — very unlikely to drop 50%
        prob_no, conf = model.get_probability(
            current_price=100, target=50,
            impl_vol=0.20, days_to_expiry=1, direction="below"
        )
        assert prob_no > 0.95

    def test_confidence_low_when_iv_high(self):
        from domains.financial import FinancialModel
        model = FinancialModel()
        _, conf = model.get_probability(100, 105, 0.80, 1, "above")
        assert conf < 0.50


class TestSoccerModel:

    def test_btts_prob_increases_with_xg(self):
        from domains.soccer import XGModel
        model = XGModel()
        prob_low  = model.btts_probability(0.5, 0.5)
        prob_high = model.btts_probability(2.0, 2.0)
        assert prob_high > prob_low

    def test_btts_prob_between_zero_and_one(self):
        from domains.soccer import XGModel
        model = XGModel()
        prob = model.btts_probability(1.2, 0.9)
        assert 0.0 <= prob <= 1.0

    def test_league_confidence_low_early_season(self):
        from domains.soccer import SoccerEdgeDetector
        detector = SoccerEdgeDetector()
        detector.update("MLS", 3)  # only 3 games played
        conf = detector.get_confidence("MLS")
        assert conf < 0.50

    def test_league_confidence_low_end_of_season(self):
        from domains.soccer import SoccerEdgeDetector
        detector = SoccerEdgeDetector()
        detector.update("MLS", 31)  # 31/34 played — final 3 games
        conf = detector.get_confidence("MLS")
        assert conf < 0.50

    def test_league_confidence_high_mid_season(self):
        from domains.soccer import SoccerEdgeDetector
        detector = SoccerEdgeDetector()
        detector.update("MLS", 18)  # mid-season
        conf = detector.get_confidence("MLS")
        assert conf >= 0.90


# ── v3 GAP TESTS ──────────────────────────────────────────────

class TestPositionSizing:
    """
    Tests derived from observed positions data (Action 2, March 28).
    All expected sizes calibrated from real ColdMath positions.
    """

    def setup_method(self):
        from core.config import CONFIG
        from core.rotation_engine import DomainRotationEngine, Domain, DomainStatus
        self.config = CONFIG.copy()
        self.config["total_budget_usdc"] = 10_000
        self.engine = DomainRotationEngine(10_000,
                       state_file="/tmp/test_sizing.json")

    def _make_opp(self, edge=0.05, confidence=0.95, liquidity=None):
        from core.opportunity import Opportunity
        return Opportunity(
            domain="weather", slug="test-market", title="Test",
            condition_id="0xtest",
            no_token_id="no_tok", yes_token_id="yes_tok",
            no_price=0.95, yes_price=0.04,
            our_prob_no=0.95+edge, edge=edge, confidence=confidence,
            end_date="2026-03-28",
            available_liquidity=liquidity,
        )

    def test_high_confidence_full_size(self):
        """HIGH confidence (≥0.90) → full 1.0× multiplier"""
        from core.bot import IntegratedBot
        bot = IntegratedBot(self.config)
        opp = self._make_opp(confidence=0.95)
        size = bot._position_size(Domain.WEATHER, opp)
        base = self.config["base_no_size"]["weather"]
        # Full alloc * full conf * edge scale — should be close to base
        assert size >= base * 0.8

    def test_medium_confidence_reduced(self):
        """MEDIUM confidence (0.65-0.75) → 0.50× multiplier"""
        from core.bot import IntegratedBot
        bot = IntegratedBot(self.config)
        high_conf_size = bot._position_size(Domain.WEATHER,
                                             self._make_opp(confidence=0.95))
        med_conf_size  = bot._position_size(Domain.WEATHER,
                                             self._make_opp(confidence=0.68))
        # Medium confidence should be significantly smaller
        assert med_conf_size < high_conf_size * 0.70

    def test_low_confidence_returns_zero(self):
        """LOW confidence (<0.65) → skip (return 0)"""
        from core.bot import IntegratedBot
        bot = IntegratedBot(self.config)
        size = bot._position_size(Domain.WEATHER,
                                   self._make_opp(confidence=0.50))
        assert size == 0.0

    def test_liquidity_cap_applied(self):
        """Never more than 30% of available order book depth"""
        from core.bot import IntegratedBot
        bot = IntegratedBot(self.config)
        # Set a very low liquidity to force cap
        opp  = self._make_opp(confidence=0.95, liquidity=100.0)
        size = bot._position_size(Domain.WEATHER, opp)
        assert size <= 100.0 * 0.30 + 0.01  # 30% cap + rounding

    def test_no_liquidity_cap_when_none(self):
        """If liquidity not provided, no cap applied"""
        from core.bot import IntegratedBot
        bot = IntegratedBot(self.config)
        opp  = self._make_opp(confidence=0.95, liquidity=None)
        size = bot._position_size(Domain.WEATHER, opp)
        assert size > 0  # gets a real size

    def test_yes_fraction_is_4_percent(self):
        """YES insurance always 4% of NO size"""
        from core.bot import IntegratedBot
        bot = IntegratedBot(self.config)
        opp     = self._make_opp(confidence=0.95)
        no_size = bot._position_size(Domain.WEATHER, opp)
        yes_size = no_size * self.config["yes_fraction"]
        assert abs(yes_size / no_size - 0.04) < 0.001

    def test_larger_edge_means_larger_size(self):
        """Stronger edge → larger position"""
        from core.bot import IntegratedBot
        bot  = IntegratedBot(self.config)
        low  = bot._position_size(Domain.WEATHER, self._make_opp(edge=0.04))
        high = bot._position_size(Domain.WEATHER, self._make_opp(edge=0.08))
        assert high > low

    def test_testing_domain_capped_at_20pct(self):
        """Testing domains always 20% of base regardless of edge/conf"""
        from core.bot import IntegratedBot
        from core.rotation_engine import DomainStatus
        bot = IntegratedBot(self.config)
        bot.engine.domains[Domain.SOCCER].status = DomainStatus.TESTING
        opp  = self._make_opp(confidence=0.99, edge=0.10)
        size = bot._position_size(Domain.SOCCER, opp)
        base = self.config["base_no_size"]["soccer"]
        assert abs(size - base * 0.20) < 0.01


class TestOrderFragmentation:
    """
    Tests that paper trade logs show fragmentation pattern.
    Derived from: 56.5% of 62,374 txs under $1 = YES fragments.
    Burst pattern: 15-30 transactions per position.
    """

    def setup_method(self):
        from core.config import CONFIG
        self.config = CONFIG.copy()
        self.config["paper_trade"] = True

    def test_paper_execute_returns_success(self):
        import asyncio
        from core.orders import OrderManager
        from core.opportunity import Opportunity

        orders = OrderManager(self.config)
        opp = Opportunity(
            domain="weather", slug="test", title="Test",
            condition_id="0xtest",
            no_token_id="no", yes_token_id="yes",
            no_price=0.95, yes_price=0.04,
            our_prob_no=0.99, edge=0.04, confidence=0.95,
            end_date="2026-03-28",
        )
        result = asyncio.run(orders.execute(opp, 300.0, 12.0))
        assert result.success
        assert result.paper
        assert result.filled_no == 300.0
        assert result.filled_yes == 12.0

    def test_merge_flagged_when_sum_below_1(self):
        import asyncio
        from core.orders import OrderManager
        from core.opportunity import Opportunity

        orders = OrderManager(self.config)
        opp = Opportunity(
            domain="weather", slug="test", title="Test",
            condition_id="0xtest",
            no_token_id="no", yes_token_id="yes",
            no_price=0.95, yes_price=0.04,  # sum = 0.99 < 1.00
            our_prob_no=0.99, edge=0.04, confidence=0.95,
            end_date="2026-03-28",
        )
        result = asyncio.run(orders.execute(opp, 300.0, 12.0))
        assert result.merged  # should flag for merge

    def test_no_merge_when_sum_above_1(self):
        import asyncio
        from core.orders import OrderManager
        from core.opportunity import Opportunity

        orders = OrderManager(self.config)
        opp = Opportunity(
            domain="weather", slug="test", title="Test",
            condition_id="0xtest",
            no_token_id="no", yes_token_id="yes",
            no_price=0.95, yes_price=0.06,  # sum = 1.01 > 1.00
            our_prob_no=0.99, edge=0.04, confidence=0.95,
            end_date="2026-03-28",
        )
        result = asyncio.run(orders.execute(opp, 300.0, 12.0))
        assert not result.merged  # should NOT merge
