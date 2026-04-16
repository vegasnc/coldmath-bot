"""
tests/run_v4_tests.py

Tests for all v4 changes. Run: python3 tests/run_v4_tests.py
"""

import sys, asyncio
sys.path.insert(0, '.')

import unittest.mock as mock
sys.modules['aiohttp'] = mock.MagicMock()

from core.rotation_engine import DomainRotationEngine, Domain, DomainStatus
from core.config import CONFIG
from core.opportunity import Opportunity
from core.orders import OrderManager

passed = failed = 0

def chk(name, cond, detail=""):
    global passed, failed
    sym = "PASS" if cond else "FAIL"
    print(f"  {sym}  {name}" + (f"  [{detail}]" if detail else ""))
    if cond: passed += 1
    else:    failed += 1

def opp(edge=0.05, conf=0.95, liq=None, yes_price=0.04, domain="weather"):
    return Opportunity(
        domain=domain, slug="test", title="Test",
        condition_id="0x", no_token_id="no", yes_token_id="yes",
        no_price=0.95, yes_price=yes_price,
        our_prob_no=0.99, edge=edge, confidence=conf,
        end_date="2026-04-06", available_liquidity=liq,
    )

print("ColdMath Bot v4 — full test suite")
print("="*55)

# ── ROTATION ENGINE ───────────────────────────────────────
print("\n[1] Rotation engine")
e = DomainRotationEngine(10000, state_file="/tmp/v4test.json")
chk("Weather ACTIVE",    e.domains[Domain.WEATHER].status == DomainStatus.ACTIVE)
chk("Soccer ACTIVE",     e.domains[Domain.SOCCER].status  == DomainStatus.ACTIVE)
chk("Cycling TESTING",   e.domains[Domain.CYCLING].status == DomainStatus.TESTING)
chk("Financial INACTIVE",e.domains[Domain.FINANCIAL].status == DomainStatus.INACTIVE)
chk("Allocations sum 1", abs(sum(s.allocation for s in e.domains.values())-1.0) < 0.01)
chk("Weather > Soccer",  e.domains[Domain.WEATHER].allocation > e.domains[Domain.SOCCER].allocation)

# ── SESSION DETECTION ──────────────────────────────────────
print("\n[2] Session detection (6 sessions)")
from core.bot import IntegratedBot
from datetime import datetime, timezone

bot = IntegratedBot(CONFIG)

def active_at(h, m=0, weekday=0):
    dt = datetime(2026,4,6,h,m,0, tzinfo=timezone.utc)
    dt = dt.replace()
    class FakeDT:
        hour=h; minute=m
        def weekday(self): return weekday
    return bot._active_sessions(FakeDT())

chk("S0 active at 06:30", "S0" in active_at(6,30))
chk("S1 active at 07:00", "S1" in active_at(7,0))
chk("S1 active at 09:00", "S1" in active_at(9,0))
chk("S4 active at 13:30", "S4" in active_at(13,30))
chk("S2 active Mon 15:30","S2" in active_at(15,30,weekday=0))
chk("S2 absent Sat 15:30","S2" not in active_at(15,30,weekday=5))
chk("S3 active at 17:00", "S3" in active_at(17,0))   # NEW — was 18z before
chk("S3 active at 19:00", "S3" in active_at(19,0))
chk("S3 absent at 16:59", "S3" not in active_at(16,59))  # confirms 17z start
chk("S5 active at 22:00", "S5" in active_at(22,0))

# ── CONFIG VALIDATION ─────────────────────────────────────
print("\n[3] Config v4")
chk("S3 starts at 17z",  CONFIG["session_3_start"] == (17,0))
chk("S5 exists",         "session_5_start" in CONFIG)
chk("S0 exists",         "session_0_start" in CONFIG)
chk("Early sell enabled",CONFIG["early_sell_enabled"] == True)
chk("Early sell at 99c", CONFIG["early_sell_threshold"] == 0.990)
chk("Soccer base $50",   CONFIG["base_no_size"]["soccer"] == 50.0)
chk("Cycling base $20",  CONFIG["base_no_size"]["cycling"] == 20.0)
chk("A-League in leagues","A-League" in CONFIG["soccer_leagues"])
chk("Moscow in cities",   "Moscow" in CONFIG["weather_cities"])
chk("KL in cities",       "Kuala Lumpur" in CONFIG["weather_cities"])
chk("Helsinki in cities", "Helsinki" in CONFIG["weather_cities"])
chk("Denver in cities",   "Denver" in CONFIG["weather_cities"])
chk("Cycling enabled",    CONFIG.get("cycling_enabled") == True)

# ── POSITION SIZING ────────────────────────────────────────
print("\n[4] Position sizing (v3 gaps retained)")
bot2 = IntegratedBot(CONFIG)
high = bot2._position_size(Domain.WEATHER, opp(conf=0.95))
med  = bot2._position_size(Domain.WEATHER, opp(conf=0.70))
low  = bot2._position_size(Domain.WEATHER, opp(conf=0.50))
chk("HIGH conf positive",    high > 0, str(round(high)))
chk("MED conf positive",     med  > 0, str(round(med)))
chk("LOW conf = zero",       low == 0.0)
chk("MED < HIGH",            med < high)
capped   = bot2._position_size(Domain.WEATHER, opp(conf=0.95, liq=100.0))
uncapped = bot2._position_size(Domain.WEATHER, opp(conf=0.95, liq=None))
chk("Liquidity cap 30%",     capped <= 30.5, str(round(capped,1)))
chk("No cap when liq=None",  uncapped > 30)
s_edge = bot2._position_size(Domain.WEATHER, opp(edge=0.04))
l_edge = bot2._position_size(Domain.WEATHER, opp(edge=0.08))
chk("Larger edge = larger",  l_edge > s_edge)

# Cycling in testing → 20% cap
bot2.engine.domains[Domain.CYCLING].status = DomainStatus.TESTING
cyc = bot2._position_size(Domain.CYCLING, opp(conf=0.95, domain="cycling"))
chk("Cycling testing cap 20%", abs(cyc - 20.0*0.20) < 0.01, str(round(cyc,1)))

# ── ORDERS ────────────────────────────────────────────────
print("\n[5] Orders (paper mode)")
cfg = CONFIG.copy(); cfg["paper_trade"] = True
orders = OrderManager(cfg)
o1 = asyncio.run(orders.execute(opp(yes_price=0.04), 300.0, 12.0))
chk("Paper executes",    o1.success)
chk("Merge at sum=0.99", o1.merged)
o2 = asyncio.run(orders.execute(opp(yes_price=0.06), 300.0, 18.0))
chk("No merge sum=1.01", not o2.merged)
sell = asyncio.run(orders.sell_position("tok123", 100.0, 0.995))
chk("Paper sell succeeds", sell.success)
opns = asyncio.run(orders.get_open_positions())
chk("Paper positions = []", opns == [])

# ── CYCLING DOMAIN ────────────────────────────────────────
print("\n[6] Cycling domain")
from domains.cycling import CyclingScanner, CyclingModel
model = CyclingModel()
p, c = model.get_probability("wout-van-aert", "tour-de-flanders", 3)
chk("Returns prob tuple",     isinstance(p, float) and isinstance(c, float))
chk("Prob between 0 and 1",   0.0 <= p <= 1.0)
chk("Unknown rider conf=0",   c == 0.0)  # not implemented yet — returns 0

scanner = CyclingScanner(CONFIG)
chk("Scanner created",        scanner is not None)
chk("Model attached",         scanner.model is not None)

# ── SEASONAL WEIGHTS ──────────────────────────────────────
print("\n[7] Seasonal weights with cycling")
from core.rotation_engine import SEASONAL_WEIGHTS
chk("Apr cycling peak",  SEASONAL_WEIGHTS[4]["cycling"] >= 0.80)
chk("Jul cycling peak",  SEASONAL_WEIGHTS[7]["cycling"] >= 0.90)
chk("Jan cycling low",   SEASONAL_WEIGHTS[1]["cycling"] <= 0.10)
chk("Jun financial peak",SEASONAL_WEIGHTS[6]["financial"] >= 0.90)

# ── EARLY SELL CONFIG ────────────────────────────────────
print("\n[8] Early sell")
from core.bot import IntegratedBot as Bot4
b = Bot4(CONFIG)
chk("Early sell enabled in config", b.config.get("early_sell_enabled"))
chk("Threshold at 99c",  b.config.get("early_sell_threshold") == 0.990)

print()
print("="*55)
print(f"Results: {passed} passed  {failed} failed")
if failed == 0:
    print("All v4 changes verified")
print("="*55)
