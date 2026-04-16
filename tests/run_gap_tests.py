import sys
sys.path.insert(0, '/home/claude/coldmath_bot_v3')

from core.rotation_engine import DomainRotationEngine, Domain, DomainStatus
from core.config import CONFIG
from core.bot import IntegratedBot
from core.opportunity import Opportunity
from core.orders import OrderManager
import asyncio

def make_opp(edge=0.05, confidence=0.95, liquidity=None, yes_price=0.04):
    return Opportunity(
        domain='weather', slug='test', title='Test',
        condition_id='0x', no_token_id='no', yes_token_id='yes',
        no_price=0.95, yes_price=yes_price,
        our_prob_no=0.99, edge=edge, confidence=confidence,
        end_date='2026-03-28', available_liquidity=liquidity,
    )

passed = 0
failed = 0

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  PASS  {name}" + (f" — {detail}" if detail else ""))
        passed += 1
    else:
        print(f"  FAIL  {name}" + (f" — {detail}" if detail else ""))
        failed += 1

print("=" * 55)
print("ColdMath Bot v3 — gap closure tests")
print("=" * 55)

# Rotation engine
engine = DomainRotationEngine(10000, state_file='/tmp/v3test.json')
check("Engine initializes", engine.domains[Domain.WEATHER].status == DomainStatus.ACTIVE)
check("Soccer starts testing", engine.domains[Domain.SOCCER].status == DomainStatus.TESTING)

# Confidence scaling (Gap 1)
bot = IntegratedBot(CONFIG)
high = bot._position_size(Domain.WEATHER, make_opp(confidence=0.95))
med  = bot._position_size(Domain.WEATHER, make_opp(confidence=0.70))
low  = bot._position_size(Domain.WEATHER, make_opp(confidence=0.50))

check("High confidence positive",  high > 0,   f"${high:.0f}")
check("Medium confidence positive", med > 0,   f"${med:.0f}")
check("Low confidence is zero",    low == 0.0, f"${low:.0f}")
check("Medium < high",             med < high, f"ratio={med/high:.2f}")
check("Medium ~50% of high",       0.40 < med/high < 0.70, f"{med/high:.2f}")

# Liquidity cap (Gap 3)
capped   = bot._position_size(Domain.WEATHER, make_opp(confidence=0.95, liquidity=100.0))
uncapped = bot._position_size(Domain.WEATHER, make_opp(confidence=0.95, liquidity=None))
check("Liquidity cap at 30%",      capped <= 30.5,  f"${capped:.1f}")
check("No cap when liquidity=None", uncapped > 30,  f"${uncapped:.0f}")

# Edge scaling
s_edge = bot._position_size(Domain.WEATHER, make_opp(edge=0.04))
l_edge = bot._position_size(Domain.WEATHER, make_opp(edge=0.08))
check("Larger edge = larger size",  l_edge > s_edge, f"${s_edge:.0f} vs ${l_edge:.0f}")

# Testing domain cap
bot.engine.domains[Domain.SOCCER].status = DomainStatus.TESTING
soc = bot._position_size(Domain.SOCCER, make_opp(confidence=0.99, edge=0.10))
base_soc = CONFIG['base_no_size']['soccer']
check("Testing domain 20% cap", abs(soc - base_soc * 0.20) < 0.01, f"${soc:.1f}")

# YES fraction
no_s  = bot._position_size(Domain.WEATHER, make_opp())
yes_s = no_s * CONFIG['yes_fraction']
check("YES fraction = 4%", abs(yes_s/no_s - 0.04) < 0.001, f"{yes_s/no_s:.3f}")

# Order fragmentation (Gap 2) — paper mode
cfg = CONFIG.copy()
cfg['paper_trade'] = True
orders = OrderManager(cfg)

r1 = asyncio.run(orders.execute(make_opp(yes_price=0.04), 300.0, 12.0))
check("Paper execution succeeds",   r1.success)
check("Paper mode flagged",         r1.paper)
check("NO filled correctly",        r1.filled_no == 300.0, f"${r1.filled_no:.0f}")
check("Merge when sum<1 (0.99)",    r1.merged)

r2 = asyncio.run(orders.execute(make_opp(yes_price=0.06), 300.0, 18.0))
check("No merge when sum>1 (1.01)", not r2.merged)

# Opportunity dataclass has liquidity field
opp = make_opp(liquidity=500.0)
check("Opportunity has liquidity field", opp.available_liquidity == 500.0)
check("should_merge correct",           opp.should_merge == True)

print()
print("=" * 55)
print(f"Results: {passed} passed  {failed} failed")
if failed == 0:
    print("All gaps confirmed closed in v3")
print("=" * 55)
