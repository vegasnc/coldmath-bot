"""
main.py — ColdMath Bot v2 Entry Point

Reverse-engineered from wallet 0x594edb9112f526fa6a80b8f858a6379c8a2c1c11
Confirmed P&L: $36,432 cumulative (Dec 2025 - Mar 28, 2026)
March 2026 net: $27,697 on $3.1M gross volume (62,374 transactions)

Strategy: Identify near-certain binary outcomes using domain-specific
probability models, buy the favored side large (92-97¢), buy insurance
tiny (3-8¢), merge when YES+NO < $1.00 or hold to resolution.

Domains (in order of activation):
  1. Weather temperature  — winter primary, all year partial
  2. Soccer BTTS/spreads  — spring/autumn primary (xG Poisson model)
  3. Financial closes     — summer fill, year-round (Black-Scholes implied)

Usage:
  python main.py                    # paper trade (default)
  python main.py --live             # live trading (implement orders.py first)
  python main.py --validate SOCCER  # run model validation for a domain
  python main.py --status           # print current engine state
"""

import asyncio
import argparse
import logging
import json
import pathlib
import sys

from core.bot import IntegratedBot
from core.rotation_engine import DomainRotationEngine, Domain, DomainStatus
from core.config import CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/bot.log"),
    ],
)
log = logging.getLogger("main")


def parse_args():
    p = argparse.ArgumentParser(description="ColdMath Bot v2")
    p.add_argument("--live",     action="store_true", help="Enable live trading")
    p.add_argument("--validate", metavar="DOMAIN",    help="Validate a domain model")
    p.add_argument("--status",   action="store_true", help="Print engine status and exit")
    return p.parse_args()


def print_status():
    state_file = pathlib.Path("engine_state.json")
    if not state_file.exists():
        print("No engine state found — bot has not run yet.")
        return
    state = json.loads(state_file.read_text())
    print(f"\nEngine state as of {state['date']}")
    print(f"Total budget: ${state['total_budget']:,.0f}")
    print()
    print(f"  {'Domain':14} {'Status':12} {'Health':8} {'Alloc':8} {'Budget':10}")
    print(f"  {'-'*56}")
    for name, info in state["domains"].items():
        print(
            f"  {name:14} {info['status']:12} "
            f"{info['health']:6.2f}   "
            f"{info['allocation']*100:5.1f}%  "
            f"${info['budget']:>8,.0f}"
        )
    print()


def validate_domain(domain_name: str):
    """
    Runs back-validation for a domain model.
    Prints results and optionally marks as validated.
    """
    try:
        domain = Domain[domain_name.upper()]
    except KeyError:
        print(f"Unknown domain: {domain_name}. Options: {[d.name for d in Domain]}")
        return

    print(f"\nValidating {domain.value} model...")

    if domain == Domain.WEATHER:
        # from models.weather_model import WeatherModel
        from domains.weather import WeatherModel
        model = WeatherModel()
        results = model.backtest(days=30)
    elif domain == Domain.SOCCER:
        # from models.soccer_model import SoccerModel
        from domains.soccer import SoccerModel
        model = SoccerModel()
        results = model.backtest(days=30)
    elif domain == Domain.FINANCIAL:
        # from models.financial_model import FinancialModel
        from domains.financial import FinancialModel
        model = FinancialModel()
        results = model.backtest(days=30)
    else:
        print(f"No validator implemented for {domain.value}")
        return

    print(f"\nBack-test results ({results['days']} days):")
    print(f"  Positions tested:    {results['total_positions']}")
    print(f"  Model accuracy:      {results['accuracy']*100:.1f}%")
    print(f"  Avg edge found:      {results['avg_edge']*100:.2f}%")
    print(f"  Win rate on NO side: {results['no_win_rate']*100:.1f}%")
    print(f"  Simulated net PnL:   ${results['simulated_pnl']:+,.2f}")
    print()

    if results["accuracy"] >= 0.75 and results["no_win_rate"] >= 0.85:
        confirm = input(f"Results look good. Mark {domain.value} as validated? [y/N] ")
        if confirm.lower() == "y":
            engine = DomainRotationEngine(CONFIG["total_budget_usdc"])
            engine.mark_model_validated(domain)
            print(f"{domain.value} marked as validated.")
    else:
        print("Results below threshold. Model needs refinement before scaling.")
        print(f"  Required accuracy:     >= 75% (got {results['accuracy']*100:.1f}%)")
        print(f"  Required NO win rate:  >= 85% (got {results['no_win_rate']*100:.1f}%)")


async def main():
    pathlib.Path("logs").mkdir(exist_ok=True)

    args = parse_args()

    if args.status:
        print_status()
        return

    if args.validate:
        validate_domain(args.validate)
        return

    if args.live:
        log.warning("LIVE TRADING ENABLED — real money at risk")
        log.warning("Ensure orders.py _place_real_order() is implemented")
        CONFIG["paper_trade"] = False
    else:
        log.info("Paper trading mode (default). Use --live to trade real funds.")
        CONFIG["paper_trade"] = True

    bot = IntegratedBot(CONFIG)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
