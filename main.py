"""
main.py — ColdMath Bot v4 Entry Point

Per developer guide (ColdMath v4):
  • Core rule: P_model(NO) − market_price(NO) ≥ min_edge AND confidence ≥ min_confidence
  • Six UTC sessions (S0–S5), early sell on NO ≥ 99¢, four domains (weather, soccer, financial, cycling)

Usage:
  python main.py                      # paper trade — same session clock as live
  python main.py --live               # live trading (requires .env CLOB credentials)
  python main.py --validate WEATHER   # domain model backtest stub
  python main.py --validate CYCLING
  python main.py --status             # print engine state from engine_state.json
  python main.py --web               # bot + web UI on web_bind_host:web_port (build frontend first)
"""

import asyncio
import argparse
import logging
import json
import pathlib
import socket
import sys

from core.bot import IntegratedBot
from core.rotation_engine import DomainRotationEngine, Domain, DomainStatus
from core.config import CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("main")


def _install_asyncio_network_exception_handler() -> None:
    """
    aiohttp may resolve hosts in background tasks; DNS failures (e.g. Windows
    socket.gaierror 11001) can otherwise print as ERROR [asyncio] on stderr even
    when the caller catches ClientError. Downgrade known transient cases to WARNING.
    """
    loop = asyncio.get_running_loop()
    prev = loop.get_exception_handler()
    try:
        import aiohttp

        _connector_err = (aiohttp.ClientConnectorError,)
    except Exception:
        _connector_err = ()

    def _handler(l: asyncio.AbstractEventLoop, context: dict) -> None:
        exc = context.get("exception")
        if isinstance(exc, socket.gaierror):
            log.warning(
                "DNS lookup failed during async I/O (errno=%s). Check network, VPN, or DNS. %s",
                getattr(exc, "errno", None),
                exc,
            )
            return
        if _connector_err and isinstance(exc, _connector_err):
            log.warning("HTTP client connector error (often DNS or blocked TLS): %s", exc)
            return
        if prev is not None:
            prev(l, context)
        else:
            l.default_exception_handler(context)

    loop.set_exception_handler(_handler)


def parse_args():
    p = argparse.ArgumentParser(description="ColdMath Bot v4")
    p.add_argument("--live",     action="store_true", help="Enable live trading")
    p.add_argument("--validate", metavar="DOMAIN",    help="Validate a domain model")
    p.add_argument("--status",   action="store_true", help="Print engine status and exit")
    p.add_argument(
        "--web",
        action="store_true",
        help="Start web backend (FastAPI + static UI + WS) on web_bind_host:web_port with the bot",
    )
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
    elif domain == Domain.CYCLING:
        from domains.cycling import CyclingModel
        model = CyclingModel()
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
    _install_asyncio_network_exception_handler()

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

    if args.web:
        CONFIG["web_enabled"] = True

    from core.monitor_hub import set_event_loop
    from core.web_server import dashboard_url, run_web_server, web_should_run

    set_event_loop(asyncio.get_running_loop())

    bot = IntegratedBot(CONFIG)

    if web_should_run(args.web, CONFIG):
        log.info("Web dashboard (bot + UI + API): %s", dashboard_url(CONFIG))
        log.info(
            "Optional Vite UI (hot reload): run `cd frontend && npm run dev` in another terminal, "
            "then open http://127.0.0.1:5173/ (bot must stay running on this port)."
        )
        await asyncio.gather(bot.run(), run_web_server(CONFIG))
    else:
        await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
