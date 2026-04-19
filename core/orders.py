"""
core/orders.py

v4 CHANGES:
  - get_open_positions() stub added (required for early sell loop)
  - sell_position() stub added (required for early sell loop)
  - Fragmentation retained from v3 (Gap 2 fix)

DEVELOPER:
  Implement get_open_positions() — fetch from Polymarket data API:
    GET https://data-api.polymarket.com/positions?user={wallet}&limit=500
    Filter for redeemable=false (still live positions)

  Implement sell_position() — place a SELL order on CLOB:
    Same as _place_single_order() but side=SELL
    Use FOK (Fill or Kill) to ensure immediate execution at threshold

  Implement _place_single_order() — live order placement

  Live merge: CTF mergePositions on Polygon (requires POLYGON_RPC_URL, MATIC for gas).
"""

import os
import math
import threading
import aiohttp
import asyncio
import logging
from dotenv import load_dotenv
from dataclasses import dataclass, field
from typing import Any, Optional
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
from web3 import Web3
from py_clob_client.config import get_contract_config

from core.monitor_hub import emit as mon_emit
from core.monitor_hub import is_enabled as mon_enabled


log = logging.getLogger("orders")

load_dotenv()

NO_FRAGMENT_SIZE  = 20.0
YES_FRAGMENT_SIZE = 0.50
FRAGMENT_PAUSE    = 0.5


@dataclass
class OrderResult:
    success:    bool
    filled_no:  float = 0.0
    filled_yes: float = 0.0
    merged:     bool  = False
    order_ids:  list  = field(default_factory=list)
    error:      Optional[str] = None
    paper:      bool  = False


class OrderManager:

    def __init__(self, config: dict):
        self.config      = config
        self.paper_trade = config.get("paper_trade", True)
        # In-memory Polymarket-shaped rows for paper mode (early sell + sell logs).
        self._paper_virtual_positions: list[dict] = []
        self._paper_pf_lock = threading.Lock()
        if not self.paper_trade:
            self._init_live_client()

    def _init_live_client(self):
        """
        DEVELOPER: Initialize py-clob-client here.

        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
        import os

        self.client = ClobClient(
            host     = self.config["polymarket_clob_url"],
            key      = os.environ["POLYMARKET_PRIVATE_KEY"],
            chain_id = 137,
            creds    = ApiCreds(
                api_key        = os.environ["POLYMARKET_API_KEY"],
                api_secret     = os.environ["POLYMARKET_API_SECRET"],
                api_passphrase = os.environ["POLYMARKET_PASSPHRASE"],
            )
        )
        """

        self.client = ClobClient(
            host     = self.config["polymarket_clob_url"],
            key      = os.environ["POLYMARKET_PRIVATE_KEY"],
            chain_id = 137,
            creds    = ApiCreds(
                api_key        = os.environ["POLYMARKET_API_KEY"],
                api_secret     = os.environ["POLYMARKET_API_SECRET"],
                api_passphrase = os.environ["POLYMARKET_PASSPHRASE"],
            )
        )

        if self.config["paper_trade"]:
            log.warning("Live client not implemented — forcing paper trade")
            self.paper_trade = True
        else:
            log.warning("This is Live Mode")

    def _trade_details_enabled(self) -> bool:
        return bool(self.config.get("log_trade_execution_details", True))

    @staticmethod
    def _opp_description(opp: Any) -> str:
        dm = getattr(opp, "domain_meta", None) or {}
        if isinstance(dm, dict) and dm:
            parts = []
            for k in ("city", "threshold", "direction", "unit", "group_item_title"):
                if dm.get(k) is not None and str(dm.get(k)).strip() != "":
                    parts.append(f"{k}={dm.get(k)}")
            if parts:
                return "; ".join(parts)
        return (getattr(opp, "slug", None) or "").strip() or "(no extra description)"

    def _fmt_price(self, p: float) -> str:
        return f"{p:.6f} ({p * 100:.2f}c per $1 notional)"

    def _short_token(self, token_id: str) -> str:
        t = str(token_id or "")
        if len(t) <= 14:
            return t or "(none)"
        return f"{t[:8]}...{t[-4:]}"

    def _log_buy_ticket(
        self,
        opp: Any,
        *,
        leg: str,
        shares: float,
        price: float,
        mode: str,
        fragment_index: int = 0,
        fragment_total: int = 0,
        order_id: Optional[str] = None,
        annotation: str = "",
    ) -> None:
        if not self._trade_details_enabled():
            return
        title = (getattr(opp, "title", None) or "").strip()
        if len(title) > 180:
            title = title[:177] + "..."
        slug = (getattr(opp, "slug", None) or "").strip()
        desc = self._opp_description(opp)
        domain = (getattr(opp, "domain", None) or "").strip()
        tok = self._short_token(
            getattr(opp, "no_token_id", "") if leg.upper() == "NO" else getattr(opp, "yes_token_id", "")
        )
        if fragment_total > 1:
            slice_line = f"Live clip:   {fragment_index} of {fragment_total} (this leg only)"
        else:
            slice_line = "Live clip:   1 of 1 (this log line)"
        oid = (order_id or "")[:28] + ("..." if order_id and len(order_id) > 28 else "")
        lines = [
            "---------- BUY TICKET ----------",
            f"Title:       {title or '(unknown)'}",
            f"Description: {desc}",
            f"Slug:        {slug or '(none)'}",
            f"Domain:      {domain or '-'}",
            f"Outcome leg: {leg} (outcome token)",
            f"Token id:    {tok}",
            slice_line,
            f"Shares:      {shares:.6f}",
            f"Buy price:   {self._fmt_price(float(price))}",
            f"Est. cost:   ${float(shares) * float(price):,.4f}",
            f"Mode:        {mode}",
        ]
        if annotation:
            lines.append(f"Note:        {annotation}")
        if oid:
            lines.append(f"Order id:    {oid}")
        lines.append("--------------------------------")
        log.info("\n".join(lines))
        if mon_enabled(self.config):
            mon_emit(
                "buy",
                leg=leg,
                shares=float(shares),
                price=float(price),
                mode=mode,
                slug=slug,
                title=title,
                domain=domain,
                fragment_index=fragment_index,
                fragment_total=fragment_total,
                order_id=(order_id or "")[:64],
            )

    def _log_sell_ticket(
        self,
        *,
        title: str,
        description: str,
        slug: str,
        outcome: str,
        shares: float,
        min_price: float,
        mark_price: float,
        token_id: str,
        mode: str,
        order_id: Optional[str] = None,
        success: bool = True,
        error: Optional[str] = None,
    ) -> None:
        if not self._trade_details_enabled():
            return
        t = (title or "").strip()
        if len(t) > 180:
            t = t[:177] + "..."
        desc = (description or "").strip() or "(none)"
        sl = (slug or "").strip() or "(none)"
        oid = (order_id or "")[:28] + ("..." if order_id and len(order_id) > 28 else "")
        lines = [
            "---------- SELL TICKET ----------",
            f"Title:          {t or '(unknown)'}",
            f"Description:    {desc}",
            f"Slug:           {sl}",
            f"Outcome leg:    {outcome or '-'}",
            f"Token id:       {self._short_token(token_id)}",
            f"Shares sold:    {shares:.6f}",
            f"Min sell price: {self._fmt_price(float(min_price))} (floor you set)",
            f"Mark / spot:    {self._fmt_price(float(mark_price))} (price when sell triggered)",
            f"Est. proceeds:  ${float(shares) * float(min_price):,.4f} (at floor)",
            f"Mode:           {mode}",
            f"Result:         {'SUCCESS' if success else 'FAILED'}",
        ]
        if error:
            lines.append(f"Error:          {error}")
        if oid:
            lines.append(f"Order id:       {oid}")
        lines.append("---------------------------------")
        log.info("\n".join(lines))
        if mon_enabled(self.config):
            mon_emit(
                "sell",
                title=t,
                slug=sl,
                description=desc,
                outcome=outcome,
                shares=float(shares),
                min_price=float(min_price),
                mark_price=float(mark_price),
                mode=mode,
                token_id=self._short_token(token_id),
                success=success,
                error=error or "",
                order_id=(order_id or "")[:64],
            )

    def _wallet_address(self) -> Optional[str]:
        """POLYMARKET_WALLET or address derived from POLYMARKET_PRIVATE_KEY."""
        w = os.environ.get("POLYMARKET_WALLET") or os.environ.get("POLYMARKET_ADDRESS")
        if w:
            return w.strip()
        pk = os.environ.get("POLYMARKET_PRIVATE_KEY")
        if not pk:
            return None
        try:
            from eth_account import Account

            return Account.from_key(pk).address
        except Exception:
            try:
                return Web3().eth.account.from_key(pk).address
            except Exception:
                return None

    # ── MAIN EXECUTION ────────────────────────────────────────────────────

    def log_virtual_trade_plan(
        self,
        opp: Any,
        no_size: float,
        yes_size: float,
        *,
        session: str = "",
        domain: str = "",
        source: str = "SCAN",
        note: Optional[str] = None,
    ) -> None:
        """
        Plain-language log of what a BUY-NO + BUY-YES would look like (no wallet).
        """
        if not self.config.get("log_virtual_suitable_trades", True):
            return

        def _pct(x: float) -> str:
            return f"{x * 100:.0f}%"

        def _cents(x: float) -> str:
            return f"{x * 100:.0f}c"

        title = (getattr(opp, "title", None) or "").strip()
        if len(title) > 100:
            title = title[:97] + "..."
        slug = (getattr(opp, "slug", None) or "").strip()
        if len(slug) > 80:
            slug = slug[:77] + "..."
        dom = (domain or getattr(opp, "domain", None) or "market").strip()
        sess = session or "-"
        src = source or "scan"

        no_px = float(getattr(opp, "no_price", 0) or 0)
        yes_px = float(getattr(opp, "yes_price", 0) or 0)
        prob_no = float(getattr(opp, "our_prob_no", 0) or 0)
        edge = float(getattr(opp, "edge", 0) or 0)
        conf = float(getattr(opp, "confidence", 0) or 0)
        should_merge = (no_px + yes_px) < 0.998
        no_notional = no_size * no_px
        yes_notional = yes_size * yes_px
        combined = no_notional + yes_notional
        merge_lock = max(0.0, (1.0 - no_px - yes_px) * no_size) if should_merge else 0.0

        if self.paper_trade:
            banner = "VIRTUAL TRADE (practice only — no money moves)"
        else:
            banner = "ORDER PREVIEW (live mode — real orders only if bot executes next)"

        why = (
            f"We think NO happens about {_pct(prob_no)} of the time. "
            f"The market asks {_cents(no_px)} for NO and {_cents(yes_px)} for YES. "
            f"That is an edge of about {edge * 100:+.1f} points for us (model minus market), "
            f"with {_pct(conf)} model confidence."
        )

        if no_size <= 0 and yes_size <= 0:
            buys = "No buy sizes (zero shares) — see note below."
        else:
            buys = (
                f"Would buy about {no_size:.0f} NO at {_cents(no_px)} (~${no_notional:,.0f}) "
                f"and about {yes_size:.1f} YES at {_cents(yes_px)} (~${yes_notional:,.0f}). "
                f"Rough total about ${combined:,.0f}."
            )

        if should_merge and no_size > 0:
            then_line = (
                f"If filled, we would try to MERGE: YES+NO cost under $1, "
                f"so about ${merge_lock:,.0f} of profit is locked on this NO size."
            )
        elif should_merge:
            then_line = "Merge would be possible (YES+NO under $1), but size is zero so nothing to do."
        else:
            then_line = (
                "If filled, we would HOLD: combined price is not cheap enough to merge for a locked profit."
            )

        lines = [
            banner,
            f"Where: {dom} | when: session {sess} | trigger: {src}",
            f"Question: {title or '(no title)'}",
            f"Slug: {slug or '(no slug)'}",
            why,
            buys,
            then_line,
        ]
        if note:
            lines.append(f"What happened: {note}")
        log.info("\n".join(lines))
        if mon_enabled(self.config):
            mon_emit(
                "virtual_plan",
                domain=dom,
                session=sess,
                source=src,
                slug=slug,
                title=title,
                no_size=float(no_size),
                yes_size=float(yes_size),
                no_price=no_px,
                yes_price=yes_px,
                edge=edge,
                confidence=conf,
                note=note or "",
                ticket_detail=(note or "").strip(),
            )

    async def execute(
        self,
        opp,
        no_size: float,
        yes_size: float,
        *,
        quiet_paper: bool = False,
    ) -> OrderResult:
        should_merge = (opp.no_price + opp.yes_price) < 0.998

        if self.paper_trade and not quiet_paper:
            log.info(
                "Paper execute (verbose): %s no=%.2f yes=%.2f merge_eligible=%s",
                getattr(opp, "slug", ""),
                no_size,
                yes_size,
                should_merge,
            )

        if self.paper_trade:
            return self._paper_execute(opp, no_size, yes_size, should_merge, quiet=quiet_paper)
        return await self._live_execute(opp, no_size, yes_size, should_merge)

    def _paper_execute(
        self,
        opp,
        no_size: float,
        yes_size: float,
        should_merge: bool,
        *,
        quiet: bool = False,
    ) -> OrderResult:
        no_frags = max(1, int(no_size / NO_FRAGMENT_SIZE))
        yes_frags = max(1, int(yes_size / YES_FRAGMENT_SIZE))
        action = "MERGE" if should_merge else "HOLD"
        guaranteed = max(0, (1.0 - opp.no_price - opp.yes_price) * no_size)
        if self._trade_details_enabled():
            self._log_buy_ticket(
                opp,
                leg="NO",
                shares=no_size,
                price=float(opp.no_price),
                mode="paper (simulated)",
                fragment_index=1,
                fragment_total=1,
                annotation=(
                    f"Live trading would clip NO into about {no_frags} orders "
                    f"(max {NO_FRAGMENT_SIZE:g} shares each)."
                ),
            )
            self._log_buy_ticket(
                opp,
                leg="YES",
                shares=yes_size,
                price=float(opp.yes_price),
                mode="paper (simulated)",
                fragment_index=1,
                fragment_total=1,
                annotation=(
                    f"Live trading would clip YES into about {yes_frags} orders "
                    f"(max {YES_FRAGMENT_SIZE:g} shares each)."
                ),
            )
            log.info(
                "Paper follow-up | slug=%s | %s | est. merge profit about $%.4f on this NO size",
                (getattr(opp, "slug", "") or "")[:56],
                action,
                guaranteed if should_merge else 0.0,
            )
            if mon_enabled(self.config) and should_merge:
                mon_emit(
                    "merge",
                    slug=(getattr(opp, "slug", "") or "")[:120],
                    title=(getattr(opp, "title", "") or "")[:200],
                    est_profit_usd=round(guaranteed, 4),
                    mode="paper",
                    merged=True,
                )
        elif quiet:
            log.debug(
                "[PAPER] fill slug=%s NO=%.2f YES=%.2f %s +$%.3f",
                (getattr(opp, "slug", "") or "")[:40],
                no_size,
                yes_size,
                action,
                guaranteed if should_merge else 0.0,
            )
        else:
            log.info(
                f"[PAPER] {opp.domain} | {opp.slug[:40]}\n"
                f"         NO  ${no_size:.2f} @ {opp.no_price:.4f} ({no_frags} frags)\n"
                f"         YES ${yes_size:.2f} @ {opp.yes_price:.4f} ({yes_frags} frags)\n"
                f"         {action}" + (f" +${guaranteed:.3f}" if should_merge else "")
            )
        self._paper_register_fills(opp, no_size, yes_size)
        return OrderResult(success=True, filled_no=no_size, filled_yes=yes_size, merged=should_merge, paper=True)

    def _paper_register_fills(self, opp: Any, no_size: float, yes_size: float) -> None:
        """Record virtual outcome-token positions after a paper execute (for get_open_positions / early sell)."""
        if not self.paper_trade:
            return
        th = float(self.config.get("early_sell_threshold", 0.99))
        mark_no = float(self.config.get("paper_virtual_no_mark_price", 0.995))
        mark_no = max(mark_no, th + 0.0005)
        title = (getattr(opp, "title", None) or "").strip()
        slug = (getattr(opp, "slug", None) or "").strip()
        desc = self._opp_description(opp)
        cid = str(getattr(opp, "condition_id", None) or "")

        def _row(asset: str, outcome: str, sz: float, avg_px: float, cur_px: float) -> dict:
            return {
                "asset": str(asset),
                "title": title,
                "slug": slug,
                "description": desc,
                "outcome": outcome,
                "size": float(sz),
                "avgPrice": float(avg_px),
                "curPrice": float(cur_px),
                "currentPrice": float(cur_px),
                "redeemable": False,
                "conditionId": cid,
            }

        added = 0
        no_tok = getattr(opp, "no_token_id", None) or ""
        yes_tok = getattr(opp, "yes_token_id", None) or ""
        with self._paper_pf_lock:
            if no_size > 0.001 and no_tok:
                self._paper_virtual_positions.append(
                    _row(no_tok, "NO", no_size, float(opp.no_price), mark_no)
                )
                added += 1
            if yes_size > 0.001 and yes_tok:
                self._paper_virtual_positions.append(
                    _row(yes_tok, "YES", yes_size, float(opp.yes_price), float(opp.yes_price))
                )
                added += 1
            total = len(self._paper_virtual_positions)
        if added:
            log.info(
                "Paper virtual portfolio | added %d leg(s) | now %d open simulated position(s)",
                added,
                total,
            )

    def _paper_apply_sell(self, token_id: str, size: float) -> None:
        """Reduce or remove a virtual position after a simulated sell."""
        tid = str(token_id)
        rem = float(size)
        with self._paper_pf_lock:
            out: list[dict] = []
            for p in self._paper_virtual_positions:
                if str(p.get("asset")) != tid:
                    out.append(p)
                    continue
                if rem <= 0:
                    out.append(p)
                    continue
                psz = float(p.get("size", 0) or 0)
                if psz > rem + 1e-9:
                    q = dict(p)
                    q["size"] = psz - rem
                    out.append(q)
                    rem = 0.0
                else:
                    rem -= psz
            self._paper_virtual_positions = out

    async def _live_execute(self, opp, no_size, yes_size, should_merge) -> OrderResult:
        result = OrderResult(success=False)

        total_no_slices = max(1, int(math.ceil(no_size / NO_FRAGMENT_SIZE - 1e-9)))
        total_yes_slices = max(1, int(math.ceil(yes_size / YES_FRAGMENT_SIZE - 1e-9)))

        # Sweep NO in fragments
        remaining = no_size
        filled_no = 0.0
        no_slice_i = 0
        while remaining > 0.10:
            frag = min(NO_FRAGMENT_SIZE, remaining)
            oid = await self._place_single_order(opp.no_token_id, frag, opp.no_price)
            if oid:
                no_slice_i += 1
                result.order_ids.append(oid)
                filled_no += frag
                remaining -= frag
                if self._trade_details_enabled():
                    self._log_buy_ticket(
                        opp,
                        leg="NO",
                        shares=frag,
                        price=float(opp.no_price),
                        mode="live",
                        fragment_index=no_slice_i,
                        fragment_total=total_no_slices,
                        order_id=str(oid),
                    )
                await asyncio.sleep(FRAGMENT_PAUSE)
            else:
                break
        result.filled_no = filled_no

        if filled_no < no_size * 0.5:
            result.success = filled_no > 0
            return result

        # Sweep YES in fragments
        remaining = yes_size
        filled_yes = 0.0
        yes_slice_i = 0
        while remaining > 0.01:
            frag = min(YES_FRAGMENT_SIZE, remaining)
            oid = await self._place_single_order(opp.yes_token_id, frag, opp.yes_price)
            if oid:
                yes_slice_i += 1
                result.order_ids.append(oid)
                filled_yes += frag
                remaining -= frag
                if self._trade_details_enabled():
                    self._log_buy_ticket(
                        opp,
                        leg="YES",
                        shares=frag,
                        price=float(opp.yes_price),
                        mode="live",
                        fragment_index=yes_slice_i,
                        fragment_total=total_yes_slices,
                        order_id=str(oid),
                    )
                await asyncio.sleep(FRAGMENT_PAUSE)
            else:
                break
        result.filled_yes = filled_yes

        if should_merge and filled_no > 0 and filled_yes > 0:
            try:
                amount_sets = min(filled_no, filled_yes)
                await self._merge(opp.condition_id, amount_sets)
                result.merged = True
                profit = (1 - opp.no_price - opp.yes_price) * filled_no
                log.info(
                    "MERGE on-chain | title=%s | slug=%s | sets=%.4f | est. locked profit ~$%.4f",
                    (getattr(opp, "title", "") or "")[:80],
                    (getattr(opp, "slug", "") or "")[:56],
                    amount_sets,
                    profit,
                )
                if mon_enabled(self.config):
                    mon_emit(
                        "merge",
                        slug=(getattr(opp, "slug", "") or "")[:120],
                        title=(getattr(opp, "title", "") or "")[:200],
                        amount_sets=amount_sets,
                        est_profit_usd=round(profit, 4),
                        mode="live",
                    )
            except Exception as e:
                log.error(f"Merge failed: {e}")
                if mon_enabled(self.config):
                    mon_emit("merge", success=False, error=str(e), mode="live")

        result.success = filled_no > 0
        return result

    # ── EARLY SELL ──────────────────────────────────────────────────────

    async def get_open_positions(self) -> list[dict]:
        """
        Live positions from Polymarket Data API (dev guide Priority 2):
        GET {polymarket_data_url}/positions?user={wallet}&limit=500&redeemable=false
        Keep only positions that are not yet redeemable (still open).
        """
        if self.paper_trade:
            with self._paper_pf_lock:
                snap = list(self._paper_virtual_positions)
            n = len(snap)
            if n:
                log.info("Paper virtual positions | returning %d simulated open leg(s) for early-sell scan", n)
            return snap

        wallet = self._wallet_address()
        if not wallet:
            log.warning("No wallet for positions API — set POLYMARKET_WALLET or POLYMARKET_PRIVATE_KEY")
            return []

        base = self.config.get("polymarket_data_url", "https://data-api.polymarket.com").rstrip("/")
        url = f"{base}/positions"
        params: dict[str, Any] = {"user": wallet, "limit": 500, "redeemable": "false"}

        try:
            timeout = aiohttp.ClientTimeout(total=25)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        log.warning("positions API HTTP %s: %s", resp.status, text[:200])
                        return []
                    data = await resp.json()
        except Exception as e:
            log.warning("get_open_positions failed: %s", e)
            return []

        if not isinstance(data, list):
            log.warning("positions API returned %s, expected list", type(data).__name__)
            return []

        def _still_open(row: dict) -> bool:
            r = row.get("redeemable")
            if r is False:
                return True
            if isinstance(r, str) and r.strip().lower() in ("false", "0", "no"):
                return True
            return False

        return [p for p in data if isinstance(p, dict) and _still_open(p)]

    async def sell_position(
        self,
        token_id: str,
        size: float,
        min_price: float,
        *,
        title: str = "",
        description: str = "",
        slug: str = "",
        outcome: str = "",
        mark_price: Optional[float] = None,
    ) -> OrderResult:
        """
        SELL on CLOB (FOK) for early-recycle loop (dev guide). Paper mode simulates success.
        Pass title / slug / mark_price for readable SELL logs.
        """
        mark = float(mark_price) if mark_price is not None else float(min_price)

        if self.paper_trade:
            if self._trade_details_enabled():
                self._log_sell_ticket(
                    title=title,
                    description=description,
                    slug=slug,
                    outcome=outcome or "NO",
                    shares=size,
                    min_price=min_price,
                    mark_price=mark,
                    token_id=token_id,
                    mode="paper (simulated)",
                    order_id=None,
                    success=True,
                )
            else:
                tid = self._short_token(token_id)
                log.info(
                    "[PAPER SELL] %s | shares=%.4f min=%.4f ~$%.2f",
                    tid,
                    size,
                    min_price,
                    size * min_price,
                )
            self._paper_apply_sell(token_id, size)
            return OrderResult(success=True, paper=True)

        if not hasattr(self, "client") or self.client is None:
            log.error("sell_position: CLOB client not initialized")
            if self._trade_details_enabled():
                self._log_sell_ticket(
                    title=title,
                    description=description,
                    slug=slug,
                    outcome=outcome or "NO",
                    shares=size,
                    min_price=min_price,
                    mark_price=mark,
                    token_id=token_id,
                    mode="live",
                    success=False,
                    error="No CLOB client",
                )
            return OrderResult(success=False, error="No CLOB client")

        try:
            order = self.client.create_market_order(
                MarketOrderArgs(
                    token_id=token_id,
                    amount=size,
                    side=SELL,
                )
            )
            resp = self.client.post_order(order, OrderType.FOK)
            oid = (resp or {}).get("orderID") or (resp or {}).get("orderId")
            oid_s = str(oid) if oid else None
            if self._trade_details_enabled():
                self._log_sell_ticket(
                    title=title,
                    description=description,
                    slug=slug,
                    outcome=outcome or "NO",
                    shares=size,
                    min_price=min_price,
                    mark_price=mark,
                    token_id=token_id,
                    mode="live",
                    order_id=oid_s,
                    success=True,
                )
            return OrderResult(success=True, order_ids=[oid] if oid else [])
        except Exception as e:
            log.error("Sell failed: %s", e)
            if self._trade_details_enabled():
                self._log_sell_ticket(
                    title=title,
                    description=description,
                    slug=slug,
                    outcome=outcome or "NO",
                    shares=size,
                    min_price=min_price,
                    mark_price=mark,
                    token_id=token_id,
                    mode="live",
                    success=False,
                    error=str(e),
                )
            return OrderResult(success=False, error=str(e))

    # ── PRIMITIVES ────────────────────────────────────────────────────────

    async def _place_single_order(self, token_id: str, size: float, price: float) -> Optional[str]:
        """
        DEVELOPER: Place one FOK order on Polymarket CLOB.

        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        order = self.client.create_market_order(
            MarketOrderArgs(token_id=token_id, amount=size)
        )
        resp = self.client.post_order(order, OrderType.FOK)
        return resp.get("orderID")
        """
        order = self.client.create_market_order(
            MarketOrderArgs(
                token_id=token_id, 
                amount=size,
                side=BUY,
                order_type=OrderType.FOK
            )
        )
        resp = self.client.post_order(order, OrderType.FOK)
        return resp.get("orderID")
        
        # log.error("_place_single_order not implemented")
        # return None

    async def _merge(self, condition_id: str, amount_sets: float) -> None:
        """
        Merge equal YES+NO outcome balances into USDC.e via Conditional Tokens
        mergePositions (on-chain). ClobClient has no merge API.
        """
        MERGE_ABI = [
            {
                "name": "mergePositions",
                "type": "function",
                "stateMutability": "nonpayable",
                "inputs": [
                    {"name": "collateralToken", "type": "address"},
                    {"name": "parentCollectionId", "type": "bytes32"},
                    {"name": "conditionId", "type": "bytes32"},
                    {"name": "partition", "type": "uint256[]"},
                    {"name": "amount", "type": "uint256"},
                ],
            }
        ]

        def _sync_merge() -> None:
            rpc = os.environ.get("POLYGON_RPC_URL")
            if not rpc:
                raise RuntimeError("POLYGON_RPC_URL is required for on-chain merge")

            amount_wei = int(round(amount_sets * 1_000_000))
            if amount_wei <= 0:
                raise ValueError("amount_sets must be positive")

            cfg = get_contract_config(self.client.chain_id, neg_risk=False)
            w3 = Web3(Web3.HTTPProvider(rpc))
            if not w3.is_connected():
                raise RuntimeError("Polygon RPC not connected")

            acct = w3.eth.account.from_key(self.client.signer.private_key)
            ctf = w3.eth.contract(
                address=Web3.to_checksum_address(cfg.conditional_tokens),
                abi=MERGE_ABI,
            )

            parent_zero = bytes(32)
            cid = Web3.to_bytes(hexstr=condition_id)

            tx = ctf.functions.mergePositions(
                Web3.to_checksum_address(cfg.collateral),
                parent_zero,
                cid,
                [1, 2],
                amount_wei,
            ).build_transaction(
                {
                    "from": acct.address,
                    "nonce": w3.eth.get_transaction_count(acct.address),
                    "chainId": self.client.chain_id,
                }
            )

            tx["gas"] = int(w3.eth.estimate_gas(tx) * 1.25)

            try:
                gp = w3.eth.gas_price
                if gp:
                    tx["gasPrice"] = gp
            except Exception:
                pass

            if "gasPrice" not in tx or not tx.get("gasPrice"):
                block = w3.eth.get_block("latest")
                base = block.get("baseFeePerGas") or 0
                tip = w3.to_wei(2, "gwei")
                tx["maxPriorityFeePerGas"] = tip
                tx["maxFeePerGas"] = base * 2 + tip

            signed = acct.sign_transaction(tx)
            raw = getattr(signed, "raw_transaction", None) or signed.rawTransaction
            tx_hash = w3.eth.send_raw_transaction(raw)
            rcpt = w3.eth.wait_for_transaction_receipt(tx_hash)
            if rcpt["status"] != 1:
                raise RuntimeError("mergePositions transaction reverted")

        await asyncio.to_thread(_sync_merge)
        log.info(
            "mergePositions ok condition=%s amount_sets=%.6f",
            (condition_id[:16] + "...") if len(condition_id) > 16 else condition_id,
            amount_sets,
        )
