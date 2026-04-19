"""
Polymarket CLOB Market WebSocket feed.

Spec: https://docs.polymarket.com/market-data/websocket/market-channel
Overview: https://docs.polymarket.com/market-data/websocket/overview

Public endpoint (no auth):
  wss://ws-subscriptions-clob.polymarket.com/ws/market

Subscribe with asset (token) IDs from Gamma/REST, then consume real-time
book / price_change / best_bid_ask updates for YES/NO pricing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from typing import Any, Awaitable, Callable, Iterable, Optional

import websockets

log = logging.getLogger("polymarket_market_ws")

# Keep in sync with domains.weather.WeatherScanner.KEYWORDS (avoid import cycle).
_WEATHER_KEYWORDS = (
    "highest temperature",
    "will the temperature",
    "temperature in",
)


def _text_matches_weather(text: str) -> bool:
    t = (text or "").lower()
    return any(kw in t for kw in _WEATHER_KEYWORDS)


def _f(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except (TypeError, ValueError):
        return None


def _short_hex(s: str, *, head: int = 8, tail: int = 6) -> str:
    """Shorten long condition / token ids for logs."""
    t = str(s or "").strip()
    if not t:
        return "(unknown)"
    is0x = t.lower().startswith("0x")
    body = t[2:] if is0x else t
    pref = "0x" if is0x else ""
    if len(body) <= head + tail + 1:
        return t[:20] + ("..." if len(t) > 20 else "")
    return f"{pref}{body[:head]}...{body[-tail:]}"


def _fmt_px_cents(x: Optional[float]) -> str:
    """Polymarket price 0..1 shown as cents (readable in logs)."""
    if x is None:
        return "empty"
    c = x * 100.0
    if abs(c) < 1e-9:
        return "0c"
    s = f"{c:.2f}".rstrip("0").rstrip(".")
    return f"{s}c"


def _fmt_side_change(side: str, old: Optional[float], new: Optional[float]) -> str:
    """One readable phrase for bid or ask (avoids raw None->0.0)."""
    if old is None and new is None:
        return f"{side}: still empty"
    if old is None:
        return f"{side}: was empty, now {_fmt_px_cents(new)}"
    if new is None:
        return f"{side}: was {_fmt_px_cents(old)}, now empty"
    if old == new:
        return f"{side}: still {_fmt_px_cents(new)}"
    return f"{side}: {_fmt_px_cents(old)} -> {_fmt_px_cents(new)}"


class PolymarketMarketWsFeed:
    """
    Single shared connection to the Market channel.
    - REST/Gamma supplies clob token IDs (asset_ids).
    - This class subscribes and keeps best bid / ask per asset_id.
    """

    DEFAULT_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self, config: dict):
        self._config = config
        self._url = config.get("polymarket_ws_market_url", self.DEFAULT_URL)
        self._wanted: set[str] = set()
        self._subscribed: set[str] = set()
        self._quotes: dict[str, dict[str, Optional[float]]] = {}
        self._asset_to_market: dict[str, str] = {}
        # Human-readable context for logs (title, slug, optional outcome label).
        self._asset_display: dict[str, dict[str, str]] = {}
        # Optional per-asset gate evidence from WeatherScanner (JSON-serializable dicts).
        self._ticket_evidence: dict[str, dict[str, Any]] = {}
        self._dirty_quote_assets: set[str] = set()
        self._quote_batch_handler: Optional[Callable[[set[str]], Awaitable[None]]] = None
        self._weather_pending_slugs: list[str] = []
        self._weather_pending_seen: set[str] = set()
        self._pending_thread_lock = threading.Lock()
        self._lock = asyncio.Lock()
        self._run_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._ping_task: Optional[asyncio.Task] = None
        self._ws: Any = None

    @property
    def running(self) -> bool:
        return self._run_task is not None and not self._run_task.done()

    async def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self._run_task = asyncio.create_task(self._runner(), name="polymarket_market_ws")

    async def stop(self) -> None:
        self._stop.set()
        if self._ping_task:
            self._ping_task.cancel()
            self._ping_task = None
        if self._ws and not getattr(self._ws, "closed", True):
            await self._ws.close()
        self._ws = None
        if self._run_task:
            self._run_task.cancel()
            try:
                await self._run_task
            except asyncio.CancelledError:
                pass
            self._run_task = None

    def get_buy_asks(self, yes_token_id: str, no_token_id: str) -> tuple[Optional[float], Optional[float]]:
        """Best ask to BUY YES and BUY NO (None if unknown)."""
        y = self._quotes.get(yes_token_id) or {}
        n = self._quotes.get(no_token_id) or {}
        return _f(y.get("best_ask")), _f(n.get("best_ask"))

    def apply_ticket_evidence_map(self, m: Optional[dict[str, dict[str, Any]]]) -> None:
        """Replace gate/reject evidence keyed by CLOB token id (YES and NO both present)."""
        self._ticket_evidence = dict(m or {})

    def set_asset_display(self, asset_id: str, *, title: str, slug: str, outcome: str = "") -> None:
        """Register stable labels for WS logs (weather scanner fills this each scan)."""
        aid = str(asset_id or "").strip()
        if not aid:
            return
        self._asset_display[aid] = {
            "title": title or "",
            "slug": slug or "",
            "outcome": outcome or "",
        }

    def set_quote_batch_handler(self, handler: Optional[Callable[[set[str]], Awaitable[None]]]) -> None:
        """Single async consumer for batched quote-touch events (weather-only use)."""
        self._quote_batch_handler = handler

    def emit_dashboard_price_snapshot(self, *, max_rows: int = 200) -> None:
        """
        One-shot push of current best bid/ask for every labeled asset to the monitor UI.
        Call after subscribe + short sleep so the dashboard fills even before the next tick.
        """
        try:
            from core.monitor_hub import emit as mon_emit
            from core.monitor_hub import is_enabled
        except Exception:
            return
        if not is_enabled(self._config):
            return
        rows: list[dict[str, Any]] = []
        for aid, disp in list(self._asset_display.items()):
            if len(rows) >= max_rows:
                break
            q = self._quotes.get(aid) or {}
            bb, ba = _f(q.get("best_bid")), _f(q.get("best_ask"))
            title_t = (disp.get("title") or "").strip() or "Polymarket"
            slug_t = (disp.get("slug") or "").strip() or _short_hex(aid)
            outcome_t = (disp.get("outcome") or "").strip()
            ev = dict(self._ticket_evidence.get(aid) or {})
            rows.append(
                {
                    "asset_key": aid,
                    "title": title_t[:200],
                    "slug": slug_t[:120],
                    "outcome": outcome_t[:16],
                    "best_bid": bb,
                    "best_ask": ba,
                    "source": "snapshot",
                    **ev,
                }
            )
        if rows:
            mon_emit("weather_prices", row_count=len(rows), rows=rows)

    def drain_weather_pending_slugs(self) -> list[str]:
        """Slugs queued from WS `new_market` (weather-shaped); cleared on read."""
        with self._pending_thread_lock:
            out = list(self._weather_pending_slugs)
            self._weather_pending_slugs = []
            self._weather_pending_seen.clear()
            return out

    def _enqueue_weather_slug_sync(self, slug: str) -> None:
        s = str(slug or "").strip()
        if not s:
            return
        with self._pending_thread_lock:
            if s in self._weather_pending_seen:
                return
            self._weather_pending_seen.add(s)
            self._weather_pending_slugs.append(s)

    def _parse_token_pair_from_ws(self, raw: object) -> tuple[str, str]:
        """Best-effort YES/NO clob ids from WS / REST-shaped fields."""
        data = raw
        if isinstance(data, str):
            s = data.strip()
            if not s:
                return "", ""
            if s.startswith("["):
                try:
                    data = json.loads(s)
                except Exception:
                    data = s
            if isinstance(data, str):
                parts = [p.strip().strip("\"'") for p in data.split(",") if p.strip()]
                if len(parts) >= 2:
                    return str(parts[0]), str(parts[1])
                return "", ""
        if isinstance(data, list) and len(data) >= 2:
            return str(data[0]), str(data[1])
        return "", ""

    async def subscribe(self, asset_ids: Iterable[str]) -> None:
        """Register interest in token IDs; sends WS subscribe / dynamic subscribe."""
        new_ids = {str(a).strip() for a in asset_ids if a and str(a).strip()}
        if not new_ids:
            return
        await self.start()
        async with self._lock:
            self._wanted |= new_ids
        ws = self._ws
        if ws and not getattr(ws, "closed", True):
            try:
                await self._sync_subscriptions(ws)
            except Exception as e:
                log.debug("WS subscribe sync failed: %s", e)

    def _quote_log_level_unlabeled(self) -> int:
        """INFO = always log; DEBUG = only log unlabeled tokens at debug (less noise)."""
        if self._config.get("polymarket_ws_log_all_quotes", False):
            return logging.INFO
        return logging.DEBUG

    def _log_quote_readable(
        self,
        *,
        source: str,
        asset_id: str,
        prev_bb: Optional[float],
        prev_ba: Optional[float],
        bb: Optional[float],
        ba: Optional[float],
    ) -> None:
        disp = self._asset_display.get(asset_id) or {}
        title = (disp.get("title") or "").strip()
        slug = (disp.get("slug") or "").strip()
        outcome = (disp.get("outcome") or "").strip()
        if len(title) > 90:
            title = title[:87] + "..."
        market_raw = self._asset_to_market.get(asset_id, "") or ""
        condition = _short_hex(market_raw) if market_raw else "unknown"
        token = _short_hex(asset_id)
        bid_s = _fmt_side_change("Bid", prev_bb, bb)
        ask_s = _fmt_side_change("Ask", prev_ba, ba)
        outcome_part = f" | side={outcome.upper()}" if outcome else ""

        if title or slug:
            msg = (
                f"WS quote ({source}) | {title or '(no title)'} | slug={slug or '-'}{outcome_part}\n"
                f"    condition={condition} | token={token}\n"
                f"    {bid_s} | {ask_s}"
            )
            log.info(msg)
        else:
            msg = (
                f"WS quote ({source}) | not labeled yet (subscribe from Gamma to get title/slug)\n"
                f"    condition={condition} | token={token}\n"
                f"    {bid_s} | {ask_s}"
            )
            lvl = self._quote_log_level_unlabeled()
            log.log(lvl, msg)

    def _update_quote(self, asset_id: str, best_bid: Any, best_ask: Any, *, source: str) -> None:
        if not asset_id:
            return
        bb, ba = _f(best_bid), _f(best_ask)
        prev = self._quotes.get(asset_id) or {}
        prev_bb = _f(prev.get("best_bid"))
        prev_ba = _f(prev.get("best_ask"))
        self._quotes[asset_id] = {"best_bid": bb, "best_ask": ba}
        if prev_bb != bb or prev_ba != ba:
            self._log_quote_readable(
                source=source,
                asset_id=str(asset_id),
                prev_bb=prev_bb,
                prev_ba=prev_ba,
                bb=bb,
                ba=ba,
            )
            self._dirty_quote_assets.add(str(asset_id))
            aid = str(asset_id)
            disp = self._asset_display.get(aid)
            if disp is None:
                return
            try:
                from core.monitor_hub import emit_price_throttled
                from core.monitor_hub import is_enabled as _mon_on

                if not _mon_on(self._config):
                    return
                title_t = (disp.get("title") or "").strip() or "Polymarket"
                slug_t = (disp.get("slug") or "").strip() or _short_hex(aid)
                outcome_t = (disp.get("outcome") or "").strip()
                ev = dict(self._ticket_evidence.get(aid) or {})
                emit_price_throttled(
                    aid,
                    title=title_t[:200],
                    slug=slug_t[:120],
                    outcome=outcome_t[:16],
                    best_bid=bb,
                    best_ask=ba,
                    source=source,
                    **ev,
                )
            except Exception:
                pass

    def _apply_book(self, msg: dict[str, Any]) -> None:
        aid = msg.get("asset_id")
        if not aid:
            return
        bids = msg.get("bids") or []
        asks = msg.get("asks") or []
        best_bid = None
        best_ask = None
        bid_prices = [
            p for p in (_f(b.get("price")) for b in bids if isinstance(b, dict)) if p is not None
        ]
        ask_prices = [
            p for p in (_f(a.get("price")) for a in asks if isinstance(a, dict)) if p is not None
        ]
        if bid_prices:
            best_bid = max(bid_prices)
        if ask_prices:
            best_ask = min(ask_prices)
        market = msg.get("market")
        if market:
            self._asset_to_market[str(aid)] = str(market)
        self._update_quote(str(aid), best_bid, best_ask, source="book")

    def _apply_best_bid_ask(self, msg: dict[str, Any]) -> None:
        aid = msg.get("asset_id")
        market = msg.get("market")
        if market and aid:
            self._asset_to_market[str(aid)] = str(market)
        self._update_quote(str(aid), msg.get("best_bid"), msg.get("best_ask"), source="best_bid_ask")

    def _apply_price_change(self, msg: dict[str, Any]) -> None:
        for ch in msg.get("price_changes") or []:
            if not isinstance(ch, dict):
                continue
            aid = ch.get("asset_id")
            if not aid:
                continue
            market = msg.get("market")
            if market:
                self._asset_to_market[str(aid)] = str(market)
            self._update_quote(str(aid), ch.get("best_bid"), ch.get("best_ask"), source="price_change")

    def _handle_message(self, raw: str) -> None:
        raw = raw.strip()
        if raw == "PONG":
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self._handle_one(item)
            return
        if isinstance(data, dict):
            self._handle_one(data)

    def _handle_one(self, msg: dict[str, Any]) -> None:
        et = msg.get("event_type")
        if et == "book":
            self._apply_book(msg)
        elif et == "best_bid_ask":
            self._apply_best_bid_ask(msg)
        elif et == "price_change":
            self._apply_price_change(msg)
        elif et == "new_market":
            title = str(msg.get("question") or msg.get("title") or "")
            slug = str(msg.get("slug") or "")
            if _text_matches_weather(f"{title} {slug}"):
                raw_ids = msg.get("assets_ids") or msg.get("clob_token_ids") or msg.get("clobTokenIds")
                yid, nid = self._parse_token_pair_from_ws(raw_ids)
                log.info(
                    "WS new weather market | %s | slug=%s | active=%s | YES token=%s | NO token=%s",
                    (title[:100] + "...") if len(title) > 100 else title,
                    slug,
                    msg.get("active"),
                    _short_hex(yid),
                    _short_hex(nid),
                )
                self._enqueue_weather_slug_sync(slug)
                for aid, outcome in ((yid, "yes"), (nid, "no")):
                    if aid:
                        self.set_asset_display(aid, title=title, slug=slug, outcome=outcome)
                new_assets = [a for a in (yid, nid) if a]
                if new_assets:
                    try:
                        loop = asyncio.get_running_loop()
                        loop.create_task(self.subscribe(new_assets))
                    except RuntimeError:
                        log.debug("WS new_market: no running loop to subscribe new assets")
            else:
                log.debug(
                    "WS new_market (non-weather) slug=%s title=%s",
                    slug,
                    (title[:80] + "…") if len(title) > 80 else title,
                )
        elif et == "market_resolved":
            mkt = str(msg.get("market") or "")
            log.info(
                "WS market settled | condition=%s | winner_outcome=%s | winning_asset=%s",
                _short_hex(mkt),
                msg.get("winning_outcome"),
                _short_hex(str(msg.get("winning_asset_id") or "")),
            )
        elif et == "tick_size_change":
            mkt = str(msg.get("market") or "")
            aid = str(msg.get("asset_id") or "")
            log.info(
                "WS tick size changed | condition=%s | token=%s | %s -> %s",
                _short_hex(mkt),
                _short_hex(aid),
                msg.get("old_tick_size"),
                msg.get("new_tick_size"),
            )
        elif et == "last_trade_price":
            aid = str(msg.get("asset_id") or "")
            disp = self._asset_display.get(aid) or {}
            title = (disp.get("title") or "").strip()
            slug = (disp.get("slug") or "").strip()
            outcome = (disp.get("outcome") or "").strip()
            mkt = str(msg.get("market") or "")
            side = str(msg.get("side") or "?")
            px = _f(msg.get("price"))
            sz = _f(msg.get("size"))
            price_s = _fmt_px_cents(px) if px is not None else "?"
            size_s = f"{sz:g}" if sz is not None else "?"
            outcome_part = f" | leg={outcome.upper()}" if outcome else ""
            if title or slug:
                log.info(
                    "WS last trade | %s | slug=%s%s | %s @ %s | size %s shares | condition=%s | token=%s",
                    (title[:90] + "...") if len(title) > 90 else title or "(no title)",
                    slug or "-",
                    outcome_part,
                    side,
                    price_s,
                    size_s,
                    _short_hex(mkt),
                    _short_hex(aid),
                )
            else:
                lvl = self._quote_log_level_unlabeled()
                log.log(
                    lvl,
                    "WS last trade (unlabeled) | %s @ %s | size %s | condition=%s | token=%s",
                    side,
                    price_s,
                    size_s,
                    _short_hex(mkt),
                    _short_hex(aid),
                )
        else:
            log.debug("WS unknown event_type=%s keys=%s", et, list(msg.keys())[:8])

    async def _send_json(self, ws: Any, payload: dict) -> None:
        await ws.send(json.dumps(payload, separators=(",", ":")))

    async def _ping_loop(self, ws: Any) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(9.0)
                if getattr(ws, "closed", True):
                    break
                await ws.send("PING")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("WS ping loop: %s", e)

    async def _sync_subscriptions(self, ws: Any) -> None:
        """Send initial or dynamic subscribe for ids in _wanted but not _subscribed."""
        async with self._lock:
            wanted = set(self._wanted)
        pending = [a for a in wanted if a not in self._subscribed]
        if not pending:
            return
        custom = bool(self._config.get("polymarket_ws_custom_features", True))
        if not self._subscribed:
            sample = list(wanted)[:6]
            log.info(
                "WS market initial subscribe count=%d sample=%s custom=%s",
                len(wanted),
                sample,
                custom,
            )
            await self._send_json(
                ws,
                {"assets_ids": list(wanted), "type": "market", "custom_feature_enabled": custom},
            )
            self._subscribed = set(wanted)
            log.info("WS market subscribed to %d asset(s)", len(self._subscribed))
            return
        sample = pending[:6]
        log.info(
            "WS market dynamic subscribe count=%d sample=%s custom=%s",
            len(pending),
            sample,
            custom,
        )
        await self._send_json(
            ws,
            {
                "assets_ids": pending,
                "operation": "subscribe",
                "custom_feature_enabled": custom,
            },
        )
        self._subscribed |= set(pending)
        log.info("WS market dynamic subscribe +%d asset(s)", len(pending))

    async def _invoke_quote_batch(self, batch: set[str]) -> None:
        h = self._quote_batch_handler
        if not h or not batch:
            return
        try:
            await h(batch)
        except Exception as e:
            log.warning("WS quote batch handler failed: %s", e)

    async def _runner(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            async with self._lock:
                if not self._wanted:
                    await asyncio.sleep(0.25)
                    continue
            try:
                async with websockets.connect(
                    self._url,
                    ping_interval=None,
                    close_timeout=5,
                    max_size=10_000_000,
                ) as ws:
                    self._ws = ws
                    log.info("WS market connected to %s", self._url)
                    self._subscribed.clear()
                    await self._sync_subscriptions(ws)
                    self._ping_task = asyncio.create_task(self._ping_loop(ws), name="polymarket_ws_ping")
                    backoff = 1.0
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="ignore")
                        self._handle_message(raw)
                        if self._dirty_quote_assets and self._quote_batch_handler:
                            batch = set(self._dirty_quote_assets)
                            self._dirty_quote_assets.clear()
                            asyncio.create_task(
                                self._invoke_quote_batch(batch),
                                name="polymarket_ws_quote_batch",
                            )
                        # allow subscription updates while connected
                        await self._sync_subscriptions(ws)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("WS market connection error: %s — retry in %.1fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
            finally:
                if self._ping_task:
                    self._ping_task.cancel()
                    try:
                        await self._ping_task
                    except asyncio.CancelledError:
                        pass
                    self._ping_task = None
                if self._ws is not None:
                    log.info("WS market disconnected from %s", self._url)
                self._ws = None
                self._subscribed.clear()


_feed: Optional[PolymarketMarketWsFeed] = None
_feed_lock = asyncio.Lock()


async def get_shared_market_ws_feed(config: dict) -> PolymarketMarketWsFeed:
    """Process-wide singleton feed (config should be stable CONFIG dict)."""
    global _feed
    async with _feed_lock:
        if _feed is None:
            _feed = PolymarketMarketWsFeed(config)
            await _feed.start()
        return _feed
