"""
In-process event hub for the monitoring web UI.

Thread-safe emit() from sync code (orders) or async code (bot, WS);
WebSocket handlers await subscriber queues fed via the main asyncio loop.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Optional

_MAX_EVENTS = 3000
_MAX_SUBSCRIBERS = 64
_PRICE_THROTTLE_SEC = 0.4

_lock = threading.Lock()
_events: deque[dict[str, Any]] = deque(maxlen=_MAX_EVENTS)
_next_id = 1
_main_loop: Optional[asyncio.AbstractEventLoop] = None
_subscriber_queues: list[asyncio.Queue] = []
_last_price_mono: dict[str, float] = {}


def set_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    global _main_loop
    _main_loop = loop


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def emit(event_type: str, **payload: Any) -> None:
    """Record one monitor event and notify WebSocket subscribers (thread-safe)."""
    global _next_id
    e: dict[str, Any]
    with _lock:
        eid = _next_id
        _next_id += 1
        e = {"id": eid, "ts": _iso_now(), "type": str(event_type), **payload}
        _events.append(e)
        qs = list(_subscriber_queues)

    loop = _main_loop
    if not loop or not loop.is_running() or not qs:
        return

    def _fan_out() -> None:
        for q in qs:
            try:
                q.put_nowait(e)
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    q.put_nowait(e)
                except Exception:
                    pass
            except Exception:
                pass

    try:
        loop.call_soon_threadsafe(_fan_out)
    except Exception:
        pass


def emit_price_throttled(
    asset_key: str,
    *,
    min_interval_sec: float = _PRICE_THROTTLE_SEC,
    **payload: Any,
) -> None:
    """Rate-limited price ticks (many WS updates per second)."""
    k = str(asset_key or "").strip() or "_"
    now = time.monotonic()
    with _lock:
        last = _last_price_mono.get(k, 0.0)
        if now - last < min_interval_sec:
            return
        _last_price_mono[k] = now
    emit("price", asset_key=k, **payload)


def snapshot_events(limit: int = 500) -> list[dict[str, Any]]:
    with _lock:
        out = list(_events)
    if limit and len(out) > limit:
        return out[-limit:]
    return out


def register_subscriber(maxsize: int = 256) -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
    with _lock:
        if len(_subscriber_queues) >= _MAX_SUBSCRIBERS:
            raise RuntimeError("too many monitor subscribers")
        _subscriber_queues.append(q)
    return q


def unregister_subscriber(q: asyncio.Queue) -> None:
    with _lock:
        try:
            _subscriber_queues.remove(q)
        except ValueError:
            pass


def is_enabled(config: dict) -> bool:
    """True when the in-process web backend is on (bot publishes UI events)."""
    return bool(config.get("web_enabled") or config.get("monitor_enabled"))

