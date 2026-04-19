"""
HTTP + WebSocket server bundled with the trading bot (same process, same port).

Serves:
  • Built UI from frontend/dist (/)
  • REST /api/*
  • WebSocket /ws/events

There is no separate frontend port in production: build the React app, run the bot
with web_enabled or --web, open http://web_bind_host:web_port/
"""

import asyncio
import logging
import math
from pathlib import Path
from typing import Any

log = logging.getLogger("web_server")


def _ws_client_gone(exc: BaseException) -> bool:
    """True when the browser tab closed or navigated away (normal, not a server bug)."""
    try:
        from starlette.websockets import WebSocketDisconnect as WSD

        if isinstance(exc, WSD):
            return True
    except Exception:
        pass
    s = str(exc).lower()
    return (
        "close message" in s
        or "connection closed" in s
        or "disconnect" in s
        or "client disconnected" in s
    )


def _json_safe(value: Any) -> Any:
    """Ensure values are JSON-serializable (WebSocket send_json)."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    return str(value)


def _bind_host(config: dict) -> str:
    return str(config.get("web_bind_host") or config.get("monitor_host") or "127.0.0.1")


def _bind_port(config: dict) -> int:
    if config.get("web_port") is not None:
        return int(config["web_port"])
    if config.get("monitor_port") is not None:
        return int(config["monitor_port"])
    return 8765


def dashboard_url(config: dict) -> str:
    """Human-readable base URL for the single web backend port."""
    h = _bind_host(config)
    p = _bind_port(config)
    return f"http://{h}:{p}/"


def create_app(static_dir: Path | None = None):
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import HTMLResponse
    from fastapi.staticfiles import StaticFiles
    from starlette.requests import Request

    from core.monitor_hub import register_subscriber, snapshot_events, unregister_subscriber

    app = FastAPI(title="ColdMath Bot", version="1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    dist = static_dir or (Path(__file__).resolve().parent.parent / "frontend" / "dist")
    if dist.is_dir() and (dist / "index.html").is_file():
        assets_dir = dist / "assets"
        if assets_dir.is_dir():
            app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")

        _index_template = (dist / "index.html").read_text(encoding="utf-8")

        @app.get("/")
        async def root_index(request: Request) -> HTMLResponse:
            origin = str(request.base_url).rstrip("/")
            html = _index_template.replace("__INJECT_BACKEND_ORIGIN__", origin)
            return HTMLResponse(
                content=html,
                status_code=200,
                headers={"Cache-Control": "no-store"},
            )
    else:

        @app.get("/")
        async def root_fallback() -> HTMLResponse:
            return HTMLResponse(
                "<html><body style=\"font-family:system-ui;padding:1.5rem;max-width:42rem\">"
                "<h1>ColdMath — web backend is running</h1>"
                "<p>This port serves <strong>the bot + API + UI</strong> together. "
                "The UI files are missing.</p>"
                "<p>Build them once:</p>"
                "<pre style=\"background:#f4f4f4;padding:0.75rem\">cd frontend\n"
                "npm install\nnpm run build</pre>"
                "<p>Then <a href=\"/\">reload this page</a>.</p>"
                "<p>JSON: <a href=\"/api/events\">/api/events</a></p>"
                "</body></html>",
                status_code=200,
            )

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        return {"ok": True, "role": "coldmath_web_backend"}

    @app.get("/api/events")
    async def api_events(limit: int = 500) -> dict[str, Any]:
        evs = snapshot_events(limit=min(max(limit, 1), 2000))
        return {"events": [_json_safe(e) for e in evs]}

    @app.websocket("/ws/events")
    async def ws_events(ws: WebSocket) -> None:
        await ws.accept()
        q = register_subscriber()
        try:
            for ev in snapshot_events(300):
                try:
                    await ws.send_json(_json_safe(ev))
                except Exception as e:
                    if _ws_client_gone(e):
                        log.debug("ws client gone during snapshot: %s", e)
                        return
                    log.warning("ws snapshot send_json: %s", e)
                    return
            while True:
                ev = await q.get()
                try:
                    await ws.send_json(_json_safe(ev))
                except Exception as e:
                    if _ws_client_gone(e):
                        log.debug("ws client gone: %s", e)
                        return
                    log.warning("ws send_json: %s", e)
                    return
        except WebSocketDisconnect:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("ws handler exit: %s", e)
        finally:
            unregister_subscriber(q)
            while True:
                try:
                    q.get_nowait()
                except asyncio.QueueEmpty:
                    break

    return app


async def run_web_server(config: dict) -> None:
    import uvicorn

    from core.monitor_hub import set_event_loop

    set_event_loop(asyncio.get_running_loop())

    host = _bind_host(config)
    port = _bind_port(config)
    app = create_app()

    log.warning(
        "Web backend (bot + UI + API) — open http://%s:%s/  (REST /api/events, WS /ws/events)",
        host,
        port,
    )

    config_uv = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="warning",
        access_log=False,
        loop="asyncio",
        ws_ping_interval=None,
        ws_ping_timeout=None,
    )
    server = uvicorn.Server(config_uv)
    await server.serve()


def web_should_run(cli_web: bool, config: dict) -> bool:
    from core.monitor_hub import is_enabled

    return bool(cli_web or is_enabled(config))
