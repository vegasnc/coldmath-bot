"""
core/config.py

All tunable parameters — edit here only, nothing hardcoded elsewhere.

v4 CHANGES (from April 1-5 CSV + Action 1 + Action 2 analysis):
  - Session 3 start moved 18:00 → 17:00 UTC (Bundesliga 17:30z confirmed Apr 1)
  - Session 4 (13:00-14:30) added — US market open, confirmed Mar 23+
  - Session 5 (21:00-23:59) added — A-League + Asian weather, confirmed Mar 29+
  - Session 0 (06:00-06:59) added — pre-S1 burst, confirmed Apr 3-5
  - 9 new cities (Helsinki, KL, Busan, Moscow, Munich, Chongqing, Shenzhen, Denver)
  - A-League added to soccer leagues (Melbourne v Wellington confirmed Apr 5)
  - Cycling domain config added (Tour de Flanders confirmed Apr 5)
  - Early sell threshold added (sell NO >= 99c, confirmed Apr 5 Action 1)
  - Soccer base size raised $20 → $50 (graduated from test to co-primary)
"""

CONFIG = {

    # ── BUDGET ─────────────────────────────────────────────────────────────
    "total_budget_usdc": 10_000,

    # ── TRADING MODE ────────────────────────────────────────────────────────
    "paper_trade": True,
    # If no opportunity passes min_edge/min_confidence, still simulate one virtual BUY/SELL
    # cycle on the best scan row so paper logs and portfolio stay active.
    "paper_simulate_trades_without_threshold": True,
    # When simulating, NO leg size if model sizing would otherwise be zero.
    "paper_virtual_min_no_size": 25.0,
    # Mark price stored on virtual NO positions so early-sell (>= early_sell_threshold) can fire.
    "paper_virtual_no_mark_price": 0.995,
    # Log a clear virtual BUY/NO+YES (and merge/hold) block for every suitable opportunity.
    # No wallet balance required — for operator visibility only.
    "log_virtual_suitable_trades": True,
    # After a real or paper execute(), log full BUY/SELL ticket lines (title, price, shares).
    "log_trade_execution_details": True,
    # Paper uses same 6-session UTC clock as live (dev guide Priority 1).
    # Set True to always run session "S0" only + fast loop (local debugging only).
    "paper_dev_fast_loop": False,
    # Main loop sleeps (seconds): active = inside a session window, idle = outside.
    "loop_sleep_active_sec": 120,
    "loop_sleep_idle_sec":   300,

    # ── SESSIONS (UTC) ─────────────────────────────────────────────────────
    # S0  06:00-06:59  Pre-S1 burst (A-League Saturday + Asian pre-positioning)
    # S1  07:00-09:30  European morning / GFS 00z
    # S4  13:00-14:30  US market open (emerged Mar 23)
    # S2  15:00-16:45  US afternoon NWS 12z — WEEKDAYS ONLY
    # S3  17:00-20:00  Soccer evening — START MOVED from 18z (confirmed Apr 1)
    # S5  21:00-23:59  A-League + Asian weather (emerged Mar 29)

    "session_0_start": (6,  0),
    "session_0_end":   (6, 59),

    "session_1_start": (7,  0),
    "session_1_end":   (9, 30),

    "session_4_start": (13, 0),
    "session_4_end":   (14, 30),

    "session_2_start":         (15,  0),
    "session_2_end":           (16, 45),
    "session_2_weekdays_only": True,

    "session_3_start": (17,  0),    # CHANGED from (18, 0) — Apr 1 17z: 790 txs
    "session_3_end":   (20,  0),

    "session_5_start": (21,  0),    # NEW
    "session_5_end":   (23, 59),

    # ── EDGE THRESHOLDS (unchanged across all data Dec-Apr) ─────────────────
    "min_edge":              0.04,
    "min_confidence":        0.65,
    "min_forecast_buffer_f": 10.0,
    "min_forecast_buffer_c":  5.5,

    # ── POSITION SIZING ─────────────────────────────────────────────────────
    # Soccer raised from $20 test to $50 co-primary (Apr avg deployment 3x March)
    "base_no_size": {
        "weather":   300.0,
        "soccer":     50.0,    # raised from $20 — now co-primary domain
        "financial":  10.0,    # not yet started
        "cycling":    20.0,    # NEW — test scale matching Apr 5 observed sizes
    },
    "yes_fraction":       0.04,    # unchanged throughout
    "max_daily_rotation": 0.05,

    # ── EARLY SELL — NEW confirmed Apr 5 ────────────────────────────────────
    # 16 sells at avg 99.74c on Apr 5 totalling $5,387
    # Recycles capital same-session instead of waiting 1-3 days for resolution
    "early_sell_enabled":   True,
    "early_sell_threshold": 0.990,

    # ── DOMAIN BOUNDS ───────────────────────────────────────────────────────
    "min_test_allocation": 0.05,
    "max_single_domain":   0.80,

    # ── WEATHER ─────────────────────────────────────────────────────────────
    "gfs_ensemble_min_members": 10,
    "max_ensemble_spread_f":    8.0,   # DO NOT lower — spring chaos confirmed
    "max_ensemble_spread_c":    4.5,

    "weather_cities": [
        # US
        "Dallas", "Houston", "Miami", "Atlanta", "Chicago",
        "New York", "Austin", "Los Angeles", "San Francisco",
        "Seattle", "Denver",
        # Europe
        "London", "Madrid", "Helsinki", "Munich", "Moscow",
        # Asia
        "Ankara", "Seoul", "Tokyo", "Beijing", "Singapore",
        "Shanghai", "Busan", "Chongqing", "Shenzhen",
        # SE Asia / Tropical (highest GFS confidence)
        "Kuala Lumpur",
        # Other
        "Wellington", "Sao Paulo", "Buenos Aires",
        "Lucknow", "Mexico City", "Toronto",
    ],

    # ── SOCCER ──────────────────────────────────────────────────────────────
    "soccer_market_types":       ["btts", "spread"],
    "soccer_min_games_played":   10,
    "soccer_final_games_buffer": 4,
    "soccer_min_no_price":       0.88,
    "soccer_max_yes_price":      0.12,
    "soccer_leagues": [
        "Turkish Super Lig",
        "MLS",
        "J2 Japan",
        "Norwegian Eliteserien",
        "A-League",              # NEW — confirmed Apr 5
    ],

    # ── CYCLING — NEW domain ────────────────────────────────────────────────
    # Apr 5: vdPoel NO, Evenepoel NO, Pedersen NO, Laporte YES, van Aert YES
    # All merged for guaranteed profit — model correct on all 5 riders
    "cycling_enabled": True,
    "cycling_min_no_price": 0.88,
    "cycling_base_size":    20.0,
    "cycling_events": [
        "tour-de-flanders",    # Apr 5 — confirmed
        "paris-roubaix",       # Apr (spring classic)
        "liege-bastogne-liege",
        "amstel-gold-race",
        "giro-d-italia",       # May
        "tour-de-france",      # Jul
        "vuelta-a-espana",     # Aug-Sep
    ],

    # ── FINANCIAL (not yet started) ─────────────────────────────────────────
    "financial_max_iv":   0.35,
    "financial_min_days": 1,
    "financial_max_days": 7,

    # ── WEB BACKEND (bot + HTTP + WebSocket + static UI, ONE TCP port) ─────
    # After `cd frontend && npm install && npm run build`, run:
    #   python main.py --web
    # Open in browser:  http://web_bind_host:web_port/
    # (Legacy keys monitor_enabled / monitor_host / monitor_port still work.)
    "web_enabled": True,
    "web_bind_host": "127.0.0.1",
    "web_port": 8765,

    # ── POLYMARKET API ──────────────────────────────────────────────────────
    "polymarket_clob_url":  "https://clob.polymarket.com",
    "polymarket_gamma_url": "https://gamma-api.polymarket.com",
    "polymarket_data_url":  "https://data-api.polymarket.com",

    # CLOB Market WebSocket (real-time book / prices). See:
    # https://docs.polymarket.com/market-data/websocket/overview
    "polymarket_ws_enabled": True,
    "polymarket_ws_market_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "polymarket_ws_custom_features": True,
    "polymarket_ws_after_subscribe_sleep": 0.25,
    # If True, log every quote tick at INFO even when title/slug are unknown (noisy).
    # If False, unknown tokens log quotes at DEBUG; labeled tokens stay INFO.
    "polymarket_ws_log_all_quotes": False,

    # ── ROTATION ENGINE ─────────────────────────────────────────────────────
    "signal_lookback_days":     7,
    "edge_decay_threshold":     0.65,
    "edge_critical_threshold":  0.35,
    "preemptive_test_lead_wks": 5,
}
