# ColdMath Bot v4

Reverse-engineered from Polymarket wallet `0x594edb9112f526fa6a80b8f858a6379c8a2c1c11`.

**Confirmed PnL:** ~$44,000–52,000 cumulative (Dec 7, 2025 – Apr 5, 2026)
**Data basis:** 16 CSVs, 2 live API pulls, 36 days of full daily resolution

---

## What changed from v3 → v4

All changes confirmed from April 1–5 CSV data, Action 1 activity pull, and Action 2 positions pull.

### Session structure — 3 sessions → 6 sessions

| Code | UTC Window | What drives it | Status |
|------|-----------|----------------|--------|
| S0 | 06:00–06:59 | Pre-S1 burst: A-League Sat + Asian pre-positioning | New Apr 3-5 |
| S1 | 07:00–09:30 | European morning / GFS 00z digest | Core (unchanged) |
| S4 | 13:00–14:30 | US market open | Emerged Mar 23 |
| S2 | 15:00–16:45 | US NWS 12z — **weekdays only** | Core (unchanged) |
| S3 | **17:00**–20:00 | Soccer evening — **start moved from 18:00** | Apr 1: 790 txs at 17z |
| S5 | 21:00–23:59 | A-League + Asian weather | Emerged Mar 29 |

The S3 start time change is the most operationally critical update. April 1 saw 790 transactions at 17:00 UTC — the largest single hour in the entire dataset — driven by Bundesliga (17:30z) and Turkish Super Lig (17:00z) kickoffs.

### Early sell — new behavior confirmed Apr 5

ColdMath now sells NO positions at ≥99¢ immediately rather than holding to $1.00 resolution. Confirmed: 16 sells at average 99.74¢ totalling $5,387 in a single session.

```
Early sell is now enabled by default:
  early_sell_enabled:   True
  early_sell_threshold: 0.990
```

The `_early_sell_loop()` runs at the start of every session before scanning for new positions. At 99¢, holding gains only 1¢ more per dollar over 1–3 days. Selling now and redeploying into a fresh 4%+ edge position is strictly better capital allocation.

**Developer action:** implement `orders.get_open_positions()` and `orders.sell_position()` — stubs with full instructions are in `core/orders.py`.

### Cycling — Domain 4 confirmed

Tour de Flanders April 5 confirmed the full mechanic applied to cycling:

```
van der Poel  NOT top 3 → NO 91-95¢ large + YES 5-8¢   → MERGE (guaranteed profit)
Evenepoel     NOT top 3 → NO 93-97¢ large + YES 3-6¢   → MERGE
Pedersen      NOT top 3 → NO 91.9¢ large + YES 6.9¢    → MERGE
Laporte       WILL top 3 → YES 92-95¢ large + NO 5¢    → MERGE
van Aert      WILL top 3 → YES 52-75¢ + NO 45-75¢      → MERGE
Stuyven       NOT top 3 → NO 99¢ (no insurance needed)
```

All six positions merged for guaranteed profit before the race finished. The model correctly predicted all outcomes (van Aert 1st, Laporte 2nd confirmed).

`domains/cycling.py` is fully structured. **Developer action:** implement `CyclingModel._get_form_score()` using ProCyclingStats or FirstCycling data.

### Soccer graduated to co-primary

Soccer base position size raised from $20 (test) to $50 (co-primary). April 1 confirmed soccer is now fully at scale — 1,049 Session 3 transactions, $13,503 deployed in a single evening session.

A-League (Australia) added as 5th confirmed soccer league.

### 9 new cities added

Helsinki, Kuala Lumpur, Busan, Moscow, Munich, Chongqing, Shenzhen, Denver, Madrid (expanded).

Kuala Lumpur is the highest-value addition — tropical climate, 30–35°C year-round, extremely tight GFS ensemble spread = near-perfect model confidence on every market.

---

## Strategy (unchanged since December 2025)

```
TRADE if:  Model_P(NO) − Market_Price(NO)  ≥  0.04
           AND Model_Confidence            ≥  0.65
```

Buy the near-certain side large (88–99¢). Buy the opposite side tiny as insurance (4% of primary size). Merge when YES + NO combined cost < $1.00 for guaranteed profit. Or sell the near-certain side at ≥99¢ early to recycle capital.

The formula has not changed by a single parameter across 120 days and four domains.

---

## Domain status as of April 5, 2026

| Domain | Status | Allocation | Developer task |
|--------|--------|-----------|----------------|
| Weather | ACTIVE | 58% | Implement `_fetch_gfs_ensemble()` in weather.py |
| Soccer | ACTIVE | 37% | Implement `get_team_xg()` in soccer.py |
| Cycling | TESTING | 5% | Implement `_get_form_score()` in cycling.py |
| Financial | INACTIVE | 0% | Implement `get_market_data()` in financial.py |

---

## Developer checklist

### Immediate (to run paper trade)

- [ ] `pip install -r requirements.txt`
- [ ] `cp .env.example .env`
- [ ] `python3 tests/run_v4_tests.py` → confirm 53/53
- [ ] Implement `WeatherModel._fetch_gfs_ensemble()` — use Open-Meteo free API
- [ ] `python3 main.py` → paper trade starts, verify 6 sessions fire at correct times

### Week 1–2 — soccer data

- [ ] Implement `XGModel.get_team_xg()` — FBref/Sofascore scraping
- [ ] `python3 main.py --validate SOCCER`
- [ ] When accuracy ≥ 75%: `engine.mark_model_validated(Domain.SOCCER)`

### Week 2–3 — cycling data

- [ ] Implement `CyclingModel._get_form_score()` — ProCyclingStats
- [ ] `python3 main.py --validate CYCLING`
- [ ] Mark validated when confirmed

### Early sell (1 day of work)

- [ ] Implement `OrderManager.get_open_positions()` in orders.py
  - Fetch from `https://data-api.polymarket.com/positions?user={wallet}`
  - Filter for `redeemable=false` (still live)
- [ ] Implement `OrderManager.sell_position()` in orders.py
  - Use `py-clob-client` with `Side.SELL` and `OrderType.FOK`

### Go live

- [ ] Implement `OrderManager._place_single_order()` in orders.py
- [ ] Implement `OrderManager._merge()` in orders.py
- [ ] Start at `TOTAL_BUDGET_USDC=500` in `.env`
- [ ] Paper trade minimum 2 weeks before going live
- [ ] Scale only after 2 consecutive profitable weeks

---

## Session timing reference

```
UTC   Activity
06z   S0 — Pre-S1 burst (A-League Sat, Asian weather pre-positioning)
07z   S1 START — European morning, GFS 00z positions (peak 07z-09z)
09z   S1 END
13z   S4 START — US market open, capital recycling
14z   S4 END
15z   S2 START — US NWS 12z (WEEKDAYS ONLY — confirmed zero on weekends)
16z   S2 END
17z   S3 START — Soccer evening (Bundesliga 17:30z, Turkish 17:00z, CL 17:45z)
20z   S3 END
21z   S5 START — A-League kickoffs (21-23z UTC = 10-12pm AEDT)
23z   S5 END
```

---

## File map

```
coldmath_bot_v4/
├── main.py                       CLI entry point
├── requirements.txt
├── .env.example
├── core/
│   ├── config.py                 All parameters — edit here only
│   ├── rotation_engine.py        Edge decay + auto capital rotation (4 domains)
│   ├── bot.py                    6-session loop + early sell
│   ├── orders.py                 Trade execution + sell stubs
│   ├── metrics.py                Daily tracker → feeds rotation engine
│   └── opportunity.py            Shared dataclass (all domains)
├── domains/
│   ├── weather.py                GFS ensemble model
│   ├── soccer.py                 xG Poisson model (5 leagues)
│   ├── cycling.py                NEW — podium rate model
│   └── financial.py              Black-Scholes model (inactive)
└── tests/
    ├── run_v4_tests.py            53 tests — run before deploying
    └── test_rotation_engine.py    Original rotation tests
```

---

## Cumulative P&L confirmed

| Period | Net | Source |
|--------|-----|--------|
| Dec 7 – Feb 28, 2026 | +$8,735 | Positions API realized PnL |
| Mar 1 – Mar 28 | +$27,697 | 13 CSVs (62,374 transactions) |
| Mar 29 – Apr 1 | +$461 | CSV export |
| Apr 2 – Apr 3 | -$14,678 | CSV export (deploy lag) |
| Apr 4 – Apr 5 | pending | $151,636 deployed, resolves Apr 6-9 |
| **Positions API (Apr 5)** | **~$44,000–52,000** | True realized PnL |

Negative days reflect capital deployment lag (1–3 day settlement), not losses. Every confirmed negative day in the dataset was followed by full recovery within 3 trading days.
