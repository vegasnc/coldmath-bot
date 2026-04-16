"""
core/opportunity.py

Shared dataclass for opportunities across all domains.

v3 CHANGES:
  Added available_liquidity field — required for Gap 3 fix
  (order book depth cap in _position_size).
  Domain scanners should populate this from the Polymarket
  order book before returning opportunities.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Opportunity:
    # Identity
    domain:       str
    slug:         str
    title:        str
    condition_id: str

    # Token IDs for order placement
    no_token_id:  str
    yes_token_id: str

    # Prices (0.0-1.0)
    no_price:  float
    yes_price: float

    # Model outputs
    our_prob_no: float   # model's estimated P(NO)
    edge:        float   # our_prob_no - no_price
    confidence:  float   # 0.0-1.0 from domain model

    # Metadata
    end_date:    str
    domain_meta: Optional[dict] = None

    # Order book depth (GAP 3 field — v3 addition)
    # Set by scanner from Polymarket CLOB order book.
    # Bot uses this to cap position at MAX_BOOK_FRACTION (30%).
    # If None, no cap is applied (safe default for paper trading).
    available_liquidity: Optional[float] = None

    @property
    def combined_cost(self) -> float:
        return self.no_price + self.yes_price

    @property
    def merge_profit_per_dollar(self) -> float:
        return max(0.0, 1.0 - self.combined_cost)

    @property
    def should_merge(self) -> bool:
        return self.combined_cost < 0.998

    def __repr__(self):
        liq = (f"  liq=${self.available_liquidity:.0f}" if self.available_liquidity else "")
        return (
            f"Opportunity({self.domain} | {self.slug[:35]} | "
            f"NO={self.no_price:.3f} YES={self.yes_price:.3f} | "
            f"edge={self.edge:+.3f} conf={self.confidence:.2f}"
            f"{liq})"
        )
