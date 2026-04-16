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
import aiohttp
import asyncio
import logging
from dotenv import load_dotenv
from dataclasses import dataclass, field
from typing import Optional
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
from web3 import Web3
from py_clob_client.config import get_contract_config


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
        

    # ── MAIN EXECUTION ────────────────────────────────────────────────────

    async def execute(self, opp, no_size: float, yes_size: float) -> OrderResult:
        should_merge = (opp.no_price + opp.yes_price) < 0.998

        if self.paper_trade:
            log.info(f"Executing in paper trade mode: opp: {opp} \n\n no_size: {no_size:.0f} \n\n yes_size: {yes_size:.0f} \n\n should_merge: {should_merge}\n\n")

        if self.paper_trade:
            return self._paper_execute(opp, no_size, yes_size, should_merge)
        return await self._live_execute(opp, no_size, yes_size, should_merge)

    def _paper_execute(self, opp, no_size, yes_size, should_merge) -> OrderResult:
        no_frags  = max(1, int(no_size  / NO_FRAGMENT_SIZE))
        yes_frags = max(1, int(yes_size / YES_FRAGMENT_SIZE))
        action    = "MERGE" if should_merge else "HOLD"
        guaranteed = max(0, (1.0 - opp.no_price - opp.yes_price) * no_size)
        log.info(
            f"[PAPER] {opp.domain} | {opp.slug[:40]}\n"
            f"         NO  ${no_size:.2f} @ {opp.no_price:.4f} ({no_frags} frags)\n"
            f"         YES ${yes_size:.2f} @ {opp.yes_price:.4f} ({yes_frags} frags)\n"
            f"         {action}" + (f" +${guaranteed:.3f}" if should_merge else "")
        )
        return OrderResult(success=True, filled_no=no_size, filled_yes=yes_size, merged=should_merge, paper=True)

    async def _live_execute(self, opp, no_size, yes_size, should_merge) -> OrderResult:
        result = OrderResult(success=False)

        # Sweep NO in fragments
        remaining = no_size
        filled_no = 0.0
        while remaining > 0.10:
            frag = min(NO_FRAGMENT_SIZE, remaining)
            oid  = await self._place_single_order(opp.no_token_id, frag, opp.no_price)
            if oid:
                result.order_ids.append(oid)
                filled_no += frag
                remaining -= frag
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
        while remaining > 0.01:
            frag = min(YES_FRAGMENT_SIZE, remaining)
            oid  = await self._place_single_order(opp.yes_token_id, frag, opp.yes_price)
            if oid:
                result.order_ids.append(oid)
                filled_yes += frag
                remaining  -= frag
                await asyncio.sleep(FRAGMENT_PAUSE)
            else:
                break
        result.filled_yes = filled_yes

        if should_merge and filled_no > 0 and filled_yes > 0:
            try:
                amount_sets = min(filled_no, filled_yes)
                await self._merge(opp.condition_id, amount_sets)
                result.merged = True
                log.info(f"MERGED {opp.slug[:40]} +${(1-opp.no_price-opp.yes_price)*filled_no:.4f}")
            except Exception as e:
                log.error(f"Merge failed: {e}")

        result.success = filled_no > 0
        return result

    # ── EARLY SELL ──────────────────────────────────────────────────────

    async def get_open_positions(self) -> list[dict]:
        """
        DEVELOPER: Fetch live open positions.

        import os, aiohttp
        wallet = os.environ["POLYMARKET_WALLET"]
        url = f"{self.config['polymarket_data_url']}/positions"
        params = {"user": wallet, "limit": 500, "redeemable": "false"}
        async with aiohttp.ClientSession() as s:
            async with s.get(url, params=params) as r:
                data = await r.json()
        # data is a list of position dicts with fields:
        #   asset, conditionId, title, size, avgPrice, curPrice,
        #   outcome, outcomeIndex, redeemable, mergeable
        return [p for p in data if not p.get("redeemable", True)]
        """
        if self.paper_trade:
            log.info(f"Getting Open positions...")

        wallet = os.environ["POLYMARKET_WALLET"]
        url = f"{self.config['polymarket_data_url']}/positions"
        params = {"user": wallet, "limit": 1000, "redeemable": "false"}
        
        if self.paper_trade:
            log.info(f"Getting Open positions from {url} with params {params} : wallet {wallet}")

        async with aiohttp.ClientSession() as s:
            async with s.get(url, params=params) as r:
                data = await r.json()
                log.info(f"Data: {data}")
        # data is a list of position dicts with fields:
        #   asset, conditionId, title, size, avgPrice, curPrice,
        #   outcome, outcomeIndex, redeemable, mergeable
        return [p for p in data if not p.get("redeemable", True)]


        # if self.paper_trade:
        #     return []   # no open positions to sell in paper mode
        # log.warning("get_open_positions() not implemented")
        # return []

    async def sell_position(self, token_id: str, size: float, min_price: float) -> OrderResult:
        """
        DEVELOPER: Sell an open position on the CLOB.

        from py_clob_client.clob_types import MarketOrderArgs, OrderType, Side

        try:
            order = self.client.create_market_order(
                MarketOrderArgs(
                    token_id = token_id,
                    amount   = size,
                    side     = Side.SELL,
                )
            )
            resp = self.client.post_order(order, OrderType.FOK)
            return OrderResult(success=True, order_ids=[resp.get("orderID")])
        except Exception as e:
            log.error(f"Sell failed: {e}")
            return OrderResult(success=False, error=str(e))
        """
        try:
            order = self.client.create_market_order(
                MarketOrderArgs(
                    token_id = token_id,
                    amount   = size,
                    side     = SELL,
                )
            )
            resp = self.client.post_order(order, OrderType.FOK)
            return OrderResult(success=True, order_ids=[resp.get("orderID")])
        except Exception as e:
            log.error(f"Sell failed: {e}")
            return OrderResult(success=False, error=str(e))
        
        # if self.paper_trade:
        #     val = size * min_price
        #     log.info(f"[PAPER SELL] {token_id[:20]}... {size:.1f} @ {min_price:.4f} = ${val:.2f}")
        #     return OrderResult(success=True, paper=True)
        # log.error("sell_position() not implemented")
        # return OrderResult(success=False, error="Not implemented")

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
            condition_id[:18] + "…",
            amount_sets,
        )
