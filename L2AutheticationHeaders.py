from py_clob_client.client import ClobClient
import os

client = ClobClient(
    host="https://clob.polymarket.com",
    chain_id=137,
    key="0xdaa933f71b0a740d45f3a9f33de0e2a9ed1330219042b2e672f7517f9a838752",
    creds={
        "apiKey": "a6be8b88-8153-5653-029e-81ea22c37d8d",
        "secret": "ByGq0RBJ0lkDEyuj3XODVy0JUF1Tk7PYd6aXBFl3S0E=",
        "passphrase": "aa7fbbf94a70d4e033bc54665d368a74734145ff683a6804e68422e09e88e8bb"
    },  # Generated from L1 auth, API credentials enable L2 methods
    signature_type=1,  # signatureType explained below
    funder="0x77C3FDC76FC8cef703a45d51A15577D3eebF50Bc" # funder explained below
)

# Now you can trade!
order = client.create_and_post_order(
    {"token_id": "123456", "price": 0.65, "size": 100, "side": "BUY"},
    {"tick_size": "0.01", "neg_risk": False}
)