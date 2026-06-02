"""Shared constants for the copy trading engine."""

APP_NAME = "bybit-copy-trader"
PHASE = "phase-5-production-hardening"

BYBIT_PRIVATE_WS_MAINNET = "wss://stream.bybit.com/v5/private"
BYBIT_PRIVATE_WS_TESTNET = "wss://stream-testnet.bybit.com/v5/private"

SUPPORTED_PRIVATE_TOPICS = frozenset({"execution", "order", "position"})
DEFAULT_PRIVATE_TOPICS = ("execution", "order", "position")

DEFAULT_ACCOUNTS_FILE = "accounts.json"
DEFAULT_RUNTIME_STATE_FILE = "app/storage/runtime_state.json"
DEFAULT_RUNTIME_STATE_VERSION = 1

SUPPORTED_COPY_CATEGORIES = frozenset({"linear", "inverse"})
SUPPORTED_POSITION_SYNC_CATEGORIES = frozenset({"linear", "inverse"})
ONE_WAY_POSITION_IDX = 0
