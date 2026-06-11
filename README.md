# Bybit V5 Event-Driven Copy Trading Engine

Production-style, websocket-first copy trading infrastructure for Bybit V5 Unified Trading Accounts (UTA). The engine monitors one master account and mirrors supported trading behavior to multiple follower accounts with strong safety checks, event deduplication, reconnect handling, structured logging, runtime state persistence, and testnet-first controls.

> This project handles financial execution. It is built for Bybit testnet first. Do not use on mainnet until you have validated credentials, symbols, quantity behavior, follower state, and emergency procedures with very small trades.

## Goal

The goal is to build an event-driven copy trading engine where the master account is the source of truth.

When the master account opens, closes, partially closes, changes leverage, or updates full-position TP/SL, the engine normalizes the master websocket event, deduplicates it, validates follower state, and sends the corresponding Bybit V5 action to each follower account independently.

The engine is designed to avoid the most dangerous copy trading failure modes:

- duplicate orders from websocket replay or reconnects
- one follower failure stopping all followers
- accidentally opening a position when trying to close
- follower side mismatch during reduce-only close
- invalid order quantities due to symbol precision rules
- starting live execution with missing credentials

## Current Status

All requested phases are implemented.

| Phase | Status | Description |
| --- | --- | --- |
| Phase 1 | Complete | Master private websocket listener, event normalization, structured logs, reconnect handling, dedupe |
| Phase 2 | Complete | Follower initialization, market open replication, failure isolation |
| Phase 3 | Complete | Leverage sync and full-position TP/SL sync |
| Phase 4 | Complete | Close and partial-close replication using reduce-only market orders |
| Phase 5 | Complete | Runtime state persistence, follower health, retry/backoff, precision normalization |

## Implemented Behavior

The engine listens to the master account private websocket topics:

- `execution`
- `order`
- `position`

Supported follower actions:

- market open replication
- reduce-only market close replication
- partial-close replication using master `closedSize`
- leverage synchronization
- full-position TP/SL synchronization

Supported trading assumptions:

- Bybit V5 APIs only
- Unified Trading Account compatible
- testnet-first
- one-way mode
- `linear` and `inverse` categories
- market orders for copied open/close execution
- full-position TP/SL mode

## Safety Design

### Emergency Kill Switch

Execution is disabled unless:

```dotenv
COPY_TRADER_ENABLED=true
```

When disabled, the engine loads config and exits without websocket or execution activity.

### Startup Validation

When `COPY_TRADER_ENABLED=true`, config validation requires:

- master API key and secret
- at least one follower
- every follower API key and secret

If credentials are blank, startup and `--config-check` fail fast.

### Event Deduplication

Master events are fingerprinted before dispatch.

- execution events use master `execId`
- order events include order id, status, update time, cancel type, reject reason, and message id
- position events include symbol, position index, sequence, updated time, size, side, TP, SL, and leverage

Recent event fingerprints are persisted in:

```text
app/storage/runtime_state.json
```

Durable dedupe checkpoints are written before follower dispatch. This intentionally favors duplicate-order prevention during reconnect or replay.

### Deterministic Follower Order IDs

Follower `orderLinkId` values are deterministic from:

- follower account name
- master execution id

This gives a second safety layer if Bybit or the process sees the same master execution again.

### Follower Failure Isolation

Follower actions are dispatched independently with `asyncio.gather(..., return_exceptions=True)`.

One follower failure is logged and recorded, but it does not stop other followers from receiving their actions.

### Open Safety

Before mirroring a master open, the follower position is checked.

If the follower already has an open one-way position on the symbol, the open mirror is skipped to avoid accidental duplicate exposure.

### Close Safety

Master closes and partial closes are mirrored with:

```text
reduceOnly=true
```

Before a close is sent, the engine checks:

- follower has an open one-way position
- follower side matches the side that the master close would reduce
- close quantity does not exceed follower size, unless clipping is enabled

If the follower position is smaller than master `closedSize`, the default behavior is to clip close quantity to follower size:

```dotenv
CLIP_CLOSE_QTY_TO_FOLLOWER_POSITION=true
```

### Precision Safety

Before order submission, follower quantities are normalized using Bybit instrument metadata:

- `qtyStep`
- `minOrderQty`
- market max quantity

Very small orders that round below `minOrderQty` are skipped before reaching Bybit.

### Retry And Rate-Limit Handling

Transient REST failures use retry/backoff. The engine retries known transient Bybit errors, including rate-limit style retCode:

```text
10006
```

Retry settings are configurable in `.env`.

## Architecture

```text
app/
  main.py
  config.py
  logger.py
  constants.py

  exchanges/
    bybit_client.py          # async wrapper around pybit HTTP
    websocket_manager.py     # Bybit private websocket auth/subscription/reconnect
    execution_listener.py    # raw websocket message handling and event normalization
    order_executor.py        # follower open/close/settings execution
    instrument_cache.py      # instruments-info precision cache

  core/
    event_bus.py             # async event dispatch
    event_models.py          # normalized execution/order/position models
    replication_models.py    # follower action request/result models
    dedupe.py                # replay suppression
    risk_engine.py           # phase policies and action decisions
    sync_engine.py           # event-to-follower-action orchestration
    follower_manager.py      # follower fan-out and health recording
    state_manager.py         # runtime JSON persistence and health snapshots

  utils/
    retry.py                 # API retry/backoff helpers
    helpers.py               # parsing/formatting helpers
    precision.py             # quantity normalization

  storage/
    runtime_state.json

tests/
accounts.json
.env
requirements.txt
```

## Event Flow

```text
Bybit master private websocket
        |
        v
websocket_manager.py
        |
        v
execution_listener.py
        |
        v
normalize_ws_message(...)
        |
        v
EventDeduplicator + RuntimeStateManager
        |
        v
EventBus
        |
        v
SyncEngine
        |
        v
Risk / sync policies
        |
        v
FollowerManager
        |
        v
BybitOrderExecutor per follower
        |
        v
Bybit V5 HTTP API
```

## Configuration

Configuration comes from `.env` and `accounts.json`.

### accounts.json

```json
{
  "master": {
    "name": "master",
    "api_key": "",
    "api_secret": ""
  },
  "followers": [
    {
      "name": "copy_1",
      "api_key": "",
      "api_secret": ""
    }
  ]
}
```

### Important .env Settings

```dotenv
COPY_TRADER_ENABLED=true
BYBIT_TESTNET=true
ACCOUNTS_FILE=accounts.json
LOG_LEVEL=INFO
PRINT_NORMALIZED_EVENTS=true
VALIDATE_MASTER_ON_STARTUP=true

BYBIT_WS_TOPICS=execution,order,position
BYBIT_WS_MAX_ACTIVE_TIME=1m
BYBIT_WS_PING_INTERVAL_SECONDS=20

RECONNECT_INITIAL_DELAY_SECONDS=1
RECONNECT_MAX_DELAY_SECONDS=30
RECONNECT_JITTER_RATIO=0.2

DEDUPE_TTL_SECONDS=86400
DEDUPE_MAX_ITEMS=50000
RUNTIME_STATE_FILE=app/storage/runtime_state.json
PERSIST_RUNTIME_STATE=true

BLOCK_IF_FOLLOWER_POSITION_EXISTS=true
FOLLOWER_REPLICATION_CONCURRENCY=10
MIRROR_ORDER_LINK_PREFIX=ct

SYNC_LEVERAGE=true
SYNC_TPSL=true
SYNC_EMPTY_TPSL_TO_CANCEL=true
DEFAULT_TP_TRIGGER_BY=MarkPrice
DEFAULT_SL_TRIGGER_BY=MarkPrice

CLIP_CLOSE_QTY_TO_FOLLOWER_POSITION=true

API_CALL_ATTEMPTS=3
API_RETRY_INITIAL_DELAY_SECONDS=0.25
API_RETRY_MAX_DELAY_SECONDS=2
API_RETRY_JITTER_RATIO=0.2

NORMALIZE_ORDER_QUANTITIES=true
INSTRUMENT_CACHE_TTL_SECONDS=3600
```

## Setup

Python 3.12+ is recommended.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Run validation:

```powershell
python -m compileall app tests
python -m pytest -q
python -m app.main --config-check
```

Run the engine:

```powershell
python -m app.main
```

The correct entrypoint is:

```powershell
python -m app.main
```

Do not run `python app/main.py`, because direct script execution changes Python import context.

## Expected Logs

Startup:

- `engine starting`
- `accounts configuration loaded`
- `runtime state loaded`
- `validating account connectivity`
- `account validation succeeded`
- `follower executors initialized`
- `private websocket authenticated`
- `private websocket subscribed`

Master open:

- `normalized master event`
- `dispatching follower market replication`
- `instrument precision metadata loaded`
- `order quantity normalized to instrument precision`
- `follower market order submitted`

Master close or partial close:

- `dispatching follower market replication`
- `follower reduce-only close order submitted`

Master leverage or TP/SL update:

- `dispatching follower position settings sync`
- `follower leverage synced`
- `follower TP/SL synced`

Safety logs:

- `duplicate websocket event suppressed`
- `follower already has an open one-way position; mirror open skipped`
- `follower has no open position; reduce-only close skipped`
- `follower position side does not match master close direction; reduce-only close skipped`
- `order quantity invalid after precision normalization`
- `retrying Bybit API action after transient failure`

## Runtime State

Runtime state is stored in:

```text
app/storage/runtime_state.json
```

It contains:

- current phase
- update timestamp
- recent master event fingerprints
- last master event summary
- follower health snapshots

Example structure:

```json
{
  "version": 1,
  "phase": "phase-5-production-hardening",
  "updated_at_epoch": 0,
  "seen_event_keys": [],
  "last_master_event": null,
  "follower_health": {}
}
```

## Test Coverage

The test suite covers:

- master event normalization
- dedupe suppression
- open replication policy
- follower open safety checks
- leverage and TP/SL sync policy
- trigger type cache
- close and partial-close replication
- reduce-only close safety
- precision normalization
- invalid quantity skips
- runtime state persistence
- dedupe hydration from persisted state
- API retry behavior
- enabled-mode credential validation

Run:

```powershell
python -m pytest -q
```

Current expected result:

```text
31 passed
```

## Project Plan

The overall plan was to build the engine in controlled phases instead of implementing trading execution all at once.

### Phase 1: Master Event Detection

Goal: safely observe the master account.

Implemented:

- Bybit private websocket connection
- auth and topic subscription
- execution/order/position normalization
- structured logging
- reconnect logic
- dedupe logic
- no follower execution

### Phase 2: Follower Market Opens

Goal: mirror master opens to followers.

Implemented:

- follower client initialization
- startup validation
- market open replication
- follower failure isolation
- existing-position safety check

### Phase 3: Leverage And TP/SL Sync

Goal: keep follower position settings aligned.

Implemented:

- leverage sync
- full-position TP/SL sync
- trigger type cache
- unchanged state suppression
- follower side/position validation before TP/SL sync

### Phase 4: Close And Partial-Close Sync

Goal: mirror master reductions safely.

Implemented:

- close replication
- partial-close replication
- reduce-only follower orders
- no-position skip
- side mismatch skip
- close quantity clipping

### Phase 5: Production Hardening

Goal: make the system more stable under reconnects, retries, precision constraints, and follower failures.

Implemented:

- runtime state persistence
- replay-aware dedupe hydration
- follower health snapshots
- retry/backoff for transient Bybit failures
- symbol precision cache
- quantity rounding and min-size validation
- stricter enabled-mode config validation

## Remaining Work

This project is functionally complete for the requested phased implementation, but these items should be considered before serious mainnet use:

- add global rate-limit scheduling instead of only retry/backoff
- add alerting for degraded followers
- add encrypted secret management instead of plaintext JSON
- add a dry-run execution mode
- add richer position reconciliation after long downtime
- add optional follower sizing ratios
- add multi-symbol allowlist/denylist controls
- add hedged-mode support if needed
- add Docker/systemd deployment templates
- add integration tests against Bybit testnet with tiny orders

## Limitations

- Testnet-first only.
- Only one-way mode is supported initially.
- Only `linear` and `inverse` categories are handled.
- Reversal executions are conservative: the close portion from `closedSize` is mirrored reduce-only, but the opposite-side open from the same execution is not synthesized.
- Runtime state uses local JSON, not a distributed database.
- Quantity normalization rounds down.
- Mainnet deployment requires operational controls outside this codebase.

## References

- [Bybit V5 WebSocket Connect/Auth](https://bybit-exchange.github.io/docs/v5/ws/connect)
- [Bybit V5 Execution Stream](https://bybit-exchange.github.io/docs/v5/websocket/private/execution)
- [Bybit V5 Order Stream](https://bybit-exchange.github.io/docs/v5/websocket/private/order)
- [Bybit V5 Position Stream](https://bybit-exchange.github.io/docs/v5/websocket/private/position)
- [Bybit V5 Place Order](https://bybit-exchange.github.io/docs/v5/order/create-order)
- [Bybit V5 Position Info](https://bybit-exchange.github.io/docs/v5/position)
- [Bybit V5 Set Leverage](https://bybit-exchange.github.io/docs/v5/position/leverage)
- [Bybit V5 Set Trading Stop](https://bybit-exchange.github.io/docs/v5/position/trading-stop)
- [Bybit V5 Instruments Info](https://bybit-exchange.github.io/docs/v5/market/instrument)
- [pybit](https://github.com/bybit-exchange/pybit)
