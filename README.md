# Bybit V5 Event-Driven Copy Trader

Phase 5 is implemented. The engine listens to the master account private websocket, normalizes `execution`, `order`, and `position` events, suppresses replayed duplicates, mirrors eligible master opens, mirrors master closes/partial closes as reduce-only follower orders, syncs leverage plus full-position TP/SL, and adds production hardening around runtime state, follower health, retries, and symbol precision.

This is still testnet-first infrastructure. Do not point it at mainnet until credentials, symbols, quantities, and follower behavior have been validated with very small testnet trades.

## What Phase 5 Adds

- Durable runtime state in `app/storage/runtime_state.json`.
- Persisted master event fingerprints for replay-aware restarts.
- Follower health snapshots with success, failure, skip counts, last result, and last error.
- Retry handling for transient Bybit REST failures, including rate-limit style retCode `10006`.
- Bybit `instruments-info` based quantity normalization using `qtyStep`, `minOrderQty`, and market max quantity.
- Instrument metadata caching with configurable TTL.
- Configurable close quantity clipping when follower size is smaller than master `closedSize`.
- Expanded tests for persistence, retry behavior, precision rounding, invalid quantity skips, and dedupe hydration.

## Architecture Decisions

- Websocket-first ingestion uses Bybit V5 private streams over `websockets`, with explicit auth, subscription, heartbeat, and exponential reconnect control.
- `pybit` is used for startup validation, follower position checks, market order placement, `set_leverage`, `set_trading_stop`, and `get_instruments_info`.
- Internal events and replication requests are Pydantic models, keeping quantities, prices, order ids, TP/SL, leverage, and reduce-only fields typed.
- Execution dedupe is based on master `execId`; follower `orderLinkId` values are deterministic from follower account plus master execution id.
- Durable dedupe checkpoints are written before follower dispatch to bias toward duplicate-order prevention on reconnect or replay.
- Follower failures are isolated with `asyncio.gather(..., return_exceptions=True)`, so one failing follower does not stop the others.
- The emergency kill switch defaults to disabled: `COPY_TRADER_ENABLED=false`.

## Layout

```text
app/
  main.py
  config.py
  logger.py
  constants.py
  exchanges/
    bybit_client.py
    websocket_manager.py
    execution_listener.py
    order_executor.py
    instrument_cache.py
  core/
    event_bus.py
    event_models.py
    replication_models.py
    dedupe.py
    risk_engine.py
    sync_engine.py
    follower_manager.py
    state_manager.py
  utils/
    retry.py
    helpers.py
    precision.py
  storage/
    runtime_state.json
accounts.json
.env
requirements.txt
tests/
```

## Setup

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Add testnet API credentials for the master and every follower in `accounts.json`, then enable the engine in `.env`:

```dotenv
COPY_TRADER_ENABLED=true
BYBIT_TESTNET=true
PERSIST_RUNTIME_STATE=true
NORMALIZE_ORDER_QUANTITIES=true
BLOCK_IF_FOLLOWER_POSITION_EXISTS=true
SYNC_LEVERAGE=true
SYNC_TPSL=true
SYNC_EMPTY_TPSL_TO_CANCEL=true
CLIP_CLOSE_QTY_TO_FOLLOWER_POSITION=true
```

Run a config-only check:

```powershell
python -m app.main --config-check
```

When `COPY_TRADER_ENABLED=true`, config-check validates that the master and every follower have non-empty API credentials before reporting success. With the placeholder `accounts.json`, it will fail fast and print credential errors, which is expected.

Run the engine:

```powershell
python -m app.main
```

## Expected Logs

Startup should show:

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
- `order quantity normalized to instrument precision` when rounding is needed
- `follower market order submitted`

Master close or partial close:

- `dispatching follower market replication`
- `follower reduce-only close order submitted`

Master leverage or TP/SL change:

- `dispatching follower position settings sync`
- `follower leverage synced`
- `follower TP/SL synced`

Safety skips are explicit:

- `duplicate websocket event suppressed`
- `follower already has an open one-way position; mirror open skipped`
- `follower has no open position; reduce-only close skipped`
- `follower position side does not match master close direction; reduce-only close skipped`
- `order quantity invalid after precision normalization`

## Runtime State

`app/storage/runtime_state.json` stores:

- recent `seen_event_keys`
- `last_master_event`
- follower health snapshots
- current phase and update timestamp

This file is intentionally simple JSON so it can be inspected during testnet runs. Phase 5 persists enough state to reduce reconnect/replay risk, but it is not a full event-sourced ledger.

## Limitations

- Only `linear` and `inverse` one-way positions are handled.
- Reversal executions are treated conservatively: the close portion from `closedSize` is mirrored reduce-only, but any new opposite-side open from the same master execution is not synthesized yet.
- Precision normalization rounds quantities down. Very small orders can be skipped if they fall below `minOrderQty`.
- Runtime state persistence is local JSON, not a distributed lock or database.
- Rate-limit handling is retry/backoff based; it is not a global token-bucket scheduler.
- Mainnet use still requires operational validation, alerting, and manual emergency procedures.

## References

- Bybit V5 private websocket connection and auth: <https://bybit-exchange.github.io/docs/v5/ws/connect>
- Bybit V5 execution stream: <https://bybit-exchange.github.io/docs/v5/websocket/private/execution>
- Bybit V5 order stream: <https://bybit-exchange.github.io/docs/v5/websocket/private/order>
- Bybit V5 position stream: <https://bybit-exchange.github.io/docs/v5/websocket/private/position>
- Bybit V5 place order: <https://bybit-exchange.github.io/docs/v5/order/create-order>
- Bybit V5 get position info: <https://bybit-exchange.github.io/docs/v5/position>
- Bybit V5 set leverage: <https://bybit-exchange.github.io/docs/v5/position/leverage>
- Bybit V5 set trading stop: <https://bybit-exchange.github.io/docs/v5/position/trading-stop>
- Bybit V5 instruments info: <https://bybit-exchange.github.io/docs/v5/market/instrument>
- pybit V5 unified trading client: <https://github.com/bybit-exchange/pybit>
