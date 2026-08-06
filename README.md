# Heartbeat Keepalive for monkeycode-ai.com

## Background

Based on HAR analysis of a 19.3-minute session on `monkeycode-ai.com`, the application uses two keepalive mechanisms:

1. **WebSocket pings** every ~10 seconds on `wss://monkeycode-ai.com/api/v1/users/tasks/control?id={task_id}`
   - Ping payload: `{"type":"ping","data":null,"kind":"","timestamp":0}`
   - 116 ping messages captured over 19 minutes

2. **HTTP polling** every ~11 seconds on `https://monkeycode-ai.com/api/v1/users/tasks/{task_id}`
   - 107 polling requests captured
   - Response: `{"code":0,"message":"success","data":{"status":"processing",...}}`

## Solution

A Python script that sends periodic ping messages to keep tasks alive, preventing timeout/inactivity disconnects.

### Files

| File | Purpose |
|------|---------|
| `configs/keepalive_config.py` | Configuration constants |
| `scripts/heartbeat_keepalive.py` | Main keepalive script |
| `.github/workflows/keepalive.yml` | GitHub Actions workflow |
| `outputs/keepalive.log` | Runtime logs |

### Usage

```bash
# WebSocket mode (primary - recommended)
python3 scripts/heartbeat_keepalive.py --task-id <task_id>

# HTTP polling mode (fallback)
python3 scripts/heartbeat_keepalive.py --mode poll --task-id <task_id>

# Dual mode (both WebSocket + HTTP)
python3 scripts/heartbeat_keepalive.py --mode dual --task-id <task_id>

# Custom interval (default: 10s)
python3 scripts/heartbeat_keepalive.py --task-id <task_id> --interval 8

# Environment variables
TASK_ID=<task_id> MODE=ws INTERVAL=10 python3 scripts/heartbeat_keepalive.py
```

### GitHub Actions

Run via workflow_dispatch with inputs:
- `task_id` (required): The task ID to keep alive
- `mode` (optional, default: `ws`): ws, poll, or dual
- `duration` (optional, default: 60): How long to run keepalive

## Configuration

All settings in `configs/keepalive_config.py`:
- `PING_INTERVAL`: 10 seconds (matches observed WebSocket ping rate)
- `POLL_INTERVAL`: 11 seconds (matches observed HTTP polling rate)
- `RECONNECT_BACKOFF_*`: Exponential backoff for reconnections (15s → 60s max)
- `PING_PAYLOAD`: Exact payload from HAR analysis

## Dependencies

- Python 3.12+
- `websockets` library
