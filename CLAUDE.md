# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

Heartbeat keepalive script for `monkeycode-ai.com`. Sends periodic ping messages via WebSocket and/or HTTP polling to prevent task timeout/inactivity disconnects.

## Architecture

```
configs/keepalive_config.py    ← All timing constants, URLs, payloads
scripts/heartbeat_keepalive.py ← Main async entry point (argparse → mode dispatch)
outputs/keepalive.log          ← Runtime logs (gitignored normally)
.github/workflows/keepalive.yml ← GitHub Actions workflow_dispatch
```

The config module exports all constants. The script adds `configs/` to `sys.path` at runtime so imports work without package installation.

## Key Behavior

- Three modes: `ws` (WebSocket ping), `poll` (HTTP GET), `dual` (both concurrently)
- WebSocket: sends `{"type":"ping","data":null,"kind":"","timestamp":<ms>}` every `PING_INTERVAL` (10s default)
- HTTP: GETs `/api/v1/users/tasks/{task_id}` every `POLL_INTERVAL` (11s default) using urllib with SSL verification disabled
- Reconnection: exponential backoff 15s→60s, max 5 attempts
- Duration: `--duration <minutes>` stops the script when elapsed time exceeds limit; `--cookie` passes browser auth cookie

## Running

```bash
python3 scripts/heartbeat_keepalive.py --task-id <id> --mode dual --duration 30 --cookie "<cookie>"

# Check live logs
tail -f outputs/keepalive.log
```

## GitHub Actions

Workflow is in `.github/workflows/keepalive.yml`. Triggers via `workflow_dispatch` with inputs: `task_id`, `mode`, `duration`. Job has a 120-minute hard timeout and a `timeout` command matching the user-supplied duration. Logs uploaded as artifacts.

## Ignored Files (per .gitignore)

- `monkeycode-ai.com.har` and all `*.har` files — large capture data, excluded from git
- `outputs/keepalive.log` — runtime logs (excluded to avoid commit noise; can be force-added for archiving)
- `__pycache__/`, `*.pyc`, `*.pyo`, `*.egg-info/`, `.eggs/` — Python cache/artifacts
- `.vscode/`, `.idea/` — IDE settings
- `.DS_Store`, `Thumbs.db` — OS metadata
- `.env` — sensitive credentials

When creating or editing files, do not commit anything matching the patterns above. If a log file needs to be preserved (e.g., test evidence), use `git add -f`.

30-minute dual-mode test completed successfully:
- WS pings: 160 sent / 160 received
- HTTP polls: 158 total / 157 success (1 SSL handshake timeout, auto-recovered)
