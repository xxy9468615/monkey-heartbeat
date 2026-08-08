#!/usr/bin/env python3
"""
Heartbeat Keepalive for monkeycode-ai.com

Based on HAR analysis of 19.3-minute session:
- WebSocket pings every 10s on wss://monkeycode-ai.com/api/v1/users/tasks/control?id={task_id}
- HTTP polling every 11s on https://monkeycode-ai.com/api/v1/users/tasks/{task_id}

Usage:
  # WebSocket mode (primary)
  python3 heartbeat_keepalive.py --task-id <task_id>

  # HTTP polling mode (fallback)
  python3 heartbeat_keepalive.py --mode poll --task-id <task_id>

  # Dual mode (WebSocket + HTTP polling)
  python3 heartbeat_keepalive.py --mode dual --task-id <task_id>

  # Custom interval + duration (minutes)
  python3 heartbeat_keepalive.py --task-id <task_id> --interval 8 --duration 30

GitHub Actions:
  Set TASK_ID, MODE, INTERVAL, and DURATION env vars.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, InvalidStatus

# Load .env file if present (for local runs)
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
if os.path.isfile(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _key, _, _val = _line.partition("=")
                os.environ.setdefault(_key.strip(), _val.strip())

# Add configs directory to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "configs"))
from keepalive_config import (
    PING_INTERVAL,
    POLL_INTERVAL,
    RECONNECT_BACKOFF_INITIAL,
    RECONNECT_BACKOFF_MAX,
    RECONNECT_MAX_ATTEMPTS,
    PING_PAYLOAD,
    WS_BASE_URL,
    WS_CONTROL_PATH,
    HTTP_BASE_URL,
    POLL_TASK_PATH,
    REQUEST_HEADERS,
    WS_HEADERS,
    LOG_FILE,
    LOG_LEVEL,
    LOG_FORMAT,
    LOG_DATE_FORMAT,
)

# Ensure output directory exists
os.makedirs(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "outputs"), exist_ok=True)

# Setup logging
log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", LOG_FILE)
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    datefmt=LOG_DATE_FORMAT,
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def get_ws_url(task_id: str) -> str:
    """Build WebSocket control URL from task ID."""
    return f"{WS_BASE_URL}{WS_CONTROL_PATH}?id={task_id}"


def get_poll_url(task_id: str) -> str:
    """Build HTTP polling URL from task ID."""
    return f"{HTTP_BASE_URL}{POLL_TASK_PATH.format(task_id=task_id)}"


def build_headers(cookie: str = "") -> dict:
    """Build request headers, optionally with cookies."""
    headers = dict(REQUEST_HEADERS)
    if cookie:
        headers["Cookie"] = cookie
    return headers


async def send_ping(websocket, task_id: str, ping_count: int) -> bool:
    """Send a ping message over the WebSocket connection."""
    payload = PING_PAYLOAD.copy()
    payload["timestamp"] = int(time.time() * 1000)
    message = json.dumps(payload)
    try:
        await websocket.send(message)
        logger.info(f"[WS] Ping #{ping_count} sent: {message}")
        return True
    except Exception as e:
        logger.error(f"[WS] Failed to send ping #{ping_count}: {e}")
        return False


async def handle_ws_message(websocket, message: str) -> dict | None:
    """Handle an incoming WebSocket message."""
    try:
        data = json.loads(message)
        msg_type = data.get("type", "unknown")
        logger.info(f"[WS] Received: type={msg_type}")
        return data
    except json.JSONDecodeError as e:
        logger.warning(f"[WS] Could not parse message: {e}")
        return None


async def websocket_keepalive(task_id: str, interval: float = PING_INTERVAL,
                              duration: float = 0, cookie: str = ""):
    """
    Maintain WebSocket connection with periodic ping messages.
    Implements reconnection with exponential backoff.

    Args:
        task_id: Task ID for the WebSocket connection
        interval: Seconds between pings (default 10s from HAR analysis)
        duration: If > 0, stop after this many minutes
        cookie: Optional browser cookie string for authentication
    """
    ws_url = get_ws_url(task_id)
    backoff = RECONNECT_BACKOFF_INITIAL
    attempt = 0
    ping_count = 0
    start_time = time.time()

    # Build headers with optional cookie
    headers = dict(WS_HEADERS)
    if cookie:
        headers["Cookie"] = cookie

    while attempt < RECONNECT_MAX_ATTEMPTS:
        # Check duration limit
        if duration > 0:
            elapsed_min = (time.time() - start_time) / 60
            if elapsed_min >= duration:
                logger.info(f"[WS] Duration limit ({duration}min) reached. Stopping.")
                return

        try:
            logger.info(f"[WS] Connecting to {ws_url} (attempt {attempt + 1}/{RECONNECT_MAX_ATTEMPTS})")

            async with websockets.connect(
                ws_url,
                additional_headers=headers,
                ping_interval=None,  # Disable auto-ping, we do manual pings
            ) as websocket:
                logger.info(f"[WS] Connected. Sending ping every {interval}s...")
                attempt = 0  # Reset on successful connection
                backoff = RECONNECT_BACKOFF_INITIAL

                while True:
                    # Check duration limit
                    if duration > 0:
                        elapsed_min = (time.time() - start_time) / 60
                        if elapsed_min >= duration:
                            logger.info(f"[WS] Duration limit ({duration}min) reached. Stopping.")
                            return

                    # Send ping at specified interval
                    await asyncio.sleep(interval)
                    ping_count += 1

                    # Check if connection is still alive
                    try:
                        if websocket.closecode is not None:
                            logger.warning(f"[WS] Connection closed with code {websocket.closecode}")
                            break
                    except Exception:
                        pass

                    await send_ping(websocket, task_id, ping_count)

                    # Try to receive any pending messages (non-blocking with timeout)
                    try:
                        msg = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        await handle_ws_message(websocket, msg)
                    except asyncio.TimeoutError:
                        pass  # No message available, continue to next ping

        except (ConnectionClosed, ConnectionClosedError) as e:
            logger.warning(f"[WS] Connection closed: {e}. Reconnecting in {backoff}s...")
            attempt += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX)
        except InvalidStatus as e:
            logger.error(f"[WS] Invalid status: {e}. Reconnecting in {backoff}s...")
            attempt += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX)
        except Exception as e:
            logger.error(f"[WS] Unexpected error: {e}. Reconnecting in {backoff}s...")
            attempt += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX)

    logger.error(f"[WS] Exhausted {RECONNECT_MAX_ATTEMPTS} connection attempts. Giving up.")


async def http_polling_keepalive(task_id: str, interval: float = POLL_INTERVAL,
                                  duration: float = 0, cookie: str = ""):
    """
    HTTP polling fallback: periodically poll task status endpoint.
    Uses urllib for HTTPS (no external HTTP library dependency).
    """
    import urllib.request
    import ssl

    poll_path = POLL_TASK_PATH.format(task_id=task_id)
    poll_url = f"https://monkeycode-ai.com{poll_path}"
    ping_count = 0
    start_time = time.time()

    while True:
        # Check duration limit
        if duration > 0:
            elapsed_min = (time.time() - start_time) / 60
            if elapsed_min >= duration:
                logger.info(f"[HTTP] Duration limit ({duration}min) reached. Stopping.")
                return

        ping_count += 1
        try:
            await asyncio.sleep(interval)

            # Build request
            req = urllib.request.Request(poll_url)
            req.add_header("User-Agent", REQUEST_HEADERS["User-Agent"])
            req.add_header("Accept", "*/*")
            if cookie:
                req.add_header("Cookie", cookie)

            # Use unverified SSL context for compatibility
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE

            with urllib.request.urlopen(req, timeout=10, context=ssl_ctx) as resp:
                status = resp.status
                body = resp.read()
                logger.info(f"[HTTP] Poll #{ping_count}: HTTP {status} | {len(body)} bytes")

        except Exception as e:
            logger.error(f"[HTTP] Poll #{ping_count} failed: {e}")
            await asyncio.sleep(min(interval * 2, RECONNECT_BACKOFF_MAX))


async def dual_keepalive(task_id: str, ws_interval: float = PING_INTERVAL,
                          poll_interval: float = POLL_INTERVAL,
                          duration: float = 0, cookie: str = ""):
    """
    Run WebSocket ping and HTTP polling concurrently for maximum reliability.
    WebSocket is primary, HTTP polling is fallback.
    """
    ws_task = asyncio.create_task(
        websocket_keepalive(task_id, ws_interval, duration, cookie)
    )
    poll_task = asyncio.create_task(
        http_polling_keepalive(task_id, poll_interval, duration, cookie)
    )

    done, pending = await asyncio.wait(
        {ws_task, poll_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    # Cancel remaining tasks
    for task in pending:
        task.cancel()


def parse_args():
    """Parse command-line arguments."""
    import argparse
    parser = argparse.ArgumentParser(description="Heartbeat keepalive for monkeycode-ai.com")
    parser.add_argument("--task-id", required=False,
                        default=os.environ.get("TASK_ID"),
                        help="Task ID for keepalive (or set TASK_ID env var)")
    parser.add_argument("--mode", choices=["ws", "poll", "dual"],
                        default=os.environ.get("MODE", "ws"),
                        help="Keepalive mode: ws=websocket, poll=HTTP polling, dual=both")
    parser.add_argument("--interval", type=float,
                        default=float(os.environ.get("INTERVAL", PING_INTERVAL)),
                        help=f"Ping interval in seconds (default: {PING_INTERVAL})")
    parser.add_argument("--duration", type=float,
                        default=float(os.environ.get("DURATION", 0)),
                        help="Duration in minutes (0 = run forever). Default: 0")
    parser.add_argument("--cookie", default=os.environ.get("COOKIE", ""),
                        help="Browser cookie string for authentication (optional)")
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    if not args.task_id:
        logger.error("Task ID is required. Pass --task-id or set TASK_ID env var.")
        sys.exit(1)

    logger.info(f"Starting heartbeat keepalive")
    logger.info(f"  Task ID: {args.task_id}")
    logger.info(f"  Mode: {args.mode}")
    logger.info(f"  Interval: {args.interval}s")
    logger.info(f"  Duration: {args.duration} min" if args.duration > 0 else "  Duration: unlimited")
    logger.info(f"  WS URL: {get_ws_url(args.task_id)}")
    logger.info(f"  Poll URL: {get_poll_url(args.task_id)}")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Setup signal handlers for clean shutdown
    def signal_handler(sig, frame):
        logger.info(f"\nReceived signal {sig}. Shutting down...")
        for task in asyncio.all_tasks(loop):
            loop.create_task(task.cancel())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        if args.mode == "ws":
            loop.run_until_complete(
                websocket_keepalive(args.task_id, args.interval, args.duration, args.cookie)
            )
        elif args.mode == "poll":
            loop.run_until_complete(
                http_polling_keepalive(args.task_id, args.interval, args.duration, args.cookie)
            )
        elif args.mode == "dual":
            loop.run_until_complete(
                dual_keepalive(args.task_id, args.interval, args.interval, args.duration, args.cookie)
            )
    finally:
        loop.close()


if __name__ == "__main__":
    main()
