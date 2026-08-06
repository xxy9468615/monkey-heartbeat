"""
Heartbeat Keepalive Configuration for monkeycode-ai.com

Based on HAR analysis showing:
- WebSocket ping messages every ~10 seconds on /api/v1/users/tasks/control
- HTTP polling every ~11 seconds on /api/v1/users/tasks/{task_id}
- Ping payload: {"type":"ping","data":null,"kind":"","timestamp":0}
"""

# WebSocket connection settings
WS_HOST = "monkeycode-ai.com"
WS_BASE_URL = f"wss://{WS_HOST}"
WS_CONTROL_PATH = "/api/v1/users/tasks/control"
WS_STREAM_PATH = "/api/v1/users/tasks/stream"

# HTTP API settings
HTTP_BASE_URL = f"https://{WS_HOST}"
POLL_TASK_PATH = "/api/v1/users/tasks/{task_id}"
POLL_WALLET_PATH = "/api/v1/users/wallet"
POLL_WALLET_CHECKIN_PATH = "/api/v1/users/wallet/checkin"
POLL_SUBSCRIPTION_PATH = "/api/v1/users/subscription"

# Timing (seconds)
PING_INTERVAL = 10          # Matches observed WS ping interval (avg 10.0s)
POLL_INTERVAL = 11          # Matches observed HTTP polling interval (avg 10.9s)
RECONNECT_BACKOFF_INITIAL = 15
RECONNECT_BACKOFF_MAX = 60
RECONNECT_MAX_ATTEMPTS = 5

# Ping payload (exact match from HAR)
PING_PAYLOAD = {
    "type": "ping",
    "data": None,
    "kind": "",
    "timestamp": 0
}

# HTTP headers matching observed browser requests
REQUEST_HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,zh-TW;q=0.5",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151.0.0.0 Safari/537.36 Edg/151.0.0.0",
    "sec-ch-ua": '"Not=A?brand";v="99", "Microsoft Edge";v="151", "Chromium";v="151"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# WebSocket headers (browser-simulated)
WS_HEADERS = {
    "User-Agent": REQUEST_HEADERS["User-Agent"],
    "Origin": f"https://{WS_HOST}",
    "Accept-Encoding": REQUEST_HEADERS["Accept-Encoding"],
    "Accept-Language": REQUEST_HEADERS["Accept-Language"],
}

# Logging
LOG_FILE = "outputs/keepalive.log"
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
