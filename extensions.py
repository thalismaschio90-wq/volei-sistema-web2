import os

from flask_socketio import SocketIO


def _socketio_async_mode():
    return os.environ.get("SOCKETIO_ASYNC_MODE", "threading").strip() or "threading"


socketio = SocketIO(
    cors_allowed_origins="*",
    async_mode=_socketio_async_mode(),
    ping_timeout=20,
    ping_interval=10,
    logger=False,
    engineio_logger=False,
)
