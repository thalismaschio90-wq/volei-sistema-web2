import os

from flask_socketio import SocketIO

from core.security import origens_permitidas_socket


def _socketio_async_mode():
    return os.environ.get("SOCKETIO_ASYNC_MODE", "threading").strip() or "threading"


def _env_bool(nome: str, padrao: bool = False) -> bool:
    valor = str(os.environ.get(nome, "")).strip().lower()
    if not valor:
        return padrao
    return valor in {"1", "true", "sim", "yes", "on"}


def _socketio_message_queue() -> str | None:
    explicita = str(os.environ.get("SOCKETIO_MESSAGE_QUEUE", "")).strip()
    if explicita:
        return explicita

    redis_url = str(os.environ.get("REDIS_URL") or os.environ.get("REALTIME_REDIS_URL") or "").strip()
    backend = str(os.environ.get("REALTIME_STATE_BACKEND", "local") or "local").strip().lower()

    # Em modo auto/redis, a fila é ativada somente quando a URL existe. Sem
    # Redis, o ambiente atual continua funcionando com um worker e memória local.
    if redis_url and (_env_bool("SOCKETIO_USE_REDIS", False) or backend in {"auto", "redis"}):
        return redis_url
    return None


socketio = SocketIO(
    cors_allowed_origins=origens_permitidas_socket(),
    async_mode=_socketio_async_mode(),
    message_queue=_socketio_message_queue(),
    ping_timeout=20,
    ping_interval=10,
    logger=False,
    engineio_logger=False,
)
