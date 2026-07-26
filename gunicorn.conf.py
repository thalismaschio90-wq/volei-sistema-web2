import os

bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"
# O estado offline/local da partida vive na memória do processo. Com 2 workers,
# cada requisição pode cair em uma cópia diferente da escalação. Portanto, até
# migrar esse estado para Redis, é obrigatório operar com um único worker.
workers = 1
threads = int(os.environ.get("GUNICORN_THREADS", "4"))
timeout = int(os.environ.get("GUNICORN_TIMEOUT", "120"))
worker_class = os.environ.get("GUNICORN_WORKER_CLASS", "gthread")

# Mantém compatibilidade com Flask-SocketIO via simple-websocket sem exigir eventlet.
