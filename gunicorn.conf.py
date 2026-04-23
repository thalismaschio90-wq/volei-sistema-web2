import os

bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"
workers = int(os.environ.get("GUNICORN_WORKERS", "1"))
threads = int(os.environ.get("GUNICORN_THREADS", "8"))
timeout = int(os.environ.get("GUNICORN_TIMEOUT", "120"))
worker_class = os.environ.get("GUNICORN_WORKER_CLASS", "gthread")

# Mantém compatibilidade com Flask-SocketIO via simple-websocket sem exigir eventlet.
