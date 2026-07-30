import os

from core.runtime_config import assert_runtime_safe, env_int

# Falha o deploy imediatamente quando alguém tenta usar múltiplos workers sem
# Redis compartilhando estado e Socket.IO. É melhor não iniciar do que publicar
# uma configuração capaz de separar placar, rotação e árbitros entre processos.
_runtime = assert_runtime_safe()

bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"
workers = _runtime.workers
threads = _runtime.threads
timeout = env_int("GUNICORN_TIMEOUT", 120, minimo=30, maximo=600)
graceful_timeout = env_int("GUNICORN_GRACEFUL_TIMEOUT", 30, minimo=10, maximo=120)
keepalive = env_int("GUNICORN_KEEPALIVE", 5, minimo=2, maximo=30)
worker_class = os.environ.get("GUNICORN_WORKER_CLASS", "gthread")

# Reciclagem suave reduz o risco de crescimento indefinido de memória sem
# interromper todos os workers ao mesmo tempo.
max_requests = env_int("GUNICORN_MAX_REQUESTS", 2000, minimo=0, maximo=50000)
max_requests_jitter = env_int("GUNICORN_MAX_REQUESTS_JITTER", 200, minimo=0, maximo=5000)

# Não usar preload: conexões, locks e clientes Redis devem nascer dentro de cada
# worker depois do fork.
preload_app = False

accesslog = "-"
errorlog = "-"
capture_output = True
