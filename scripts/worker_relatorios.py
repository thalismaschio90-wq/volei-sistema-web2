"""Inicializa o worker dedicado de relatórios."""
from __future__ import annotations

import os


def main() -> None:
    url = str(os.getenv("REDIS_URL", "") or "").strip()
    if not url:
        raise SystemExit("REDIS_URL não configurada.")
    import redis
    from rq import Worker
    from services.relatorios.fila import nome_fila

    conexao = redis.Redis.from_url(url)
    worker = Worker([nome_fila()], connection=conexao, name=os.getenv("RQ_WORKER_NAME") or None)
    worker.work(with_scheduler=False)


if __name__ == "__main__":
    main()
