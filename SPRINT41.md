# Fase 2 — Sprint 41

## Configuração segura do Render e prontidão

Esta sprint transforma as regras operacionais discutidas nas etapas anteriores em proteções executáveis.

### Entregas

- `core/runtime_config.py`: valida workers, Redis e fila do Socket.IO.
- `core/readiness.py`: diagnóstico com cache curto de PostgreSQL, estado vivo e runtime.
- `gunicorn.conf.py`: workers por variável, reciclagem suave e bloqueio de configuração insegura.
- `/readyz`: prontidão real, separada do liveness `/healthz`.
- `/admin/runtime-status`: diagnóstico restrito ao Super ADM, incluindo métricas do pool.
- `scripts/verificar_configuracao_runtime.py`: validação no build do Render.
- `render.yaml`: configuração inicial segura com um worker.
- documentação de homologação Redis e dois workers.

### Regra obrigatória

`GUNICORN_WORKERS > 1` somente inicia quando:

- `REALTIME_STATE_BACKEND=redis`;
- uma URL Redis está configurada;
- o Socket.IO possui fila Redis compartilhada.

Assim, um erro de configuração não chega à produção silenciosamente.
