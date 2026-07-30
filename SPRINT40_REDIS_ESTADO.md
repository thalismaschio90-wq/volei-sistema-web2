# Fase 2 — Sprint 40: backend Redis para estado vivo

## Objetivo

Permitir que o estado da partida seja compartilhado entre processos Gunicorn,
sem obrigar a ativação do Redis antes da homologação.

## Modos

### Compatibilidade atual — um worker

```env
REALTIME_STATE_BACKEND=local
GUNICORN_WORKERS=1
SOCKETIO_USE_REDIS=0
```

### Redis — preparado para múltiplos workers

```env
REDIS_URL=redis://...
REALTIME_STATE_BACKEND=redis
REALTIME_REDIS_REQUIRED=1
REALTIME_STATE_TTL_SECONDS=86400
SOCKETIO_USE_REDIS=1
GUNICORN_WORKERS=2
```

O aumento dos workers só deve ocorrer depois de confirmar em homologação que:

1. o estado está usando Redis;
2. o Socket.IO está usando a fila Redis;
3. apontador, árbitros, telão e visualizador recebem a mesma versão;
4. reconexão e troca de set foram testadas.

## Segurança

- Cada partida usa uma chave própria.
- O estado possui TTL configurável.
- Escritas concorrentes usam `WATCH/MULTI`.
- Versões antigas continuam sendo rejeitadas pelo validador da Sprint 37.
- Se Redis estiver indisponível e `REALTIME_REDIS_REQUIRED=0`, o sistema volta ao
  modo local e deve permanecer com um worker.
