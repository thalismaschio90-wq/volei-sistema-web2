# Encerramento da Fase 3 — Performance

A Fase 3 adicionou instrumentação SQL, painel de performance, EXPLAIN controlado, laboratório e benchmark PostgreSQL, índices candidatos, detecção de N+1, caches de leitura, Redis opcional, protocolo de delta, renderização seletiva, telemetria, prioridade de eventos, degradação controlada, reconexão adaptativa e ensaio de campeonato.

## O que significa “pronto para produção”

A existência do código não substitui homologação. A aprovação final exige:

1. implantação em serviço de homologação;
2. `/readyz` saudável;
3. teste completo de uma partida descartável;
4. ensaio com público simultâneo;
5. validação do painel `/admin/realtime-delta`;
6. ausência de regressões no painel `/admin/performance`;
7. relatório `scripts/validar_homologacao.py` aprovado.

## Configuração conservadora inicial

Enquanto Redis e múltiplos workers não forem homologados:

```env
GUNICORN_WORKERS=1
GUNICORN_THREADS=4
REALTIME_STATE_BACKEND=local
SOCKETIO_USE_REDIS=0
SOCKET_LEGACY_STATE_EVENTS=1
```

Depois de homologar Redis:

```env
GUNICORN_WORKERS=2
GUNICORN_THREADS=4
REALTIME_STATE_BACKEND=redis
REALTIME_REDIS_REQUIRED=1
SOCKETIO_USE_REDIS=1
SOCKET_LEGACY_STATE_EVENTS=0
SOCKET_LEGACY_REQUIRE_DELTA_HEALTHY=1
```

A troca deve ser feita em duas etapas: Redis com um worker; depois Redis com dois workers.
