# Fase 4 — Sprint 81: Recuperação incremental da partida

## Objetivo

Recuperar clientes desconectados usando somente os deltas perdidos quando o
intervalo ainda está disponível e é contíguo. O snapshot completo permanece
como fallback autoritativo.

## Componentes

- `realtime/event_history.py`: janela curta local ou Redis dos deltas publicados.
- `realtime/recovery.py`: decide entre eventos, cliente já atualizado e snapshot.
- `socket_events.py`: novo evento `recuperar_eventos_partida` e resposta
  `recuperacao_partida`.
- `static/js/realtime/delta_client.js`: aplica lotes recuperados e usa snapshot
  apenas quando o histórico não resolve a lacuna.
- seis telas críticas integradas ao novo fluxo.

## Configuração

```env
REALTIME_RECOVERY_MAX_EVENTS=200
REALTIME_RECOVERY_BATCH_LIMIT=100
REALTIME_RECOVERY_TTL_SECONDS=86400
REALTIME_RECOVERY_REDIS_PREFIX=vtp:historico_delta
```

Quando `REALTIME_STATE_BACKEND=redis`, o histórico também usa Redis. Com modo
local, ele permanece compatível com um único worker.

## Segurança operacional

- o snapshot oficial continua sendo a fonte de verdade;
- deltas só são usados quando formam uma sequência completa até a versão atual;
- qualquer lacuna, expiração ou histórico insuficiente força snapshot;
- o histórico é limitado por partida e possui TTL no Redis;
- nenhuma regra de jogo ou tabela PostgreSQL foi alterada.
