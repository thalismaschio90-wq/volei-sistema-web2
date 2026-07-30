# Fase 3 — Sprint 68: prioridade e despacho de eventos Socket.IO

## Objetivo

Garantir que eventos críticos da partida sejam emitidos imediatamente e nunca
aguardem atrás de telemetria ou eventos auxiliares. Eventos de baixa prioridade
podem ser agrupados, e duplicatas exatas podem ser descartadas dentro de uma
janela curta configurável.

## Implementação

- `realtime/event_priority.py`
  - classificação em `critica`, `normal` e `baixa`;
  - emissão imediata dos eventos críticos;
  - agrupamento por `evento + sala` para baixa prioridade;
  - deduplicação exata opcional;
  - métricas agregadas, sem armazenar payloads.
- `realtime/publisher.py`
  - todas as publicações padronizadas passam pelo despachante.
- `socket_events.py`
  - delta, placar e saque marcados como críticos;
  - snapshots e compatibilidade marcados como normais;
  - deduplicação curta de payloads auxiliares idênticos.
- `/admin/realtime-delta`
  - novos indicadores de fila, agrupamento, duplicatas e espera.

## Variáveis

```env
SOCKET_PRIORITY_ENABLED=1
SOCKET_LOW_PRIORITY_BATCH_MS=100
```

## Segurança

O despachante não guarda o conteúdo dos payloads nas métricas. Somente
contadores, tamanho máximo da fila e tempos agregados são mantidos.

## Compatibilidade

Nenhum nome de evento, sala, endpoint ou payload foi alterado. Eventos críticos
continuam síncronos e imediatos. O agrupamento é aplicado somente a eventos
explicitamente classificados como baixa prioridade.
