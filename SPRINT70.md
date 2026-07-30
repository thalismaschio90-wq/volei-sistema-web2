# Sprint 70 — Degradação controlada sob carga

Foi adicionado um mecanismo conservador de proteção do Socket.IO quando a taxa de eventos ou a fila de baixa prioridade cresce acima dos limites configurados.

## Modos

- `normal`: funcionamento padrão;
- `controlado`: eventos de baixa prioridade são agrupados por uma janela maior;
- `critico`: eventos de baixa prioridade podem ser descartados para preservar a operação da partida.

Eventos críticos — ponto, saque, substituição, confirmações e placar — nunca são bloqueados pelo mecanismo.

## Configuração

```env
SOCKET_DEGRADATION_ENABLED=1
SOCKET_DEGRADATION_WINDOW_SECONDS=5
SOCKET_DEGRADATION_CONTROLLED_EVENTS_PER_SEC=120
SOCKET_DEGRADATION_CRITICAL_EVENTS_PER_SEC=300
SOCKET_DEGRADATION_CONTROLLED_QUEUE=25
SOCKET_DEGRADATION_CRITICAL_QUEUE=100
SOCKET_DEGRADATION_COOLDOWN_SECONDS=10
SOCKET_DEGRADATION_DROP_LOW_ON_CRITICAL=1
SOCKET_DEGRADATION_CONTROLLED_BATCH_FACTOR=2
SOCKET_DEGRADATION_CRITICAL_BATCH_FACTOR=5
```

O painel `/admin/realtime-delta` mostra o modo atual, taxa estimada, fila, transições e descartes de baixa prioridade.
