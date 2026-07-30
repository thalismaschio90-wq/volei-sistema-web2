# Fase 3 — Sprint 69: Medição e comparação do tráfego Socket.IO

## Alterações

- Mede bytes estimados recebidos e emitidos pelo despachante.
- Calcula bytes evitados por deduplicação e agrupamento.
- Lista os eventos com maior tráfego estimado no painel `/admin/realtime-delta`.
- Não armazena payloads, dados da partida ou informações pessoais.
- Adiciona `scripts/comparar_trafego_realtime.py` para comparar dois snapshots JSON.

## Uso da comparação

1. Limpe as métricas no painel.
2. Execute o mesmo roteiro de partida antes da mudança e salve `/admin/realtime-delta-status` em JSON.
3. Repita após a mudança.
4. Execute:

```powershell
py scripts/comparar_trafego_realtime.py antes.json depois.json --saida comparacao.md
```

Os bytes são estimativas do JSON e não incluem o overhead do Engine.IO/WebSocket.
