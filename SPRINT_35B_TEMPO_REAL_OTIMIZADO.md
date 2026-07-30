# Sprint 35B — Otimização do tempo real

## Arquivos alterados

- `socket_events.py`
- `services/apontadores/publicacao.py`
- `realtime/publisher.py`
- `realtime/event_history.py`
- `realtime/recovery.py`
- `realtime/state_store.py`

## Correções

1. Publicações Socket.IO agora são deduplicadas por versão oficial da partida.
2. Uma versão já publicada não gera novamente placar, eventos legados,
   snapshot completo, última ação e saque.
3. Ao limpar o estado da partida, os controles de publicação também são removidos.
4. Salas duplicadas são eliminadas antes do despacho.
5. A recuperação local de deltas interrompe a varredura ao atingir o limite.
6. Recovery deixa de fazer uma segunda cópia profunda dos deltas já isolados.
7. A leitura da versão do store local não copia mais o snapshot inteiro.
8. A leitura do store Redis evita uma cópia profunda redundante após decodificação.
9. A publicação do apontador reaproveita o estado retornado pelo cache,
   incluindo versão e horário oficiais.

## Validação

- testes direcionados de realtime: 74 aprovados;
- suíte oficial completa: 403 aprovados;
- falhas: 0;
- compilação Python: aprovada.
