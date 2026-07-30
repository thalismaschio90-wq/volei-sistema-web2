# Sprint 14 — Pedido de tempo e cronômetro

## Arquivos alterados

- `banco.py`
- `routes/apontadores.py`
- `static/js/apontador/tempo-controller.js`

## Correções

- removida preparação de schema do registro de tempo;
- persistência atômica com lock da partida;
- limite de tempos lido na mesma consulta da partida;
- idempotência usando o `id_local` da fila como `comando_id`;
- sincronização em lote não reconstrói o estado completo a cada pedido;
- consulta de tempos restantes reduzida para uma única consulta;
- cronômetro envia somente início e fim pelo Socket.IO;
- telas conectadas mantêm a contagem regressiva localmente;
- mensagem de última ação corrigida para pedido de tempo.

## Validação

- testes direcionados: 7 aprovados;
- suíte completa: 381 aprovados;
- falhas: 0.
