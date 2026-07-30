# Sprint 17 — Estado Único da Partida

## Objetivo
Centralizar o estado operacional vivo da partida em uma única fachada e impedir versões duplicadas para uma única ação.

## Arquivos de produção
- `realtime/live_state.py` — novo coordenador/fachada do estado vivo.
- `socket_events.py` — leitura, gravação e publicação passam pelo coordenador.
- `routes/apontadores.py` — o cache auxiliar local deixa de guardar uma segunda cópia do estado vivo.

## Correções
- Uma sequência `atualizar_estado_cache()` + `emitir_estado_partida()` não incrementa mais a versão duas vezes.
- Snapshots idênticos reutilizam a versão existente.
- Snapshots realmente diferentes continuam criando uma nova versão.
- Metadados de versão/data não fazem um estado idêntico parecer diferente.
- `_CACHE_OPERACAO_LOCAL` guarda somente dados estáticos/auxiliares; placar, rotação, set e saque ficam exclusivamente no estado vivo de `realtime`.
- Compatibilidade das funções públicas preservada.

## Resultado esperado
- apontador, árbitros, telão e visualizador passam a consumir a mesma versão oficial;
- menor risco de saltos artificiais de versão;
- menos deltas vazios e snapshots redundantes;
- menor chance de rotação ou placar divergirem após reconexão;
- preparação segura para Redis e múltiplos workers.

## Validação
- testes específicos: 3 aprovados;
- suíte oficial: 384 aprovados;
- falhas: 0.
