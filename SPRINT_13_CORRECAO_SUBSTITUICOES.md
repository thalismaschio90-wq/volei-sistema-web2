# Sprint 13 — Correção do fluxo de substituições

## Arquivos de produção alterados

- `banco.py`
- `routes/apontadores.py`
- `static/js/apontador/substituicao-controller.js`
- `static/js/apontador/substituicao-excepcional-controller.js`

## Correções

1. Removida a preparação de schema do caminho de persistência das substituições normal e excepcional.
2. Adicionado parâmetro `emitir_tempo_real` às funções de persistência para permitir sincronização silenciosa em lote.
3. A sincronização do set passa a persistir substituições sem publicar um snapshot por evento.
4. Após o lote, somente um estado consolidado é recarregado, armazenado em cache e publicado.
5. Removidas consultas extras de histórico e tempos no retorno da substituição normal; o retorno usa o estado já calculado.
6. Os dois modais de substituição possuem trava `confirmando`, impedindo clique duplo e aplicação local duplicada.

## Compatibilidade

- As assinaturas antigas continuam válidas; o novo argumento é opcional e keyword-only.
- Chamadas diretas continuam emitindo o estado em tempo real por padrão.
- A sincronização offline mantém os IDs locais e a idempotência existente.

## Validação

- Sintaxe Python: aprovada.
- Sintaxe JavaScript: aprovada.
- Testes direcionados: 10 aprovados.
- Suíte completa: 381 aprovados, 0 falhas.
