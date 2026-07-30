# Fase 3 — Sprint 77 — Primeira eliminação concreta de N+1

## Alteração

Foi adicionado um cache efêmero por contexto de execução e aplicado à geração
de relatórios. Durante um único relatório, as leituras de eventos da mesma
partida são reutilizadas.

Antes, ao montar estatísticas dos lados A e B, `resumir_scout_equipe_partida`
consultava `eventos` duas vezes para a mesma partida. Agora a segunda leitura
usa a cópia já carregada no escopo do relatório.

## Segurança

- O cache não atravessa requisições.
- Não fica em variável global compartilhada entre usuários.
- Os valores são devolvidos por cópia defensiva.
- O fluxo ao vivo não foi colocado em cache.

## Impacto esperado

Para relatórios que calculam scout dos dois lados, a quantidade de consultas à
tabela de eventos cai aproximadamente pela metade por partida. O ganho real
deve ser confirmado pelo painel de performance e pelo benchmark em homologação.
