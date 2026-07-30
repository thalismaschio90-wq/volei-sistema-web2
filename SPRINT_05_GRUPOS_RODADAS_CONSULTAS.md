# Sprint 05 — Grupos e rodadas com consultas leves

## Objetivo
Reduzir consultas repetidas e remover `SELECT *` de fluxos usados pelo organizador, tabela e equipes.

## Arquivos alterados
- `repositories/rodadas.py`
- `repositories/grupos.py`
- `routes/competicoes.py`

## Alterações
- `listar_rodadas_competicao` agora busca somente as colunas necessárias.
- As consultas de grupos e vínculos de equipes deixaram de usar `SELECT *`/`ge.*`.
- O cálculo de rodadas classificatórias deixou de consultar as equipes grupo por grupo.
- As equipes de todos os grupos agora são carregadas em lote por competição.
- Mantida a mesma estrutura de retorno usada pelas telas e serviços.

## Benefício esperado
- Menos tráfego entre Render e PostgreSQL.
- Menos consultas ao abrir/configurar rodadas.
- Ganho crescente em competições com muitos grupos.
- Menor pressão sobre o pool de conexões.

## Testes
- 379 testes aprovados.
- 0 falhas.
