# Fase 2 — Sprint 25: estrutura e sorteio dos grupos

Nesta sprint, a criação automática, sincronização do grupo único e sorteio das equipes saíram de `routes/tabela.py`.

## Nova divisão

- `rules/grupos_estrutura.py`: regras puras de quantidade, nomes, normalização e distribuição balanceada.
- `services/competicoes/grupos_estrutura.py`: coordenação da estrutura, grupo único e sorteio.
- `repositories/grupos.py`: substituição da distribuição em lote e limpeza transacional dos vínculos.
- `routes/tabela.py`: apenas fachadas para as rotas e callbacks de cache/bloqueio.

## Melhoria de desempenho

O sorteio antigo chamava a persistência uma vez para cada equipe. Agora todos os vínculos são apagados e inseridos em uma única conexão e transação usando `executemany`.

## Compatibilidade

Os endpoints, mensagens, formulários, templates e nomes internos utilizados pela rota foram preservados.

## Validação

- 107 testes aprovados.
- Compilação de todos os módulos Python concluída.
