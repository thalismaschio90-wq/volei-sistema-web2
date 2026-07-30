# Sprint 21 — Organizador sem DDL em runtime

## Arquivos alterados

- `banco.py`
- `core/schema_migrations.py`
- `routes/competicoes.py`

## Correções

- Abertura de `/competicoes` não executa mais `ALTER TABLE`.
- Removido `UPDATE` global das flags de configuração durante navegação.
- Estruturas de fluxo inicial e destaques foram movidas para migrações versionadas.
- Leitura de destaques deixou de usar `SELECT *`.
- Removidas chamadas redundantes de garantia de schema na rota.
- A tela continua calculando o status concluído em memória, sem gravar no GET.

## Migrações adicionadas

- `2026_07_30_023` — fluxo de configuração inicial.
- `2026_07_30_024` — premiação e destaques da competição.

## Validação

- Testes direcionados: 20 aprovados.
- Suíte oficial: 400 aprovados.
- Falhas: 0.
