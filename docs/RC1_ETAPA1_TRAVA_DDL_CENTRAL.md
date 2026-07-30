# RC1 — Etapa 1: trava central de DDL

## Objetivo

Impedir definitivamente alterações estruturais no PostgreSQL durante requisições,
rotas, sockets, tarefas normais e importação da aplicação.

## Implementação

- `core/schema_ddl_guard.py`: identifica instruções DDL e usa `ContextVar` para
  permitir alterações somente no contexto de migração.
- `core/sql_performance.py`: o proxy central de cursor passou a validar todas as
  instruções antes de enviá-las ao PostgreSQL, mesmo quando a medição de
  desempenho está desabilitada.
- `core/schema_migrations.py`: o executor versionado abre explicitamente o
  contexto que permite DDL durante as migrações.
- `tests/test_schema_ddl_guard.py`: cobre bloqueio, liberação controlada,
  comentários antes do SQL e permissão de DML/consultas.

## Comandos bloqueados fora das migrações

`CREATE`, `ALTER`, `DROP`, `TRUNCATE`, `COMMENT`, `GRANT`, `REVOKE`, `REINDEX`,
`CLUSTER` e `VACUUM`.

## Comportamento em caso de schema desatualizado

A operação falha imediatamente com uma mensagem clara solicitando a execução
das migrações. O sistema não tenta corrigir o banco silenciosamente durante o
uso.

## Validação

- Compilação integral com `python -m compileall -q .`.
- Testes automatizados: 363 aprovados.

## Próximo bloco

Concluir a revisão das rotinas legadas de schema e, em seguida, iniciar a
refatoração modular do `jogo_apontador` preservando integralmente o comportamento.
