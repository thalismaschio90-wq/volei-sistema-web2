# Fase 3 — Sprint 57: diagnóstico automático de SQL lento

Esta sprint amplia o painel de performance sem executar qualquer alteração automática no PostgreSQL.

## Alterações

- extração segura de tabelas e colunas estruturais de consultas lentas;
- identificação de filtros, ordenações, agrupamentos, JOINs e `SELECT *`;
- sugestões conservadoras de investigação e de possíveis índices;
- exibição do diagnóstico em `/admin/performance` e no endpoint JSON;
- nenhuma coleta de parâmetros ou valores usados nas consultas;
- nenhuma execução automática de `CREATE INDEX`.

## Uso

Com a instrumentação ativada, navegue pelas páginas lentas e abra `/admin/performance`. As recomendações devem ser confirmadas com `EXPLAIN (ANALYZE, BUFFERS)` em homologação antes de qualquer alteração no banco.
