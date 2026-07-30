# Fase 3 — Sprint 58: Exportação do diagnóstico de performance

- Exportação segura em Markdown e JSON pelo painel `/admin/performance`.
- Priorização das rotas e consultas lentas já agregadas.
- Candidatos conservadores de índice, sem aplicação automática.
- Blocos de `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS)` preparados para homologação.
- Nenhum SQL bruto, parâmetro ou dado pessoal é exportado.

Novos endpoints restritos ao Super ADM:

- `/admin/performance/exportar.md`
- `/admin/performance/exportar.json`
