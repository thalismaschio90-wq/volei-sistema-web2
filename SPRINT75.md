# Fase 3 — Sprint 75 — Verificação do uso dos índices

Esta sprint adiciona uma verificação segura dos cinco índices críticos preparados
na Sprint 74. O script usa `EXPLAIN (FORMAT JSON)` por padrão e não registra os
parâmetros das consultas.

## Execução em homologação

```powershell
$env:VTP_LAB_COMPETICAO="Competição de homologação"
$env:VTP_LAB_EQUIPE="Equipe de homologação"
$env:VTP_LAB_PARTIDA_ID="123"
py scripts/verificar_indices_criticos.py
```

Relatórios:

- `sql_lab_reports/verificacao_indices.json`
- `sql_lab_reports/verificacao_indices.md`

O modo `--analyze` executa as consultas e só é liberado explicitamente:

```powershell
$env:SQL_INDEX_VERIFY_ANALYZE_ALLOWED="1"
py scripts/verificar_indices_criticos.py --analyze
```

Use `--analyze` somente em homologação. Um índice não aparecer no plano não
significa automaticamente um problema: em tabelas pequenas, o PostgreSQL pode
preferir `Seq Scan` por custo.
