# Fase 3 — Sprint 72: Benchmark controlado do PostgreSQL

Esta sprint cria um executor reproduzível para medir funções de acesso e consultas SELECT em homologação.

## Segurança

- SQL direto permanece bloqueado por padrão.
- Para consultas reais, use `SQL_BENCHMARK_ALLOW_DATABASE=1` somente em homologação.
- Apenas `SELECT`, `WITH` e `EXPLAIN` são aceitos.
- Cada execução usa `statement_timeout` e termina com `rollback`.
- Parâmetros não são gravados no JSON nem no Markdown.

## Uso inicial

```powershell
py scripts/executar_benchmark_sql.py scripts/benchmark_exemplo.json
```

Para consultas reais, crie um cenário privado de homologação. Não versione valores sensíveis.

## Ciclo de otimização

1. Execute o cenário e guarde o baseline.
2. Aplique uma única reescrita ou índice.
3. Execute exatamente o mesmo cenário.
4. Compare média, P95 e P99.
5. Mantenha a alteração somente quando houver ganho comprovado e sem regressão de escrita.
