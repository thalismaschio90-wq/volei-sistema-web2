# Fase 3 — Sprint 59: Profiling e planos PostgreSQL

## Entregas

- separação do tempo médio de PostgreSQL, Python estimado e renderização Jinja;
- decorador e contexto `medir_tempo()` / `medir_secao()` para funções pesadas;
- captura opcional de plano PostgreSQL para consultas SELECT lentas;
- resumo seguro dos operadores do plano, sem exportar parâmetros SQL;
- exibição do operador dominante no painel `/admin/performance`;
- manutenção da compatibilidade com a instrumentação anterior.

## Configuração recomendada

```env
PERFORMANCE_LOG_ENABLED=1
SQL_PERFORMANCE_LOG_ENABLED=1
SQL_SLOW_QUERY_THRESHOLD_MS=250

# Captura apenas o plano estimado. Pode ser usada em homologação.
SQL_EXPLAIN_ENABLED=1
SQL_EXPLAIN_SAMPLE_RATE=0.10
SQL_EXPLAIN_TIMEOUT_MS=1500

# NÃO ativar em produção sem teste: executa o SELECT novamente.
SQL_EXPLAIN_ANALYZE_ENABLED=0
```

## Segurança

- somente `SELECT` e `WITH` são elegíveis;
- parâmetros e valores não são exibidos no painel nem exportados;
- o plano é resumido em operadores, custos e linhas;
- `EXPLAIN ANALYZE` permanece desativado por padrão;
- a captura é amostrada para evitar ampliar a carga do banco.

## Validação

- 247 testes aprovados;
- compilação dos módulos alterados concluída;
- nenhum endpoint ou tabela do banco alterado.
