# Fase 3 — Sprint 54: Instrumentação SQL

Esta sprint inicia a Fase 3 com medição real do PostgreSQL por requisição.

## Variáveis

```env
PERFORMANCE_LOG_ENABLED=1
PERFORMANCE_LOG_THRESHOLD_MS=500
SQL_PERFORMANCE_LOG_ENABLED=1
SQL_SLOW_QUERY_THRESHOLD_MS=250
SQL_SLOW_QUERY_MAX_PER_REQUEST=5
```

## Dados registrados

- tempo total da rota;
- quantidade de consultas SQL;
- tempo total e maior tempo SQL;
- fingerprint anônimo das consultas lentas;
- operação (`SELECT`, `UPDATE`, etc.).

Parâmetros e valores não são escritos nos logs.

## Navegador

O cabeçalho `Server-Timing` passa a informar `app`, `db` e quantidade de consultas.
Isso permite visualizar as métricas na aba Network do navegador.
