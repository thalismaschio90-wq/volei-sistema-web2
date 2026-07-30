# Fase 3 — Sprint 55: painel agregado de performance

## Entrega
- armazenamento limitado e thread-safe das métricas por rota;
- P95 aproximado com janela das últimas amostras;
- média e máximo do tempo de aplicação e PostgreSQL;
- agregação dos fingerprints de SQL lento sem guardar SQL ou parâmetros;
- painel HTML restrito ao Super ADM em `/admin/performance`;
- endpoint JSON em `/admin/performance-status`;
- limpeza manual das métricas;
- limites configuráveis para impedir crescimento de memória.

## Variáveis
```env
PERFORMANCE_LOG_ENABLED=1
SQL_PERFORMANCE_LOG_ENABLED=1
PERFORMANCE_SAMPLE_LIMIT=200
PERFORMANCE_ROUTE_LIMIT=300
PERFORMANCE_QUERY_LIMIT=300
SQL_SLOW_QUERY_THRESHOLD_MS=250
```

## Observação
Nesta sprint o agregador é local ao processo. Com a configuração atual de um worker, ele representa todo o Web Service. Antes de usar múltiplos workers, a agregação deverá ser movida para Redis ou para uma plataforma externa de observabilidade.
