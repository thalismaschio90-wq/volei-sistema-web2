# Sprint 76 — Detector de consultas repetidas (N+1)

## Objetivo
Identificar, por requisição, a mesma estrutura SQL executada repetidamente e apontar a rota e a origem no código sem guardar SQL bruto, parâmetros ou dados pessoais.

## Implementação
- `core/n_plus_one.py`: classificação e diagnóstico seguro.
- `core/sql_performance.py`: contabiliza fingerprints de todas as consultas enquanto a instrumentação está ativa.
- `core/performance_store.py`: agrega padrões por rota e fingerprint.
- `templates/admin_performance.html`: nova tabela “Possíveis consultas N+1”.

## Configuração
```env
PERFORMANCE_LOG_ENABLED=1
SQL_PERFORMANCE_LOG_ENABLED=1
SQL_N_PLUS_ONE_THRESHOLD=4
```

## Interpretação
Um alerta não prova sozinho que existe N+1. Ele indica que a mesma consulta apareceu várias vezes na mesma requisição. A correção pode ser JOIN, consulta em lote, `IN/ANY`, pré-carregamento ou cache local da requisição.
