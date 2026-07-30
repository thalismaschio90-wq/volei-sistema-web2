# Fase 3 — Sprint 60: baseline e comparação antes/depois

Esta sprint não cria índices às cegas. Ela adiciona uma ferramenta para comparar duas exportações reais do painel de performance e provar se uma alteração melhorou ou piorou o sistema.

## Uso

1. Antes da alteração, exporte `/admin/performance/exportar.json`.
2. Limpe as métricas e repita o mesmo cenário de teste.
3. Faça a alteração em homologação.
4. Exporte novamente o JSON.
5. Execute:

```bash
python scripts/comparar_performance.py antes.json depois.json --saida comparacao.md
```

O relatório compara P95 das rotas, tempo médio de SQL, quantidade média de consultas e fingerprints lentos.
