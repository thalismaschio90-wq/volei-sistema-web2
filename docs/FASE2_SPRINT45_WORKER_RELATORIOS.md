# Fase 2 — Sprint 45: worker de relatórios

Esta sprint adiciona execução assíncrona opcional por RQ/Redis, sem alterar o fluxo síncrono atual.

## Variáveis

```env
RELATORIOS_ASYNC_ENABLED=1
RELATORIOS_RQ_QUEUE=relatorios
RELATORIOS_RQ_TIMEOUT_SECONDS=600
RELATORIOS_RQ_RESULT_TTL_SECONDS=3600
REDIS_URL=redis://...
```

## Worker no Render

Criar um **Background Worker** usando o mesmo repositório e ambiente do Web Service:

```bash
python scripts/worker_relatorios.py
```

O Web Service enfileira a tarefa e volta a atender outras requisições. O worker calcula o relatório e grava o resultado no cache compartilhado. O preview e o PDF existentes reaproveitam esse cache.

## Endpoints opcionais

- `POST /relatorios/<tipo>/gerar-assincrono`
- `GET /relatorios/tarefas/<tarefa_id>`

Os endpoints antigos continuam síncronos e compatíveis.
