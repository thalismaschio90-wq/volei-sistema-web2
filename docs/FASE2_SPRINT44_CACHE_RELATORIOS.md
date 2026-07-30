# Fase 2 — Sprint 44: cache dos relatórios

- Cache curto compartilhável entre preview e PDF.
- Backend local por padrão e Redis opcional.
- Chaves incluem competição, perfil, equipe e filtros.
- `?recalcular=1` ignora o cache.
- TTL padrão: 120 segundos (`RELATORIOS_CACHE_TTL_SECONDS`).
- Contrato de tarefa criado em `tasks/relatorios.py`; worker ainda não é ativado.

Configuração segura atual:

```env
RELATORIOS_CACHE_BACKEND=local
RELATORIOS_CACHE_TTL_SECONDS=120
```

Com Redis homologado:

```env
RELATORIOS_CACHE_BACKEND=redis
REDIS_URL=redis://...
```
