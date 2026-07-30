# Fase 3 — Sprint 78: Cache inteligente de leituras

## Objetivo

Reduzir viagens repetidas ao PostgreSQL para configurações de competição que
mudam raramente, sem comprometer consistência após edições do organizador.

## Implementação

- `cache/domain_read.py`: cache versionado local/Redis, cópias defensivas e
  invalidação O(1) por domínio e competição.
- `services/competicoes/configuracao.py`: cache das configurações avançadas e
  da agenda.
- `services/competicoes/basico.py`: invalidação após alterações básicas,
  estruturais, regras e pontuação.

## Configuração

```env
DOMAIN_READ_CACHE_BACKEND=local
DOMAIN_READ_CACHE_TTL_SECONDS=60
```

Com Redis homologado:

```env
DOMAIN_READ_CACHE_BACKEND=redis
REDIS_URL=redis://...
```

`off` desativa completamente o recurso.

## Segurança de consistência

PostgreSQL permanece como fonte de verdade. Toda escrita feita pelos serviços
invalida imediatamente a versão da competição; chaves antigas expiram pelo TTL.
O cache devolve cópias defensivas para impedir mutações acidentais.
