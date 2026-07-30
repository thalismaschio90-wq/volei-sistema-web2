# Fase 2 — Sprint 47: cache do cabeçalho global

O cabeçalho global consultava a equipe no PostgreSQL toda vez que qualquer template era renderizado. Como quase todas as páginas usam o mesmo layout, essa consulta adicionava latência a praticamente toda navegação da equipe.

Foi criado `services/ui/topbar.py`, com cache fora da sessão do Flask, cópia defensiva, TTL e backend local ou Redis. Nome, perfil e escudo são invalidados imediatamente após alterações.

Configuração padrão:

```env
TOPBAR_CACHE_BACKEND=local
TOPBAR_CACHE_TTL_SECONDS=60
```

Com Redis homologado:

```env
TOPBAR_CACHE_BACKEND=redis
REDIS_URL=redis://...
```

Nenhum escudo base64 é gravado no cookie da sessão.
