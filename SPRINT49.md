# Fase 2 — Sprint 49: Painel do organizador

- Contexto do painel carregado com uma única conexão PostgreSQL.
- Contagem de solicitações usa `COUNT(*)`, sem listar até 200 registros para contar.
- Configuração inicial, últimas solicitações e notificações são consultadas no mesmo fluxo.
- DDL de garantia de schema removido da abertura normal do painel.
- Cache curto do contexto completo preservado na rota.
