# Sprint 48 — Painel do Super ADM

- Contexto, totais e lista de clientes passaram para `repositories/superadmin_painel.py`.
- O painel usa uma única conexão por preenchimento de cache.
- Os três `COUNT(*)` são obtidos em uma única consulta SQL.
- A listagem de clientes não executa DDL durante a navegação.
- Cache do painel permanece curto (30 s) e é invalidado após criar/excluir Super ADM cliente.
- Endpoints, template e campos do contexto foram preservados.
