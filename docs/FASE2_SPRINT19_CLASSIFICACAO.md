# Fase 2 — Sprint 19

A classificação, os critérios de desempate, os averages e a coordenação do cache saíram de `routes/tabela.py`.

- `rules/classificacao.py`: cálculo puro, pontuação e desempates.
- `services/competicoes/classificacao.py`: interface estável para as rotas.
- `routes/tabela.py`: mantém apenas coordenação HTTP e apresentação.

Nenhum endpoint, template ou tabela foi alterado.
