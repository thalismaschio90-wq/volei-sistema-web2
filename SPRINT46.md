# VolleyTablePro — Fase 2 / Sprint 46

Invalidação coerente do cache de relatórios por competição.

- namespace versionado por competição;
- invalidação O(1) no backend local e Redis;
- invalidação automática após finalização da partida;
- isolamento entre competições;
- 211 testes aprovados.
