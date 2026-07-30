# Sprint 44 — Cache e preparação de worker para relatórios

Esta sprint adiciona cache curto para evitar que a pré-visualização e o PDF recalcularem o mesmo relatório pesado em sequência.

Arquivos principais:
- `services/relatorios/cache.py`
- `tasks/relatorios.py`
- `routes/relatorios.py`
- `tests/test_relatorios_cache.py`
- `docs/FASE2_SPRINT44_CACHE_RELATORIOS.md`

Validação: 205 testes aprovados.
