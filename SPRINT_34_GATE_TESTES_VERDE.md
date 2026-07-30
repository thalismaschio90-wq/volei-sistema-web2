# Sprint 34 — Gate oficial de testes restaurado

## Arquivos alterados

- `game_engine/events.py`
- `tests/test_game_engine_ponto_sombra.py`
- `tests/test_repositories_equipes_cadastro.py`
- `tests/test_repositories_equipes_escrita.py`
- `tests/test_repositories_equipes_perfil.py`

## Correções

1. Restaurada a fachada compatível `evento_ponto_registrado()` no Game Engine.
2. O teste do modo sombra passou a usar a API oficial atual `comparar_ponto_em_modo_sombra()`.
3. Os testes dos repositórios de equipes deixaram de simular a antiga camada `_legacy()`/`banco.py`.
4. Os mocks agora são aplicados diretamente nos símbolos importados pelos módulos testados.
5. Fixtures foram atualizadas para refletir as consultas atuais de travamento, vínculo, senha, perfil e renomeação.
6. Nenhum teste abre conexão real ou depende de `DATABASE_URL`.

## Validação

- testes direcionados: 16 aprovados;
- suíte oficial `pytest -q tests`: 403 aprovados;
- falhas: 0;
- compilação dos arquivos alterados: aprovada.
