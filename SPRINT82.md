# Fase 4 — Sprint 82: replay e auditoria somente leitura

## Entrega

- `repositories/replay_partida.py`: leitura cronológica da partida e dos eventos persistidos.
- `services/replay_partida.py`: normalização, categorização, descrição e identificação do autor quando já registrado.
- `routes/replay.py`: tela e endpoint JSON restritos ao Super ADM.
- `templates/admin_replay_partida.html`: linha do tempo pesquisável por partida e competição.
- `tests/test_replay_partida.py`: testes das regras de preparação e resumo.

## Endpoints

- `GET /admin/replay-partida`
- `GET /admin/replay-partida/dados`

## Limites desta sprint

O replay usa a tabela `eventos` já existente e não altera o fluxo da partida. Eventos antigos que não gravaram operador continuarão aparecendo como `autor não registrado`. A persistência obrigatória de autoria para todas as novas ações será tratada em uma sprint específica de auditoria.
