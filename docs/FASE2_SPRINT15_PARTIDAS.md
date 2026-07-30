# Fase 2 — Sprint 15: cadastro básico de partidas

Foram separados do `banco.py` o cadastro, a listagem, a busca, a edição, a exclusão e a limpeza de partidas. A operação ao vivo do apontador permanece intacta.

## Novos módulos

- `rules/partidas.py`: normalização de fases, estados bloqueados e proteção de partidas iniciadas.
- `repositories/partidas.py`: SQL do cadastro e agenda básica das partidas.
- `services/competicoes/partidas.py`: coordenação das regras, quadras e persistência.

## Compatibilidade

Os nomes públicos no `banco.py` foram preservados. Também foi removida uma referência inválida a `partida_id` dentro de `criar_partida`, que poderia causar `NameError` ao criar partidas classificatórias sem grupo informado.
