# Fase 2 — Sprint 14: rodadas e agenda programada

Nesta sprint, as regras e a persistência de `competicao_rodadas` foram retiradas do núcleo de `banco.py`.

- `rules/rodadas.py`: normalização pura;
- `repositories/rodadas.py`: SQL e transações;
- `services/competicoes/rodadas.py`: coordenação e trava de edição;
- `banco.py`: fachadas compatíveis com os imports antigos.

A listagem deixou de executar DDL em toda abertura. A criação da tabela permanece disponível para o boot/migração.
