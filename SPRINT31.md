# Fase 2 — Sprint 31: Motor único de rotação e saque

## Objetivo
Centralizar as regras que alteram saque e as seis posições, impedindo que ponto,
substituição, banco e payloads legados reconstruam a rotação de formas diferentes.

## Arquivos novos
- `rules/rotacao.py`
- `services/apontadores/rotacao.py`
- `tests/test_rules_rotacao.py`
- `tests/test_service_rotacao.py`

## Integrações
- `banco.py` mantém os nomes antigos como aliases de compatibilidade.
- Os dois fluxos de ponto usam a mesma transição atômica de rotação.
- A substituição rápida do apontador usa o mesmo normalizador e não reordena posições.
- O payload legado `rotacao.equipe_a/equipe_b` continua aceito.

## Regra central
A equipe gira somente quando marca o ponto sem estar sacando. O resultado da
transição contém as duas rotações, saque anterior, saque atual e indicação de giro,
permitindo persistir e publicar um estado completo.
