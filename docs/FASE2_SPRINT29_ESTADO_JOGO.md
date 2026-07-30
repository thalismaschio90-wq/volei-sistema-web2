# Fase 2 — Sprint 29: Estado operacional do jogo

Esta sprint separa o carregamento e a reconstrução leve do estado usado pela tela do apontador.

## Novos módulos

- `rules/estado_jogo.py`: normalização de atletas, rotações e campos autoritativos.
- `services/apontadores/estado_jogo.py`: coordenação entre cache, snapshot local, banco, papeletas e elenco.

## Garantias adicionadas

- `set_atual`, placar e sets do registro oficial vencem cache antigo.
- Equipes operacionais não são trocadas por estado atrasado.
- Rotação só usa a papeleta validada como fallback.
- Elenco dos modais é mesclado sem duplicar camisas.
- A abertura do jogo não varre histórico/eventos.
- A rota continua responsável apenas pelo fluxo HTTP e renderização.

## Compatibilidade

Nenhum endpoint, template, tabela ou payload público foi renomeado.
