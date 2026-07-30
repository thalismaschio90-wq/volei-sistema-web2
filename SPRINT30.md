# Fase 2 — Sprint 30: motor inicial de pontos

## Objetivo
Separar da rota do apontador a validação do lance, a definição da equipe pontuadora, a autoria do scout, a preparação do evento, a publicação no Socket.IO e a resposta compacta ao navegador.

## Novos módulos
- `rules/pontos_jogo.py`
- `services/apontadores/pontos.py`
- `tests/test_pontos_jogo.py`

## Alterado
- `routes/apontadores.py`

## Garantias preservadas
- ponto direto pertence à equipe clicada;
- erro ou falta gera ponto para o adversário;
- ataque, bloqueio e ace exigem atleta;
- ponto simples não exige scout;
- falha no Socket.IO depois do commit não transforma o ponto salvo em erro HTTP;
- final da partida continua direcionando para observações;
- endpoints e payloads públicos permanecem compatíveis.

## Validação
- 131 testes aprovados;
- compilação Python concluída.
