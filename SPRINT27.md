# Fase 2 — Sprint 27: Pré-jogo do apontador

## Novos módulos

- `rules/pre_jogo.py`
- `services/apontadores/pre_jogo.py`
- `tests/test_rules_pre_jogo.py`

## Responsabilidades extraídas

- normalização e validação dos lados A/B;
- resolução da equipe correspondente ao lado;
- autorização do operador da partida;
- normalização da fase do pré-jogo;
- montagem do contexto da tela inicial do pré-jogo;
- validação conjunta das numerações na conferência;
- detecção de números repetidos;
- identificação das numerações realmente alteradas;
- montagem do contexto da escolha do capitão.

## Compatibilidade

Nenhum endpoint, template, formulário, tabela ou assinatura pública foi alterado.

## Validação

- compilação dos módulos Python;
- 115 testes aprovados;
- quatro testes novos específicos do pré-jogo.
