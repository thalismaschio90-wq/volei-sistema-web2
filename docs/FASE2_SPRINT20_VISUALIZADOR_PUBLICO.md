# Fase 2 — Sprint 20: regras do visualizador público

## Objetivo

Retirar de `routes/tabela.py` as regras de apresentação dos eventos públicos e
transformá-las em funções puras, reutilizáveis e testáveis.

## Nova estrutura

- `rules/visualizador_publico.py`: interpretação dos eventos, autoria do lance,
  descrições, linha do tempo, evolução por set e estatísticas.
- `routes/tabela.py`: mantém somente aliases de compatibilidade e coordenação HTTP.
- `tests/test_rules_visualizador_publico.py`: cobre ataque, ace, erro do adversário,
  modo simples/scout e evolução dos sets.

## Comportamento preservado

- ataque, bloqueio e ace pertencem à equipe pontuadora;
- erro de saque, erro geral, falta, rotação, invasão, condução e dois toques são
  atribuídos à equipe adversária que cometeu a ação;
- o modo simples exibe apenas a equipe que recebeu o ponto;
- eventos antigos com `detalhes` em JSON continuam sendo aceitos;
- os nomes internos usados pela rota foram mantidos por aliases.

## Validação

- 83 testes aprovados;
- compilação de todos os módulos Python;
- nenhum endpoint, template ou tabela alterado.
