# Fase 2 — Sprint 21: contexto e consultas do visualizador público

## Objetivo

Retirar de `routes/tabela.py` as consultas SQL e a montagem pesada do contexto da partida pública, deixando a rota responsável apenas por HTTP, template e JSON.

## Novos módulos

- `repositories/visualizador_publico.py`
  - consulta do destaque da partida;
  - versões de eventos e destaque em uma única conexão.
- `services/competicoes/visualizador_publico.py`
  - prioridade do estado vivo com fallback para o banco;
  - contexto completo da partida;
  - payload leve do polling público.

## Compatibilidade

As URLs, templates, formatos JSON e nomes internos das rotas foram preservados. `_contexto_partida_publica` continua existindo como fachada temporária.

## Ganhos

- remove SQL direto da rota pública;
- concentra leitura do estado vivo em um único serviço;
- reduz duplicação entre página completa, detalhes e polling;
- mantém eventos e destaque fora do polling frequente;
- facilita a futura migração do estado para Redis.

## Validação

- 87 testes aprovados;
- compilação Python concluída;
- nenhum endpoint ou template alterado.
