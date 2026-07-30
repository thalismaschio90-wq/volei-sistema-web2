# Fase 2 — Sprint 16: geração e ordenação da agenda classificatória

## Objetivo

Retirar de `routes/tabela.py` as regras puras de geração de rodadas e distribuição física dos jogos, sem alterar rotas, banco de dados ou comportamento visual.

## Nova responsabilidade

O módulo `rules/agenda_partidas.py` passou a concentrar:

- geração todos-contra-todos pelo método do círculo;
- tratamento de rodadas e confrontos legados;
- normalização de IDs de quadras;
- montagem da fila classificatória;
- verificação de descanso e conflitos;
- escolha do próximo jogo possível;
- distribuição dos jogos em uma ou várias quadras;
- preservação da rodada lógica independentemente do slot físico.

`routes/tabela.py` continua coordenando formulário, permissões, consultas, gravações e mensagens, mas importa essas regras prontas.

## Compatibilidade

Os nomes internos usados pela rota foram preservados por aliases de importação. Nenhum endpoint, template, tabela ou assinatura pública foi alterado.

## Validação

- compilação de todos os arquivos Python;
- 68 testes aprovados;
- testes específicos para números pares e ímpares de equipes;
- verificação de folgas, conflitos, quadra única e múltiplas quadras.
