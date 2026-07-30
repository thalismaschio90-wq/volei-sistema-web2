# Fase 3 — Sprint 73: Laboratório de otimização SQL

Foi criado um laboratório reproduzível para comparar baseline e candidato das
consultas críticas do VolleyTablePro sem versionar parâmetros sensíveis e sem
aplicar índices automaticamente.

## Consultas iniciais

- listagem das partidas da competição;
- busca de uma partida pelo ID;
- versões leves do visualizador público;
- reconstrução do estado operacional do jogo.

## Critério de aprovação

O candidato precisa demonstrar ganho mínimo configurável em média ou P95 e não
pode introduzir regressão superior ao limite definido.

## Limitação honesta

Esta sprint prepara e valida o laboratório, mas não declara ganho de banco sem
executá-lo em homologação contra um PostgreSQL com dados representativos.
