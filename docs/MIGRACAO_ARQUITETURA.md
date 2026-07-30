# Migração arquitetural do VolleyTablePro

## Regra de dependência

A direção permitida é:

`routes -> services -> rules/repositories -> conexão`

- `rules`: funções puras, sem Flask e sem banco;
- `repositories`: somente persistência;
- `services`: coordenação de casos de uso;
- `realtime`: contratos, estado e publicação;
- `tasks`: relatórios e cálculos fora do clique do usuário;
- `cache`: dados derivados e temporários.

## Compatibilidade

O arquivo `banco.py` continua sendo a API legada. Novos módulos devem importar
`repositories`, e as funções serão migradas por domínio em lotes pequenos.

## Ordem de migração

1. conexão e transações;
2. equipes e atletas;
3. competições e configurações;
4. partidas e classificação;
5. relatórios;
6. estado/eventos do jogo;
7. Socket.IO com Redis;
8. divisão do frontend do apontador.

## Critérios para mover uma função

Uma função só deve ser migrada quando:

- seus chamadores foram identificados;
- existe teste ou roteiro de validação;
- SQL e regra foram separados;
- os imports antigos continuam funcionando;
- o tempo antes/depois foi medido.
