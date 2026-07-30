# Fase 2 — Sprint 18: geração do mata-mata

A geração automática de quartas, semifinais, final e terceiro lugar saiu da rota `routes/tabela.py`.

## Novos módulos

- `rules/mata_mata.py`: cruzamentos, classificação intercalada, vencedores/perdedores e mensagens de validação.
- `services/competicoes/mata_mata.py`: coordena limpeza segura dos jogos pendentes, montagem dos registros e inserção em lote.
- `tests/test_rules_mata_mata.py`: testes das quatro fases.

## Preservado

- séries Ouro/Prata isoladas por `origem`;
- partidas iniciadas ou com resultado não são apagadas;
- ordem e quadra escolhida pelo organizador;
- datas/horários programados por fase e série;
- placeholders para vencedores/perdedores ainda não definidos;
- inserção em uma única transação.

## Validação

- 75 testes aprovados;
- compilação Python concluída;
- nenhuma alteração em templates, endpoints ou estrutura do banco.
