# Fase 2 — Sprint 34: Finalização de set e partida

Esta sprint separa as regras e a coordenação da finalização do apontador.

## Novos módulos

- `rules/finalizacao.py`
- `services/apontadores/finalizacao.py`

## Responsabilidades centralizadas

- normalização do placar e dos sets recebidos do navegador;
- extração das parciais e metadados finais;
- separação de eventos já sincronizados;
- confirmação do placar final persistido;
- estados consistentes de `entre_sets` e `finalizada`;
- respostas JSON para papeleta e observações;
- proteção da rota de observações contra estado antigo;
- normalização do destaque da partida.

## Compatibilidade

Nenhum endpoint, template, nome de campo ou tabela foi alterado.

## Validação

- 163 testes aprovados;
- compilação completa dos módulos Python;
- testes específicos de finalização, eventos pendentes, WO, observações e destaque.
