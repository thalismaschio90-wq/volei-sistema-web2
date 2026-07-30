# Sprint 35C — Motor do jogo otimizado

## Arquivos alterados

- `services/apontadores/finalizacao.py`
- `services/apontadores/pontos.py`
- `services/apontadores/rotacao.py`
- `services/apontadores/substituicoes.py`
- `game_engine/events.py`

## Alterações

1. A separação dos eventos pendentes passou a usar uma única varredura.
2. IDs locais não são mais normalizados repetidamente na mesma operação.
3. A publicação de ponto reaproveita o estado autoritativo devolvido pelo cache.
4. A montagem do payload não cria uma cópia intermediária desnecessária do estado.
5. Substituições deixaram de executar `deepcopy` no estado inteiro.
6. Somente rotação, status e listas realmente alteradas são copiadas.
7. A fachada do Game Engine usa `dataclasses.replace` para trocar apenas a sequência.
8. Todas as assinaturas públicas foram preservadas.

## Validação disponível

- compilação Python aprovada nos cinco arquivos;
- assinaturas públicas comparadas e preservadas;
- não foi executada a suíte completa porque `tests/` e o restante das dependências não foram enviados nesta etapa.
