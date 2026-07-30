# Fase 2 — Sprint 38: versão do estado no apontador

Esta sprint adapta o JavaScript do apontador ao controle otimista de versão criado no servidor.

## Alterações

- guarda em memória a versão oficial mais recente recebida;
- envia `estado_versao_base` em cada snapshot local;
- ignora eventos Socket.IO com versão inferior à já aplicada;
- trata `conflito_versao` e `snapshot_atrasado` usando o estado oficial devolvido pelo servidor;
- reidrata a tela sem F5 quando uma aba atrasada tenta sobrescrever o jogo;
- não persiste a versão entre recargas, evitando reaproveitar versão inválida após reinício do processo.

## Compatibilidade

Nenhum endpoint, nome de evento Socket.IO, tabela ou campo visual foi removido.
