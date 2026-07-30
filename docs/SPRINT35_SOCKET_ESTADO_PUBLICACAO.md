# Fase 2 — Sprint 35: Estado e publicação Socket.IO

## Objetivo

Retirar do `socket_events.py` o armazenamento compartilhado, a convenção de salas e a publicação genérica, mantendo todos os eventos e endpoints existentes.

## Novos módulos

- `realtime/state_store.py`: armazenamento local thread-safe, cópias defensivas, versão monotônica e data da atualização.
- `realtime/rooms.py`: fonte única para nomes de salas da partida, árbitros e placar do apontador.
- `realtime/publisher.py`: publicação padronizada em todas as salas compatíveis.

## Compatibilidade

`socket_events.py` continua exportando:

- `obter_estado_cache`
- `obter_estado_versao`
- `atualizar_estado_cache`
- `limpar_estado_cache`
- `emitir_estado_partida`
- demais funções e handlers existentes

Nenhum nome de evento Socket.IO foi removido ou renomeado.

## Proteções

- O cache nunca entrega uma referência interna mutável.
- Toda gravação recebe uma versão crescente por partida.
- O payload salvo recebe `estado_versao` e `estado_atualizado_em`.
- Entradas de apontador, árbitro e tempo real leem o estado pelo mesmo store.
- Estados locais e do jogo avulso passam a publicar a versão efetivamente salva.

## Preparação para Redis

A aplicação continua usando armazenamento local nesta sprint, compatível com a configuração atual de um worker. Como o contrato está isolado, uma implementação Redis poderá substituir o store local sem alterar as rotas e os serviços consumidores.

## Validação

- 167 testes aprovados.
- Compilação de todos os módulos Python concluída.
- Testes de cópia defensiva, versão monotônica, remoção e salas.
