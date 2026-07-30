# Sprint 10 — Deduplicação do tempo real

## Problema confirmado
As conexões modernas entravam simultaneamente em várias salas alternativas da mesma partida. O publicador, por compatibilidade, também enviava o mesmo evento a todas essas salas. Assim, uma única atualização podia chegar repetida várias vezes à mesma tela.

Além disso, o apontador assinava três nomes legados que podiam transportar o mesmo snapshot completo.

## Alterações
- `realtime/synchronization.py`: conexões modernas entram apenas na sala canônica da partida e na sala de capacidade (`delta` ou `legacy`). Salas antigas continuam disponíveis para clientes que as usam explicitamente.
- `static/js/apontador/socket-sync.js`: snapshots legados iguais são deduplicados por partida e versão antes de renderizar.
- testes atualizados e adicionados para proteger o comportamento.

## Resultado esperado
- menos mensagens Socket.IO por ponto;
- menos renderizações repetidas;
- menor uso de CPU e rede;
- menor chance de placar, rotação ou cronômetro processarem o mesmo estado várias vezes;
- compatibilidade mantida com clientes antigos.

## Validação
- 384 testes aprovados;
- 0 falhas.
