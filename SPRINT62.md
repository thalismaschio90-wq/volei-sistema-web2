# Fase 3 — Sprint 62: clientes incrementais do Socket.IO

## Objetivo

Adaptar apontador, árbitros, placar profissional e visualizador público para aplicar `estado_partida_delta`, mantendo snapshots completos e eventos legados durante a homologação.

## Entrega

- `static/js/realtime/delta_client.js`: cliente compartilhado de patch recursivo e controle de versão.
- Verificação estrita de `estado_versao_base` antes de aplicar um delta.
- Descarte de deltas duplicados ou antigos.
- Solicitação automática de snapshot quando uma versão intermediária for perdida.
- Aplicação de chaves removidas e patches aninhados.
- Compatibilidade mantida com os eventos completos existentes.

## Telas adaptadas

- `jogo_apontador.html`
- `primeiro_arbitro.html`
- `segundo_arbitro.html`
- `arbitro_unico.html`
- `placar_profissional.html`
- `visualizador_partida_publica.html`

## Segurança operacional

`SOCKET_LEGACY_STATE_EVENTS` deve continuar em `1` até o teste de homologação comprovar que todas as telas recebem deltas e recuperam snapshots sem divergência.
