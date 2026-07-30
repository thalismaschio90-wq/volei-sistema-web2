# Fase 3 — Sprint 64: negociação de capacidade do Socket.IO

## Objetivo
Evitar que clientes modernos recebam simultaneamente o delta e quatro eventos legados repetidos.

## Implementação
- Clientes que enviam `suporta_delta: true` entram em `delta:<partida_id>`.
- Clientes antigos entram em `legacy:<partida_id>`.
- `estado_partida_delta` é publicado somente na sala delta.
- Eventos antigos são publicados somente na sala legacy.
- O estado completo inicial continua sendo enviado diretamente ao cliente ao entrar ou reconectar.
- `placar_rapido` continua universal por ser pequeno e usado como fallback visual.

## Segurança
O delta é enviado em toda transição não vazia para clientes modernos, mesmo quando a economia percentual é pequena. Isso evita lacunas quando os eventos legados não são recebidos por esses clientes.

## Compatibilidade
Clientes antigos que não informam capacidade continuam recebendo os mesmos eventos. As seis telas modernas foram marcadas com `suporta_delta: true`.
