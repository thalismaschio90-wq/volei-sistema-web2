# Fase 3 — Sprint 63: homologação segura dos deltas

Esta sprint adiciona telemetria agregada para comprovar a economia e a
confiabilidade do protocolo incremental antes de desligar os eventos Socket.IO
legados.

## Painel

- `/admin/realtime-delta`
- `/admin/realtime-delta-status`

## Variáveis

```env
SOCKET_DELTA_ENABLED=1
SOCKET_LEGACY_STATE_EVENTS=1
SOCKET_LEGACY_REQUIRE_DELTA_HEALTHY=0
SOCKET_DELTA_HEALTH_MIN_APPLIED=50
SOCKET_DELTA_HEALTH_MAX_GAP_PERCENT=1
SOCKET_DELTA_HEALTH_REQUIRED_CLIENTS=apontador,arbitro,placar_profissional,visualizador_publico
SOCKET_DELTA_TELEMETRY_MAX_PER_MINUTE=120
```

Para homologar o desligamento protegido dos eventos antigos:

```env
SOCKET_LEGACY_STATE_EVENTS=0
SOCKET_LEGACY_REQUIRE_DELTA_HEALTHY=1
```

Nesse modo, os eventos antigos continuam sendo emitidos após o processo iniciar
e só são desligados quando a telemetria atingir os critérios configurados. Após
um reinício, a proteção volta a manter os eventos antigos até uma nova amostra
saudável ser coletada.

A telemetria do navegador é enviada em lotes de até dez ocorrências ou a cada
cinco segundos para não criar uma requisição por delta.
