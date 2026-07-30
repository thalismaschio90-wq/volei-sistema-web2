# Fase 3 — Sprint 67

## Telemetria de renderização em tempo real

- Mede o tempo de atualização do DOM após deltas Socket.IO.
- Agrupa as medições no navegador e envia lotes a cada 10 renderizações ou 5 segundos.
- Não envia estado da partida, nomes, placar, atletas ou dados pessoais.
- Exibe média, máxima, quantidade de renderizações e atualizações agrupadas por tipo de tela em `/admin/realtime-delta`.
- Telas cobertas: apontador, árbitros, placar profissional e visualizador público.

A telemetria é apenas diagnóstica e não altera regras, endpoints ou payloads do jogo.
