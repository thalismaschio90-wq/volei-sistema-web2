# Sprint 20 — Confiabilidade, ACK e heartbeat

## Arquivos de produção alterados

- realtime/presence.py
- socket_events.py
- static/js/realtime/heartbeat_client.js
- static/js/apontador/offline-storage.js
- static/js/apontador/fila-eventos-controller.js
- static/js/apontador/jogo-apontador-main.js
- static/js/apontador/modulos-dependencias.js
- templates/apontador/components/runtime_assets.html
- templates/primeiro_arbitro.html
- templates/segundo_arbitro.html
- templates/arbitro_unico.html
- templates/placar_profissional.html
- templates/visualizador_partida_publica.html
- routes/dashboard_operacional.py
- services/dashboard_operacional.py
- templates/admin_dashboard_operacional.html

## Alterações

- heartbeat de aplicação a cada 15 segundos;
- confirmação `cliente_heartbeat_ok` com versão oficial e latência;
- presença por SID, perfil e partida;
- armazenamento local ou Redis conforme disponibilidade;
- remoção automática da presença ao desconectar ou expirar;
- apontador, árbitros, telão e visualizador enviam heartbeat;
- dashboard operacional mostra clientes ativos, perfis e latência média;
- fila offline possui estados `pendente`, `enviando` e `confirmado`;
- tentativas e horários de envio/confirmação ficam registrados;
- falhas devolvem os eventos para o estado pendente;
- eventos confirmados são removidos somente após ACK do lote.

## Validação

- 400 testes Python aprovados;
- 0 falhas;
- arquivos JavaScript validados com `node --check`.
