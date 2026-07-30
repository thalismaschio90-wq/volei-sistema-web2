# Fase 2 — Sprint 36: entrada em salas e sincronização inicial

Esta sprint centraliza a inscrição dos clientes nas salas Socket.IO e o envio do estado inicial.

## Alterações

- novo `realtime/synchronization.py`;
- handlers `entrar_partida`, `entrar_partida_tempo_real`, `join` e `entrar_arbitro` menores;
- uma única lista canônica de salas por partida;
- confirmação de entrada padronizada;
- fallback de estado inicial centralizado;
- emissão de múltiplos eventos ao mesmo `sid` sem repetição.

## Compatibilidade

Nenhum nome de evento ou sala antiga foi removido. O estado continua local e o Gunicorn deve permanecer com um worker até a implantação de Redis.
