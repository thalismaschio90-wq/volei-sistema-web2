# Sprint 71 — Reconexão e polling adaptativos

## Objetivo

Evitar tempestades de reconexão e de polling quando a internet do ginásio oscila ou o serviço reinicia.

## Alterações

- cliente compartilhado `static/js/realtime/connection_guard.js`;
- backoff exponencial após falhas consecutivas;
- jitter aleatório para que dezenas de telas não consultem o servidor no mesmo milissegundo;
- polling mais lento em abas ocultas e partidas finalizadas;
- polling de árbitros convertido de intervalo fixo para agendamento recursivo adaptativo;
- reconexões Socket.IO com `randomizationFactor`;
- retorno rápido ao intervalo normal depois de uma resposta bem-sucedida.

## Compatibilidade

Nenhum endpoint, evento, payload, regra do jogo ou tabela foi alterado.
