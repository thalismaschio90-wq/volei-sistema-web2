# Homologação Redis e múltiplos workers no Render

## Objetivo

Validar o estado compartilhado e o Socket.IO antes de aumentar a capacidade do serviço de produção.

## Etapa A — Redis com um worker

Configure no serviço de homologação:

```env
GUNICORN_WORKERS=1
GUNICORN_THREADS=4
REALTIME_STATE_BACKEND=redis
REALTIME_REDIS_REQUIRED=1
SOCKETIO_USE_REDIS=1
REDIS_URL=<URL interna do Redis>
REALTIME_STATE_TTL_SECONDS=86400
```

Confirme:

1. `/healthz` responde `200`.
2. `/readyz` responde `200` e informa `realtime.backend = redis`.
3. O Super ADM consegue abrir `/admin/runtime-status` sem credenciais expostas.
4. Apontador, primeiro árbitro, segundo árbitro, telão e visualizador entram na mesma partida.
5. Ponto, desfazer, substituição, tempo, cartão e troca de set aparecem em todas as telas sem F5.

## Etapa B — Dois workers

Somente após a Etapa A:

```env
GUNICORN_WORKERS=2
```

Repita o teste com, no mínimo:

- 1 apontador;
- 2 telas de árbitros;
- 1 placar profissional;
- 10 a 30 navegadores no visualizador público;
- duas partidas simultâneas, quando possível.

## Critérios para aprovação

- nenhuma regressão de placar ou rotação;
- nenhuma tela exige F5;
- troca de set chega a todas as telas;
- versão do estado cresce continuamente;
- `/readyz` permanece saudável;
- pool do PostgreSQL não fica saturado;
- não há reinício por memória no Render.

## Recuo seguro

Em caso de falha:

```env
GUNICORN_WORKERS=1
REALTIME_STATE_BACKEND=local
REALTIME_REDIS_REQUIRED=0
SOCKETIO_USE_REDIS=0
```

Faça novo deploy. Não use dois workers no modo local.
