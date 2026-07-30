# Sprint 18 — Redis e estado compartilhado

## Objetivo
Preparar a aplicação para ativar Redis de forma automática e segura, sem quebrar o ambiente atual quando `REDIS_URL` ainda não estiver configurada.

## Arquivos de produção alterados
- `core/runtime_config.py`
- `realtime/state_store.py`
- `realtime/event_history.py`
- `extensions.py`
- `render.yaml`

## Alterações
- Novo modo `REALTIME_STATE_BACKEND=auto`.
- Sem `REDIS_URL`, o estado e o histórico continuam em memória local.
- Com `REDIS_URL`, estado vivo e histórico de deltas passam automaticamente para Redis.
- A fila do Socket.IO passa a utilizar a mesma URL quando Redis estiver disponível.
- A validação de runtime informa o backend solicitado e o backend efetivamente resolvido.
- Múltiplos workers continuam bloqueados se estado Redis e fila Socket.IO não estiverem configurados.
- `render.yaml` foi preparado com modo automático, mantendo um worker até a homologação do Redis.

## Ativação no Render
1. Provisionar Redis.
2. Configurar `REDIS_URL` no serviço web.
3. Fazer deploy ainda com `GUNICORN_WORKERS=1`.
4. Conferir `/readyz` e logs; o backend deve aparecer como `redis`.
5. Somente depois alterar `GUNICORN_WORKERS` para `2`.

## Validação
- Testes direcionados: 21 aprovados.
- Suíte completa: 388 aprovados.
- Falhas: 0.
