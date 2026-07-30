# Sprint 11 — Game Engine em modo sombra

## Objetivo
Criar o primeiro núcleo real e isolado do Game Engine e integrá-lo ao registro de ponto sem alterar o fluxo oficial.

## Arquivos de produção
- `game_engine/__init__.py`
- `game_engine/contracts.py`
- `game_engine/validators.py`
- `game_engine/events.py`
- `game_engine/reducer.py`
- `game_engine/service.py`
- `routes/apontadores.py`

## Funcionamento
Antes do registro oficial, a rota captura uma cópia do estado já disponível no cache. Depois que o sistema atual registra o ponto, o Game Engine calcula em memória o resultado esperado e compara placar e saque com o estado oficial.

O modo sombra:
- não grava no banco;
- não modifica cache;
- não publica no Socket.IO;
- não altera a resposta HTTP;
- não adiciona nova consulta ao banco;
- nunca bloqueia o registro oficial.

Divergências aparecem no log com `GAME_ENGINE_SHADOW_DIVERGENCIA`.

## Configuração
- Habilitado por padrão.
- Para desativar: `GAME_ENGINE_SHADOW_ENABLED=0`.

## Validação
- `python -m py_compile`: aprovado.
- Testes específicos: 3 aprovados.
- Suíte completa: 384 aprovados, 0 falhas.
