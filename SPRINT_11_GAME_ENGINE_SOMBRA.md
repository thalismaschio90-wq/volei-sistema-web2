# Sprint 11 — Game Engine em modo sombra

## Objetivo
Criar o primeiro núcleo puro do novo motor de partidas e integrá-lo ao registro de ponto sem substituir o fluxo legado.

## Arquivos de produção
- `game_engine/__init__.py`
- `game_engine/contracts.py`
- `game_engine/validators.py`
- `game_engine/events.py`
- `game_engine/reducer.py`
- `game_engine/service.py`
- `routes/apontadores.py`

## Funcionamento
O registro oficial continua sendo executado por `registrar_ponto_partida`. Antes dele, a rota captura o estado anterior do cache. Depois da confirmação oficial, o Game Engine:

1. cria um comando imutável;
2. transforma o comando em `PONTO_REGISTRADO`;
3. aplica o evento a uma cópia do estado anterior;
4. compara placar e saque com o estado oficial;
5. registra `GAME_ENGINE_SHADOW_DIVERGENCIA` somente se houver diferença.

O modo sombra não grava no banco, não publica no Socket.IO, não altera o cache e não muda a resposta HTTP.

## Controle
A variável `GAME_ENGINE_SHADOW_ENABLED=0` desativa a comparação. Por padrão, o modo sombra fica habilitado.

## Testes
- Testes específicos: 9 aprovados.
- Suíte completa: 384 aprovados, 0 falhas.
