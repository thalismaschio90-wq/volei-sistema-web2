# Sprint 02 — Otimização dos painéis administrativos

## Escopo
Otimização conservadora dos painéis iniciais do SuperAdmin e do Organizador, sem alterar regras de negócio ou rotas públicas.

## Alterações

### `repositories/superadmin_painel.py`
- O painel inicial deixou de carregar a lista completa de SuperADMs/clientes.
- A listagem completa continua disponível normalmente em `/superadmins`.
- Removida do painel inicial uma consulta que carregava campos desnecessários, inclusive senha/hash.
- Consultas do painel master: **3 → 2**.

### `repositories/organizador_painel.py`
- Substituído `SELECT * FROM competicoes` por seleção apenas do campo necessário (`nome`).
- Contagem de solicitações e últimas solicitações consolidadas em uma única consulta com `COUNT(*) OVER()`.
- Removida a consulta de notificações do carregamento inicial, pois o template atual não exibe essa lista.
- Consultas com competição ativa: **5 → 3**.
- Eliminados `SELECT *` desse fluxo.

## Compatibilidade
- Mesmas chaves de contexto continuam sendo retornadas.
- A rota `/superadmins` não foi alterada.
- O painel do organizador continua exibindo contagem e últimas solicitações.
- Nenhuma regra de competição, equipe, apontador ou partida foi modificada.

## Testes
- 377 testes aprovados.
- 0 falhas.

## Arquivos de produção modificados
- `repositories/superadmin_painel.py`
- `repositories/organizador_painel.py`

## Arquivos de teste modificados/adicionados
- `tests/test_superadmin_painel.py`
- `tests/test_organizador_painel.py`
- `tests/test_paineis_administrativos_performance.py`
