# Sprint 01 — Estabilidade do banco, métricas e Socket.IO

## Objetivo

Reduzir o risco de o site inteiro cair para o modo de conexão direta após uma conexão ociosa expirar, melhorar a visibilidade do uso do banco e corrigir o registro duplicado de handlers do apontador.

## Arquivos alterados

- `repositories/conexao.py`
- `core/performance.py`
- `static/js/apontador/socket-sync.js`
- `render.yaml`
- `tests/test_repositories_conexao_resilience.py` (novo)

## Alterações

### Banco e pool

- Uma conexão inválida recebida do pool agora é descartada individualmente.
- O pool inteiro não é mais fechado apenas porque uma conexão ociosa falhou no ping.
- Foram adicionadas métricas de conexões ativas e pico de uso.
- Foram adicionadas métricas de conexões descartadas.
- Foram adicionadas métricas específicas do fallback direto.
- O limite do semáforo de fallback acompanha a configuração do ambiente.
- O fallback emergencial foi configurado para no máximo duas conexões simultâneas.

### Desempenho

- As métricas de rota agora registram também o tamanho aproximado da resposta em KB.
- Logs de rotas lentas mostram o tamanho da resposta.
- A instrumentação de rotas e SQL foi habilitada no `render.yaml`.
- `EXPLAIN` automático permanece desativado em produção.

### Socket.IO do apontador

- Os handlers anteriores são removidos antes de uma nova instalação.
- O registro usa `WeakMap`, evitando manter sockets descartados em memória.
- Corrige risco de pontos, tempos ou atualizações serem processados mais de uma vez após reconexões.

## Configurações adicionadas ao Render

- `DB_POOL_ENABLED=1`
- `DB_POOL_MIN_SIZE=1`
- `DB_POOL_MAX_SIZE=8`
- `DB_POOL_TIMEOUT=10`
- `DB_DIRECT_FALLBACK_ENABLED=1`
- `DB_DIRECT_FALLBACK_MAX=2`
- `DB_DIRECT_FALLBACK_TIMEOUT=3`
- `PERFORMANCE_LOG_ENABLED=1`
- `PERFORMANCE_LOG_THRESHOLD_MS=500`
- `SQL_PERFORMANCE_LOG_ENABLED=1`
- `SQL_SLOW_QUERY_THRESHOLD_MS=250`
- `SQL_EXPLAIN_ENABLED=0`

## Testes

- Compilação dos arquivos Python alterados: aprovada.
- Testes específicos do pool, DDL, SQL e métricas: 19 aprovados.
- Suíte completa do projeto: 375 aprovados.
- Falhas: 0.

## Aplicação

Copiar os arquivos mantendo exatamente as mesmas pastas. O arquivo `render.yaml` somente terá efeito depois de um novo deploy no Render.
