# Fase 2 — Sprint 1: conexão e transações

## Alteração aplicada

A implementação de conexão PostgreSQL/Neon saiu do arquivo `banco.py` e passou a
existir somente em `repositories/conexao.py`.

O arquivo legado continua exportando `conectar`, portanto imports existentes
como `from banco import conectar` permanecem válidos.

## Benefícios imediatos

- elimina duas futuras implementações concorrentes de pool;
- concentra recuperação de falhas SSL/Neon;
- mede espera do pool e uso do fallback;
- prepara a migração dos repositórios por domínio;
- adiciona contexto explícito para consultas somente leitura;
- reduz risco de conexões `idle in transaction`.

## Variáveis mantidas

- `DB_POOL_ENABLED`
- `DB_POOL_MIN_SIZE`
- `DB_POOL_MAX_SIZE`
- `DB_POOL_TIMEOUT`
- `DB_POOL_MAX_IDLE`
- `DB_POOL_MAX_LIFETIME`
- `DB_POOL_RECONNECT_TIMEOUT`
- `DB_POOL_PING`
- `DB_DIRECT_FALLBACK_ENABLED`
- `DB_DIRECT_FALLBACK_MAX`
- `DB_DIRECT_FALLBACK_TIMEOUT`
- `DB_CONNECT_TIMEOUT`

## Compatibilidade

Nenhuma rota foi renomeada e nenhuma regra de negócio foi alterada nesta sprint.
