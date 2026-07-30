# Sprint 04 — Banco e schema em runtime

## Objetivo
Reduzir consultas estruturais executadas durante operações normais do organizador, deixando a validação/criação do schema sob responsabilidade das migrações de inicialização.

## Arquivos de produção alterados
- `repositories/quadras.py`
- `repositories/grupos.py`

## Alterações realizadas

### Quadras
- Removidas consultas a `information_schema` durante criação de PINs, normalização de vínculos, salvamento de quadras e vínculos com grupos/partidas.
- As operações passam a usar diretamente as colunas garantidas pelas migrações.
- Mantidas as verificações estruturais apenas no caminho explícito de migração `force=True`.
- Reduzidas consultas extras antes de atualizações de `competicoes`, `grupos` e `partidas`.

### Grupos
- Substituído `SELECT *` por seleção explícita de `id`, `nome`, `competicao`, `quadra_id` e `quadra_nome`.

## Benefícios esperados
- Menos idas ao PostgreSQL ao salvar configurações de quadras.
- Menor risco de espera por catálogo do banco durante uso normal.
- Menos trabalho por requisição no painel do organizador.
- Separação mais clara entre migração e operação da aplicação.

## Compatibilidade
A estrutura esperada já é exigida por `core.schema_requirements` e pelas migrações de inicialização. Nenhuma regra funcional de quadras ou grupos foi alterada.

## Testes
- 379 testes aprovados.
- 0 falhas.
