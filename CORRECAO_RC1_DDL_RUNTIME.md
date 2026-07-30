# Correção RC1 — DDL fora das requisições

## Alterações aplicadas

- As rotinas legadas controladas por `_schema_ja_pronto` não executam mais DDL quando chamadas normalmente.
- Alterações estruturais só são permitidas pelo executor de migrações com `force=True`.
- Foi adicionado `core/schema_requirements.py` para validar tabelas/colunas sem criar ou alterar o banco.
- `repositories/runtime_schema.py` agora valida o schema durante o runtime e só executa DDL quando chamado pela migração.
- O bootstrap do organizador não tenta mais criar a tabela de atletas durante a requisição.
- O painel do apontador apenas valida as tabelas de oficiais, sem criá-las durante a abertura.
- A definição efetiva de `criar_tabelas_oficiais` foi adaptada para separar validação normal de execução por migração.
- O executor versionado recebeu novas etapas para cache, código público, atletas, oficiais, quadras, agenda, rodadas, eventos, campos das partidas e índices.

## Validação

- Compilação completa com `python -m compileall -q .`
- Suíte completa: `352 passed`

## Inicialização recomendada

```bash
python scripts/iniciar_servidor.py
```

Ou manualmente:

```bash
python scripts/executar_migracoes.py
python app.py
```
