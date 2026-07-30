# Sprint 28 — Competições sem dependência reversa de banco.py

## Arquivos alterados

- `banco.py`
- `repositories/competicoes_campos.py`
- `repositories/competicoes_basico.py`
- `repositories/competicoes_ciclo.py`
- `repositories/competicoes_config.py`

## Alterações

1. Removidos todos os `_legacy()` dos três repositórios de competições.
2. Os repositórios agora usam diretamente `repositories.conexao`.
3. A inspeção temporária de colunas usa `core.schema_inspection`.
4. A montagem de campos compatíveis da tabela `competicoes` foi extraída para
   `repositories/competicoes_campos.py`.
5. A validação de competição travada passou para
   `validar_competicao_editavel_persistencia`.
6. Criação de login e senha do organizador foi isolada no repositório de ciclo.
7. A criação da quadra padrão chama diretamente `repositories.quadras`.
8. `banco.py` preserva `_campos_competicao`, `_campo_ou_alias` e
   `validar_competicao_editavel` apenas como fachadas compatíveis.

## Resultado arquitetural

Antes:

```text
repositories/competicoes_* → banco.py → repositories
```

Depois:

```text
repositories/competicoes_*
        ↓
repositories.conexao / core / rules
        ↓
PostgreSQL
```

## Validação

- compilação Python aprovada;
- nenhuma ocorrência de `_legacy()` ou `import banco` nos repositórios de competição;
- testes direcionados: 16 aprovados;
- suíte oficial completa: 400 aprovados;
- falhas: 0.
