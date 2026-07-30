# Sprint 29 — Equipes sem dependência reversa de banco.py

## Arquivos de produção alterados

- `repositories/equipes_contexto.py`
- `repositories/equipes_cadastro.py`
- `repositories/equipes_escrita.py`
- `repositories/equipes_perfil.py`

## Alterações

1. Removidos todos os `_legacy()` e `import banco` dos repositórios de equipes.
2. Cadastro e vínculo usam diretamente `repositories.conexao`.
3. Cliente da competição, geração de login/senha e busca global de equipe foram
   isolados em `repositories/equipes_contexto.py`.
4. Operações normais não executam mais DDL nem funções de preparação de schema.
5. Redefinição de senha passou a buscar e atualizar a equipe na mesma conexão.
6. Exclusão e renomeação verificam o travamento da competição na mesma transação.
7. Perfil e escudo usam `core.schema_inspection` apenas para compatibilidade de
   colunas opcionais.
8. As assinaturas públicas e os wrappers existentes em `banco.py` foram preservados.

## Resultado arquitetural

Antes:

```text
repositories/equipes_* → banco.py → repositories
```

Depois:

```text
repositories/equipes_*
        ↓
repositories.conexao / rules / core
        ↓
PostgreSQL
```

## Validação

- compilação Python aprovada;
- nenhuma ocorrência de `_legacy()` ou `import banco` nos repositórios de equipes;
- testes direcionados: 45 aprovados;
- suíte oficial completa: 400 aprovados;
- falhas: 0.
