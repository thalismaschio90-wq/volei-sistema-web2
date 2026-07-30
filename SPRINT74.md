# Fase 3 — Sprint 74: índices críticos controlados

Esta sprint prepara a primeira otimização concreta do PostgreSQL, sem aplicar
mudanças automaticamente no banco de produção.

## Índices candidatos

- `eventos (competicao, partida_id)` para contagens e histórico por partida;
- índice funcional de `equipes_competicoes` por competição e nome normalizado;
- índices funcionais de `partidas` para equipe A e equipe B;
- índice de ordenação por competição, rodada, ordem e ID.

## Segurança

O comando padrão é somente uma simulação:

```powershell
py scripts/aplicar_indices_criticos.py
```

Para aplicar em homologação são necessárias duas autorizações explícitas:

```powershell
$env:SQL_INDEX_APPLY_ALLOWED="1"
py scripts/aplicar_indices_criticos.py --apply
```

O script usa `CREATE INDEX CONCURRENTLY`, autocommit e ignora índices já
existentes. Ele nunca é executado automaticamente pelo Flask ou pelo Render.

## Processo de validação

1. gerar o baseline da Sprint 73;
2. aplicar os índices em homologação;
3. repetir exatamente o mesmo laboratório;
4. comparar média e P95;
5. manter somente os índices com ganho comprovado.
