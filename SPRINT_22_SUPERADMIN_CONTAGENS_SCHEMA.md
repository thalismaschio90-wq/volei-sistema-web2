# Sprint 22 — SuperAdmin: contagens consolidadas e schema fora da requisição

## Arquivo alterado

- `banco.py`

## Alterações

1. As funções `contar_competicoes`, `contar_equipes` e `contar_partidas` agora
   compartilham `_resumo_contagens_superadmin`.
2. A primeira chamada executa uma única consulta com três subconsultas.
3. As chamadas seguintes, durante a mesma requisição, usam `core.request_cache`.
4. O filtro multiempresa continua respeitando `cliente_id` e o acesso master.
5. `listar_superadmins_clientes` deixou de chamar
   `garantir_schema_multiempresa_superadmin`, que executava DDL e backfills.
6. A listagem agora apenas valida o schema com `require_schema`.

## Resultado esperado

- três contadores: de três conexões/consultas para uma;
- menos buscas repetidas do contexto do SuperAdmin;
- menor latência no dashboard;
- nenhuma migração pesada ao abrir a listagem de SuperADMs;
- menor risco de locks no banco durante navegação administrativa.

## Validação

- compilação Python aprovada com `py_compile`;
- assinaturas públicas preservadas.
