# Sprint 23 — Competições sem UPDATE global ao listar

## Arquivo de produção alterado

- `repositories/competicoes_ciclo.py`

## Alterações

1. `listar_competicoes_persistencia()` não chama mais
   `sincronizar_status_competicoes_persistencia()` antes da leitura.
2. O status da competição é calculado na própria consulta com base nas partidas.
3. A listagem do Organizador e a busca da competição principal usam o mesmo
   status calculado, sem gravar no banco.
4. Abrir telas de competições deixa de executar um UPDATE global.
5. `competicao_esta_travada_persistencia()` não consulta mais
   `information_schema`.
6. `travar_competicao_persistencia()` e
   `destravar_competicao_persistencia()` não executam mais DDL nem verificações
   estruturais durante o uso normal.

## Benefícios esperados

- nenhuma escrita global ao abrir listas de competições;
- menor risco de lock entre telas administrativas e partidas ao vivo;
- menos conexões e processamento no PostgreSQL;
- status sempre derivado do estado real das partidas;
- primeiro ponto não tenta criar colunas de travamento;
- menor latência nos painéis SuperAdmin e Organizador.

## Validação

- compilação Python aprovada;
- testes direcionados: 3 aprovados;
- suíte oficial: 400 aprovados;
- falhas: 0.
