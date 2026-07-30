# Sprint 35A — Listagem leve e paginada de partidas

## Arquivos alterados

- `repositories/partidas.py`
- `services/competicoes/partidas.py`
- `services/competicoes/tabela_gateway.py`
- `services/competicoes/tabela_acoes.py`
- `routes/bootstrap.py`
- `routes/apontadores.py`
- `routes/tabela.py`

## Alterações

1. Criada `listar_partidas_leve()` com campos explícitos, `LIMIT` e `OFFSET`.
2. A consulta leve não executa a agregação de eventos e não usa `p.*`.
3. O bootstrap do Organizador passa a usar a listagem leve.
4. O painel e o pacote offline do apontador passam a usar a listagem leve.
5. O endpoint público de partidas ao vivo usa a listagem leve sem joins de escudos.
6. A consulta completa `listar_partidas()` foi preservada para relatórios, classificação e telas detalhadas.
7. Criada `proxima_ordem_partida()`, usando `MAX(ordem) + 1` em vez de carregar todas as partidas.
8. A ação real de criação manual usa a nova consulta de próxima ordem; a injeção antiga foi preservada como fallback para compatibilidade e testes.

## Benefícios esperados

- menor payload nos painéis frequentes;
- menos joins e nenhuma contagem de eventos no bootstrap/apontador;
- paginação disponível para próximos consumidores;
- endpoint ao vivo mais barato;
- criação manual não transfere todas as partidas apenas para calcular a ordem;
- nenhuma alteração nas regras, relatórios ou consultas detalhadas.

## Validação

- compilação Python aprovada;
- testes direcionados: 24 aprovados;
- suíte oficial, com o gate da Sprint 34 aplicado à Cópia 26: 403 aprovados;
- falhas: 0.
