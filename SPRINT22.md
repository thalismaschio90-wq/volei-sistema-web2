# VolleyTablePro — Fase 2 / Sprint 22

## Objetivo
Separar de `routes/tabela.py` a preparação visual das partidas, centralizando status, fases, datas, quadras, escudos, parciais e ordenação.

## Novos módulos
- `rules/partidas_exibicao.py`: regras puras de apresentação e interpretação do estado da partida.
- `services/competicoes/partidas_exibicao.py`: prepara os registros completos usados por tabela, painéis e visualizador.
- `tests/test_partidas_exibicao.py`: testes de prioridade de finalização, estado ao vivo, datas, fases, escudos e ordenação.

## Compatibilidade
Os nomes internos históricos de `routes/tabela.py` foram preservados por aliases e uma fachada para `_preparar_partidas`. Nenhum endpoint, template, tabela ou contrato JSON foi alterado.

## Validação
- 91 testes aprovados.
- Compilação dos módulos alterados concluída.
- `routes/tabela.py` reduzido de aproximadamente 2.765 para 2.445 linhas.
