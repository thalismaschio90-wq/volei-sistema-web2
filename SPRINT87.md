# Sprint 87 — Scout Inteligente

Implementa análise determinística pós-partida baseada nos eventos persistidos.

## Entregas

- `analytics/scout_inteligente.py`: estatísticas por equipe, atleta e set; sequências; trocas de liderança; fundamentos e momentos decisivos.
- `routes/scout_inteligente.py`: página e JSON restritos ao Super ADM.
- `templates/admin_scout_inteligente.html`: dashboard responsivo.
- Testes unitários das principais regras.

## Limites

A qualidade da análise depende dos campos existentes nos eventos. Eventos legados sem equipe pontuadora, fundamento ou atleta são ignorados nas métricas correspondentes. Nenhuma IA externa é utilizada e nenhuma carga é adicionada à operação ao vivo.
