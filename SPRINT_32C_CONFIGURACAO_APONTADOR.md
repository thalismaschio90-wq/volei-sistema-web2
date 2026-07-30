# Sprint 32C — Configuração operacional fora da rota

## Arquivos alterados

- `routes/apontadores.py`
- `services/apontadores/configuracao.py`

## Alterações

1. Cache curto da competição centralizado no novo serviço.
2. Resolução do modo simples/avançado removida da rota.
3. Regras específicas por jogo, série, fase e grupo preservadas.
4. Cálculo de `sets_max` e `sets_para_vencer` movido para o serviço.
5. Limites de tempos e substituições centralizados.
6. A rota deixou de importar diretamente `buscar_competicao_por_nome` e
   `buscar_configuracao_avancada_competicao` de `banco.py`.
7. Helpers privados antigos foram mantidos como wrappers compatíveis.

## Resultado arquitetural

```text
routes/apontadores.py
        ↓
services/apontadores/configuracao.py
        ↓
persistência temporária compatível
```

## Validação

- compilação Python aprovada;
- testes direcionados: 18 aprovados;
- suíte oficial completa: 400 aprovados;
- falhas: 0.
