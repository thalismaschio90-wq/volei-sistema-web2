# Sprint 30 — Rota da Tabela sem import direto de banco.py

## Arquivos alterados

- `routes/tabela.py`
- `services/competicoes/tabela_gateway.py`

## Alterações

1. `routes/tabela.py` deixou de importar 47 símbolos diretamente de `banco.py`.
2. A rota agora usa um gateway de domínio em `services/competicoes/tabela_gateway.py`.
3. Grupos, partidas, quadras, agenda, rodadas, classificação e conexão usam serviços/repositórios já extraídos.
4. Imports que não eram utilizados pela rota foram removidos.
5. As assinaturas antigas de criação, atualização e exclusão de partidas foram preservadas pelo gateway.
6. Avanço e link público permanecem temporariamente encapsulados como adaptadores legados no gateway, preparando a próxima extração sem manter a rota acoplada ao arquivo gigante.

## Resultado arquitetural

Antes:

```text
routes/tabela.py
    ↓ 47 imports
banco.py
```

Depois:

```text
routes/tabela.py
    ↓
services/competicoes/tabela_gateway.py
    ↓
services / repositories / rules
```

## Validação

- compilação Python aprovada;
- imports diretos de `banco.py` na rota: 0;
- testes direcionados: 35 aprovados;
- suíte oficial completa: 400 aprovados;
- falhas: 0.
