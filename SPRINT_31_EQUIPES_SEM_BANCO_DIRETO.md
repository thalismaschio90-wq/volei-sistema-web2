# Sprint 31 — Rota de Equipes sem import direto de banco.py

## Arquivos alterados

- `routes/equipes.py`
- `services/equipes/route_gateway.py`

## Alterações

1. `routes/equipes.py` deixou de importar diretamente dezenas de funções de `banco.py`.
2. A rota agora usa `services/equipes/route_gateway.py`.
3. Operações já extraídas usam diretamente:
   - `repositories.conexao`;
   - `repositories.equipes_escrita`;
   - `services.competicoes.ciclo`;
   - `services.competicoes.partidas`.
4. Operações ainda não extraídas ficaram isoladas em wrappers temporários no gateway.
5. As assinaturas e retornos usados pelos formulários atuais foram preservados.
6. Cadastro, perfil, escudo e vínculo já continuam usando seus serviços específicos.

## Resultado arquitetural

Antes:

```text
routes/equipes.py
    ↓ imports diretos
banco.py
```

Depois:

```text
routes/equipes.py
    ↓
services/equipes/route_gateway.py
    ↓
services / repositories / compatibilidade isolada
```

## Validação

- compilação Python aprovada;
- imports diretos de `banco.py` na rota: 0;
- testes direcionados: 61 aprovados;
- suíte oficial completa: 400 aprovados;
- falhas: 0.
