# Sprint 32D — Respostas HTTP e publicação fora da rota

## Arquivos alterados

- `routes/apontadores.py`
- `services/apontadores/responses.py`
- `services/apontadores/publicacao.py`

## Alterações

1. As respostas JSON sem cache foram centralizadas em
   `services/apontadores/responses.py`.
2. O helper privado `_json_no_cache` foi preservado como wrapper, mantendo todos
   os endpoints e testes compatíveis.
3. Atualização do estado vivo e publicação para Socket.IO/placar do apontador
   foram centralizadas em `services/apontadores/publicacao.py`.
4. Foram removidos blocos duplicados de `try/except` em:
   - publicação geral do estado;
   - saída rápida da papeleta;
   - início do jogo local;
   - inversão de lados sem competição na sessão.
5. Falha de Socket.IO continua não desfazendo ações já persistidas.
6. A rota continua responsável por preparar o estado e decidir o fluxo; o novo
   serviço cuida somente da entrega/publicação.

## Resultado arquitetural

Antes:

```text
routes/apontadores.py
  ├── cria respostas Flask
  ├── atualiza cache realtime
  ├── publica estado Socket.IO
  └── publica placar privado
```

Depois:

```text
routes/apontadores.py
        ↓
services/apontadores/responses.py
services/apontadores/publicacao.py
        ↓
realtime / Socket.IO
```

## Validação

- compilação Python aprovada;
- testes direcionados: 21 aprovados;
- suíte oficial completa: 400 aprovados;
- falhas: 0.
