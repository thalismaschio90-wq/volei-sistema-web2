# Auditoria RC1 — Migrações fora dos workers

## Correção

O `app.py` não executa mais `CREATE TABLE`, `ALTER TABLE` ou rotinas de garantia de schema durante o import. Assim, cada worker do Gunicorn deixa de disputar locks e de repetir migrações ao iniciar.

## Novo fluxo no Render

```text
python scripts/iniciar_servidor.py
  → executa scripts/executar_migracoes.py
  → obtém advisory lock global no PostgreSQL
  → aplica somente etapas ainda não registradas
  → inicia o Gunicorn
```

A tabela `vtp_schema_migrations` registra as etapas concluídas. O advisory lock impede dois deploys de alterarem o schema simultaneamente.

## Uso local

Antes de iniciar uma base nova ou após receber uma versão com mudanças de schema:

```powershell
py scripts/executar_migracoes.py
py app.py
```

Para apenas listar as etapas:

```powershell
py scripts/executar_migracoes.py --dry-run
```

## Variável

```env
DB_MIGRATIONS_ON_START=1
```

No Render, mantenha `1`. Em ambientes onde a migração é executada por uma etapa externa de deploy, pode ser alterada para `0`.
