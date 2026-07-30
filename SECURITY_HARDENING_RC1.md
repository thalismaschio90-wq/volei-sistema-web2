# VolleyTablePro — Segurança RC1

## Correções aplicadas

- Senhas novas são armazenadas com PBKDF2-SHA256 e salt aleatório.
- Credenciais antigas continuam funcionando e são migradas automaticamente após login bem-sucedido.
- Foi incluído um script controlado para migrar todas as credenciais antigas de uma vez.
- `SECRET_KEY` fraca ou ausente bloqueia a inicialização em produção.
- Cookies de sessão usam `HttpOnly`, `SameSite=Lax` e `Secure` no Render.
- Requisições mutáveis possuem proteção CSRF global.
- Formulários, `fetch`, `XMLHttpRequest` e `sendBeacon` recebem o token automaticamente.
- CORS do Socket.IO deixou de aceitar qualquer origem.
- Uploads passam a ter limite global padrão de 8 MB.
- Senhas persistidas não são mais exibidas na listagem das equipes.

## Variáveis obrigatórias/recomendadas no Render

```env
SECRET_KEY=<chave aleatória com pelo menos 32 caracteres>
SESSION_COOKIE_SECURE=1
SESSION_COOKIE_SAMESITE=Lax
CSRF_ENABLED=1
MAX_UPLOAD_BYTES=8388608
SOCKETIO_ALLOWED_ORIGINS=https://volleytablepro.com.br,https://www.volleytablepro.com.br
```

## Migração das senhas existentes

Primeiro simule:

```powershell
py scripts/migrar_senhas_hash.py
```

Depois, em janela controlada:

```powershell
$env:PASSWORD_MIGRATION_ALLOWED="1"
py scripts/migrar_senhas_hash.py --apply
```

O login também migra automaticamente uma senha antiga quando o usuário entra com sucesso.
