# Fase 3 — Sprint 80: homologação final e prontidão de produção

Esta sprint encerra a Fase 3 com uma auditoria automatizada que reúne configuração, saúde do serviço e evidências do teste de carga.

## Arquivos

- `core/release_readiness.py`
- `scripts/validar_homologacao.py`
- `tests/test_release_readiness.py`

## Uso recomendado

```powershell
$env:VTP_RELEASE_BASE_URL="https://homologacao.exemplo.com"
$env:VTP_RELEASE_LOAD_REPORT="load_reports/campeonato.json"
py scripts/validar_homologacao.py
```

Para incluir `/admin/runtime-status`, informe o cookie apenas como variável temporária:

```powershell
$env:VTP_RELEASE_ADMIN_COOKIE="session=..."
```

O cookie não é salvo nos relatórios.

## Verificações bloqueantes

- configuração segura de workers e Redis;
- `DATABASE_URL` configurada;
- chave de sessão forte;
- debug desativado;
- `/healthz` e `/readyz`, quando uma URL é informada;
- ensaio de carga, quando o relatório é informado;
- eventos legados desligados somente com proteção de saúde dos deltas.

## Verificações com aviso

- Redis ainda não ativo quando o serviço usa somente um worker;
- relatórios ainda síncronos;
- métricas de performance desativadas;
- eventos legados ainda ativos durante homologação;
- ausência de URL ou relatório de carga.

O script não revela URLs do banco, tokens, cookies ou credenciais.
