# Sprint 36 — Laboratório de carga corrigido

## Arquivos

- `scripts/executar_teste_carga.py`
- `tests/load/config.py`
- `tests/load/scenario.py`
- `tests/load/http_client.py`
- `tests/load/metrics.py`
- `tests/load/report.py`
- `tests/load/snapshots.py`
- `tests/load/socket_client.py`
- `tests/load/__init__.py`

## Correções

- fecha sondas Socket mesmo quando a conexão inicial falha;
- reprova conexões Socket com falha;
- em teste com escrita, reprova receptores que não recebem eventos;
- registra marcadores de entrega ainda pendentes;
- remove marcadores quando o POST de ponto falha;
- inclui pré-validação e quantidade de sockets no relatório;
- mantém o modo somente leitura como padrão seguro.

## Execução mínima, somente leitura

```powershell
$env:VTP_LOAD_BASE_URL="https://SEU-ENDERECO"
$env:VTP_LOAD_COMPETICAO="COMPETICAO DE HOMOLOGACAO"
$env:VTP_LOAD_PARTIDA_ID="123"
$env:VTP_LOAD_PUBLIC_CODE="ABC123"
$env:VTP_LOAD_VIEWERS="50"
$env:VTP_LOAD_SOCKET_VIEWERS="10"
$env:VTP_LOAD_DURATION_SECONDS="120"
$env:VTP_LOAD_COLLECT_ADMIN_METRICS="0"
python scripts/executar_teste_carga.py
```

## Teste com escrita

Use somente uma partida descartável de homologação.

```powershell
$env:VTP_LOAD_ALLOW_WRITES="1"
$env:VTP_LOAD_SESSION_COOKIE="session=COOKIE_DA_HOMOLOGACAO"
python scripts/executar_teste_carga.py
```

Não execute escrita em partida real.
