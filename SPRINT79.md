# Sprint 79 — Ensaio completo de campeonato

Esta sprint integra em um único ensaio:

- espectadores HTTP com entrada gradual;
- espectadores conectados por Socket.IO;
- árbitros, telão e visualizador;
- registro opcional de pontos em partida descartável;
- checagem periódica de `/readyz` durante a carga;
- snapshots antes/depois de performance, realtime e runtime;
- métricas de pool, PostgreSQL, delta, despacho e degradação;
- critérios configuráveis de aprovação.

## Execução segura, somente leitura

```powershell
$env:VTP_LOAD_BASE_URL="https://homologacao.exemplo.com"
$env:VTP_LOAD_COMPETICAO="Competição de homologação"
$env:VTP_LOAD_PARTIDA_ID="123"
$env:VTP_LOAD_PUBLIC_CODE="ABC123"
$env:VTP_LOAD_VIEWERS="30"
$env:VTP_LOAD_SOCKET_VIEWERS="10"
$env:VTP_LOAD_DURATION_SECONDS="120"
$env:VTP_LOAD_COLLECT_ADMIN_METRICS="0"
py scripts/executar_teste_carga.py
```

## Ensaio completo com métricas administrativas e pontos

Use somente uma partida descartável e um cookie de Super ADM/apontador de homologação.

```powershell
$env:VTP_LOAD_ALLOW_WRITES="1"
$env:VTP_LOAD_SESSION_COOKIE="session=..."
$env:VTP_LOAD_COLLECT_ADMIN_METRICS="1"
py scripts/executar_teste_carga.py
```

Os relatórios são gravados em `load_reports/` nos formatos JSON e Markdown.
