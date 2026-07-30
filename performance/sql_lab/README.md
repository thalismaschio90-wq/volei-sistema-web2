# Laboratório SQL do VolleyTablePro

Este diretório documenta a execução controlada do baseline e do candidato.
Nenhum índice é aplicado automaticamente.

## Variáveis obrigatórias

```powershell
$env:VTP_LAB_COMPETICAO="Competição de homologação"
$env:VTP_LAB_PARTIDA_ID="123"
```

Use somente dados descartáveis de homologação.

## Execução

```powershell
py scripts/executar_laboratorio_sql.py scripts/laboratorio_sql_critico.json --saida sql_lab_reports/baseline_candidato
```

O cenário candidato deve ser executado após a reescrita ou índice candidato,
mantendo o mesmo banco, a mesma competição e a mesma partida.
