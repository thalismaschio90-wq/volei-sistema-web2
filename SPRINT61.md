# Fase 3 — Sprint 61: transporte incremental do estado

## Entrega

- Novo evento Socket.IO `estado_partida_delta`.
- Patch recursivo por versão, com substituição segura de listas.
- Snapshot completo permanece no store para entrada/reconexão.
- Medição do tamanho do delta e do estado completo.
- Emissão do delta somente quando existe economia real.
- Chave de compatibilidade para manter ou desligar eventos antigos.

## Variáveis

```env
SOCKET_DELTA_ENABLED=1
SOCKET_DELTA_MIN_SAVING_PERCENT=10
SOCKET_LEGACY_STATE_EVENTS=1
```

`SOCKET_LEGACY_STATE_EVENTS` deve continuar em `1` na produção atual. Ele só
pode ser alterado para `0` depois que apontador, árbitros, telão, treinador e
visualizador estiverem homologados consumindo `estado_partida_delta`.

## Formato

```json
{
  "partida_id": "12",
  "payload_delta": true,
  "estado_versao_base": 41,
  "estado_versao": 42,
  "patch": {"pontos_a": 18},
  "chaves_removidas": [],
  "bytes_delta": 154,
  "bytes_estado": 10240
}
```

Se um cliente estiver em outra versão-base, deve ignorar o patch e solicitar
ou aguardar um snapshot completo.
