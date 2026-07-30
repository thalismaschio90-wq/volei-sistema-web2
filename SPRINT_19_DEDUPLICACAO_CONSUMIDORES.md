# Sprint 19 — Deduplicação dos consumidores em tempo real

## Problema
As telas modernas já aplicavam `estado_partida_delta`, mas também processavam em seguida snapshots legados da mesma versão, provocando renderizações repetidas, maior uso de CPU e risco de efeitos visuais duplicados.

## Correção
- Árbitro principal, segundo árbitro e árbitro único ignoram snapshots legados quando a versão já foi aplicada.
- O placar profissional faz a mesma deduplicação.
- O visualizador público da partida ignora eventos legados cuja versão já chegou via delta.
- Payloads sem versão continuam aceitos para compatibilidade.
- Correções autorizadas e regressões explícitas continuam permitidas.

## Arquivos de produção
- `templates/primeiro_arbitro.html`
- `templates/segundo_arbitro.html`
- `templates/arbitro_unico.html`
- `templates/placar_profissional.html`
- `templates/visualizador_partida_publica.html`

## Validação
- Testes direcionados: 14 aprovados.
- Suíte oficial: 400 aprovados.
- Falhas: 0.
