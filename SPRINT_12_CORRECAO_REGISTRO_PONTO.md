# Sprint 12 — Correção do registro de ponto

## Arquivos alterados
- `banco.py`
- `services/apontadores/pontos.py`
- `static/js/apontador/pontos-api.js`

## Alterações
- Removida preparação/validação de schema do caminho de registro de ponto.
- `SELECT * FROM partidas ... FOR UPDATE` substituído por colunas explícitas.
- Regras da competição passaram a ser consultadas apenas como fallback para partidas legadas sem configuração congelada.
- Adicionado `comando_id` por ponto no navegador.
- Adicionada idempotência no servidor para evitar ponto duplicado após timeout/reenvio.
- Eliminada emissão duplicada do placar privado do apontador.

## Compatibilidade
- A rota HTTP e o formato principal da resposta foram mantidos.
- Partidas antigas sem `sets_tipo` explícito continuam usando fallback da competição.
- Nenhum DDL foi adicionado.

## Validação
- Testes direcionados: 11 aprovados.
- Suíte completa: 381 aprovados.
- Falhas: 0.
