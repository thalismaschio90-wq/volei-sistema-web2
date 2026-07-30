# Sprint 15 — Sanções, retardamento e cartão verde

## Arquivos alterados
- `banco.py`
- `routes/apontadores.py`
- `rules/acoes_jogo.py`
- `static/js/apontador/sancoes-controller.js`

## Correções
- Removidas chamadas de preparação de schema no registro de sanção, retardamento e cartão verde.
- Corrigida a ordem dos parâmetros usados ao persistir sanções na sincronização e na finalização.
- Normalizados os aliases de interface: `jogador`/`jogadora` passam a `atleta`; `comissao` passa a `membro`.
- Preservados separadamente número e nome do alvo na persistência.
- Observações passam a ser preservadas na sincronização.
- Removidas consultas redundantes de tempos após reconstruir o estado disciplinar.
- Adicionada proteção contra clique duplo nos três modais.

## Validação
- Testes direcionados: 5 aprovados.
- Suíte completa: 381 aprovados.
- Falhas: 0.
