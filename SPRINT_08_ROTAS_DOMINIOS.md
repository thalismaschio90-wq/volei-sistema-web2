# Sprint 08 — Rotas ligadas diretamente aos serviços de domínio

## Arquivos de produção alterados
- `routes/competicoes.py`
- `routes/equipes.py`

## Alterações
- As operações de quadras usadas por `routes/competicoes.py` agora são importadas diretamente de `services.competicoes.quadras`.
- A leitura das rodadas em `routes/competicoes.py` agora vem diretamente de `services.competicoes.rodadas`.
- O salvamento das rodadas permanece temporariamente pela fachada `banco.py`, porque ela ainda injeta a validação de competição bloqueada.
- As consultas de grupos e equipes por grupo em `routes/equipes.py` agora vêm diretamente de `services.competicoes.grupos`.
- A leitura das rodadas em `routes/equipes.py` agora vem diretamente de `services.competicoes.rodadas`.

## Compatibilidade
- Nenhuma assinatura de função foi alterada.
- Nenhuma regra de competição, grupo, quadra ou rodada foi modificada.
- O objetivo é reduzir a dependência direta de `banco.py` de forma gradual.

## Validação
- Compilação Python: aprovada.
- Testes direcionados: 25 aprovados.
- Suíte completa: 371 aprovados e 2 falhas preexistentes fora destes arquivos:
  1. `static/js/apontador/socket-sync.js` sem o mecanismo esperado pelo teste de remoção de handlers antigos.
  2. `tests/test_organizador_painel.py` desatualizado em relação à consulta consolidada atual do painel.
