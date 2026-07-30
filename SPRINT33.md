# Fase 2 — Sprint 33

## Tempos e ações disciplinares

Esta sprint separa de `routes/apontadores.py` as regras e a atualização visual imediata de:

- pedidos de tempo;
- cronômetro inicial do tempo;
- retardamentos;
- sanções;
- cartões verdes.

## Novos módulos

- `rules/acoes_jogo.py`
- `services/apontadores/acoes_jogo.py`
- `tests/test_rules_acoes_jogo.py`

## Melhorias principais

- o limite de tempos configurado pelo organizador é validado antes de alterar o estado local;
- a mensagem quando não há mais pedidos de tempo é única e explícita;
- o cronômetro ativo passa a integrar o estado como `tempo_ativo`;
- equipe, alvo e tipo de sanção são validados em um único módulo;
- retardamentos, sanções e cartões verdes deixam de manipular listas diretamente na rota;
- uma advertência local não altera o placar;
- as rotas retornam erro 400 para comandos inválidos, sem tratar erro de regra como falha interna 500.

## Compatibilidade

Não foram alterados endpoints, templates, tabelas ou formatos principais dos payloads existentes.
A persistência definitiva continua no fluxo já existente de sincronização/encerramento.

## Validação

- 152 testes aprovados;
- compilação dos módulos alterados concluída.
