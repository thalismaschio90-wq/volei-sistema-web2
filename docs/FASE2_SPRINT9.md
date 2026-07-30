# Fase 2 — Sprint 9: configurações básicas de competições

Esta sprint separa do `banco.py` as operações de atualização dos dados gerais,
estrutura, regras de jogo e pontuação/desempate da competição.

## Novos módulos

- `rules/competicoes_basico.py`: normalização pura e atualização parcial.
- `repositories/competicoes_basico.py`: SQL e transações.
- `services/competicoes/basico.py`: interface de serviço.

## Compatibilidade

As funções públicas no `banco.py` foram mantidas com os mesmos nomes e
assinaturas. As versões finais delegam aos novos repositórios.

## Segurança preservada

- competição travada continua bloqueando alterações;
- atualizações de estrutura e regras continuam parciais;
- renomeação continua atualizando vínculos em `usuarios` e `equipes` na mesma
  transação;
- campos inexistentes no schema são ignorados como anteriormente.

## Validação

- 41 testes aprovados;
- compilação Python concluída;
- validação AST dos arquivos alterados.
