# Sprint 06 — Consultas de atletas com colunas explícitas

## Arquivos de produção alterados

- `repositories/atletas.py`
- `routes/equipes.py`

## Alterações

- Removido `SELECT *` das consultas de atletas por equipe.
- Removido `SELECT *` da consulta que carrega e agrupa atletas de uma competição.
- Mantidos explicitamente todos os campos usados pelas telas e regras atuais:
  `id`, `nome`, `cpf`, `data_nascimento`, `numero`, `equipe`, `competicao`,
  `status`, `equipe_login`, `equipe_id`, `foto_atleta`, `instagram`,
  `temporario`, `capitao_padrao` e `libero`.
- A resposta deixa de crescer automaticamente caso novas colunas pesadas sejam
  adicionadas futuramente à tabela `atletas`.

## Compatibilidade

Nenhuma regra, rota ou formato dos registros retornados foi alterado.

## Testes

- 381 testes aprovados.
- 0 falhas.
