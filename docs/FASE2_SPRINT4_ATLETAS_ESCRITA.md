# Fase 2 — Sprint 4: persistência de atletas

## Objetivo

Retirar do arquivo central `banco.py` as operações SQL de cadastro, edição e
exclusão de atletas, preservando integralmente as assinaturas públicas usadas
pelas rotas atuais.

## Alterações

- Criado `repositories/atletas_escrita.py`.
- Migradas as implementações de:
  - `cadastrar_atleta`;
  - `atualizar_atleta_equipe`;
  - `excluir_atleta`.
- `banco.py` mantém fachadas com os mesmos nomes e parâmetros.
- As regras puras continuam em `rules/atletas.py`.
- A normalização continua em `services/atletas/dados.py`.

## Compatibilidade

As chamadas antigas continuam válidas:

```python
from banco import cadastrar_atleta, atualizar_atleta_equipe, excluir_atleta
```

Nenhuma rota, template, tabela ou contrato de retorno foi alterado.

## Segurança da migração

As dependências legadas ainda necessárias — CPF, garantias de esquema e
sincronização do cadastro global — são injetadas pela fachada. Isso evita
importação circular entre o repositório e `banco.py` durante a transição.

## Próximo passo

Migrar as operações de escrita das equipes e seus vínculos com competições,
antes de iniciar a separação das rotas de apontadores.
