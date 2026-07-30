# Fase 2 — Sprint 3: regras de atletas

## Objetivo

Retirar do `banco.py` a normalização e as regras puras repetidas no cadastro e na edição de atletas, sem alterar os nomes públicos usados pelas rotas.

## Arquivos novos

- `rules/atletas.py`
- `services/atletas/dados.py`
- `services/atletas/__init__.py`
- `tests/test_rules_atletas.py`

## Arquivo alterado

- `banco.py`

## Regras centralizadas

- limpeza dos campos de atleta;
- normalização do Instagram;
- conversão e validação básica da numeração;
- campos obrigatórios de cadastro e edição;
- exigência de foto e Instagram configurada pelo organizador;
- mensagens padronizadas para pendências.

## Compatibilidade

As funções abaixo continuam disponíveis no `banco.py` com a mesma assinatura:

- `cadastrar_atleta`;
- `atualizar_atleta_equipe`.

As consultas SQL e o comportamento de persistência não foram alterados nesta sprint.

## Validação

- compilação dos módulos Python;
- 10 testes automatizados aprovados;
- nenhuma alteração de rota, template, Socket.IO ou estrutura do banco.

## Próxima etapa recomendada

Migrar a persistência de atletas para um repositório de escrita, deixando o `banco.py` apenas como fachada de compatibilidade. Depois, aplicar o mesmo padrão às operações de equipes.
