# Fase 2 — Sprint 6: criação e vínculo de equipes

## Objetivo

Retirar do `banco.py` as operações transacionais de criação de equipe, geração
de credenciais e vínculo à competição, preservando os contratos usados pelas
rotas atuais.

## Novos módulos

- `rules/equipes.py`: normalização e validação pura.
- `repositories/equipes_cadastro.py`: SQL e transações.
- `services/equipes/cadastro.py`: porta de entrada para migração das rotas.

## Compatibilidade

O final do `banco.py` mantém as funções públicas:

- `vincular_equipe_a_competicao`
- `vincular_equipe_existente_competicao`
- `criar_nova_equipe_com_credenciais`
- `criar_equipe_com_credenciais`

Elas apenas delegam ao novo repositório.

## Garantias preservadas

- Uma equipe existente mantém login e senha.
- O vínculo é reativado via `ON CONFLICT`.
- O isolamento por `cliente_id` é mantido.
- A criação de equipe, vínculo e usuário acontece numa única transação.
- A rota atual continua recebendo `ja_existia` e `ja_vinculada`.

## Limite desta sprint

Não foram alterados templates, Socket.IO, regras de jogo ou schema físico.
O teste integrado com PostgreSQL/Neon deve ocorrer em homologação antes do
deploy principal.
