# Fase 2 — Sprint 5: persistência de equipes

## Objetivo
Retirar do `banco.py` operações de escrita do domínio de equipes, preservando os nomes e retornos usados pelas rotas antigas.

## Operações migradas
- atualização do quadro técnico;
- redefinição da senha da equipe;
- exclusão do vínculo da equipe com uma competição.

## Nova implementação
As consultas e transações foram movidas para `repositories/equipes_escrita.py`.
O `banco.py` mantém fachadas pequenas com as mesmas assinaturas públicas.

## Compatibilidade
Nenhuma rota, template, tabela ou regra de partida foi alterada nesta sprint.
A exclusão continua removendo apenas o vínculo com a competição atual e os atletas daquele vínculo; o cadastro global e as credenciais permanecem preservados.

## Validação executada
- compilação de todos os módulos Python;
- 16 testes automatizados aprovados;
- testes novos cobrindo atualização do quadro técnico, redefinição de senha e exclusão do vínculo.

## Limitação
Os testes utilizam banco simulado. Antes do deploy principal, validar em ambiente de homologação conectado a uma cópia do banco PostgreSQL.
