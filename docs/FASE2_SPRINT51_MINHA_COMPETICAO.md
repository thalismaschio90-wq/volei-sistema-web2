# Fase 2 — Sprint 51: Minha Competição e navegação da equipe

## Objetivo
Reduzir trabalho duplicado nas telas `Minha Equipe` e `Minhas Partidas`, retirando montagem de contexto da rota e reutilizando o cache curto de avisos já existente.

## Alterações
- criado `services/equipes/minha_competicao.py`;
- `minha_equipe()` deixou de consultar notificações e contagem por caminhos diferentes;
- avisos e solicitações agora vêm de `_avisos_equipe_cache()` em uma única chamada;
- após editar quadro técnico, os caches relacionados são invalidados;
- o contexto de `minhas_partidas.html` passou a ser montado pelo serviço;
- criado resumo reutilizável de documentação/conferência de atletas para próximas etapas.

## Compatibilidade
Nenhum endpoint, template, tabela ou nome de campo foi alterado.

## Validação
- 226 testes aprovados;
- compilação de `routes/equipes.py` e do novo serviço concluída.
