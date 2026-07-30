# Fase 2 — Sprint 50: Painel da equipe e modo treinador

## Objetivo

Reduzir consultas repetidas ao abrir o painel da equipe, a tela de atletas e as abas do modo treinador.

## Alterações

- Criado `services/equipes/painel.py` para calcular métricas, status e próxima partida sem acesso ao banco ou ao Flask.
- Adicionado cache curto de avisos, solicitações e contagem de notificações da equipe.
- O painel inicial agora envia corretamente avisos e solicitações ao template usando o mesmo resultado armazenado.
- A tela de atletas reutiliza o mesmo cache de avisos, evitando três consultas repetidas ao alternar entre lista e cadastro.
- O modo treinador reutiliza os atletas já carregados por `montar_contexto_treinador()` nas abas que exigem banco, evitando consultar o mesmo elenco duas vezes na mesma requisição.
- O resultado reutilizado também alimenta o cache das próximas trocas de aba.

## Compatibilidade

Não foram alterados endpoints, templates, tabelas, campos de formulário, regras de jogo ou eventos Socket.IO.

## Validação

- 223 testes aprovados.
- Compilação completa dos módulos Python concluída.
