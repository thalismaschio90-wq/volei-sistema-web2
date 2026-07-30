# Fase 2 — Sprint 24

Separação das ações administrativas da tabela.

## Novo módulo

- `services/competicoes/tabela_acoes.py`

## Operações centralizadas

- vínculo entre grupo e quadra;
- inclusão e remoção de equipe em grupo;
- exclusão de grupo;
- limpeza completa da tabela;
- limpeza por fase;
- criação manual de partida;
- edição manual de partida;
- exclusão de partida.

As rotas continuam responsáveis somente por sessão, formulário, mensagens e redirecionamento.
Nenhum endpoint, template ou formato de formulário foi alterado.

## Validação

- 100 testes aprovados;
- compilação Python completa;
- novos testes das ações administrativas.
