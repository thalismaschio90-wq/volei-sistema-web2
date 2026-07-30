# Fase 2 — Sprint 11: exclusão segura de competições

## Objetivo

Retirar do `banco.py` a rotina extensa de exclusão completa de competições sem alterar o contrato público `excluir_competicao(nome)`.

## Nova divisão

- `repositories/competicoes_exclusao.py`: introspecção do esquema, limpeza ordenada e transação.
- `services/competicoes/exclusao.py`: ponto de entrada do domínio.
- `banco.py`: fachada compatível.

## Comportamento preservado

- cadastros permanentes de equipes e atletas de competições normais são preservados;
- atletas temporários de competição rápida são removidos;
- dados operacionais, partidas, eventos, scouts, papeletas, grupos, quadras, pins e vínculos são removidos;
- o organizador específico é removido somente depois da competição, evitando violação de chave estrangeira;
- superadministradores, apontadores, equipes e oficiais globais são preservados;
- toda a operação usa uma única transação, com rollback em caso de erro.

## Validação

- compilação completa dos módulos Python;
- 45 testes aprovados;
- teste de rejeição de nome vazio;
- preservação da assinatura pública.

Antes do deploy principal, validar a exclusão em uma cópia do banco ou ambiente de homologação, especialmente em competições rápida e normal.
