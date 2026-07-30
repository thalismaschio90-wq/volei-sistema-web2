# Fase 2 — Sprint 13: Quadras da competição

Migração do domínio de quadras para camadas próprias, mantendo as funções públicas de `banco.py` como fachadas de compatibilidade.

## Novos módulos

- `rules/quadras.py`: normalização de PIN, nomes, exibição e formulário de quadras.
- `repositories/quadras.py`: estrutura SQL, consultas, gravações, PINs e vínculos.
- `services/competicoes/quadras.py`: interface do domínio.
- `tests/test_rules_quadras.py`: testes das regras puras.

## Operações migradas

- criação/garantia da tabela de quadras;
- listagem, busca, cadastro, edição e ativação;
- geração e consulta de PIN de arbitragem;
- vínculo de quadra com grupo;
- aplicação de quadra em partida;
- normalização de registros antigos de grupos e partidas.

## Compatibilidade

As funções antigas permanecem disponíveis em `banco.py`, com as mesmas assinaturas.

## Validação

- compilação Python concluída;
- 53 testes aprovados;
- não houve conexão com o banco Neon de produção.
