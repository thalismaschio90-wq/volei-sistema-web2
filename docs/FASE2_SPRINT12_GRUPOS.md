# Fase 2 — Sprint 12: grupos da competição

As consultas e gravações de grupos foram extraídas de `banco.py` para:

- `rules/grupos.py`
- `repositories/grupos.py`
- `services/competicoes/grupos.py`

O arquivo legado mantém fachadas com as mesmas assinaturas. A trava da fase de
grupos continua sendo decidida pelo domínio atual e é passada ao novo serviço,
evitando importação circular.

Melhorias adicionais:

- exclusão do grupo agora usa `WHERE id = ... AND competicao = ...`;
- remoção de equipe não abre uma segunda conexão dentro da transação;
- validação de existência do grupo antes de criar vínculo;
- nomes são normalizados antes da persistência;
- listagens vazias retornam estruturas previsíveis.
