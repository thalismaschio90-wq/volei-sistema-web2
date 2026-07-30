# RC1 — Fechamento da Etapa 1: Integridade do Código

Data: 29/07/2026

## Alterações deste bloco

- Repositórios de partidas, grupos, rodadas e quadras deixaram de executar DDL quando chamados no funcionamento normal.
- As funções estruturais agora usam `require_schema` sem `force=True` e falham com orientação explícita quando a migração está ausente.
- DDL permanece disponível apenas para o executor versionado de migrações por meio de `force=True` e da trava central.
- Adicionadas migrações versionadas para o cadastro básico de partidas e para grupos/vínculos de equipes.
- Fachadas de compatibilidade de `banco.py` e serviços propagam o parâmetro `force`.
- Criados testes de regressão para impedir retorno de DDL automático nesses repositórios.

## Validações

- `python -m compileall -q .`: aprovado.
- `pytest -q`: 365 testes aprovados.
- `requirements.txt`: revisão estática aprovada, sem entradas duplicadas.
- O `pip check` global apontou conflito externo do ambiente com `moviepy`/`Pillow`; `moviepy` não faz parte do `requirements.txt` do projeto.

## Regra operacional

Antes de iniciar o servidor em um banco novo ou após atualização estrutural, execute:

```bash
python scripts/executar_migracoes.py
```

O aplicativo não deve tentar corrigir o schema durante requisições HTTP, eventos Socket.IO ou carregamento de painéis.
