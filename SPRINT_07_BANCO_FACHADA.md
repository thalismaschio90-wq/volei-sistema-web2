# Sprint 07 — banco.py como fachada

## Arquivo de produção alterado
- `banco.py`

## Alterações
1. Removida uma implementação antiga e duplicada de rodadas programadas.
   - Permanecem somente as fachadas finais que delegam para `services.competicoes.rodadas`.
   - Evita manutenção duplicada, `SELECT *` antigo e DDL duplicado.

2. `garantir_schema_codigo_publico_competicoes()` não executa mais `ALTER TABLE` nem `CREATE INDEX` durante requisições normais.
   - Em runtime, apenas valida o schema com `core.schema_requirements.require_schema`.
   - O DDL permanece disponível somente com `force=True`, reservado ao executor explícito de migrações.

## Compatibilidade
- Assinaturas públicas preservadas.
- Rotas que importam funções de rodadas pelo `banco.py` continuam funcionando.
- Geração e busca de código público preservadas, desde que as migrações tenham sido executadas antes do Gunicorn.

## Validação
- `python -m py_compile banco.py`: aprovado.
- Testes específicos de schema, migrações, grupos, quadras e rodadas: 35 aprovados.

## Observação sobre a suíte completa
A suíte geral da cópia-base apresentou duas falhas preexistentes fora desta sprint:
- teste do `socket-sync.js` esperando `REGISTROS_APONTADOR`;
- teste antigo do painel do organizador incompatível com a consulta consolidada atual.
Nenhuma dessas falhas envolve o `banco.py` alterado nesta sprint.
