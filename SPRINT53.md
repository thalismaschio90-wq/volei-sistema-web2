# Fase 2 — Sprint 53: auditoria final de arquitetura

## Objetivo

Revisar a integridade da modularização, impedir DDL no fluxo normal das rotas e eliminar o erro de importação que impedia a inicialização local.

## Correções aplicadas

### 1. Pacote `services`

O arquivo raiz `services/__init__.py` importava `services.dados`, módulo inexistente. O pacote raiz agora não importa implementações de domínio automaticamente. Os serviços continuam sendo importados por caminhos explícitos, como `services.atletas.dados`.

### 2. DDL centralizado no startup

Foi criado `repositories/runtime_schema.py`. As estruturas auxiliares abaixo são garantidas uma vez na inicialização:

- `demos_temporarias`;
- `configuracoes_sistema`;
- `apontador_eventos_sincronizados`;
- campos `exigir_foto_atleta` e `exigir_instagram_atleta`.

As rotas não executam mais `CREATE TABLE` ou `ALTER TABLE` durante a navegação, salvamento de configurações ou sincronização de eventos.

### 3. Compatibilidade legada

As funções antigas permanecem disponíveis, mas encaminham para o inicializador centralizado. O inicializador possui trava e marcador de conclusão, evitando repetir DDL no mesmo processo.

### 4. Regex

O literal JavaScript com `\d` em `routes/competicoes.py` foi escapado corretamente no texto Python. A compilação com `SyntaxWarning` tratado como erro passou sem avisos.

## Auditoria executada

- compilação de todos os módulos Python;
- compilação tratando `SyntaxWarning` como erro;
- verificação AST de imports locais;
- busca de DDL em `routes/`, `app.py` e `socket_events.py`;
- execução da suíte de testes.

## Resultado

- 230 testes aprovados;
- 0 erros de sintaxe;
- 0 imports locais apontando para módulos inexistentes;
- 0 comandos DDL executados diretamente pelas rotas;
- erro `ModuleNotFoundError: No module named 'services.dados'` corrigido.

## Itens mapeados para a Fase 3

A auditoria ainda encontrou pontos que exigem medição antes de alteração:

- 52 ocorrências de `SELECT *` no código legado e módulos atuais;
- 419 chamadas textuais a `conectar(...)` em banco, rotas, repositórios e serviços;
- possíveis padrões N+1 que devem ser confirmados com instrumentação e `EXPLAIN ANALYZE`;
- índices do PostgreSQL devem ser definidos com base nas consultas reais e nos planos de execução.

Esses números não significam, isoladamente, 52 ou 419 gargalos. Eles formam a lista de investigação da Fase 3, que deve priorizar rotas medidas como lentas.
