# Sprint 35 — Auditoria completa de performance (estática)

## Escopo e limite da conclusão

A análise foi feita sobre `Volei_sistema_web - Copia(26).zip` e cobre Python, SQL embutido, rotas, templates e JavaScript.

Esta auditoria **ranqueia candidatos por evidência estática**. Ela não afirma que uma consulta é a mais lenta em produção sem `EXPLAIN (ANALYZE, BUFFERS)`, métricas do Neon e tráfego real.

## Base analisada

- 344 arquivos Python;
- 38 arquivos JavaScript;
- 105 templates HTML;
- 814 chamadas SQL estáticas identificadas;
- 48 chamadas SQL encontradas dentro de loops;
- 36 ocorrências de `SELECT *` em produção;
- `banco.py`: 15.635 linhas;
- `routes/apontadores.py`: 4.743 linhas;
- `jogo-apontador-main.js`: aproximadamente 143 KB.

## Diferença de versão importante

O ZIP analisado ainda não contém a Sprint 34. `pytest -q tests` para na coleta porque `evento_ponto_registrado` não existe no Game Engine desta cópia. Portanto, a auditoria de performance usa esta cópia como enviada, mas o gate verde de 403 testes pertence ao pacote posterior da Sprint 34.

# Conclusão executiva

Os maiores ganhos prováveis estão em quatro frentes:

1. eliminar funções (`REGEXP_REPLACE`, `LOWER`, `TRIM`) dos lados filtrados/joinados, usando colunas normalizadas e índices;
2. criar consultas leves e paginadas para partidas, oficiais, apontadores e demos;
3. substituir a assinatura global de classificação por invalidação/versionamento na escrita;
4. instrumentar produção/homologação para medir duração SQL, lock wait, cache hit e volume Socket antes de alterar mais regras.

# Top 20 consultas candidatas

O CSV anexo contém o ranking completo. As primeiras candidatas são:

1. `banco.py:listar_oficiais_competicao` — joins por CPF calculado com `REGEXP_REPLACE` e ordenação sem limite.
2. `repositories/partidas.py:listar_partidas` — `p.*`, agregação de eventos, múltiplos joins e ausência de paginação.
3. `routes/apontadores.py:listar_apontadores` — joins/filtros por CPF calculado e filtro com `OR`.
4. `banco.py:listar_competicoes_apontador` — `TRIM(LOWER())` no join e CPF calculado.
5. `repositories/equipes_perfil.py:atualizar_nome_equipe_persistencia` — vários updates sequenciais com normalização.
6. consultas de demo — `SELECT *`, CPF/WhatsApp calculados e listagem sem paginação.
7. `repositories/classificacao_cache.py:assinatura_classificacao_competicao` — `MD5(STRING_AGG(...))` sobre o campeonato.
8. `banco.py:registrar_ponto_partida` — `SELECT * FOR UPDATE` no caminho crítico.
9. `banco.py:salvar_partida_completa_final` — lock e carregamento amplo na finalização.
10. `banco.py:listar_papeleta` e listagens de atletas — `SELECT *` em contratos conhecidos.

# 10 gargalos de CPU e I/O

## 1. Listagem geral de partidas

`repositories/partidas.py:listar_partidas` combina `p.*`, escudos, quadra, vínculo de equipe e contagem de eventos em uma consulta. Ela deveria ter contratos distintos:

- lista leve para painéis/tabela;
- detalhe completo por partida;
- consulta pública sem dados operacionais;
- paginação por competição/rodada.

## 2–4. Normalização de CPF/nome no SQL

`REGEXP_REPLACE`, `LOWER` e `TRIM` dentro de joins/filtros reduzem a utilidade de índices comuns. Prioridade: persistir `cpf_normalizado` e, gradualmente, usar IDs/chaves relacionais em vez de nomes.

## 5. Assinatura da classificação

O `STRING_AGG` global é correto funcionalmente, mas cresce com o torneio. Um contador de versão atualizado nas escritas de partidas/grupos evita recalcular toda a assinatura a cada leitura.

## 6–7. Locks com `SELECT * FOR UPDATE`

Ponto e finalização devem selecionar somente os campos usados e registrar:

- duração da transação;
- tempo esperando lock;
- quantidade de linhas/eventos inseridos;
- payload de resposta.

## 8. Renomeação de equipe

É uma operação rara, mas toca várias tabelas e faz comparação normalizada repetida. O desenho ideal usa IDs estáveis; enquanto isso, execute apenas quando necessário e fora do horário de partidas.

## 9. Demo

Ainda existe duplicação entre `banco.py` e `routes/demo.py`, metadados de schema e listagens amplas.

## 10. Validação de schema

`require_schema` consulta `information_schema` por tabela/coluna. Deve ocorrer no startup e ser fortemente cacheada, nunca em ação rápida.

# Apontador — 10 prioridades

1. reduzir os 66 imports diretos de `banco.py` por fluxo;
2. trocar `SELECT * FOR UPDATE` do ponto por campos explícitos;
3. normalizar CPF de apontadores/oficiais em coluna indexável;
4. medir hit/miss e invalidação dos caches locais/Redis;
5. dividir `jogo-apontador-main.js` (3.251 linhas);
6. remover timer visual de 1,2 s quando atualização orientada a evento for suficiente;
7. autosave/heartbeat somente quando necessário;
8. pausar relógios/timers quando a aba estiver oculta;
9. quebrar `sincronizar_acao_view` por tipo de ação;
10. oferecer payloads leves por consumidor.

# Visualizador/telão — 10 prioridades

1. consolidar os dois fetches iniciais com o snapshot Socket/bootstrap;
2. usar consulta pública leve, sem `p.*` e sem dados internos;
3. medir e retirar eventos legados após homologação do delta;
4. compartilhar o cliente realtime entre telas de arbitragem/jogo avulso;
5. amostrar a telemetria de renderização;
6. não enviar heartbeat de aplicação para todo espectador anônimo em massa;
7. permitir ETag/cache para dados finalizados;
8. otimizar imagens e escudos;
9. limitar o DOM da evolução de pontos;
10. garantir que polling fallback desligue quando Socket estiver saudável.

# SuperAdmin/Organizador — 10 prioridades

1. versão paginada e leve de `listar_partidas`;
2. continuar desacoplamento de `routes/competicoes.py`;
3. criar serviço consolidado para `routes/painel.py`;
4. repository de oficiais com CPF normalizado;
5. componentizar `editar_competicao.html` (~109 KB);
6. paginar o painel do apontador/partidas;
7. paginar demos;
8. substituir assinatura global da classificação;
9. impor limites máximos em todas as listagens;
10. limpar o pacote de deploy (`.git`, caches e relatórios históricos).

# Medição recomendada antes de corrigir consultas

Ativar em homologação:

```text
SQL_PERFORMANCE_LOG_ENABLED=1
SQL_SLOW_QUERY_THRESHOLD_MS=100
PERFORMANCE_METRICS_ENABLED=1
```

Executar cenários reais e exportar:

- top fingerprints por duração total;
- top fingerprints por quantidade;
- consultas repetidas por request;
- planos de consultas lentas;
- latência HTTP p50/p95/p99;
- tempo de ponto, substituição, set e finalização;
- bytes e eventos Socket por ação;
- cache hit/miss.

# Ordem segura das próximas correções

## Sprint 35A — Partidas leves e paginação

Criar consultas separadas para lista, detalhe e público. É o ganho mais transversal.

## Sprint 35B — CPF normalizado

Adicionar/migrar `cpf_normalizado` para oficiais, acessos e vínculos; criar índices e trocar os joins calculados.

## Sprint 35C — Classificação por versão

Trocar `MD5(STRING_AGG)` por versão invalidada em escritas.

## Sprint 35D — Instrumentação e baseline

Rodar homologação e produzir comparação antes/depois. Só então escolher índices adicionais.

## Sprint 36 — Carga

Redis + dois workers, 1 e 4 partidas, 50/200 espectadores, reconexão, troca de set e reinício controlado.
