# Fase 3 — Sprint 56: Origem das consultas lentas

A instrumentação SQL agora registra, somente quando uma consulta ultrapassa o limite de lentidão, o arquivo, a função e a linha que dispararam a operação.

Exemplo seguro:

```text
repositories/partidas.py:listar_partidas:184
```

Não são armazenados parâmetros, variáveis locais, CPF, e-mail, senha ou conteúdo do SQL. O painel `/admin/performance` passou a exibir as origens agregadas de cada fingerprint, tornando possível localizar a implementação responsável sem procurar manualmente em todo o projeto.

Variáveis existentes continuam válidas:

```env
PERFORMANCE_LOG_ENABLED=1
SQL_PERFORMANCE_LOG_ENABLED=1
SQL_SLOW_QUERY_THRESHOLD_MS=250
```

A inspeção da pilha é feita apenas para consultas que já ultrapassaram o limite, evitando custo adicional relevante nas consultas normais.
