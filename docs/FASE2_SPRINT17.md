# Fase 2 — Sprint 17

## Geração e persistência da agenda classificatória

Nesta sprint a rota `routes/tabela.py` deixou de controlar diretamente duas responsabilidades pesadas:

- montagem dos pools de quadras e da agenda classificatória;
- inserção em lote das partidas no PostgreSQL.

Novos módulos:

- `services/competicoes/geracao_partidas.py`
- função `inserir_partidas_em_lote` em `repositories/partidas.py`

A rota mantém fachadas temporárias para preservar o comportamento e os nomes internos atuais.

## Ganhos

- uma única transação e um único `executemany` para a agenda completa;
- regras de quadras fixas e compartilhadas fora da rota;
- geração testável sem Flask/request;
- menor risco de consultas e commits por partida;
- preparação para mover toda a geração automática para um worker no futuro.

## Validação

- compilação completa dos módulos Python;
- 71 testes aprovados;
- testes específicos para quadras fixas, compartilhadas e pools.

A escrita ainda deve ser validada em homologação com PostgreSQL/Neon antes do deploy principal.
