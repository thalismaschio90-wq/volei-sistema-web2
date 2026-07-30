# Sprint 24 — Tabela e agenda em modo somente-leitura

## Arquivos de produção alterados

- `repositories/competicoes_config.py`
- `routes/tabela.py`

## Alterações

1. A leitura da configuração da agenda não chama mais `criar_tabela_competicao_agenda_config()`.
2. O repositório apenas valida a estrutura criada pela migração `2026_07_28_015`.
3. A atualização da agenda também valida o schema, sem tentar executar DDL.
4. `_config_agenda_cache()` não chama mais `inicializar_configuracao_agenda_competicao()`.
5. Abrir Tabela, classificação ou visualizador não executa mais UPSERT apenas para inicializar valores padrão.
6. Os valores padrão continuam sendo retornados pela normalização quando não existir registro específico para a competição.

## Benefícios esperados

- nenhuma criação ou alteração de tabela ao abrir a Tabela;
- nenhuma escrita desnecessária na configuração da agenda;
- menor risco de locks durante partidas ao vivo;
- menor latência no Organizador, Tabela e Visualizador;
- cache da configuração continua funcionando;
- migrações permanecem como única fonte de alterações estruturais.

## Validação

- compilação Python aprovada;
- testes direcionados: 25 aprovados;
- suíte oficial: 400 aprovados;
- falhas: 0.
