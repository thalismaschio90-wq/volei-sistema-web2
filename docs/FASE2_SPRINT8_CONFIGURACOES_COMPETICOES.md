# Fase 2 — Sprint 8: configurações das competições

## Objetivo

Retirar do `banco.py` as regras e o SQL das configurações avançadas e da agenda automática, mantendo os nomes públicos antigos para compatibilidade.

## Nova divisão

- `rules/competicoes_config.py`: padrões, normalização de JSON, limites e valores permitidos.
- `repositories/competicoes_config.py`: leitura e persistência no PostgreSQL.
- `services/competicoes/configuracao.py`: interface do domínio para rotas e demais serviços.
- `banco.py`: fachadas temporárias com as mesmas assinaturas antigas.

## Funções migradas

- `buscar_configuracao_avancada_competicao`
- `atualizar_configuracao_avancada_competicao`
- `inicializar_configuracao_avancada_competicao`
- `buscar_configuracao_agenda_competicao`
- `atualizar_configuracao_agenda_competicao`
- `inicializar_configuracao_agenda_competicao`

## Compatibilidade

As rotas atuais continuam podendo importar as funções acima de `banco.py`. Nenhuma tabela ou contrato público foi renomeado.

## Validação

- Compilação Python dos arquivos alterados.
- 36 testes automatizados aprovados.
- Testes das regras de normalização e das fachadas legadas.

A validação com o PostgreSQL/Neon deve ser feita primeiro em homologação.
