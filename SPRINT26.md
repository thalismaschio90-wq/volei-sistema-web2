# Fase 2 — Sprint 26

## Painel e seleção de partidas do apontador

Esta sprint inicia a modularização de `routes/apontadores.py` sem alterar endpoints, templates ou contratos públicos.

### Novos módulos

- `rules/apontador_painel.py`
  - normalização de fases;
  - resolução do modo simples/avançado;
  - interpretação das regras por fase, grupo, série e jogo do avanço;
  - resumo M3/M5/SU;
  - redução segura do payload das partidas;
  - agrupamento das rodadas para exibição.

- `services/apontadores/painel.py`
  - resolução segura do CPF da sessão;
  - montagem do contexto da tela inicial;
  - preparação e ordenação das partidas do painel;
  - cálculo do maior formato disponível no lançamento manual.

### Resultado

`routes/apontadores.py` foi reduzido de aproximadamente 6.033 para 5.608 linhas.

A rota continua responsável apenas por sessão, cache, mensagens, consultas e renderização. As regras dos cards e da seleção de partidas não ficam mais misturadas com a operação ao vivo.

### Compatibilidade

- endpoints preservados;
- templates preservados;
- campos enviados ao HTML preservados;
- cache existente preservado;
- banco e Socket.IO não alterados;
- regras de jogo ao vivo não alteradas.

### Validação

- 111 testes aprovados;
- compilação de todos os módulos Python;
- testes específicos de CPF, contexto inicial, modo avançado, ordenação, rodadas e payload leve.
