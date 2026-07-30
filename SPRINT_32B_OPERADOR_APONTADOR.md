# Sprint 32B — Operador, heartbeat, PIN e travas fora da rota

## Arquivos alterados

- `routes/apontadores.py`
- `services/apontadores/operador.py`

## Alterações

1. A rota deixou de importar diretamente de `banco.py`:
   - `validar_operador_partida`;
   - `heartbeat_partida_operacional`;
   - `liberar_trava_partida_operacional`;
   - `assumir_partida_operacional`;
   - `abandonar_partida_operacional`;
   - `garantir_pin_operacional_apontador`;
   - `buscar_vinculo_operacional_por_pin`.
2. A resolução do login da sessão foi centralizada no novo serviço.
3. A validação do schema de oficiais foi movida para o serviço.
4. Os endpoints e mensagens HTTP foram preservados.
5. O cache de PIN da Sprint 32A continua sendo usado, recebendo agora o callback do novo serviço.
6. A persistência autoritativa continua temporariamente compatível por imports locais no serviço.

## Validação

- compilação Python aprovada;
- testes direcionados: 29 aprovados;
- suíte oficial completa: 400 aprovados;
- falhas: 0.
