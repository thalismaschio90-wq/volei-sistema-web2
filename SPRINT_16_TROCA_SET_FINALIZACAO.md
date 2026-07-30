# Sprint 16 — Troca de set e finalização

## Arquivo de produção alterado

- `banco.py`

## Correções

- `registrar_resultado_set` não executa mais DDL durante o jogo.
- O encerramento do set usa `SELECT` explícito com `FOR UPDATE`.
- Fim do set, incremento de sets e preparação do próximo set ficam na mesma transação.
- Removidas a segunda leitura da partida, o segundo `UPDATE` corretivo e a segunda sincronização da competição.
- Removido o avanço de chaveamento síncrono dentro do fechamento do set; o fluxo administrativo já agenda o avanço em segundo plano após a finalização.
- A finalização completa deixou de criar/verificar tabelas e colunas via DDL durante a requisição.
- `finalizar_partida_completa` agora valida o schema preparado pelas migrações e usa campos explícitos no lock inicial.
- `encerrar_partida` também deixou de usar `SELECT * FOR UPDATE`.

## Benefícios esperados

- troca de set mais rápida;
- menor tempo segurando lock da partida;
- menor chance de travar ao passar para a papeleta seguinte;
- finalização sem `ALTER TABLE`/`CREATE TABLE` durante a operação;
- menos conexões e consultas após o commit;
- menor risco de timeout ao encerrar a partida.

## Validação

- Testes direcionados: 15 aprovados.
- Suíte oficial: 384 aprovados.
- Falhas: 0.
