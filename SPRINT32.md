# Fase 2 — Sprint 32: Motor de substituições

## Objetivo
Centralizar as regras de substituição normal e excepcional, mantendo as rotas e assinaturas públicas existentes.

## Novos módulos
- `rules/substituicoes.py`: regras puras, duplas titular/reserva, retorno, limite e substituição excepcional.
- `services/apontadores/substituicoes.py`: validação de comandos e atualização atômica do estado vivo.
- `tests/test_rules_substituicoes.py`: testes das principais regras.

## Integrações alteradas
- `routes/apontadores.py`: usa o serviço para validar e aplicar a troca visual sob o lock da partida.
- `banco.py`: usa o mesmo motor puro antes de registrar evento e snapshot.

## Compatibilidade
- Endpoints mantidos.
- Assinaturas de `registrar_substituicao_partida` e `registrar_substituicao_excepcional_partida` mantidas.
- Payloads principais preservados.
- Nenhuma tabela foi alterada.

## Validação
- 147 testes aprovados.
- Compilação de todos os módulos Python concluída.
