# Sprint 32A — Infraestrutura do apontador fora da rota

## Arquivos alterados

- `routes/apontadores.py`
- `services/apontadores/cache_runtime.py`
- `services/apontadores/operacao_local.py`
- `services/apontadores/avanco.py`

## Alterações

1. Os caches de painel, home e PIN foram centralizados em `cache_runtime.py`.
2. O snapshot auxiliar da operação local foi extraído para `operacao_local.py`.
3. A coordenação síncrona e assíncrona do avanço foi movida para `avanco.py`.
4. A rota deixou de manter os dicionários globais de cache e o conjunto global de avanços em execução.
5. O estado vivo continua exclusivamente na camada `realtime`.
6. O store de operação local mantém somente partida, papeletas e dados auxiliares, ignorando o argumento legado `estado`.
7. Os caches agora possuem lock, TTL e limite de tamanho centralizados.
8. A limpeza final da partida remove o snapshot pelo novo store.

## Redução da rota

- antes: 5.068 linhas;
- depois: 4.919 linhas;
- redução: 149 linhas.

## Compatibilidade

Os helpers privados antigos da rota continuam disponíveis como wrappers, preservando os fluxos existentes.

## Validação

- compilação Python aprovada;
- testes direcionados: 51 aprovados;
- suíte oficial completa: 400 aprovados;
- falhas: 0.
