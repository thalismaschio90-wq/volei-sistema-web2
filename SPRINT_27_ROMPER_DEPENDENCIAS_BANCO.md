# Sprint 27 — Remoção de dependências simples de banco.py

## Arquivos alterados

- `banco.py`
- `repositories/classificacao_cache.py`
- `repositories/visualizador_publico.py`
- `repositories/replay_partida.py`
- `repositories/partidas.py`
- `core/schema_inspection.py`
- `rules/classificacao.py`

## Alterações

1. O cache de classificação foi extraído de `banco.py` para
   `repositories/classificacao_cache.py`.
2. `banco.py` preserva os nomes antigos apenas como fachada de compatibilidade.
3. `rules/classificacao.py` não importa mais diretamente de `banco.py`.
4. `repositories/visualizador_publico.py` usa `repositories.conexao` e valida
   o schema de destaques sem tentar criar tabela durante a leitura.
5. `repositories/replay_partida.py` usa diretamente `repositories.conexao`.
6. `repositories/partidas.py` deixou de importar `_buscar_colunas_tabela` de
   `banco.py`.
7. Foi criado `core/schema_inspection.py` como compatibilidade temporária para
   inspeção de colunas, com cache local.

## Resultado arquitetural

Foram removidos os ciclos simples:

```text
visualizador_publico → banco.py
replay_partida → banco.py
partidas → banco.py
rules/classificacao → banco.py
```

O fluxo passa a ser:

```text
rules/repositories
        ↓
repositories.conexao
        ↓
PostgreSQL
```

## Validação

- compilação Python aprovada;
- testes direcionados: 21 aprovados;
- suíte oficial completa: 400 aprovados;
- falhas: 0.
