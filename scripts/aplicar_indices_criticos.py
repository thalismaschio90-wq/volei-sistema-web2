"""Aplica índices críticos do VolleyTablePro de forma explícita e segura.

Uso padrão (não altera o banco):
    py scripts/aplicar_indices_criticos.py

Aplicação em homologação:
    py scripts/aplicar_indices_criticos.py --apply

CREATE INDEX CONCURRENTLY exige autocommit e não pode rodar dentro de uma
transação explícita. O script nunca é chamado automaticamente pelo app.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from repositories.conexao import conectar  # noqa: E402


@dataclass(frozen=True)
class IndiceCritico:
    nome: str
    sql: str
    motivo: str


INDICES: tuple[IndiceCritico, ...] = (
    IndiceCritico(
        "idx_eventos_competicao_partida",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_eventos_competicao_partida "
        "ON eventos (competicao, partida_id)",
        "Acelera a contagem e a leitura dos eventos por competição e partida.",
    ),
    IndiceCritico(
        "idx_equipes_comp_nome_normalizado",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_equipes_comp_nome_normalizado "
        "ON equipes_competicoes (competicao, LOWER(TRIM(equipe_nome)))",
        "Acelera os vínculos de escudos e equipes usados nas listagens de partidas.",
    ),
    IndiceCritico(
        "idx_partidas_comp_equipe_a_normalizada",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_partidas_comp_equipe_a_normalizada "
        "ON partidas (competicao, LOWER(TRIM(equipe_a)))",
        "Acelera a busca das partidas de uma equipe pelo lado A.",
    ),
    IndiceCritico(
        "idx_partidas_comp_equipe_b_normalizada",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_partidas_comp_equipe_b_normalizada "
        "ON partidas (competicao, LOWER(TRIM(equipe_b)))",
        "Acelera a busca das partidas de uma equipe pelo lado B.",
    ),
    IndiceCritico(
        "idx_partidas_comp_rodada_ordem_id",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_partidas_comp_rodada_ordem_id "
        "ON partidas (competicao, rodada, ordem, id)",
        "Acelera a ordenação principal das partidas da competição.",
    ),
)


def _indices_existentes(cur) -> set[str]:
    cur.execute(
        "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema()"
    )
    return {str(linha[0] if not isinstance(linha, dict) else linha.get("indexname")) for linha in (cur.fetchall() or [])}


def executar(*, aplicar: bool) -> int:
    print("VolleyTablePro — índices críticos PostgreSQL")
    print("Modo:", "APLICAÇÃO" if aplicar else "SIMULAÇÃO (nenhuma alteração)")

    if not aplicar:
        for indice in INDICES:
            print(f"\n[{indice.nome}]\nMotivo: {indice.motivo}\nSQL: {indice.sql};")
        print("\nPara aplicar em homologação, execute novamente com --apply.")
        return 0

    if os.getenv("SQL_INDEX_APPLY_ALLOWED", "0").strip().lower() not in {"1", "true", "yes", "on"}:
        print("ERRO: defina SQL_INDEX_APPLY_ALLOWED=1 para autorizar alterações.", file=sys.stderr)
        return 2

    conn = conectar()
    try:
        # psycopg/psycopg2: CREATE INDEX CONCURRENTLY precisa de autocommit.
        conn.autocommit = True
        with conn.cursor() as cur:
            existentes = _indices_existentes(cur)
            for indice in INDICES:
                if indice.nome in existentes:
                    print(f"IGNORADO {indice.nome}: já existe")
                    continue
                print(f"APLICANDO {indice.nome}...")
                cur.execute(indice.sql)
                print(f"OK {indice.nome}")
        print("\nÍndices críticos processados com sucesso.")
        return 0
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="aplica os índices no banco configurado")
    args = parser.parse_args()
    return executar(aplicar=args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
