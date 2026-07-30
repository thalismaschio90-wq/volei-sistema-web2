"""Migra credenciais legadas em texto puro para PBKDF2.

Uso seguro (simulação):
    py scripts/migrar_senhas_hash.py

Aplicação explícita em homologação/produção:
    set PASSWORD_MIGRATION_ALLOWED=1
    py scripts/migrar_senhas_hash.py --apply
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.security import gerar_hash_senha, senha_esta_hasheada
from repositories.conexao import conectar


def _env_bool(nome: str) -> bool:
    return str(os.environ.get(nome, "")).strip().lower() in {"1", "true", "yes", "on", "sim"}


def _linhas_legadas(cur, tabela: str, coluna_id: str, coluna_senha: str = "senha"):
    cur.execute(
        f"SELECT {coluna_id} AS chave, {coluna_senha} AS senha FROM {tabela} "
        f"WHERE {coluna_senha} IS NOT NULL AND {coluna_senha} <> ''"
    )
    return [row for row in (cur.fetchall() or []) if not senha_esta_hasheada(row.get("senha"))]


def migrar(*, aplicar: bool = False) -> dict:
    if aplicar and not _env_bool("PASSWORD_MIGRATION_ALLOWED"):
        raise RuntimeError("Defina PASSWORD_MIGRATION_ALLOWED=1 para aplicar a migração.")

    resultado = {"usuarios": 0, "equipes": 0, "apontadores": 0, "aplicado": aplicar}
    with conectar() as conn:
        with conn.cursor() as cur:
            alvos = [
                ("usuarios", "login", "usuarios"),
                ("equipes", "login", "equipes"),
                ("apontadores_acesso", "id", "apontadores"),
            ]
            for tabela, coluna_id, chave_resultado in alvos:
                try:
                    linhas = _linhas_legadas(cur, tabela, coluna_id)
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    continue
                resultado[chave_resultado] = len(linhas)
                if aplicar:
                    for row in linhas:
                        cur.execute(
                            f"UPDATE {tabela} SET senha = %s WHERE {coluna_id} = %s",
                            (gerar_hash_senha(row["senha"]), row["chave"]),
                        )
        if aplicar:
            conn.commit()
        else:
            conn.rollback()
    return resultado


def main() -> int:
    parser = argparse.ArgumentParser(description="Migra senhas legadas do VolleyTablePro para hash PBKDF2.")
    parser.add_argument("--apply", action="store_true", help="Aplica as alterações. Sem esta opção, apenas simula.")
    args = parser.parse_args()
    resultado = migrar(aplicar=args.apply)
    modo = "APLICADO" if args.apply else "SIMULAÇÃO"
    print(f"[{modo}] usuários={resultado['usuarios']} equipes={resultado['equipes']} apontadores={resultado['apontadores']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
