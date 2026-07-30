"""Consultas leves para o painel inicial do Super ADM.

Este módulo não executa DDL/migrações durante a navegação. A estrutura deve ser
preparada no bootstrap da aplicação.
"""
from __future__ import annotations

from typing import Any, Callable

MASTER_SUPERADMIN_LOGIN = "ThalisADM"


def _dict(row: Any) -> dict:
    return dict(row or {})


def buscar_painel_superadmin(
    login: str,
    *,
    conectar_fn: Callable,
    master_login: str = MASTER_SUPERADMIN_LOGIN,
) -> dict:
    """Carrega contexto, totais e clientes usando uma única conexão.

    Em bancos legados sem as colunas multiempresa, usa um fallback somente de
    leitura para manter o painel disponível.
    """
    login = (login or "").strip()
    if not login:
        return {
            "eh_master": False,
            "total_competicoes": 0,
            "total_equipes": 0,
            "total_partidas": 0,
            "superadmins_clientes": [],
        }

    with conectar_fn() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT
                        login,
                        perfil,
                        ativo,
                        cliente_id,
                        COALESCE(superadmin_nivel, 'cliente') AS superadmin_nivel
                    FROM usuarios
                    WHERE LOWER(login) = LOWER(%s)
                    LIMIT 1
                    """,
                    (login,),
                )
                usuario = _dict(cur.fetchone())
                nivel = str(usuario.get("superadmin_nivel") or "cliente").strip().lower()
                eh_master = login.lower() == master_login.lower() or nivel == "master"
                cliente_id = usuario.get("cliente_id")

                # Os totais são retornados em uma única ida ao PostgreSQL.
                if cliente_id is None:
                    cur.execute(
                        """
                        SELECT
                            (SELECT COUNT(*) FROM competicoes) AS total_competicoes,
                            (SELECT COUNT(*) FROM equipes) AS total_equipes,
                            (SELECT COUNT(*) FROM partidas) AS total_partidas
                        """
                    )
                elif eh_master:
                    cur.execute(
                        """
                        SELECT
                            (SELECT COUNT(*) FROM competicoes WHERE cliente_id = %s OR cliente_id IS NULL) AS total_competicoes,
                            (SELECT COUNT(*) FROM equipes WHERE cliente_id = %s OR cliente_id IS NULL) AS total_equipes,
                            (SELECT COUNT(*) FROM partidas WHERE cliente_id = %s OR cliente_id IS NULL) AS total_partidas
                        """,
                        (cliente_id, cliente_id, cliente_id),
                    )
                else:
                    cur.execute(
                        """
                        SELECT
                            (SELECT COUNT(*) FROM competicoes WHERE cliente_id = %s) AS total_competicoes,
                            (SELECT COUNT(*) FROM equipes WHERE cliente_id = %s) AS total_equipes,
                            (SELECT COUNT(*) FROM partidas WHERE cliente_id = %s) AS total_partidas
                        """,
                        (cliente_id, cliente_id, cliente_id),
                    )
                totais = _dict(cur.fetchone())

                # A listagem completa de SuperADMs possui uma rota própria
                # (/superadmins). O painel inicial exibe apenas os totais, portanto
                # não carrega todos os clientes a cada acesso.
                clientes = []

                return {
                    "eh_master": eh_master,
                    "total_competicoes": int(totais.get("total_competicoes") or 0),
                    "total_equipes": int(totais.get("total_equipes") or 0),
                    "total_partidas": int(totais.get("total_partidas") or 0),
                    "superadmins_clientes": clientes,
                }
            except Exception:
                # Compatibilidade com bancos anteriores à camada multiempresa.
                try:
                    conn.rollback()
                except Exception:
                    pass
                with conn.cursor() as fallback:
                    fallback.execute(
                        """
                        SELECT
                            (SELECT COUNT(*) FROM competicoes) AS total_competicoes,
                            (SELECT COUNT(*) FROM equipes) AS total_equipes,
                            (SELECT COUNT(*) FROM partidas) AS total_partidas
                        """
                    )
                    totais = _dict(fallback.fetchone())
                return {
                    "eh_master": login.lower() == master_login.lower(),
                    "total_competicoes": int(totais.get("total_competicoes") or 0),
                    "total_equipes": int(totais.get("total_equipes") or 0),
                    "total_partidas": int(totais.get("total_partidas") or 0),
                    "superadmins_clientes": [],
                }
