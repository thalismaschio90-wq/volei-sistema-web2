"""Consultas de leitura para replay e auditoria de partidas.

Este módulo não altera o estado da partida. Ele apenas lê a partida e os
registros já persistidos na tabela ``eventos``.
"""
from __future__ import annotations

from typing import Any

from repositories.conexao import conectar


COLUNAS_EVENTO = """
    id,
    partida_id,
    competicao,
    set_numero,
    equipe,
    tipo,
    tipo_evento,
    fundamento,
    resultado,
    detalhe,
    detalhes,
    atleta_id,
    atleta_nome,
    numero,
    criado_em
"""


def buscar_partida_replay(partida_id: int, competicao: str) -> dict[str, Any] | None:
    """Retorna os dados mínimos da partida exibidos no replay."""
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    competicao,
                    equipe_a,
                    equipe_b,
                    sets_a,
                    sets_b,
                    set_atual,
                    pontos_a,
                    pontos_b,
                    status,
                    status_jogo,
                    fase,
                    rodada,
                    quadra,
                    data_hora
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
                """,
                (int(partida_id), str(competicao or "").strip()),
            )
            return cur.fetchone()


def listar_eventos_replay(
    partida_id: int,
    competicao: str,
    *,
    depois_do_id: int = 0,
    limite: int = 1000,
    set_numero: int | None = None,
) -> list[dict[str, Any]]:
    """Lista eventos em ordem cronológica para reprodução e auditoria.

    ``depois_do_id`` permite buscar somente a continuação da linha do tempo.
    O limite é defensivo para impedir respostas sem tamanho controlado.
    """
    limite_seguro = max(1, min(int(limite or 1000), 5000))
    depois_seguro = max(0, int(depois_do_id or 0))
    filtros = ["partida_id = %s", "competicao = %s", "id > %s"]
    parametros: list[Any] = [
        int(partida_id),
        str(competicao or "").strip(),
        depois_seguro,
    ]
    if set_numero not in (None, ""):
        filtros.append("set_numero = %s")
        parametros.append(int(set_numero))
    parametros.append(limite_seguro)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {COLUNAS_EVENTO}
                FROM eventos
                WHERE {' AND '.join(filtros)}
                ORDER BY id ASC
                LIMIT %s
                """,
                tuple(parametros),
            )
            return list(cur.fetchall() or [])
