"""Consultas específicas do visualizador público.

Este módulo mantém SQL de leitura fora das rotas HTTP. Nenhuma função decide
regras de jogo ou monta respostas Flask.
"""

from core.schema_requirements import require_schema
from repositories.conexao import conectar


def buscar_destaque_partida(partida_id: int, competicao: str):
    """Retorna o destaque mais recente da partida, incluindo foto disponível."""
    try:
        require_schema(
            tables=("destaques_partida",),
            columns={"destaques_partida": ("partida_id", "competicao", "atleta_id", "numero", "nome", "equipe")},
            context="destaque da partida no visualizador público",
        )
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        d.lado,
                        d.atleta_id,
                        d.numero,
                        d.nome,
                        d.observacao,
                        d.equipe,
                        d.criado_em,
                        COALESCE(
                            NULLIF(a.foto_atleta, ''),
                            (
                                SELECT NULLIF(a2.foto_atleta, '')
                                FROM atletas a2
                                WHERE a2.competicao = d.competicao
                                  AND LOWER(TRIM(COALESCE(a2.equipe, ''))) = LOWER(TRIM(COALESCE(d.equipe, '')))
                                  AND (
                                      (d.numero IS NOT NULL AND a2.numero = d.numero)
                                      OR LOWER(TRIM(COALESCE(a2.nome, ''))) = LOWER(TRIM(COALESCE(d.nome, '')))
                                  )
                                  AND COALESCE(a2.foto_atleta, '') <> ''
                                ORDER BY
                                    CASE WHEN d.numero IS NOT NULL AND a2.numero = d.numero THEN 0 ELSE 1 END,
                                    a2.id DESC
                                LIMIT 1
                            )
                        ) AS foto_atleta
                    FROM destaques_partida d
                    LEFT JOIN atletas a ON a.id = d.atleta_id
                    WHERE d.partida_id = %s AND d.competicao = %s
                    ORDER BY d.id DESC
                    LIMIT 1
                    """,
                    (partida_id, competicao),
                )
                return cur.fetchone()
    except Exception as exc:
        print("AVISO visualizador/destaque_partida:", repr(exc), flush=True)
        return None


def buscar_versoes_detalhes(partida_id: int, competicao: str) -> tuple[int, int]:
    """Retorna versões de eventos e destaque usando uma única conexão."""
    eventos_versao = 0
    destaque_versao = 0
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COALESCE(MAX(id), 0) AS versao
                    FROM eventos
                    WHERE partida_id = %s AND competicao = %s
                    """,
                    (partida_id, competicao),
                )
                row = cur.fetchone() or {}
                eventos_versao = int(row.get("versao") or 0)

                try:
                    cur.execute(
                        """
                        SELECT COALESCE(MAX(id), 0) AS versao
                        FROM destaques_partida
                        WHERE partida_id = %s AND competicao = %s
                        """,
                        (partida_id, competicao),
                    )
                    row = cur.fetchone() or {}
                    destaque_versao = int(row.get("versao") or 0)
                except Exception:
                    destaque_versao = 0
    except Exception as exc:
        print("AVISO visualizador/dados_leves_versao:", repr(exc), flush=True)

    return eventos_versao, destaque_versao
