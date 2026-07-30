"""Persistência do cache de classificação.

Mantém SQL e serialização fora de ``banco.py``. A tabela deve ser criada pelas
migrações; em runtime este módulo apenas valida o schema e lê/grava o cache.
"""
from __future__ import annotations

import json
import math
from typing import Any

from core.schema_requirements import require_schema
from repositories.conexao import conectar


def validar_schema_classificacao_cache() -> None:
    require_schema(
        tables=("classificacao_cache",),
        columns={
            "classificacao_cache": (
                "competicao",
                "assinatura",
                "payload_json",
                "atualizado_em",
            )
        },
        context="cache de classificação",
    )


def criar_tabela_cache_classificacao(*, force: bool = False) -> None:
    """Compatibilidade para migrações explícitas.

    Em operação normal, apenas valida o schema. ``force=True`` fica reservado ao
    executor controlado de migrações.
    """
    if not force:
        validar_schema_classificacao_cache()
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS classificacao_cache (
                    competicao TEXT PRIMARY KEY,
                    assinatura TEXT NOT NULL,
                    payload_json JSONB NOT NULL,
                    atualizado_em TIMESTAMP DEFAULT NOW()
                )
                """
            )
        conn.commit()


def _sanitizar_json_postgres(valor: Any) -> Any:
    if isinstance(valor, float):
        return valor if math.isfinite(valor) else None
    if isinstance(valor, dict):
        return {str(chave): _sanitizar_json_postgres(item) for chave, item in valor.items()}
    if isinstance(valor, (list, tuple, set)):
        return [_sanitizar_json_postgres(item) for item in valor]
    return valor


def assinatura_classificacao_competicao(competicao: str) -> str | None:
    try:
        validar_schema_classificacao_cache()
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH partidas_sig AS (
                        SELECT COALESCE(
                            md5(string_agg(
                                CONCAT_WS('|',
                                    id,
                                    COALESCE(grupo, ''),
                                    COALESCE(fase, ''),
                                    COALESCE(equipe_a, ''),
                                    COALESCE(equipe_b, ''),
                                    COALESCE(status, ''),
                                    COALESCE(status_jogo, ''),
                                    COALESCE(fase_partida, ''),
                                    COALESCE(vencedor, ''),
                                    COALESCE(sets_a::TEXT, ''),
                                    COALESCE(sets_b::TEXT, ''),
                                    COALESCE(set1_a::TEXT, ''),
                                    COALESCE(set1_b::TEXT, ''),
                                    COALESCE(set2_a::TEXT, ''),
                                    COALESCE(set2_b::TEXT, ''),
                                    COALESCE(set3_a::TEXT, ''),
                                    COALESCE(set3_b::TEXT, ''),
                                    COALESCE(set4_a::TEXT, ''),
                                    COALESCE(set4_b::TEXT, ''),
                                    COALESCE(set5_a::TEXT, ''),
                                    COALESCE(set5_b::TEXT, ''),
                                    COALESCE(pontos_a::TEXT, ''),
                                    COALESCE(pontos_b::TEXT, ''),
                                    COALESCE(origem_resultado, ''),
                                    COALESCE(tipo_encerramento, '')
                                ), '§' ORDER BY id
                            )), 'sem_partidas') AS sig
                        FROM partidas
                        WHERE competicao = %s
                    ), grupos_sig AS (
                        SELECT COALESCE(
                            md5(string_agg(
                                CONCAT_WS('|',
                                    COALESCE(g.id::TEXT, ''),
                                    COALESCE(g.nome, ''),
                                    COALESCE(ge.equipe, '')
                                ), '§' ORDER BY g.id, ge.equipe
                            )), 'sem_grupos') AS sig
                        FROM grupos g
                        LEFT JOIN grupos_equipes ge
                               ON ge.grupo_id = g.id
                              AND ge.competicao = g.competicao
                        WHERE g.competicao = %s
                    )
                    SELECT md5((SELECT sig FROM partidas_sig) || '::' || (SELECT sig FROM grupos_sig)) AS assinatura
                    """,
                    (competicao, competicao),
                )
                row = cur.fetchone() or {}
                return row.get("assinatura") or "sem_assinatura"
    except Exception as exc:
        print("AVISO assinatura_classificacao_competicao:", repr(exc), flush=True)
        return None


def obter_cache_classificacao(competicao: str, assinatura: str | None = None):
    if not competicao:
        return None
    try:
        validar_schema_classificacao_cache()
        sql = """
            SELECT payload_json
            FROM classificacao_cache
            WHERE competicao = %s
        """
        params: list[Any] = [competicao]
        if assinatura:
            sql += " AND assinatura = %s"
            params.append(assinatura)
        sql += " LIMIT 1"
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                row = cur.fetchone()
                if not row:
                    return None
                payload = row.get("payload_json")
                return json.loads(payload) if isinstance(payload, str) else payload
    except Exception as exc:
        print("AVISO obter_cache_classificacao:", repr(exc), flush=True)
        return None


def salvar_cache_classificacao(competicao: str, assinatura: str, payload: Any) -> bool:
    if not competicao or not assinatura:
        return False
    try:
        validar_schema_classificacao_cache()
        payload_limpo = _sanitizar_json_postgres(payload)
        payload_json = json.dumps(payload_limpo, ensure_ascii=False, default=str, allow_nan=False)
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO classificacao_cache (competicao, assinatura, payload_json, atualizado_em)
                    VALUES (%s, %s, %s::jsonb, NOW())
                    ON CONFLICT (competicao)
                    DO UPDATE SET
                        assinatura = EXCLUDED.assinatura,
                        payload_json = EXCLUDED.payload_json,
                        atualizado_em = NOW()
                    """,
                    (competicao, assinatura, payload_json),
                )
            conn.commit()
        return True
    except Exception as exc:
        print("AVISO salvar_cache_classificacao:", repr(exc), flush=True)
        return False


def invalidar_cache_classificacao(competicao: str) -> bool:
    if not competicao:
        return False
    try:
        validar_schema_classificacao_cache()
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM classificacao_cache WHERE competicao = %s", (competicao,))
            conn.commit()
        return True
    except Exception as exc:
        print("AVISO invalidar_cache_classificacao:", repr(exc), flush=True)
        return False
