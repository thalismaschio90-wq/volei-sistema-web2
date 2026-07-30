"""Captura opcional e segura de planos PostgreSQL para consultas SELECT lentas.

Por padrão usa EXPLAIN sem ANALYZE. EXPLAIN ANALYZE deve ser habilitado somente
em homologação, pois executa a consulta novamente e aumenta a latência.
"""
from __future__ import annotations

import json
import os
import random
from typing import Any


def _bool(nome: str, padrao: bool = False) -> bool:
    valor = os.environ.get(nome)
    if valor is None:
        return padrao
    return valor.strip().lower() in {"1", "true", "yes", "on", "sim"}


def _float(nome: str, padrao: float) -> float:
    try:
        return float(os.environ.get(nome, padrao))
    except (TypeError, ValueError):
        return padrao


def explain_habilitado() -> bool:
    return _bool("SQL_EXPLAIN_ENABLED", False)


def deve_amostrar() -> bool:
    taxa = max(0.0, min(1.0, _float("SQL_EXPLAIN_SAMPLE_RATE", 0.1)))
    return random.random() < taxa


def _operacao(sql: Any) -> str:
    return str(sql or "").lstrip().split(None, 1)[0].upper() if str(sql or "").strip() else ""


def consulta_elegivel(sql: Any) -> bool:
    return _operacao(sql) in {"SELECT", "WITH"}


def _resumir_no(no: dict[str, Any], saida: list[dict[str, Any]]) -> None:
    item = {
        "tipo": no.get("Node Type"),
        "relacao": no.get("Relation Name"),
        "indice": no.get("Index Name"),
        "custo_total": no.get("Total Cost"),
        "linhas_estimadas": no.get("Plan Rows"),
        "tempo_real_ms": no.get("Actual Total Time"),
        "linhas_reais": no.get("Actual Rows"),
    }
    saida.append({k: v for k, v in item.items() if v is not None})
    for filho in no.get("Plans") or []:
        if isinstance(filho, dict):
            _resumir_no(filho, saida)


def resumir_plano(payload: Any) -> dict[str, Any]:
    try:
        raiz = payload
        if isinstance(raiz, str):
            raiz = json.loads(raiz)
        if isinstance(raiz, list) and raiz:
            raiz = raiz[0]
        plano = raiz.get("Plan", raiz) if isinstance(raiz, dict) else {}
        nos: list[dict[str, Any]] = []
        if isinstance(plano, dict):
            _resumir_no(plano, nos)
        return {
            "ok": bool(nos),
            "tempo_planejamento_ms": raiz.get("Planning Time") if isinstance(raiz, dict) else None,
            "tempo_execucao_ms": raiz.get("Execution Time") if isinstance(raiz, dict) else None,
            "nos": nos[:30],
            "operador_dominante": max(
                nos,
                key=lambda n: float(n.get("tempo_real_ms") or n.get("custo_total") or 0),
                default={},
            ).get("tipo"),
        }
    except Exception as exc:
        return {"ok": False, "erro": type(exc).__name__}


def capturar_plano(conexao: Any, sql: Any, params: Any = None) -> dict[str, Any] | None:
    if not explain_habilitado() or not consulta_elegivel(sql) or not deve_amostrar():
        return None
    analyze = _bool("SQL_EXPLAIN_ANALYZE_ENABLED", False)
    timeout_ms = max(50, min(10000, int(_float("SQL_EXPLAIN_TIMEOUT_MS", 1500))))
    prefixo = "EXPLAIN (FORMAT JSON, COSTS, VERBOSE, SETTINGS"
    if analyze:
        prefixo += ", ANALYZE, BUFFERS"
    prefixo += ") "
    cursor = None
    try:
        cursor = conexao.cursor()
        cursor.execute("SET LOCAL statement_timeout = %s", (timeout_ms,))
        if params is None:
            cursor.execute(prefixo + str(sql))
        else:
            cursor.execute(prefixo + str(sql), params)
        linha = cursor.fetchone()
        payload = linha[0] if linha else None
        resumo = resumir_plano(payload)
        resumo["analyze"] = analyze
        return resumo
    except Exception as exc:
        return {"ok": False, "erro": type(exc).__name__, "analyze": analyze}
    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
