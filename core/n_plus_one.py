"""Detecção segura de padrões N+1 dentro de uma única requisição.

O detector trabalha somente com fingerprints e origens de código. SQL bruto,
parâmetros e valores consultados nunca são persistidos.
"""
from __future__ import annotations

import os
from typing import Any


def _env_int(nome: str, padrao: int, minimo: int, maximo: int) -> int:
    try:
        valor = int(os.environ.get(nome, padrao))
    except (TypeError, ValueError):
        valor = padrao
    return max(minimo, min(maximo, valor))


def limite_repeticoes_n_plus_one() -> int:
    """Quantidade mínima da mesma consulta na requisição para gerar alerta."""
    return _env_int("SQL_N_PLUS_ONE_THRESHOLD", 4, 2, 100)


def classificar_repeticao(item: dict[str, Any]) -> dict[str, Any]:
    """Classifica uma repetição sem acessar conteúdo ou parâmetros SQL."""
    quantidade = int(item.get("quantidade") or 0)
    total_ms = float(item.get("duracao_total_ms") or 0.0)
    media_ms = total_ms / max(1, quantidade)

    if quantidade >= 20 or total_ms >= 500:
        prioridade = "alta"
    elif quantidade >= 8 or total_ms >= 150:
        prioridade = "media"
    else:
        prioridade = "baixa"

    return {
        "fingerprint": str(item.get("fingerprint") or ""),
        "operacao": str(item.get("operacao") or "SQL"),
        "quantidade": quantidade,
        "duracao_total_ms": round(total_ms, 2),
        "duracao_media_ms": round(media_ms, 2),
        "duracao_max_ms": round(float(item.get("duracao_max_ms") or 0.0), 2),
        "origem": str(item.get("origem") or ""),
        "prioridade": prioridade,
        "diagnostico": (
            "A mesma estrutura de consulta foi executada repetidamente na mesma requisição. "
            "Revise o fluxo para usar JOIN, consulta em lote, IN/ANY, pré-carregamento ou cache local da requisição."
        ),
    }


def detectar_repeticoes(itens: list[dict[str, Any]], *, limite: int | None = None) -> list[dict[str, Any]]:
    """Retorna somente consultas que ultrapassaram o limite de repetição."""
    minimo = limite if limite is not None else limite_repeticoes_n_plus_one()
    resultado = [classificar_repeticao(item) for item in itens if int(item.get("quantidade") or 0) >= minimo]
    ordem = {"alta": 0, "media": 1, "baixa": 2}
    resultado.sort(
        key=lambda item: (
            ordem.get(item["prioridade"], 9),
            -item["duracao_total_ms"],
            -item["quantidade"],
        )
    )
    return resultado
