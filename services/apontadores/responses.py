"""Respostas HTTP padronizadas das rotas do apontador."""
from __future__ import annotations

from typing import Any, Mapping

from flask import jsonify


_HEADERS_NO_CACHE = {
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "Pragma": "no-cache",
    "Expires": "0",
}


def json_no_cache(payload: Mapping[str, Any] | list[Any], status: int = 200):
    """Cria resposta JSON sem cache preservando o contrato atual da rota."""
    resposta = jsonify(payload)
    resposta.status_code = int(status or 200)
    for nome, valor in _HEADERS_NO_CACHE.items():
        resposta.headers[nome] = valor
    return resposta


def sucesso(mensagem: str = "", *, status: int = 200, **dados: Any):
    payload: dict[str, Any] = {"ok": True, **dados}
    if mensagem:
        payload["mensagem"] = mensagem
    return json_no_cache(payload, status)


def erro(mensagem: str, *, status: int = 400, bloqueada: bool | None = None, **dados: Any):
    payload: dict[str, Any] = {"ok": False, "mensagem": mensagem, **dados}
    if bloqueada is not None:
        payload["bloqueada"] = bool(bloqueada)
    return json_no_cache(payload, status)


__all__ = ["erro", "json_no_cache", "sucesso"]
