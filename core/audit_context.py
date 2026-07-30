"""Contexto de auditoria da requisição sem acoplar banco ao Flask.

Os eventos persistidos recebem metadados mínimos de autoria e origem. Dados de
rede e dispositivo são reduzidos a fingerprints SHA-256; IP e User-Agent brutos
não são armazenados.
"""
from __future__ import annotations

from contextvars import ContextVar, Token
from copy import deepcopy
from hashlib import sha256
from typing import Any, Mapping
from uuid import uuid4

_AUDIT_CONTEXT: ContextVar[dict[str, Any]] = ContextVar("vtp_audit_context", default={})


def _texto(valor: object, limite: int = 200) -> str | None:
    texto = str(valor or "").strip()
    return texto[:limite] if texto else None


def fingerprint(valor: object) -> str | None:
    texto = _texto(valor, limite=2000)
    if not texto:
        return None
    return sha256(texto.encode("utf-8", errors="ignore")).hexdigest()[:16]


def montar_contexto_auditoria(
    *,
    usuario: object = None,
    nome: object = None,
    perfil: object = None,
    endpoint: object = None,
    metodo: object = None,
    caminho: object = None,
    ip: object = None,
    user_agent: object = None,
    request_id: object = None,
    origem: object = "web",
) -> dict[str, Any]:
    contexto = {
        "request_id": _texto(request_id, 80) or uuid4().hex,
        "usuario": _texto(usuario, 160),
        "nome": _texto(nome, 160),
        "perfil": _texto(perfil, 80),
        "endpoint": _texto(endpoint, 180),
        "metodo": _texto(metodo, 16),
        "caminho": _texto(caminho, 240),
        "origem": _texto(origem, 40) or "web",
        "ip_fingerprint": fingerprint(ip),
        "dispositivo_fingerprint": fingerprint(user_agent),
    }
    return {chave: valor for chave, valor in contexto.items() if valor not in (None, "")}


def definir_contexto_auditoria(contexto: Mapping[str, Any] | None) -> Token:
    return _AUDIT_CONTEXT.set(dict(contexto or {}))


def limpar_contexto_auditoria(token: Token | None = None) -> None:
    if token is not None:
        try:
            _AUDIT_CONTEXT.reset(token)
            return
        except Exception:
            pass
    _AUDIT_CONTEXT.set({})


def obter_contexto_auditoria() -> dict[str, Any]:
    return deepcopy(_AUDIT_CONTEXT.get() or {})


def enriquecer_detalhes_auditoria(
    detalhes: Mapping[str, Any] | None,
    *,
    contexto_extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    resultado = deepcopy(dict(detalhes or {}))
    auditoria = obter_contexto_auditoria()
    if contexto_extra:
        auditoria.update({k: v for k, v in dict(contexto_extra).items() if v not in (None, "")})
    if not auditoria:
        auditoria = {"origem": "sistema", "request_id": uuid4().hex}
    if auditoria:
        existente = resultado.get("auditoria")
        if isinstance(existente, Mapping):
            combinado = dict(auditoria)
            combinado.update(dict(existente))
            resultado["auditoria"] = combinado
        else:
            resultado["auditoria"] = auditoria
    return resultado
