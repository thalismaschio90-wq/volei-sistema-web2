"""Primitivas centrais de segurança do VolleyTablePro.

Mantém compatibilidade temporária com senhas legadas em texto puro: quando uma
senha antiga é validada com sucesso, o chamador deve persistir o novo hash.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from typing import Iterable

try:
    from werkzeug.security import check_password_hash as _werkzeug_check_password_hash
except Exception:  # testes mínimos podem não instalar Flask/Werkzeug
    _werkzeug_check_password_hash = None

_HASH_PREFIXES = ("vtp_pbkdf2_sha256$", "scrypt:", "pbkdf2:")


def senha_esta_hasheada(valor: object) -> bool:
    texto = str(valor or "")
    return texto.startswith(_HASH_PREFIXES)


def gerar_hash_senha(senha: str) -> str:
    senha = str(senha or "")
    if not senha:
        raise ValueError("A senha não pode ser vazia.")
    iteracoes = int(os.environ.get("PASSWORD_HASH_ITERATIONS", "600000"))
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", senha.encode("utf-8"), salt.encode("ascii"), iteracoes).hex()
    return f"vtp_pbkdf2_sha256${iteracoes}${salt}${digest}"


def _verificar_hash_vtp(senha: str, armazenada: str) -> bool:
    try:
        prefixo, iteracoes_txt, salt, esperado = armazenada.split("$", 3)
        if prefixo != "vtp_pbkdf2_sha256":
            return False
        calculado = hashlib.pbkdf2_hmac(
            "sha256", senha.encode("utf-8"), salt.encode("ascii"), int(iteracoes_txt)
        ).hex()
        return hmac.compare_digest(calculado, esperado)
    except (ValueError, TypeError):
        return False


def verificar_senha(senha_informada: str, senha_armazenada: object) -> tuple[bool, bool]:
    """Retorna ``(valida, precisa_migrar)``.

    Aceita o formato próprio PBKDF2, hashes legados do Werkzeug e, durante a
    migração, senhas antigas em texto puro.
    """
    informada = str(senha_informada or "")
    armazenada = str(senha_armazenada or "")
    if not informada or not armazenada:
        return False, False

    if armazenada.startswith("vtp_pbkdf2_sha256$"):
        return _verificar_hash_vtp(informada, armazenada), False

    if armazenada.startswith(("scrypt:", "pbkdf2:")):
        if _werkzeug_check_password_hash is None:
            return False, False
        try:
            return bool(_werkzeug_check_password_hash(armazenada, informada)), False
        except (ValueError, TypeError):
            return False, False

    valida = hmac.compare_digest(armazenada, informada)
    return valida, valida


def fingerprint_secreto(valor: str, tamanho: int = 16) -> str:
    return hashlib.sha256(str(valor or "").encode("utf-8")).hexdigest()[:tamanho]


def carregar_secret_key(*, producao: bool | None = None) -> str:
    """Carrega a chave de sessão e bloqueia fallback previsível em produção."""
    chave = str(os.environ.get("SECRET_KEY", "")).strip()
    if producao is None:
        producao = any(
            str(os.environ.get(nome, "")).strip().lower() in {"1", "true", "yes", "on", "production", "prod"}
            for nome in ("RENDER", "FLASK_PRODUCTION", "VTP_PRODUCTION")
        ) or str(os.environ.get("FLASK_ENV", "")).strip().lower() == "production"

    if chave and len(chave) >= 32 and chave.lower() not in {"voleitablepro", "changeme", "secret", "dev"}:
        return chave

    if producao:
        raise RuntimeError(
            "SECRET_KEY ausente ou fraca. Configure uma chave aleatória com pelo menos 32 caracteres."
        )

    # Desenvolvimento local: chave aleatória por processo, sem fallback conhecido.
    chave_dev = secrets.token_urlsafe(48)
    print("AVISO SEGURANÇA: SECRET_KEY não configurada; usando chave temporária de desenvolvimento.", flush=True)
    return chave_dev


def origens_permitidas_socket() -> list[str] | str:
    valor = str(os.environ.get("SOCKETIO_ALLOWED_ORIGINS", "")).strip()
    if valor:
        origens = [item.strip().rstrip("/") for item in valor.split(",") if item.strip()]
        return origens or []

    return [
        "https://volleytablepro.com.br",
        "https://www.volleytablepro.com.br",
        "http://localhost:5000",
        "http://127.0.0.1:5000",
    ]
