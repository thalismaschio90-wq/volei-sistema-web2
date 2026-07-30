import os

import pytest

from core.security import (
    carregar_secret_key,
    gerar_hash_senha,
    origens_permitidas_socket,
    senha_esta_hasheada,
    verificar_senha,
)


def test_hash_senha_nao_guarda_texto_puro():
    senha = "Senha-Forte-123"
    hash_senha = gerar_hash_senha(senha)
    assert senha not in hash_senha
    assert senha_esta_hasheada(hash_senha)
    assert verificar_senha(senha, hash_senha) == (True, False)
    assert verificar_senha("errada", hash_senha) == (False, False)


def test_senha_legada_pede_migracao():
    assert verificar_senha("antiga", "antiga") == (True, True)
    assert verificar_senha("outra", "antiga") == (False, False)


def test_secret_key_fraca_bloqueia_producao(monkeypatch):
    monkeypatch.setenv("SECRET_KEY", "voleitablepro")
    with pytest.raises(RuntimeError):
        carregar_secret_key(producao=True)


def test_origens_socket_nao_usam_coringa_por_padrao(monkeypatch):
    monkeypatch.delenv("SOCKETIO_ALLOWED_ORIGINS", raising=False)
    origens = origens_permitidas_socket()
    assert origens != "*"
    assert "https://volleytablepro.com.br" in origens
