"""Serviço das configurações básicas de competições."""
from __future__ import annotations

from cache.domain_read import invalidar
from repositories import competicoes_basico as repo

_DOMINIO = "competicao_config"


def _executar_e_invalidar(nome_competicao, funcao, dados):
    ok = funcao(nome_competicao, dados)
    if ok:
        invalidar(_DOMINIO, nome_competicao)
    return ok


def atualizar_dados_competicao(nome_original, dados):
    ok = repo.atualizar_dados_competicao_persistencia(nome_original, dados)
    if ok:
        invalidar(_DOMINIO, nome_original)
        novo_nome = str((dados or {}).get("nome") or "").strip()
        if novo_nome and novo_nome != str(nome_original or "").strip():
            invalidar(_DOMINIO, novo_nome)
    return ok


def atualizar_estrutura_competicao(nome_competicao, dados):
    return _executar_e_invalidar(nome_competicao, repo.atualizar_estrutura_competicao_persistencia, dados)


def atualizar_regras_jogo(nome_competicao, dados):
    return _executar_e_invalidar(nome_competicao, repo.atualizar_regras_jogo_persistencia, dados)


def atualizar_pontuacao_desempate(nome_competicao, dados):
    return _executar_e_invalidar(nome_competicao, repo.atualizar_pontuacao_desempate_persistencia, dados)


__all__ = [
    "atualizar_dados_competicao",
    "atualizar_estrutura_competicao",
    "atualizar_regras_jogo",
    "atualizar_pontuacao_desempate",
]
