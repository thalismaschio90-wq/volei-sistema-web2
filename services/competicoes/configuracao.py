"""Serviço cacheável das configurações do organizador.

Configurações mudam raramente e são consultadas por várias telas. PostgreSQL
continua como fonte de verdade; toda escrita invalida imediatamente a geração
do cache da competição.
"""
from __future__ import annotations

from cache.domain_read import invalidar, obter_ou_carregar
from repositories import competicoes_config as repo

_DOMINIO = "competicao_config"


def _invalidar(nome_competicao: str) -> None:
    invalidar(_DOMINIO, nome_competicao)


def buscar_configuracao_avancada(nome_competicao, *, ignorar_cache=False):
    return obter_ou_carregar(
        _DOMINIO,
        nome_competicao,
        "avancada",
        lambda: repo.buscar_configuracao_avancada(nome_competicao),
        ignorar_cache=ignorar_cache,
    )


def atualizar_configuracao_avancada(nome_competicao, **dados):
    ok = repo.atualizar_configuracao_avancada(nome_competicao, **dados)
    if ok:
        _invalidar(nome_competicao)
    return ok


def inicializar_configuracao_avancada(nome_competicao):
    ok = repo.inicializar_configuracao_avancada(nome_competicao)
    if ok:
        _invalidar(nome_competicao)
    return ok


def buscar_configuracao_agenda(nome_competicao, *, ignorar_cache=False):
    return obter_ou_carregar(
        _DOMINIO,
        nome_competicao,
        "agenda",
        lambda: repo.buscar_configuracao_agenda(nome_competicao),
        ignorar_cache=ignorar_cache,
    )


def atualizar_configuracao_agenda(nome_competicao, **dados):
    ok = repo.atualizar_configuracao_agenda(nome_competicao, **dados)
    if ok:
        _invalidar(nome_competicao)
    return ok


def inicializar_configuracao_agenda(nome_competicao):
    ok = repo.inicializar_configuracao_agenda(nome_competicao)
    if ok:
        _invalidar(nome_competicao)
    return ok


__all__ = [
    "buscar_configuracao_avancada",
    "atualizar_configuracao_avancada",
    "inicializar_configuracao_avancada",
    "buscar_configuracao_agenda",
    "atualizar_configuracao_agenda",
    "inicializar_configuracao_agenda",
]
