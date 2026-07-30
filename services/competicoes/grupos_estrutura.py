"""Coordenação da estrutura, sincronização e sorteio dos grupos."""
from dataclasses import dataclass
import random

from rules.grupos_estrutura import (
    distribuir_equipes_balanceado,
    estrutura_grupo_unico,
    nome_grupo_normalizado,
    nomes_equipes_unicos,
    nomes_grupos_automaticos,
    qtd_grupos_configurada,
    resumo_distribuicao,
    selecionar_grupos_estrutura,
)
from services.competicoes import grupos as grupos_service
from services.competicoes import quadras as quadras_service
from services.equipes.consultas import listar_equipes_da_competicao


@dataclass(frozen=True)
class ResultadoGrupos:
    ok: bool
    mensagem: str = ""
    mudou: bool = False


def existe_distribuicao_fora_grupo_a(nome_competicao):
    try:
        grupos = grupos_service.listar_grupos(nome_competicao) or []
        mapa = grupos_service.listar_equipes_por_grupos_competicao(nome_competicao) or {}
        for grupo in grupos:
            if nome_grupo_normalizado(grupo.get("nome")) in {"", "A"}:
                continue
            equipes = mapa.get(grupo.get("id"), [])
            if any(str((e or {}).get("equipe") or "").strip() for e in equipes):
                return True
    except Exception as exc:
        print("AVISO existe_distribuicao_fora_grupo_a:", repr(exc), flush=True)
    return False


def sincronizar_grupo_unico(competicao, *, fase_travada, limpar_cache=lambda nome: None):
    if not competicao or not estrutura_grupo_unico(competicao):
        return False
    nome_competicao = str(competicao.get("nome") or "").strip()
    if not nome_competicao or fase_travada(nome_competicao):
        return False

    grupos = grupos_service.listar_grupos(nome_competicao) or []
    grupo_a = next((g for g in grupos if nome_grupo_normalizado(g.get("nome")) == "A"), None)
    if not grupo_a:
        grupos_service.criar_grupo("A", nome_competicao, fase_travada=False)
        grupos = grupos_service.listar_grupos(nome_competicao) or []
        grupo_a = next((g for g in grupos if nome_grupo_normalizado(g.get("nome")) == "A"), None)
    if not grupo_a:
        return False

    try:
        equipes = nomes_equipes_unicos(listar_equipes_da_competicao(nome_competicao) or [])
        existentes = grupos_service.listar_equipes_por_grupo(grupo_a.get("id")) or []
        nomes_existentes = {str(e.get("equipe") or "").strip().casefold() for e in existentes}
        mudou = False
        for equipe in equipes:
            if equipe.casefold() not in nomes_existentes:
                if grupos_service.adicionar_equipe_no_grupo(
                    grupo_a.get("id"), equipe, nome_competicao, fase_travada=False
                ):
                    mudou = True

        quadras = quadras_service.listar_quadras_competicao(nome_competicao) or []
        ativas = [q for q in quadras if q.get("ativa") is not False]
        if len(ativas) == 1:
            quadras_service.vincular_grupo_a_quadra(nome_competicao, "A", ativas[0].get("id"))
        if mudou:
            limpar_cache(nome_competicao)
        return True
    except Exception as exc:
        print("AVISO sincronizar grupo único:", repr(exc), flush=True)
        return False


def garantir_grupos_estrutura(competicao, *, fase_travada, limpar_cache=lambda nome: None):
    if not competicao:
        return False
    nome_competicao = str(competicao.get("nome") or "").strip()
    if not nome_competicao or fase_travada(nome_competicao):
        return False

    if estrutura_grupo_unico(competicao):
        if existe_distribuicao_fora_grupo_a(nome_competicao):
            return False
        return sincronizar_grupo_unico(
            competicao,
            fase_travada=fase_travada,
            limpar_cache=limpar_cache,
        )

    nomes_estrutura = nomes_grupos_automaticos(qtd_grupos_configurada(competicao))
    existentes = grupos_service.listar_grupos(nome_competicao) or []
    existentes_nomes = {nome_grupo_normalizado(g.get("nome")) for g in existentes}
    mudou = False
    for nome_grupo in nomes_estrutura:
        if nome_grupo not in existentes_nomes:
            if grupos_service.criar_grupo(nome_grupo, nome_competicao, fase_travada=False):
                mudou = True
    if mudou:
        limpar_cache(nome_competicao)
    return mudou


def sortear_equipes(competicao, *, fase_travada, limpar_cache=lambda nome: None, rng=None):
    if not competicao:
        return ResultadoGrupos(False, "Competição não encontrada.")
    nome_competicao = str(competicao.get("nome") or "").strip()
    if not nome_competicao:
        return ResultadoGrupos(False, "Competição sem nome.")
    if fase_travada(nome_competicao):
        return ResultadoGrupos(False, "A fase classificatória já iniciou. Não é possível sortear grupos.")
    if estrutura_grupo_unico(competicao):
        sincronizar_grupo_unico(
            competicao,
            fase_travada=fase_travada,
            limpar_cache=limpar_cache,
        )
        return ResultadoGrupos(True, "Competição em grupo único: todas as equipes ficam no Grupo A.", True)

    garantir_grupos_estrutura(
        competicao,
        fase_travada=fase_travada,
        limpar_cache=limpar_cache,
    )
    qtd = qtd_grupos_configurada(competicao)
    grupos = selecionar_grupos_estrutura(grupos_service.listar_grupos(nome_competicao) or [], qtd)
    equipes = nomes_equipes_unicos(listar_equipes_da_competicao(nome_competicao) or [])

    if len(grupos) < qtd:
        return ResultadoGrupos(False, "Não foi possível criar todos os grupos configurados.")
    if not equipes:
        return ResultadoGrupos(False, "Cadastre as equipes antes de sortear os grupos.")

    embaralhador = rng or random
    embaralhador.shuffle(equipes)
    distribuicao = distribuir_equipes_balanceado(equipes, grupos)
    grupos_service.substituir_distribuicao_equipes(nome_competicao, distribuicao)
    limpar_cache(nome_competicao)
    resumo = resumo_distribuicao(distribuicao, grupos)
    return ResultadoGrupos(
        True,
        f"Sorteio realizado: {len(equipes)} equipe(s) distribuída(s). {resumo}.",
        True,
    )
