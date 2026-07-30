"""Montagem do contexto das abas da tabela da competição.

Este módulo não conhece Flask, sessão ou templates. Ele recebe os dados da
requisição já normalizados e um conjunto explícito de provedores. Isso deixa
``routes/tabela.py`` responsável apenas pela camada HTTP e impede que a rota
volte a concentrar consultas, regras de exibição e montagem de contexto.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, MutableMapping, Optional


Contexto = Dict[str, Any]
Provedores = Mapping[str, Callable[..., Any]]


ABAS_VALIDAS = {"geracao", "partidas", "classificacao", "visualizador"}
FASES_VALIDAS = {
    "classificatorias",
    "oitavas",
    "quartas",
    "semifinal",
    "final",
    "terceiro_lugar",
}


def normalizar_aba(valor: Any) -> str:
    aba = str(valor or "geracao").strip().lower()
    return aba if aba in ABAS_VALIDAS else "geracao"


def normalizar_fase(valor: Any, canonizar: Callable[[Any], str]) -> str:
    fase = canonizar(valor or "classificatorias")
    return fase if fase in FASES_VALIDAS else "classificatorias"


def contexto_base(
    *,
    competicao: Mapping[str, Any],
    aba: str,
    fase_subaba: str,
    fase_labels: Mapping[str, Any],
    fases_disponiveis: Mapping[str, Any],
    competicao_travada: bool,
    grupos_travados: bool,
    fase_atual_travada: bool,
    fase_banco_ativa: str,
) -> Contexto:
    return {
        "competicao": competicao,
        "aba_ativa": aba,
        "fase_ativa": fase_subaba,
        "fase_labels": fase_labels,
        "competicao_travada": competicao_travada,
        "grupos_travados": grupos_travados,
        "fase_atual_travada": fase_atual_travada,
        "fase_banco_ativa": fase_banco_ativa,
        "grupos": [],
        "equipes": [],
        "quadras": [],
        "partidas": [],
        "partidas_fase": [],
        "classificacao": {},
        "criterios_classificacao": [],
        "colunas_classificacao": [],
        "avanco": {},
        "avanco_status": {"gerado": False},
        "avanco_fases_tabs": [],
        "avanco_series_fase": [],
        "avanco_serie_ativa": "",
        "avanco_espelho": [],
        "config_agenda": None,
        "config_geracao": None,
        "grupo_unico_auto": False,
        "quadra_unica_auto": False,
        "codigo_publico": "",
        "link_publico_path": "",
        "link_publico": "",
        **dict(fases_disponiveis or {}),
    }


def _quadra_unica(quadras: Any) -> bool:
    return len([q for q in (quadras or []) if q.get("ativa") is not False]) == 1


def montar_pacote_geracao(
    competicao: Mapping[str, Any],
    nome_competicao: str,
    p: Provedores,
) -> Contexto:
    quadras = p["quadras"](nome_competicao, competicao.get("qtd_quadras") or 1)
    grupos_raw = p["grupos"](nome_competicao)
    equipes = p["equipes"](nome_competicao)
    grupos = p["grupos_com_equipes"](nome_competicao, grupos_raw)
    config_agenda = p["config_agenda"](nome_competicao)
    return {
        "grupo_unico_auto": p["estrutura_grupo_unico"](competicao),
        "quadra_unica_auto": _quadra_unica(quadras),
        "grupos": grupos,
        "equipes": equipes,
        "quadras": quadras,
        "config_agenda": config_agenda,
        "config_geracao": config_agenda,
    }


def montar_pacote_partidas(
    competicao: Mapping[str, Any],
    nome_competicao: str,
    fase_subaba: str,
    serie_param: str,
    p: Provedores,
) -> Contexto:
    avanco = p["avanco"](nome_competicao)
    status_avanco = dict(p["status_avanco"](nome_competicao) or {})
    avanco_gerado = bool(p["avanco_gerado"](nome_competicao))
    status_avanco["gerado"] = avanco_gerado

    avanco_fases_tabs = p["fases_avanco"](avanco)
    series_fase = p["series_avanco"](avanco, fase_subaba) if fase_subaba != "classificatorias" else []
    serie_ativa = str(serie_param or "").strip().lower()
    if series_fase and not any(s.get("id") == serie_ativa for s in series_fase):
        serie_ativa = series_fase[0].get("id")

    quadras = p["quadras"](nome_competicao, competicao.get("qtd_quadras") or 1)
    grupos_raw = p["grupos"](nome_competicao)
    equipes = p["equipes"](nome_competicao)
    mapa_escudos = p["mapa_escudos"](equipes)
    partidas = p["listar_partidas_frescas"](nome_competicao) or []
    if not avanco_gerado:
        partidas = [partida for partida in partidas if not p["partida_eh_avanco"](partida)]

    grupos = p["grupos_com_equipes"](nome_competicao, grupos_raw)
    partidas_preparadas = p["preparar_partidas"](partidas, mapa_escudos, competicao)
    partidas_fase = p["filtrar_partidas_fase"](partidas_preparadas, fase_subaba)
    if fase_subaba != "classificatorias":
        partidas_fase = (
            p["filtrar_partidas_serie"](partidas_fase, serie_ativa)
            if avanco_gerado
            else []
        )
    avanco_espelho = p["montar_espelho_avanco"](avanco, partidas_preparadas, avanco_gerado)
    config_agenda = p["config_agenda"](nome_competicao)

    return {
        "grupo_unico_auto": p["estrutura_grupo_unico"](competicao),
        "quadra_unica_auto": _quadra_unica(quadras),
        "grupos": grupos,
        "equipes": equipes,
        "quadras": quadras,
        "partidas": partidas_preparadas,
        "partidas_fase": partidas_fase,
        "avanco": avanco,
        "avanco_status": status_avanco,
        "avanco_fases_tabs": avanco_fases_tabs,
        "avanco_series_fase": series_fase,
        "avanco_serie_ativa": serie_ativa,
        "avanco_espelho": avanco_espelho,
        "config_agenda": config_agenda,
        "config_geracao": config_agenda,
    }


def montar_pacote_classificacao(
    competicao: Mapping[str, Any],
    nome_competicao: str,
    p: Provedores,
) -> Contexto:
    grupos_raw = p["grupos"](nome_competicao)
    equipes = p["equipes"](nome_competicao)
    mapa_escudos = p["mapa_escudos"](equipes)
    partidas = p["partidas_cache"](nome_competicao)
    partidas_preparadas = p["preparar_partidas"](partidas, mapa_escudos, competicao)
    grupos = p["grupos_com_equipes"](nome_competicao, grupos_raw)
    classificacao, _veio_cache = p["calcular_classificacao"](
        nome_competicao,
        partidas_preparadas,
        grupos,
        competicao,
        mapa_escudos,
    )
    regras = p["regras_classificacao"](competicao)
    criterios = p["criterios_classificacao"](regras.get("criterios"))
    colunas = p["colunas_classificacao"](criterios)
    return {
        "grupos": grupos,
        "equipes": equipes,
        "partidas": partidas_preparadas,
        "classificacao": classificacao,
        "criterios_classificacao": criterios,
        "colunas_classificacao": colunas,
    }


def montar_pacote_visualizador(
    nome_competicao: str,
    host_url: str,
    p: Provedores,
) -> Contexto:
    codigo_publico = p["garantir_codigo_publico"](nome_competicao)
    link_publico_path = p["url_publico_curto"](codigo_publico) if codigo_publico else p["url_publico_fallback"](nome_competicao)
    return {
        "codigo_publico": codigo_publico or "",
        "link_publico_path": link_publico_path,
        "link_publico": str(host_url or "").rstrip("/") + link_publico_path,
    }


def montar_pacote_aba(
    *,
    aba: str,
    competicao: Mapping[str, Any],
    nome_competicao: str,
    fase_subaba: str,
    serie_param: str,
    host_url: str,
    provedores: Provedores,
) -> Contexto:
    if aba == "geracao":
        return montar_pacote_geracao(competicao, nome_competicao, provedores)
    if aba == "partidas":
        return montar_pacote_partidas(
            competicao,
            nome_competicao,
            fase_subaba,
            serie_param,
            provedores,
        )
    if aba == "classificacao":
        return montar_pacote_classificacao(competicao, nome_competicao, provedores)
    if aba == "visualizador":
        return montar_pacote_visualizador(nome_competicao, host_url, provedores)
    return {}
