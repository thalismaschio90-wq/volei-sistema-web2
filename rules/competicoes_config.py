"""Regras puras para configurações de competições.

Este módulo não acessa Flask nem PostgreSQL. Ele apenas normaliza e valida
valores usados pelas configurações avançadas e pelo motor de agenda.
"""
from __future__ import annotations

import json
from copy import deepcopy

MODOS_DISTRIBUICAO = {"grupo_fixo", "quadras_compartilhadas", "automatico_inteligente"}
RODIZIOS_GRUPOS = {"por_rodada", "alternado_inteligente", "por_grupo_inteiro"}


def configuracao_agenda_padrao() -> dict:
    return {
        "modo_distribuicao": "automatico_inteligente",
        "descanso_minimo_jogos": 1,
        "rodizio_grupos": "por_rodada",
        "permitir_relaxar_descanso": True,
        "grupos_compartilhados": {},
        "quadras_compartilhadas": [],
        "usar_rodadas_programadas": False,
        "uma_partida_por_equipe_rodada": True,
    }


def normalizar_json_config(valor, padrao):
    if valor in (None, ""):
        return deepcopy(padrao)
    if isinstance(valor, (dict, list)):
        return deepcopy(valor)
    try:
        convertido = json.loads(valor)
    except (TypeError, ValueError, json.JSONDecodeError):
        return deepcopy(padrao)
    if not isinstance(convertido, type(padrao)):
        return deepcopy(padrao)
    return convertido


def normalizar_configuracao_agenda(dados: dict | None) -> dict:
    dados = dados or {}
    padrao = configuracao_agenda_padrao()

    modo = str(dados.get("modo_distribuicao") or padrao["modo_distribuicao"]).strip().lower()
    if modo not in MODOS_DISTRIBUICAO:
        modo = padrao["modo_distribuicao"]

    rodizio = str(dados.get("rodizio_grupos") or padrao["rodizio_grupos"]).strip().lower()
    if rodizio not in RODIZIOS_GRUPOS:
        rodizio = padrao["rodizio_grupos"]

    try:
        descanso = int(dados.get("descanso_minimo_jogos", padrao["descanso_minimo_jogos"]))
    except (TypeError, ValueError):
        descanso = padrao["descanso_minimo_jogos"]
    descanso = max(0, min(descanso, 5))

    return {
        "modo_distribuicao": modo,
        "descanso_minimo_jogos": descanso,
        "rodizio_grupos": rodizio,
        "permitir_relaxar_descanso": bool(dados.get("permitir_relaxar_descanso", True)),
        "grupos_compartilhados": normalizar_json_config(dados.get("grupos_compartilhados", dados.get("grupos_compartilhados_json")), {}),
        "quadras_compartilhadas": normalizar_json_config(dados.get("quadras_compartilhadas", dados.get("quadras_compartilhadas_json")), []),
        "usar_rodadas_programadas": bool(dados.get("usar_rodadas_programadas", False)),
        "uma_partida_por_equipe_rodada": bool(dados.get("uma_partida_por_equipe_rodada", True)),
    }


def fases_padrao_configuracao_avancada(config: dict | None = None) -> dict:
    config = config or {}
    return {
        "tipo_confronto": config.get("tipo_confronto") or "grupo_interno",
        "tipo_classificacao": config.get("tipo_classificacao") or "grupo",
        "cruzamentos_grupos": config.get("cruzamentos_grupos") or "",
        "grupos": {"tipo_jogo": "set_unico", "pontos": 25, "tem_tiebreak": False, "pontos_tiebreak": 15},
        "grupos_especificos": {
            "A": {"tipo_jogo": "", "pontos": ""},
            "B": {"tipo_jogo": "", "pontos": ""},
            "C": {"tipo_jogo": "", "pontos": ""},
            "D": {"tipo_jogo": "", "pontos": ""},
        },
        "quartas": {"tipo_jogo": "melhor_de_3", "pontos": 21, "tem_tiebreak": True, "pontos_tiebreak": 15},
        "semifinal": {"tipo_jogo": "melhor_de_3", "pontos": 21, "tem_tiebreak": True, "pontos_tiebreak": 15},
        "final": {"tipo_jogo": "melhor_de_3", "pontos": 25, "tem_tiebreak": True, "pontos_tiebreak": 15},
    }


def normalizar_fases_config(valor) -> dict:
    return normalizar_json_config(valor, {})
