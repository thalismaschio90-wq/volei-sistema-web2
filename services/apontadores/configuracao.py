"""Configuração operacional das partidas do apontador.

Centraliza cache curto da competição, resolução de modo de operação, regras de
sets e limites de tempos/substituições. O módulo não conhece Flask nem a rota.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Mapping

_CACHE_TTL = 30.0
_lock = threading.RLock()
_cache_competicoes: dict[str, tuple[float, dict[str, Any]]] = {}


def buscar_competicao(competicao: str) -> dict[str, Any]:
    chave = str(competicao or "").strip()
    if not chave:
        return {}
    agora = time.time()
    with _lock:
        item = _cache_competicoes.get(chave)
        if item and (agora - item[0]) < _CACHE_TTL:
            return dict(item[1])

    try:
        from banco import buscar_competicao_por_nome
        dados = dict(buscar_competicao_por_nome(chave) or {})
    except Exception:
        dados = {}

    with _lock:
        _cache_competicoes[chave] = (agora, dict(dados))
    return dados


def invalidar_competicao(competicao: str | None = None) -> None:
    with _lock:
        if competicao:
            _cache_competicoes.pop(str(competicao).strip(), None)
        else:
            _cache_competicoes.clear()


def buscar_configuracao_avancada(competicao: str) -> dict[str, Any]:
    try:
        from banco import buscar_configuracao_avancada_competicao
        return dict(buscar_configuracao_avancada_competicao(competicao) or {})
    except Exception:
        return {}


def _modo_valido(valor: Any) -> str:
    texto = str(valor or "").strip().lower()
    return texto if texto in {"simples", "avancado"} else ""


def _modo_regra(regra: Any) -> str:
    if not isinstance(regra, Mapping):
        return ""
    return _modo_valido(regra.get("modo_operacao") or regra.get("scout"))


def _buscar_chave(dic: Any, chave: Any) -> dict[str, Any]:
    if not isinstance(dic, Mapping) or not chave:
        return {}
    if chave in dic:
        valor = dic.get(chave) or {}
        return dict(valor) if isinstance(valor, Mapping) else {}
    chave_low = str(chave).lower()
    for nome, valor in dic.items():
        if str(nome).lower() == chave_low:
            return dict(valor or {}) if isinstance(valor, Mapping) else {}
    return {}


def normalizar_fase(fase: Any) -> str:
    texto = str(fase or "grupos").strip().lower()
    aliases = {
        "grupo": "grupos",
        "classificatoria": "grupos",
        "classificatória": "grupos",
        "classificatorias": "grupos",
        "classificatórias": "grupos",
        "semifinais": "semifinal",
        "semi": "semifinal",
        "semis": "semifinal",
        "finais": "final",
        "finalissima": "final",
        "finalíssima": "final",
    }
    return aliases.get(texto, texto or "grupos")


def resolver_modo_operacao(competicao: str, partida: Mapping[str, Any] | None = None) -> str:
    partida = dict(partida or {})
    comp = buscar_competicao(competicao)
    modo_partida = _modo_valido(partida.get("modo_operacao"))
    modo_competicao = _modo_valido(comp.get("modo_operacao"))
    fallback = modo_partida or modo_competicao or "simples"

    config = buscar_configuracao_avancada(competicao)
    fases_config = config.get("fases_config") or {}
    regras_avancadas = fases_config.get("regras_avancadas") or {}
    origem = str(partida.get("origem") or "").strip()

    if origem.startswith("avanco:"):
        partes = origem.split(":")
        serie_id = (partes[1] if len(partes) > 1 else "").strip().lower()
        jogo_id = (partes[2] if len(partes) > 2 else "").strip().upper()

        modo = _modo_regra(_buscar_chave(regras_avancadas.get("jogos") or {}, f"{serie_id}:{jogo_id}"))
        if modo:
            return modo
        modo = _modo_regra(_buscar_chave(regras_avancadas.get("series") or {}, serie_id))
        if modo:
            return modo

        avanco = fases_config.get("avanco") or {}
        for jogo in avanco.get("jogos") or []:
            if str(jogo.get("serie") or "").strip().lower() == serie_id and str(jogo.get("id") or "").strip().upper() == jogo_id:
                regra = jogo.get("regra") or {}
                if regra.get("usar_regra_propria"):
                    modo = _modo_regra(regra)
                    if modo:
                        return modo
                break
        for serie in avanco.get("series") or []:
            if str(serie.get("id") or "").strip().lower() == serie_id:
                modo = _modo_regra(serie.get("regra") or {})
                if modo:
                    return modo
                break

    fase_id = normalizar_fase(partida.get("fase"))
    modo = _modo_regra(_buscar_chave(regras_avancadas.get("fases") or {}, fase_id))
    if modo:
        return modo
    if fase_id == "grupos":
        grupo = str(partida.get("grupo") or "").strip().upper()
        modo = _modo_regra(_buscar_chave(regras_avancadas.get("grupos") or {}, grupo))
        if modo:
            return modo
    return fallback


def sets_max(competicao: str) -> int:
    tipo = str(buscar_competicao(competicao).get("sets_tipo") or "melhor_de_3").strip().lower()
    if tipo == "set_unico":
        return 1
    if tipo == "melhor_de_5":
        return 5
    return 3


def sets_para_vencer(competicao: str) -> int:
    maximo = sets_max(competicao)
    return 3 if maximo == 5 else 2 if maximo == 3 else 1


def _inteiro(valor: Any, padrao: int) -> int:
    try:
        if valor in (None, ""):
            return padrao
        return int(valor)
    except (TypeError, ValueError):
        return padrao


def limites_operacionais(partida: Mapping[str, Any] | None = None, estado: Mapping[str, Any] | None = None) -> dict[str, int]:
    partida = partida or {}
    estado = estado or {}

    def primeiro(*valores: Any, padrao: Any = None) -> Any:
        for valor in valores:
            if valor is not None and valor != "":
                return valor
        return padrao

    tempos = _inteiro(primeiro(
        partida.get("tempos_por_set"), partida.get("limite_tempos"), partida.get("tempos_limite"),
        estado.get("tempos_por_set"), estado.get("limite_tempos"), estado.get("tempos_limite"), padrao=2,
    ), 2)
    substituicoes = _inteiro(primeiro(
        partida.get("substituicoes_por_set"), partida.get("limite_substituicoes"), partida.get("substituicoes_limite"),
        estado.get("substituicoes_por_set"), estado.get("limite_substituicoes"), estado.get("substituicoes_limite"), padrao=6,
    ), 6)
    return {"limite_tempos": max(0, tempos), "limite_substituicoes": max(0, substituicoes)}


__all__ = [
    "buscar_competicao",
    "buscar_configuracao_avancada",
    "invalidar_competicao",
    "limites_operacionais",
    "normalizar_fase",
    "resolver_modo_operacao",
    "sets_max",
    "sets_para_vencer",
]
