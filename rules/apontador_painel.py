"""Regras puras para os cards e listas do painel do apontador.

Este módulo não acessa Flask, sessão, banco ou Socket.IO.
"""
from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping

MODOS_OPERACAO = {"simples", "avancado"}
SETS_TIPOS = {"set_unico", "melhor_de_3", "melhor_de_5"}


def fase_normalizada(partida: Mapping[str, Any] | None) -> str:
    partida = partida or {}
    fase_txt = str(partida.get("fase") or partida.get("fase_partida") or "grupos").strip().lower()
    if fase_txt in {"grupo", "grupos", "classificatoria", "classificatória", "classificatorias", "classificatórias"}:
        return "grupos"
    if "quarta" in fase_txt:
        return "quartas"
    if "semi" in fase_txt:
        return "semifinal"
    if "terceiro" in fase_txt or ("3" in fase_txt and "lugar" in fase_txt):
        return "terceiro_lugar"
    if "final" in fase_txt:
        return "final"
    return fase_txt or "grupos"


def normalizar_fase_operacao(fase: Any) -> str:
    return fase_normalizada({"fase": fase})


def _modo_valido(valor: Any) -> str:
    valor = str(valor or "").strip().lower()
    return valor if valor in MODOS_OPERACAO else ""


def _buscar_chave_case_insensitive(dic: Any, chave: Any) -> dict:
    if not isinstance(dic, dict) or not chave:
        return {}
    if chave in dic:
        return dic.get(chave) or {}
    chave_low = str(chave).lower()
    for key, value in dic.items():
        if str(key).lower() == chave_low:
            return value or {}
    return {}


def resolver_modo_operacao(competicao_cfg: Mapping[str, Any] | None, config_avancada: Mapping[str, Any] | None, partida: Mapping[str, Any] | None) -> str:
    partida = partida or {}
    competicao_cfg = competicao_cfg or {}

    def modo_regra(regra: Any) -> str:
        if not isinstance(regra, dict):
            return ""
        return _modo_valido(regra.get("modo_operacao") or regra.get("scout"))

    fallback = _modo_valido(partida.get("modo_operacao")) or _modo_valido(competicao_cfg.get("modo_operacao")) or "simples"
    try:
        fases_config = (config_avancada or {}).get("fases_config") or {}
        regras = fases_config.get("regras_avancadas") or {}
        origem = str(partida.get("origem") or "").strip()
        if origem.startswith("avanco:"):
            partes = origem.split(":")
            serie_id = (partes[1] if len(partes) > 1 else "").strip().lower()
            jogo_id = (partes[2] if len(partes) > 2 else "").strip().upper()
            modo = modo_regra(_buscar_chave_case_insensitive(regras.get("jogos") or {}, f"{serie_id}:{jogo_id}"))
            if modo:
                return modo
            modo = modo_regra(_buscar_chave_case_insensitive(regras.get("series") or {}, serie_id))
            if modo:
                return modo
            avanco = fases_config.get("avanco") or {}
            for jogo in avanco.get("jogos") or []:
                if str(jogo.get("serie") or "").strip().lower() == serie_id and str(jogo.get("id") or "").strip().upper() == jogo_id:
                    regra = jogo.get("regra") or {}
                    if regra.get("usar_regra_propria"):
                        modo = modo_regra(regra)
                        if modo:
                            return modo
                    break
            for serie in avanco.get("series") or []:
                if str(serie.get("id") or "").strip().lower() == serie_id:
                    modo = modo_regra(serie.get("regra") or {})
                    if modo:
                        return modo
                    break
        fase_id = normalizar_fase_operacao(partida.get("fase"))
        modo = modo_regra(_buscar_chave_case_insensitive(regras.get("fases") or {}, fase_id))
        if modo:
            return modo
        if fase_id == "grupos":
            grupo = str(partida.get("grupo") or "").strip().upper()
            modo = modo_regra(_buscar_chave_case_insensitive(regras.get("grupos") or {}, grupo))
            if modo:
                return modo
    except Exception:
        pass
    return fallback


def normalizar_sets_tipo(valor: Any, padrao: str = "melhor_de_3") -> str:
    valor = str(valor or padrao or "melhor_de_3").strip().lower()
    aliases = {
        "set único": "set_unico", "set unico": "set_unico", "unico": "set_unico", "único": "set_unico",
        "1_set": "set_unico", "melhor_de_1": "set_unico", "melhor de 3": "melhor_de_3", "md3": "melhor_de_3",
        "m3": "melhor_de_3", "melhor de 5": "melhor_de_5", "md5": "melhor_de_5", "m5": "melhor_de_5",
    }
    valor = aliases.get(valor, valor)
    if valor not in SETS_TIPOS:
        return padrao if padrao in SETS_TIPOS else "melhor_de_3"
    return valor


def inteiro_positivo(*valores: Any, padrao: int = 0) -> int:
    for valor in valores:
        if valor not in (None, ""):
            try:
                numero = int(valor)
                if numero > 0:
                    return numero
            except Exception:
                pass
    return int(padrao or 0)


def aplicar_regra(resultado: dict, regra: Any) -> dict:
    if not isinstance(regra, dict) or not regra:
        return resultado
    sets_tipo = str(regra.get("sets_tipo") or regra.get("tipo_partida") or "").strip().lower()
    if sets_tipo and sets_tipo != "padrao":
        resultado["sets_tipo"] = normalizar_sets_tipo(sets_tipo, resultado.get("sets_tipo"))
    pontos_set = inteiro_positivo(regra.get("pontos_set"), regra.get("ponto_alvo_set"), regra.get("pontos_para_vencer_set"), padrao=0)
    if pontos_set:
        resultado["pontos_set"] = pontos_set
    pontos_tb = inteiro_positivo(regra.get("pontos_tiebreak"), regra.get("pontos_tb"), regra.get("tiebreak"), padrao=0)
    if pontos_tb:
        resultado["pontos_tiebreak"] = pontos_tb
    modo = _modo_valido(regra.get("modo_operacao") or regra.get("scout"))
    if modo:
        resultado["modo_operacao"] = modo
    return resultado


def resolver_regra_partida(competicao_cfg: Mapping[str, Any] | None, config_avancada: Mapping[str, Any] | None, partida: Mapping[str, Any] | None) -> dict:
    partida = partida or {}
    cfg = competicao_cfg or {}
    resultado = {
        "sets_tipo": normalizar_sets_tipo(partida.get("sets_tipo") or partida.get("tipo_partida") or partida.get("formato_jogo") or cfg.get("sets_tipo") or "melhor_de_3"),
        "pontos_set": inteiro_positivo(partida.get("pontos_set"), cfg.get("pontos_set"), padrao=25),
        "pontos_tiebreak": inteiro_positivo(partida.get("pontos_tiebreak"), cfg.get("pontos_tiebreak"), padrao=15),
        "modo_operacao": _modo_valido(partida.get("modo_operacao_resolvido") or partida.get("modo_operacao") or cfg.get("modo_operacao")) or "simples",
    }
    try:
        fases_config = (config_avancada or {}).get("fases_config") or {}
        regras = fases_config.get("regras_avancadas") or {}
        origem = str(partida.get("origem") or "").strip()
        if origem.startswith("avanco:"):
            partes = origem.split(":")
            serie_id = partes[1] if len(partes) > 1 else ""
            jogo_id = partes[2] if len(partes) > 2 else ""
            aplicar_regra(resultado, (regras.get("series") or {}).get(serie_id) or {})
            aplicar_regra(resultado, (regras.get("jogos") or {}).get(f"{serie_id}:{jogo_id}") or {})
        else:
            fase_id = normalizar_fase_operacao(partida.get("fase"))
            aplicar_regra(resultado, (regras.get("fases") or {}).get(fase_id) or {})
            if fase_id == "grupos":
                grupo = str(partida.get("grupo") or "").strip().upper()
                aplicar_regra(resultado, (regras.get("grupos") or {}).get(grupo) or {})
    except Exception:
        pass
    resultado["sets_tipo"] = normalizar_sets_tipo(resultado.get("sets_tipo"))
    resultado["pontos_set"] = inteiro_positivo(resultado.get("pontos_set"), padrao=25)
    resultado["pontos_tiebreak"] = inteiro_positivo(resultado.get("pontos_tiebreak"), padrao=15)
    resultado["modo_operacao"] = _modo_valido(resultado.get("modo_operacao")) or "simples"
    return resultado


def resumo_regra(regra: Mapping[str, Any] | None) -> str:
    regra = regra or {}
    sets_tipo = normalizar_sets_tipo(regra.get("sets_tipo"))
    pontos_set = inteiro_positivo(regra.get("pontos_set"), padrao=25)
    pontos_tb = inteiro_positivo(regra.get("pontos_tiebreak"), padrao=15)
    modo = _modo_valido(regra.get("modo_operacao")) or "simples"
    sigla = "M5" if sets_tipo == "melhor_de_5" else ("SU" if sets_tipo == "set_unico" else "M3")
    partes = [sigla, f"{pontos_set}PTS"]
    if sets_tipo != "set_unico":
        partes.append(f"TB{pontos_tb}")
    if modo == "avancado":
        partes.append("SCOUT")
    return " • ".join(partes)


CAMPOS_LEVES = {
    "id", "competicao", "ordem", "rodada", "grupo", "fase", "fase_partida", "origem", "equipe_a", "equipe_b",
    "equipe_a_operacional", "equipe_b_operacional", "escudo_a", "escudo_b", "escudo_equipe_a", "escudo_equipe_b",
    "quadra", "ginasio", "local", "data", "hora", "data_hora", "status", "status_jogo", "status_operacao",
    "operador_login", "operador_nome", "placar", "placar_exibicao", "placar_sets", "placar_pontos", "resultado",
    "sets_a", "sets_b", "pontos_a", "pontos_b", "pontos_equipe_a", "pontos_equipe_b", "set_atual", "set1_a",
    "set1_b", "set2_a", "set2_b", "set3_a", "set3_b", "set4_a", "set4_b", "set5_a", "set5_b", "vencedor",
    "tipo_encerramento", "origem_resultado", "modo_operacao", "tempos_por_set", "substituicoes_por_set", "limite_tempos",
    "limite_substituicoes", "pontos_set", "pontos_tiebreak", "diferenca_minima", "sets_tipo", "sets_max",
    "sets_para_vencer", "resumo_regra",
}


def partida_leve(partida: Mapping[str, Any] | None, normalizar_escudo: Callable[[Any], Any] | None = None, escudo_padrao: str = "") -> dict:
    partida = partida or {}
    item: dict[str, Any] = {}
    for campo in CAMPOS_LEVES:
        if campo not in partida:
            continue
        valor = partida.get(campo)
        if campo.startswith("escudo"):
            valor = normalizar_escudo(valor) if valor and normalizar_escudo else (valor or escudo_padrao)
        elif isinstance(valor, str) and len(valor) > 700:
            valor = valor[:700]
        item[campo] = valor
    item["modo_operacao_resolvido"] = partida.get("modo_operacao_resolvido") or "simples"
    item["permite_scout"] = bool(partida.get("permite_scout"))
    item["fase_normalizada"] = partida.get("fase_normalizada") or fase_normalizada(partida)
    item["sets_tipo"] = partida.get("sets_tipo") or item.get("sets_tipo") or "melhor_de_3"
    item["pontos_set"] = partida.get("pontos_set") or item.get("pontos_set") or 25
    item["pontos_tiebreak"] = partida.get("pontos_tiebreak") or item.get("pontos_tiebreak") or 15
    item["resumo_regra"] = partida.get("resumo_regra") or resumo_regra({
        "sets_tipo": item["sets_tipo"], "pontos_set": item["pontos_set"], "pontos_tiebreak": item["pontos_tiebreak"],
        "modo_operacao": item["modo_operacao_resolvido"],
    })
    return item


def montar_rodadas_exibicao(partidas: Iterable[Mapping[str, Any]], configuradas: Iterable[Mapping[str, Any]] | None = None) -> list[dict]:
    partidas = list(partidas or [])
    por_numero: dict[int, Mapping[str, Any]] = {}
    for rodada in configuradas or []:
        try:
            numero = int(rodada.get("numero_rodada") or 1)
        except (TypeError, ValueError):
            numero = 1
        por_numero.setdefault(numero, rodada)
    resultado, vistos = [], set()
    for partida in partidas:
        try:
            numero = int(partida.get("rodada") or 0)
        except (TypeError, ValueError):
            numero = 0
        chave = str(numero) if numero > 0 else "SEM_RODADA"
        if chave in vistos:
            continue
        vistos.add(chave)
        cfg = por_numero.get(numero, {}) if numero > 0 else {}
        resultado.append({
            "chave": chave, "numero": numero if numero > 0 else None,
            "nome": cfg.get("nome") or (f"Rodada {numero}" if numero > 0 else "Sem rodada definida"),
            "data": cfg.get("data") or "", "hora": cfg.get("hora") or "", "data_hora": cfg.get("data_hora") or "",
            "total": sum(1 for p in partidas if str(p.get("rodada") or "SEM_RODADA") == chave),
        })
    return resultado
