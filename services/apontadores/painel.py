"""Coordenação leve do painel inicial e da lista de partidas do apontador."""
from __future__ import annotations
from typing import Any, Callable, Mapping

from rules.apontador_painel import partida_leve, resolver_modo_operacao, resolver_regra_partida, resumo_regra, fase_normalizada, montar_rodadas_exibicao


def resolver_cpf_sessao(sessao: Mapping[str, Any], somente_digitos: Callable[[Any], str], login_fallback: str = "") -> str:
    for chave in ("cpf", "usuario_cpf", "cpf_usuario", "apontador_cpf", "usuario", "usuario_login", "login"):
        limpo = somente_digitos(sessao.get(chave) or "")
        if len(limpo) == 11:
            return limpo
    return somente_digitos(login_fallback or "")


def contexto_home(dados_home: Mapping[str, Any] | None) -> dict:
    dados = dados_home or {}
    base = {
        "pode_jogo_avulso": bool(dados.get("pode_jogo_avulso")),
        "offline_habilitado": bool(dados.get("offline_habilitado")),
    }
    competicoes = list(dados.get("competicoes") or [])
    if len(competicoes) == 1:
        base["competicao_unica"] = competicoes[0]
    elif len(competicoes) > 1:
        base["competicoes"] = competicoes
    return base


def preparar_partidas_painel(
    partidas: list[dict], competicao_cfg: Mapping[str, Any] | None, config_avancada: Mapping[str, Any] | None,
    rodadas_configuradas: list[dict], sets_max_manual: int,
    normalizar_escudo: Callable[[Any], Any] | None = None, escudo_padrao: str = "",
) -> dict:
    ordenadas = sorted(partidas or [], key=lambda x: (
        x.get("rodada") if x.get("rodada") is not None else 999999,
        x.get("ordem") if x.get("ordem") is not None else 999999,
        x.get("id") if x.get("id") is not None else 999999,
    ))
    leves = []
    max_sets = int(sets_max_manual or 1)
    for partida in ordenadas:
        partida = dict(partida or {})
        partida["modo_operacao_resolvido"] = resolver_modo_operacao(competicao_cfg, config_avancada, partida)
        partida["permite_scout"] = partida["modo_operacao_resolvido"] == "avancado"
        partida["fase_normalizada"] = fase_normalizada(partida)
        regra = resolver_regra_partida(competicao_cfg, config_avancada, partida)
        regra["modo_operacao"] = partida["modo_operacao_resolvido"] or regra.get("modo_operacao") or "simples"
        partida.update({
            "sets_tipo": regra.get("sets_tipo") or "melhor_de_3",
            "pontos_set": regra.get("pontos_set") or 25,
            "pontos_tiebreak": regra.get("pontos_tiebreak") or 15,
            "resumo_regra": resumo_regra(regra),
        })
        if partida["sets_tipo"] == "melhor_de_5":
            max_sets = max(max_sets, 5)
        elif partida["sets_tipo"] == "melhor_de_3":
            max_sets = max(max_sets, 3)
        else:
            try:
                max_sets = max(max_sets, int(partida.get("sets_max") or 0))
            except Exception:
                pass
        leves.append(partida_leve(partida, normalizar_escudo, escudo_padrao))
    return {
        "partidas": leves,
        "rodadas_exibicao": montar_rodadas_exibicao(leves, rodadas_configuradas),
        "sets_max_manual": max_sets,
        "sets_para_vencer_manual": 3 if max_sets >= 5 else (2 if max_sets >= 3 else 1),
    }
