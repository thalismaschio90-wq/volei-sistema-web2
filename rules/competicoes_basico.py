"""Regras puras das configurações básicas de competições.

Não acessa Flask nem PostgreSQL. Normaliza apenas os campos que realmente
foram enviados pela rota, preservando o comportamento de atualizações parciais.
"""
from __future__ import annotations

from typing import Any, Mapping

CRITERIOS_DESEMPATE_PADRAO = (
    "vitorias,pontos,saldo_sets,sets_pro,sets_contra,saldo_pontos,"
    "pontos_pro,pontos_contra,confronto_direto,coef_sets,coef_pontos,"
    "fair_play,sorteio"
)

CAMPOS_DADOS_GERAIS = ("nome", "data", "status", "cidade", "ginasio", "categoria", "sexo", "divisao")
CAMPOS_ESTRUTURA = (
    "qtd_equipes", "formato", "tem_grupos", "qtd_grupos", "qtd_quadras",
    "modo_operacao", "tipo_confronto", "tipo_classificacao", "cruzamentos_grupos",
    "data_limite_inscricao", "hora_limite_inscricao", "bloquear_apos_inicio",
    "limite_atletas", "permitir_edicao_pos_prazo", "exigir_foto_atleta",
    "exigir_instagram_atleta",
)
CAMPOS_REGRAS_JOGO = (
    "sets_tipo", "pontos_set", "tem_tiebreak", "pontos_tiebreak",
    "diferenca_minima", "tempos_por_set", "substituicoes_por_set",
)
CAMPOS_PONTUACAO = (
    "vitoria_set_unico", "derrota_set_unico", "vitoria_2x0", "vitoria_2x1",
    "derrota_1x2", "derrota_0x2", "vitoria_3x0", "vitoria_3x1",
    "vitoria_3x2", "derrota_2x3", "derrota_1x3", "derrota_0x3",
)


def _parcial(dados: Mapping[str, Any] | None, campos: tuple[str, ...]) -> dict[str, Any]:
    if not isinstance(dados, Mapping):
        return {}
    return {campo: dados[campo] for campo in campos if campo in dados}


def normalizar_dados_gerais(dados: Mapping[str, Any] | None) -> dict[str, Any]:
    valores = _parcial(dados, CAMPOS_DADOS_GERAIS)
    for campo in ("nome", "status", "cidade", "ginasio", "categoria", "sexo", "divisao"):
        if campo in valores and valores[campo] is not None:
            valores[campo] = str(valores[campo]).strip()
    return valores


def normalizar_estrutura(dados: Mapping[str, Any] | None) -> dict[str, Any]:
    valores = _parcial(dados, CAMPOS_ESTRUTURA)
    for campo in ("data_limite_inscricao", "hora_limite_inscricao"):
        if campo in valores:
            valores[campo] = valores[campo] or None
    return valores


def normalizar_regras_jogo(dados: Mapping[str, Any] | None) -> dict[str, Any]:
    return _parcial(dados, CAMPOS_REGRAS_JOGO)


def normalizar_pontuacao_desempate(dados: Mapping[str, Any] | None) -> dict[str, Any]:
    valores = _parcial(dados, CAMPOS_PONTUACAO)
    if isinstance(dados, Mapping):
        criterios = dados.get("criterios_desempate", CRITERIOS_DESEMPATE_PADRAO)
    else:
        criterios = CRITERIOS_DESEMPATE_PADRAO
    valores["criterios_desempate"] = str(criterios or CRITERIOS_DESEMPATE_PADRAO).strip()
    return valores
