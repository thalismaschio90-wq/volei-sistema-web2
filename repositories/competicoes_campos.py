"""Campos SQL compatíveis da tabela de competições.

Centraliza a montagem das colunas sem depender da fachada ``banco.py``.
Novos ambientes devem manter o schema alinhado por migrações; os aliases
continuam preservando compatibilidade durante a transição.
"""
from __future__ import annotations

from core.schema_inspection import buscar_colunas_tabela

def campo_ou_alias(colunas, campo, alias_sql):
    if campo in colunas:
        return campo
    return alias_sql

def campos_competicao(prefixo="", incluir_senha_organizador=False):
    colunas = buscar_colunas_tabela("competicoes")
    p = f"{prefixo}." if prefixo else ""

    campos = [
        f"{p}nome",
        f"{p}data",
        f"{p}status",
        f"{p}organizador_login",
        campo_ou_alias(colunas, "codigo_publico", "NULL::text AS codigo_publico") if not prefixo else (
            f"{p}codigo_publico" if "codigo_publico" in colunas else "NULL::text AS codigo_publico"
        ),
        campo_ou_alias(colunas, "cidade", "'' AS cidade") if not prefixo else (
            f"{p}cidade" if "cidade" in colunas else "'' AS cidade"
        ),
        campo_ou_alias(colunas, "ginasio", "'' AS ginasio") if not prefixo else (
            f"{p}ginasio" if "ginasio" in colunas else "'' AS ginasio"
        ),
        campo_ou_alias(colunas, "categoria", "'' AS categoria") if not prefixo else (
            f"{p}categoria" if "categoria" in colunas else "'' AS categoria"
        ),
        campo_ou_alias(colunas, "sexo", "'' AS sexo") if not prefixo else (
            f"{p}sexo" if "sexo" in colunas else "'' AS sexo"
        ),
        campo_ou_alias(colunas, "divisao", "'' AS divisao") if not prefixo else (
            f"{p}divisao" if "divisao" in colunas else "'' AS divisao"
        ),
        campo_ou_alias(colunas, "qtd_equipes", "0 AS qtd_equipes") if not prefixo else (
            f"{p}qtd_equipes" if "qtd_equipes" in colunas else "0 AS qtd_equipes"
        ),
        campo_ou_alias(colunas, "formato", "'' AS formato") if not prefixo else (
            f"{p}formato" if "formato" in colunas else "'' AS formato"
        ),
        campo_ou_alias(colunas, "tem_grupos", "FALSE AS tem_grupos") if not prefixo else (
            f"{p}tem_grupos" if "tem_grupos" in colunas else "FALSE AS tem_grupos"
        ),
        campo_ou_alias(colunas, "qtd_grupos", "0 AS qtd_grupos") if not prefixo else (
            f"{p}qtd_grupos" if "qtd_grupos" in colunas else "0 AS qtd_grupos"
        ),
        campo_ou_alias(colunas, "qtd_quadras", "1 AS qtd_quadras") if not prefixo else (
            f"{p}qtd_quadras" if "qtd_quadras" in colunas else "1 AS qtd_quadras"
        ),
        campo_ou_alias(colunas, "modo_operacao", "'simples' AS modo_operacao") if not prefixo else (
            f"{p}modo_operacao" if "modo_operacao" in colunas else "'simples' AS modo_operacao"
        ),
        campo_ou_alias(colunas, "sets_tipo", "'melhor_de_3' AS sets_tipo") if not prefixo else (
            f"{p}sets_tipo" if "sets_tipo" in colunas else "'melhor_de_3' AS sets_tipo"
        ),
        campo_ou_alias(colunas, "pontos_set", "25 AS pontos_set") if not prefixo else (
            f"{p}pontos_set" if "pontos_set" in colunas else "25 AS pontos_set"
        ),
        campo_ou_alias(colunas, "tem_tiebreak", "TRUE AS tem_tiebreak") if not prefixo else (
            f"{p}tem_tiebreak" if "tem_tiebreak" in colunas else "TRUE AS tem_tiebreak"
        ),
        campo_ou_alias(colunas, "pontos_tiebreak", "15 AS pontos_tiebreak") if not prefixo else (
            f"{p}pontos_tiebreak" if "pontos_tiebreak" in colunas else "15 AS pontos_tiebreak"
        ),
        campo_ou_alias(colunas, "diferenca_minima", "2 AS diferenca_minima") if not prefixo else (
            f"{p}diferenca_minima" if "diferenca_minima" in colunas else "2 AS diferenca_minima"
        ),
        campo_ou_alias(colunas, "tempos_por_set", "2 AS tempos_por_set") if not prefixo else (
            f"{p}tempos_por_set" if "tempos_por_set" in colunas else "2 AS tempos_por_set"
        ),
        campo_ou_alias(colunas, "substituicoes_por_set", "6 AS substituicoes_por_set") if not prefixo else (
            f"{p}substituicoes_por_set" if "substituicoes_por_set" in colunas else "6 AS substituicoes_por_set"
        ),
        campo_ou_alias(colunas, "vitoria_set_unico", "2 AS vitoria_set_unico") if not prefixo else (
            f"{p}vitoria_set_unico" if "vitoria_set_unico" in colunas else "2 AS vitoria_set_unico"
        ),
        campo_ou_alias(colunas, "derrota_set_unico", "0 AS derrota_set_unico") if not prefixo else (
            f"{p}derrota_set_unico" if "derrota_set_unico" in colunas else "0 AS derrota_set_unico"
        ),
        campo_ou_alias(colunas, "vitoria_2x0", "3 AS vitoria_2x0") if not prefixo else (
            f"{p}vitoria_2x0" if "vitoria_2x0" in colunas else "3 AS vitoria_2x0"
        ),
        campo_ou_alias(colunas, "vitoria_2x1", "2 AS vitoria_2x1") if not prefixo else (
            f"{p}vitoria_2x1" if "vitoria_2x1" in colunas else "2 AS vitoria_2x1"
        ),
        campo_ou_alias(colunas, "derrota_1x2", "1 AS derrota_1x2") if not prefixo else (
            f"{p}derrota_1x2" if "derrota_1x2" in colunas else "1 AS derrota_1x2"
        ),
        campo_ou_alias(colunas, "derrota_0x2", "0 AS derrota_0x2") if not prefixo else (
            f"{p}derrota_0x2" if "derrota_0x2" in colunas else "0 AS derrota_0x2"
        ),
        campo_ou_alias(colunas, "vitoria_3x0", "3 AS vitoria_3x0") if not prefixo else (
            f"{p}vitoria_3x0" if "vitoria_3x0" in colunas else "3 AS vitoria_3x0"
        ),
        campo_ou_alias(colunas, "vitoria_3x1", "3 AS vitoria_3x1") if not prefixo else (
            f"{p}vitoria_3x1" if "vitoria_3x1" in colunas else "3 AS vitoria_3x1"
        ),
        campo_ou_alias(colunas, "vitoria_3x2", "2 AS vitoria_3x2") if not prefixo else (
            f"{p}vitoria_3x2" if "vitoria_3x2" in colunas else "2 AS vitoria_3x2"
        ),
        campo_ou_alias(colunas, "derrota_2x3", "1 AS derrota_2x3") if not prefixo else (
            f"{p}derrota_2x3" if "derrota_2x3" in colunas else "1 AS derrota_2x3"
        ),
        campo_ou_alias(colunas, "derrota_1x3", "0 AS derrota_1x3") if not prefixo else (
            f"{p}derrota_1x3" if "derrota_1x3" in colunas else "0 AS derrota_1x3"
        ),
        campo_ou_alias(colunas, "derrota_0x3", "0 AS derrota_0x3") if not prefixo else (
            f"{p}derrota_0x3" if "derrota_0x3" in colunas else "0 AS derrota_0x3"
        ),
        campo_ou_alias(
            colunas,
            "criterios_desempate",
            "'vitorias,pontos,saldo_sets,sets_pro,sets_contra,saldo_pontos,pontos_pro,pontos_contra,confronto_direto,coef_sets,coef_pontos,fair_play,sorteio' AS criterios_desempate"
        ) if not prefixo else (
            f"{p}criterios_desempate" if "criterios_desempate" in colunas else
            "'vitorias,pontos,saldo_sets,sets_pro,sets_contra,saldo_pontos,pontos_pro,pontos_contra,confronto_direto,coef_sets,coef_pontos,fair_play,sorteio' AS criterios_desempate"
        ),
        campo_ou_alias(colunas, "limite_atletas", "0 AS limite_atletas") if not prefixo else (
            f"{p}limite_atletas" if "limite_atletas" in colunas else "0 AS limite_atletas"
        ),
        campo_ou_alias(colunas, "permitir_edicao_pos_prazo", "FALSE AS permitir_edicao_pos_prazo") if not prefixo else (
            f"{p}permitir_edicao_pos_prazo" if "permitir_edicao_pos_prazo" in colunas else "FALSE AS permitir_edicao_pos_prazo"
        ),
        campo_ou_alias(colunas, "exigir_foto_atleta", "FALSE AS exigir_foto_atleta") if not prefixo else (
            f"{p}exigir_foto_atleta" if "exigir_foto_atleta" in colunas else "FALSE AS exigir_foto_atleta"
        ),
        campo_ou_alias(colunas, "exigir_instagram_atleta", "FALSE AS exigir_instagram_atleta") if not prefixo else (
            f"{p}exigir_instagram_atleta" if "exigir_instagram_atleta" in colunas else "FALSE AS exigir_instagram_atleta"
        ),
        campo_ou_alias(colunas, "aprovacao_automatica_atletas", "FALSE AS aprovacao_automatica_atletas") if not prefixo else (
            f"{p}aprovacao_automatica_atletas" if "aprovacao_automatica_atletas" in colunas else "FALSE AS aprovacao_automatica_atletas"
        ),
        campo_ou_alias(colunas, "travada", "FALSE AS travada") if not prefixo else (
            f"{p}travada" if "travada" in colunas else "FALSE AS travada"
        ),
        campo_ou_alias(colunas, "motivo_travamento", "'' AS motivo_travamento") if not prefixo else (
            f"{p}motivo_travamento" if "motivo_travamento" in colunas else "'' AS motivo_travamento"
        ),
        campo_ou_alias(colunas, "travada_em", "NULL::timestamp AS travada_em") if not prefixo else (
            f"{p}travada_em" if "travada_em" in colunas else "NULL::timestamp AS travada_em"
        ),
    ]

    campos.extend([
        campo_ou_alias(colunas, "tipo_classificacao", "'grupo' AS tipo_classificacao") if not prefixo else (
            f"{p}tipo_classificacao" if "tipo_classificacao" in colunas else "'grupo' AS tipo_classificacao"
        ),
        campo_ou_alias(colunas, "qtd_classificados", "0 AS qtd_classificados") if not prefixo else (
            f"{p}qtd_classificados" if "qtd_classificados" in colunas else "0 AS qtd_classificados"
        ),
        campo_ou_alias(colunas, "formato_finais", "'mata_mata' AS formato_finais") if not prefixo else (
            f"{p}formato_finais" if "formato_finais" in colunas else "'mata_mata' AS formato_finais"
        ),
        campo_ou_alias(colunas, "possui_bye", "FALSE AS possui_bye") if not prefixo else (
            f"{p}possui_bye" if "possui_bye" in colunas else "FALSE AS possui_bye"
        ),
        campo_ou_alias(colunas, "qtd_bye", "0 AS qtd_bye") if not prefixo else (
            f"{p}qtd_bye" if "qtd_bye" in colunas else "0 AS qtd_bye"
        ),
        campo_ou_alias(colunas, "fases_config", "'{}' AS fases_config") if not prefixo else (
            f"{p}fases_config" if "fases_config" in colunas else "'{}' AS fases_config"
        ),
        campo_ou_alias(colunas, "tipo_confronto", "'grupo_interno' AS tipo_confronto") if not prefixo else (
            f"{p}tipo_confronto" if "tipo_confronto" in colunas else "'grupo_interno' AS tipo_confronto"
        ),
        campo_ou_alias(colunas, "cruzamentos_grupos", "'' AS cruzamentos_grupos") if not prefixo else (
            f"{p}cruzamentos_grupos" if "cruzamentos_grupos" in colunas else "'' AS cruzamentos_grupos"
        ),
        campo_ou_alias(colunas, "data_limite_inscricao", "NULL AS data_limite_inscricao") if not prefixo else (
            f"{p}data_limite_inscricao" if "data_limite_inscricao" in colunas else "NULL AS data_limite_inscricao"
        ),
        campo_ou_alias(colunas, "hora_limite_inscricao", "NULL AS hora_limite_inscricao") if not prefixo else (
            f"{p}hora_limite_inscricao" if "hora_limite_inscricao" in colunas else "NULL AS hora_limite_inscricao"
        ),
        campo_ou_alias(colunas, "bloquear_apos_inicio", "FALSE AS bloquear_apos_inicio") if not prefixo else (
            f"{p}bloquear_apos_inicio" if "bloquear_apos_inicio" in colunas else "FALSE AS bloquear_apos_inicio"
        ),
    ])

    if incluir_senha_organizador:
        campos.append("u.senha AS organizador_senha")

    return campos
