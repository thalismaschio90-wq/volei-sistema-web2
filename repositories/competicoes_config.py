"""Persistência das configurações do organizador.

SQL fica concentrado aqui; validações de domínio ficam em rules e services.
As dependências legadas são carregadas somente durante a chamada para evitar
importações circulares enquanto banco.py ainda é a fachada principal.
"""
from __future__ import annotations

import json

from repositories.conexao import conectar
from repositories.competicoes_ciclo import validar_competicao_editavel_persistencia

from rules.competicoes_config import (
    configuracao_agenda_padrao,
    fases_padrao_configuracao_avancada,
    normalizar_configuracao_agenda,
    normalizar_fases_config,
)


def buscar_configuracao_avancada(nome_competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nome, tipo_classificacao, qtd_classificados, formato_finais,
                       possui_bye, qtd_bye, fases_config, tipo_confronto,
                       cruzamentos_grupos, data_limite_inscricao,
                       hora_limite_inscricao, bloquear_apos_inicio
                FROM competicoes WHERE nome = %s LIMIT 1
            """, (nome_competicao,))
            row = cur.fetchone()
    if not row:
        return None
    row["fases_config"] = normalizar_fases_config(row.get("fases_config"))
    return row


def atualizar_configuracao_avancada(nome_competicao, *, tipo_classificacao,
                                     qtd_classificados, formato_finais, possui_bye,
                                     qtd_bye, fases_config, tipo_confronto="grupo_interno",
                                     cruzamentos_grupos="", data_limite_inscricao=None,
                                     hora_limite_inscricao=None, bloquear_apos_inicio=False):
    ok_edicao, _ = validar_competicao_editavel_persistencia(nome_competicao, "alteração de formato")
    if not ok_edicao:
        return False
    fases_json = json.dumps(normalizar_fases_config(fases_config), ensure_ascii=False)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE competicoes SET
                    tipo_classificacao = %s, qtd_classificados = %s,
                    formato_finais = %s, possui_bye = %s, qtd_bye = %s,
                    fases_config = %s::jsonb, tipo_confronto = %s,
                    cruzamentos_grupos = %s, data_limite_inscricao = %s,
                    hora_limite_inscricao = %s, bloquear_apos_inicio = %s
                WHERE nome = %s
            """, (tipo_classificacao, qtd_classificados, formato_finais,
                  bool(possui_bye), qtd_bye, fases_json, tipo_confronto,
                  cruzamentos_grupos, data_limite_inscricao,
                  hora_limite_inscricao, bool(bloquear_apos_inicio), nome_competicao))
        conn.commit()
    return True


def inicializar_configuracao_avancada(nome_competicao):
    config = buscar_configuracao_avancada(nome_competicao)
    if not config:
        return False
    if config.get("fases_config"):
        return True
    return atualizar_configuracao_avancada(
        nome_competicao,
        tipo_classificacao=config.get("tipo_classificacao") or "grupo",
        qtd_classificados=config.get("qtd_classificados") or 0,
        formato_finais=config.get("formato_finais") or "mata_mata",
        possui_bye=config.get("possui_bye") or False,
        qtd_bye=config.get("qtd_bye") or 0,
        fases_config=fases_padrao_configuracao_avancada(config),
        tipo_confronto=config.get("tipo_confronto") or "grupo_interno",
        cruzamentos_grupos=config.get("cruzamentos_grupos") or "",
        data_limite_inscricao=config.get("data_limite_inscricao"),
        hora_limite_inscricao=config.get("hora_limite_inscricao"),
        bloquear_apos_inicio=config.get("bloquear_apos_inicio") or False,
    )


def _validar_schema_agenda() -> None:
    """Exige a estrutura criada pelas migrações, sem DDL em runtime."""
    from core.schema_requirements import require_schema

    require_schema(
        tables=("competicao_agenda_config",),
        columns={
            "competicao_agenda_config": (
                "competicao",
                "modo_distribuicao",
                "descanso_minimo_jogos",
                "rodizio_grupos",
                "permitir_relaxar_descanso",
                "grupos_compartilhados_json",
                "quadras_compartilhadas_json",
                "usar_rodadas_programadas",
                "uma_partida_por_equipe_rodada",
            )
        },
        context="configuração da agenda das competições",
    )


def buscar_configuracao_agenda(nome_competicao):
    _validar_schema_agenda()
    nome_competicao = str(nome_competicao or "").strip()
    if not nome_competicao:
        return configuracao_agenda_padrao()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT modo_distribuicao, descanso_minimo_jogos, rodizio_grupos,
                       permitir_relaxar_descanso, grupos_compartilhados_json,
                       quadras_compartilhadas_json, usar_rodadas_programadas,
                       uma_partida_por_equipe_rodada
                FROM competicao_agenda_config
                WHERE competicao = %s LIMIT 1
            """, (nome_competicao,))
            row = cur.fetchone()
    return normalizar_configuracao_agenda(row)


def atualizar_configuracao_agenda(nome_competicao, **dados):
    ok_edicao, _ = validar_competicao_editavel_persistencia(nome_competicao, "alteração da agenda automática")
    if not ok_edicao:
        return False
    _validar_schema_agenda()
    config = normalizar_configuracao_agenda(dados)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO competicao_agenda_config (
                    competicao, modo_distribuicao, descanso_minimo_jogos,
                    rodizio_grupos, permitir_relaxar_descanso,
                    grupos_compartilhados_json, quadras_compartilhadas_json,
                    usar_rodadas_programadas, uma_partida_por_equipe_rodada,
                    atualizado_em
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, NOW())
                ON CONFLICT (competicao) DO UPDATE SET
                    modo_distribuicao = EXCLUDED.modo_distribuicao,
                    descanso_minimo_jogos = EXCLUDED.descanso_minimo_jogos,
                    rodizio_grupos = EXCLUDED.rodizio_grupos,
                    permitir_relaxar_descanso = EXCLUDED.permitir_relaxar_descanso,
                    grupos_compartilhados_json = EXCLUDED.grupos_compartilhados_json,
                    quadras_compartilhadas_json = EXCLUDED.quadras_compartilhadas_json,
                    usar_rodadas_programadas = EXCLUDED.usar_rodadas_programadas,
                    uma_partida_por_equipe_rodada = EXCLUDED.uma_partida_por_equipe_rodada,
                    atualizado_em = NOW()
            """, (
                nome_competicao, config["modo_distribuicao"],
                config["descanso_minimo_jogos"], config["rodizio_grupos"],
                config["permitir_relaxar_descanso"],
                json.dumps(config["grupos_compartilhados"], ensure_ascii=False),
                json.dumps(config["quadras_compartilhadas"], ensure_ascii=False),
                config["usar_rodadas_programadas"],
                config["uma_partida_por_equipe_rodada"],
            ))
        conn.commit()
    return True


def inicializar_configuracao_agenda(nome_competicao):
    config = buscar_configuracao_agenda(nome_competicao)
    return atualizar_configuracao_agenda(nome_competicao, **config)
