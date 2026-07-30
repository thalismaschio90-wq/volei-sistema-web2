"""Controle de compatibilidade do schema durante a execução normal.

O runtime nunca cria ou altera estruturas. As funções legadas de ``banco.py``
consultam este módulo para confirmar, uma única vez por processo, que as
migrações obrigatórias já foram aplicadas. O executor de migrações usa
``force=True`` e continua responsável por qualquer DDL.
"""
from __future__ import annotations

from threading import Lock

from core.schema_requirements import require_schema


_SCHEMA_FLAGS: dict[str, bool] = {
    "campos_sets_partida": False,
    "campos_jogo_partida": False,
    "campos_rotacao_partidas": False,
    "tabela_eventos": False,
    "tabela_historico_rotacao": False,
    "indices_desempenho": False,
    "campos_quadro_tecnico_equipes": False,
    "campos_liberacao_extra_equipes": False,
    "campos_controle_inscricao_competicoes": False,
    "tabela_atletas": False,
    "tabela_competicao_quadras": False,
    "tabela_competicao_agenda_config": False,
    "tabela_competicao_rodadas": False,
    "campos_trava_operacional_partida": False,
    "codigo_publico_competicoes": False,
}

schema_lock = Lock()

_SCHEMA_REQUIREMENTS: dict[str, dict[str, object]] = {
    "codigo_publico_competicoes": {
        "tables": ("competicoes",),
        "columns": {"competicoes": ("codigo_publico",)},
    },
    "campos_controle_inscricao_competicoes": {
        "tables": ("competicoes",),
        "columns": {
            "competicoes": (
                "data_limite_inscricao",
                "hora_limite_inscricao",
                "bloquear_apos_inicio",
                "limite_atletas",
                "permitir_edicao_pos_prazo",
                "exigir_foto_atleta",
                "exigir_instagram_atleta",
            )
        },
    },
    "campos_liberacao_extra_equipes": {
        "tables": ("equipes",),
        "columns": {
            "equipes": (
                "liberacao_extra_inscricao",
                "liberacao_extra_data",
                "liberacao_extra_hora",
            )
        },
    },
    "campos_quadro_tecnico_equipes": {
        "tables": ("equipes",),
        "columns": {
            "equipes": ("treinador", "auxiliar_tecnico", "preparador_fisico", "medico")
        },
    },
    "tabela_equipes_competicoes": {
        "tables": ("equipes_competicoes",),
        "columns": {
            "equipes_competicoes": (
                "equipe_login",
                "equipe_nome",
                "competicao",
                "status",
                "cliente_id",
            )
        },
    },
    "tabela_atletas": {
        "tables": ("atletas", "atletas_globais"),
        "columns": {
            "atletas": (
                "nome",
                "cpf",
                "numero",
                "equipe",
                "competicao",
                "status",
                "equipe_login",
                "equipe_id",
                "foto_atleta",
                "instagram",
                "temporario",
            )
        },
    },
    "campos_rotacao_partidas": {
        "tables": ("partidas",),
        "columns": {
            "partidas": (
                "rotacao_a",
                "rotacao_b",
                "saque_atual",
                "saque_inicial",
                "rotacao_validacao_ativa",
            )
        },
    },
    "tabela_historico_rotacao": {"tables": ("historico_rotacao",)},
    "campos_trava_operacional_partida": {
        "tables": ("partidas",),
        "columns": {
            "partidas": (
                "operador_login",
                "operador_nome",
                "apontador_login",
                "apontador_nome",
                "status_operacao",
                "reservado_em",
                "operador_heartbeat",
                "operador_socket_id",
            )
        },
    },
    "atalhos_apontador": {"tables": ("atalhos_apontador",)},
    "campos_perfil_equipe": {
        "tables": ("equipes",),
        "columns": {
            "equipes": (
                "cidade",
                "responsavel",
                "telefone",
                "email",
                "instagram",
                "escudo",
                "escudo_blob",
                "perfil_completo",
            )
        },
    },
    "campo_escudo_equipes": {
        "tables": ("equipes",),
        "columns": {"equipes": ("escudo", "escudo_blob")},
    },
    "tabela_competicao_agenda_config": {"tables": ("competicao_agenda_config",)},
    "tabela_competicao_rodadas": {"tables": ("competicao_rodadas",)},
    "tabela_eventos": {"tables": ("eventos",)},
    "campos_sets_partida": {"tables": ("partidas",)},
    "campos_jogo_partida": {"tables": ("partidas",)},
}


def schema_ja_pronto(chave: str, force: bool = False) -> bool:
    """Retorna ``False`` somente quando o executor pode aplicar a migração.

    Em runtime, valida a estrutura uma vez e retorna ``True`` para que as
    antigas funções ``criar_*``/``garantir_*`` encerrem antes de qualquer DDL.
    """
    if force:
        return False

    with schema_lock:
        if _SCHEMA_FLAGS.get(chave):
            return True

    requirements = _SCHEMA_REQUIREMENTS.get(chave)
    if requirements:
        require_schema(
            tables=requirements.get("tables", ()),
            columns=requirements.get("columns", {}),
            context=f"estrutura {chave}",
        )

    marcar_schema_pronto(chave)
    return True


def marcar_schema_pronto(chave: str) -> None:
    """Registra que o requisito já foi validado no processo atual."""
    with schema_lock:
        _SCHEMA_FLAGS[chave] = True


def limpar_cache_schema() -> None:
    """Reinicia as flags; destinado a testes e processos de manutenção."""
    with schema_lock:
        for chave in tuple(_SCHEMA_FLAGS):
            _SCHEMA_FLAGS[chave] = False
