"""Carregamento leve e reconstrução controlada do estado da partida.

O serviço coordena cache, snapshot e banco por callbacks. Isso evita importar
Flask ou `routes.apontadores`, mantendo a direção routes -> services -> rules.
"""
from __future__ import annotations

from typing import Any, Callable

from rules.estado_jogo import (
    aplicar_campos_autoritativos,
    equipes_operacionais,
    finalizar_estado_operacional,
    mesclar_atletas,
)

Callback = Callable[..., Any]


def carregar_contexto_jogo(
    *,
    competicao: str,
    partida_id: int,
    partida: dict[str, Any],
    modo_local: bool,
    modo_operacao: str,
    obter_cache: Callback,
    buscar_estado_banco: Callback,
    obter_snapshot_local: Callback,
    aplicar_escudos: Callback,
    buscar_papeletas: Callback,
    listar_atletas: Callback,
    aplicar_regras: Callback,
    aplicar_placar_exibicao: Callback,
    buscar_competicao: Callback,
) -> dict[str, Any]:
    """Monta contexto operacional sem varrer histórico ou eventos."""
    try:
        estado = dict(obter_cache(partida_id) or {})
    except Exception:
        estado = {}

    snapshot = {}
    if modo_local:
        try:
            snapshot = dict(obter_snapshot_local(partida_id, competicao) or {})
        except Exception:
            snapshot = {}
        if not estado:
            estado = dict(snapshot.get("estado") or {})
    elif not estado:
        estado = dict(buscar_estado_banco(partida_id, competicao) or {})

    estado = aplicar_campos_autoritativos(estado, partida, competicao, partida_id)
    equipe_a, equipe_b = equipes_operacionais(partida, estado)
    if not equipe_a or not equipe_b:
        return {
            "ok": False,
            "motivo": "pre_jogo_incompleto",
            "mensagem": "Complete o pré-jogo antes de abrir a tela do jogo.",
        }

    estado = aplicar_escudos(estado, competicao, equipe_a, equipe_b)

    if modo_local:
        set_atual = int(estado.get("set_atual") or 1)
        papeletas = dict((snapshot.get("pacote_local") or {}).get("papeletas") or {})
        papeleta_a = {int(k): v for k, v in dict(papeletas.get("A") or {}).items()}
        papeleta_b = {int(k): v for k, v in dict(papeletas.get("B") or {}).items()}
        for posicao in range(1, 7):
            papeleta_a.setdefault(posicao, "")
            papeleta_b.setdefault(posicao, "")
    else:
        equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b = buscar_papeletas(
            partida_id, competicao, partida, estado
        )

    try:
        atletas_a = listar_atletas(equipe_a, competicao) if equipe_a else []
        atletas_b = listar_atletas(equipe_b, competicao) if equipe_b else []
    except Exception:
        atletas_a, atletas_b = [], []

    estado = finalizar_estado_operacional(estado, modo_operacao, papeleta_a, papeleta_b)
    atletas_a = mesclar_atletas(atletas_a, papeleta_a, estado.get("rotacao_a"))
    atletas_b = mesclar_atletas(atletas_b, papeleta_b, estado.get("rotacao_b"))

    estado = aplicar_regras(partida_id, competicao, estado, partida)

    try:
        config = buscar_competicao(competicao) or {
            "nome": competicao,
            "sets_tipo": partida.get("sets_tipo") or "melhor_de_3",
        }
        estado = aplicar_placar_exibicao(dict(estado or {}), config)
    except Exception:
        pass

    # Alguns aplicadores retornam novo dict; garante os campos finais depois.
    estado = finalizar_estado_operacional(estado, modo_operacao, papeleta_a, papeleta_b)

    return {
        "ok": True,
        "estado": estado,
        "equipe_a": equipe_a,
        "equipe_b": equipe_b,
        "set_atual": set_atual,
        "papeleta_a": papeleta_a,
        "papeleta_b": papeleta_b,
        "atletas_a": atletas_a,
        "atletas_b": atletas_b,
    }
