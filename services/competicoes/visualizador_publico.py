"""Serviço de leitura e montagem dos dados do visualizador público."""

from collections.abc import Callable

from banco import (
    buscar_competicao_por_nome,
    buscar_estado_jogo_partida,
    buscar_partida_por_id,
    listar_eventos_partida,
)
from repositories.visualizador_publico import (
    buscar_destaque_partida,
    buscar_versoes_detalhes,
)
from rules.visualizador_publico import (
    modo_scout_ativo_publico,
    montar_linha_ponto_publico,
)


def obter_estado_cache(partida_id):
    from socket_events import obter_estado_cache as _obter_estado_cache
    return _obter_estado_cache(partida_id)


def obter_estado_versao(partida_id):
    from socket_events import obter_estado_versao as _obter_estado_versao
    return _obter_estado_versao(partida_id)


def obter_estado_publico(partida_id: int, competicao_nome: str) -> dict:
    """Obtém o estado vivo e usa o banco apenas como fallback.

    O estado em memória prevalece sobre a fotografia persistida. A validação da
    competição impede que um estado residual de outra competição seja usado.
    """
    estado_vivo = {}
    try:
        candidato = obter_estado_cache(partida_id) or {}
        competicao_cache = str(candidato.get("competicao") or "").strip()
        if not competicao_cache or competicao_cache == str(competicao_nome or "").strip():
            estado_vivo = dict(candidato)
    except Exception as exc:
        print("AVISO visualizador/estado_vivo:", repr(exc), flush=True)

    estado_banco = {}
    if not estado_vivo:
        try:
            estado_banco = buscar_estado_jogo_partida(partida_id, competicao_nome) or {}
        except Exception:
            estado_banco = {}

    estado = dict(estado_banco)
    estado.update(estado_vivo)
    return estado


def montar_contexto_partida_publica(
    competicao_nome: str,
    partida_id: int,
    preparar_partidas: Callable,
    *,
    incluir_detalhes: bool = False,
):
    """Monta o contexto completo usado na página e nos detalhes sob demanda."""
    competicao = buscar_competicao_por_nome(competicao_nome) or {"nome": competicao_nome}
    partida = buscar_partida_por_id(partida_id, competicao_nome)
    if not partida:
        return None

    mapa_escudos = {
        partida.get("equipe_a"): partida.get("escudo_a"),
        partida.get("equipe_b"): partida.get("escudo_b"),
    }
    preparada = (preparar_partidas([partida], mapa_escudos, competicao) or [partida])[0]
    estado = obter_estado_publico(partida_id, competicao_nome)
    scout_ativo = modo_scout_ativo_publico(partida, competicao)
    timeline, evolucao_sets, stats = [], [], {}
    if incluir_detalhes:
        eventos = listar_eventos_partida(partida_id, competicao_nome, limite=600) or []
        timeline, evolucao_sets, stats = montar_linha_ponto_publico(partida, eventos, scout_ativo)

    return {
        "competicao": competicao,
        "partida": preparada,
        "estado": estado,
        "scout_ativo": scout_ativo,
        "timeline": timeline,
        "evolucao_sets": evolucao_sets,
        "stats": stats,
        "destaque": buscar_destaque_partida(partida_id, competicao_nome),
    }


def montar_estado_leve_partida_publica(
    competicao_nome: str,
    partida_id: int,
    preparar_partidas: Callable,
):
    """Monta o payload leve do polling, sem eventos, fotos ou scout completo."""
    competicao = buscar_competicao_por_nome(competicao_nome) or {"nome": competicao_nome}
    partida = buscar_partida_por_id(partida_id, competicao_nome)
    if not partida:
        return None

    estado = obter_estado_publico(partida_id, competicao_nome)
    mapa_escudos = {
        partida.get("equipe_a"): partida.get("escudo_a"),
        partida.get("equipe_b"): partida.get("escudo_b"),
    }
    preparada = (preparar_partidas([partida], mapa_escudos, competicao) or [partida])[0]
    eventos_versao, destaque_versao = buscar_versoes_detalhes(partida_id, competicao_nome)

    try:
        estado_versao = int(obter_estado_versao(partida_id) or 0)
    except Exception:
        estado_versao = 0

    return {
        "ok": True,
        "partida": {
            "id": preparada.get("id"),
            "equipe_a": preparada.get("equipe_a"),
            "equipe_b": preparada.get("equipe_b"),
            "status_exibicao": preparada.get("status_exibicao"),
            "ao_vivo": bool(preparada.get("ao_vivo")),
            "finalizada": bool(preparada.get("finalizada")),
            "set_unico": bool(preparada.get("set_unico")),
            "sets_a": int(estado.get("sets_a") or preparada.get("sets_a") or 0),
            "sets_b": int(estado.get("sets_b") or preparada.get("sets_b") or 0),
            "set_atual": int(estado.get("set_atual") or preparada.get("set_atual") or 1),
            "pontos_a": int(
                estado.get("pontos_a")
                or estado.get("placar_a")
                or preparada.get("placar_exibicao_a")
                or 0
            ),
            "pontos_b": int(
                estado.get("pontos_b")
                or estado.get("placar_b")
                or preparada.get("placar_exibicao_b")
                or 0
            ),
            "parciais_formatadas": preparada.get("parciais_formatadas") or "",
        },
        "eventos_versao": eventos_versao,
        "estado_versao": estado_versao,
        "ultima_acao": estado.get("ultima_acao") or "",
        "destaque_versao": destaque_versao,
    }
