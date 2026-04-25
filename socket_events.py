from flask import request
from flask_socketio import join_room

from extensions import socketio


def _room(partida_id):
    return str(partida_id).strip()


def _normalizar_payload(partida_id, dados=None):
    dados = dict(dados or {})

    pontos_a = dados.get("pontos_a", dados.get("placar_a", 0))
    pontos_b = dados.get("pontos_b", dados.get("placar_b", 0))

    try:
        pontos_a = int(pontos_a or 0)
    except Exception:
        pontos_a = 0

    try:
        pontos_b = int(pontos_b or 0)
    except Exception:
        pontos_b = 0

    rotacao_a = dados.get("rotacao_a") or (dados.get("rotacao") or {}).get("equipe_a") or []
    rotacao_b = dados.get("rotacao_b") or (dados.get("rotacao") or {}).get("equipe_b") or []

    payload = {
        **dados,
        "partida_id": str(partida_id),
        "pontos_a": pontos_a,
        "pontos_b": pontos_b,
        "placar_a": pontos_a,
        "placar_b": pontos_b,
        "rotacao_a": list(rotacao_a or []),
        "rotacao_b": list(rotacao_b or []),
        "rotacao": {
            "equipe_a": list(rotacao_a or []),
            "equipe_b": list(rotacao_b or []),
        },
    }
    return payload


def emitir_estado_partida(partida_id, dados=None):
    """
    Emite o estado real já calculado pelo backend.
    Não usa loop, não usa placar em memória e não cria atraso artificial.
    """
    sala = _room(partida_id)
    if not sala:
        return

    payload = _normalizar_payload(partida_id, dados)

    # Evento novo usado pelo apontador/treinador.
    socketio.emit("estado_jogo_atualizado", payload, room=sala)

    # Evento antigo mantido para telas que ainda escutam placar_atualizado.
    socketio.emit("placar_atualizado", payload, room=sala)


def emitir_solicitacao_treinador(partida_id, solicitacao):
    sala = _room(partida_id)
    if not sala:
        return

    socketio.emit(
        "solicitacao_treinador",
        {
            "partida_id": sala,
            "solicitacao": solicitacao,
            "mensagem": (solicitacao or {}).get("mensagem") if isinstance(solicitacao, dict) else solicitacao,
        },
        room=sala,
    )


@socketio.on("connect")
def on_connect():
    # Sem print em todo connect para não pesar o servidor durante jogo real.
    return True


@socketio.on("disconnect")
def on_disconnect():
    return True


@socketio.on("entrar_partida")
def entrar_partida(data):
    data = data or {}
    partida_id = str(data.get("partida_id") or "").strip()
    competicao = str(data.get("competicao") or "").strip()

    if not partida_id:
        return

    join_room(_room(partida_id))

    # Envia um snapshot inicial real do banco, quando a competição vier no evento.
    if competicao:
        try:
            from banco import buscar_estado_jogo_partida

            estado = buscar_estado_jogo_partida(int(partida_id), competicao) or {}
            payload = _normalizar_payload(partida_id, estado)
            socketio.emit("estado_jogo_atualizado", payload, room=request.sid)
            socketio.emit("placar_atualizado", payload, room=request.sid)
        except Exception:
            pass
