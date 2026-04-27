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

    return {
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


def emitir_estado_partida(partida_id, dados=None):
    """
    Emite somente um evento principal para evitar duplicidade,
    travamento e renderização dobrada no frontend.
    """
    sala = _room(partida_id)
    if not sala:
        return

    payload = _normalizar_payload(partida_id, dados)

    socketio.emit(
        "estado_jogo_atualizado",
        payload,
        room=sala,
    )


def emitir_solicitacao_treinador(*args):
    """
    Emite uma solicitação feita pelo treinador para a sala da partida.

    Aceita dois formatos para não quebrar arquivos antigos:
    - emitir_solicitacao_treinador(partida_id, solicitacao)
    - emitir_solicitacao_treinador(competicao, partida_id, solicitacao)
    """
    if len(args) == 2:
        partida_id, solicitacao = args
    elif len(args) == 3:
        _competicao, partida_id, solicitacao = args
    else:
        print("emitir_solicitacao_treinador: argumentos inválidos", args)
        return

    sala = _room(partida_id)
    if not sala:
        return

    payload = {
        "partida_id": sala,
        "solicitacao": solicitacao,
        "mensagem": (solicitacao or {}).get("mensagem")
        if isinstance(solicitacao, dict)
        else solicitacao,
    }

    socketio.emit(
        "solicitacao_treinador",
        payload,
        room=sala,
    )


def emitir_resposta_solicitacao(partida_id, dados=None):
    """
    Avisa treinador/apontador que uma solicitação foi atendida ou recusada.
    Usado quando o apontador registra tempo/substituição após pedido do treinador.
    """
    sala = _room(partida_id)
    if not sala:
        return

    payload = dict(dados or {})
    payload.setdefault("partida_id", sala)

    socketio.emit(
        "resposta_solicitacao",
        payload,
        room=sala,
    )


@socketio.on("connect")
def on_connect():
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

    sala = _room(partida_id)
    join_room(sala)

    if not competicao:
        return

    try:
        from banco import buscar_estado_jogo_partida

        estado = buscar_estado_jogo_partida(int(partida_id), competicao) or {}
        payload = _normalizar_payload(partida_id, estado)

        socketio.emit(
            "estado_jogo_atualizado",
            payload,
            room=request.sid,
        )

    except Exception as e:
        print(f"Erro ao enviar estado inicial da partida {partida_id}: {e}")