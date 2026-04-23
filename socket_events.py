from flask_socketio import join_room, emit
from extensions import socketio


def _nome_sala(competicao, partida_id):
    competicao = str(competicao or "").strip()
    partida_id = str(partida_id or "").strip()
    return f"{competicao}_{partida_id}"


@socketio.on("entrar_partida")
def entrar_partida(data):
    data = data or {}
    competicao = data.get("competicao")
    partida_id = data.get("partida_id")

    sala = _nome_sala(competicao, partida_id)
    join_room(sala)

    emit("socket_ok", {
        "ok": True,
        "sala": sala
    })


def emitir_estado_partida(competicao, partida_id, payload):
    sala = _nome_sala(competicao, partida_id)
    socketio.emit("estado_jogo_atualizado", payload, room=sala)


def emitir_solicitacao_treinador(competicao, partida_id, payload):
    sala = _nome_sala(competicao, partida_id)
    socketio.emit("solicitacao_treinador", payload, room=sala)


def emitir_resposta_solicitacao(competicao, partida_id, payload):
    sala = _nome_sala(competicao, partida_id)
    socketio.emit("resposta_solicitacao", payload, room=sala)