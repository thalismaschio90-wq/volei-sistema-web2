from datetime import date, datetime

from flask import request
from flask_socketio import join_room

from extensions import socketio


def _room(partida_id):
    return str(partida_id).strip()


def _to_int(valor, padrao=0):
    try:
        return int(valor or padrao)
    except Exception:
        return padrao


def _normalizar_lista(valor):
    if not valor:
        return []
    if isinstance(valor, list):
        return valor
    return []


def _json_safe(valor):
    """
    Converte datetime/date e estruturas aninhadas para formato aceito pelo Socket.IO.
    Evita erro: Object of type datetime is not JSON serializable.
    """
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()

    if isinstance(valor, dict):
        return {str(k): _json_safe(v) for k, v in valor.items()}

    if isinstance(valor, list):
        return [_json_safe(v) for v in valor]

    if isinstance(valor, tuple):
        return [_json_safe(v) for v in valor]

    return valor


def _normalizar_payload(partida_id, dados=None):
    dados = dict(dados or {})

    pontos_a = _to_int(dados.get("pontos_a", dados.get("placar_a", 0)))
    pontos_b = _to_int(dados.get("pontos_b", dados.get("placar_b", 0)))

    sets_a = _to_int(dados.get("sets_a", 0))
    sets_b = _to_int(dados.get("sets_b", 0))

    rotacao = dados.get("rotacao") or {}
    if not isinstance(rotacao, dict):
        rotacao = {}

    rotacao_a = dados.get("rotacao_a") or rotacao.get("equipe_a") or []
    rotacao_b = dados.get("rotacao_b") or rotacao.get("equipe_b") or []

    payload = {
        **dados,

        "partida_id": str(partida_id),

        "pontos_a": pontos_a,
        "pontos_b": pontos_b,
        "placar_a": pontos_a,
        "placar_b": pontos_b,

        "sets_a": sets_a,
        "sets_b": sets_b,
        "set_atual": _to_int(dados.get("set_atual", 1), 1),

        "equipe_a": dados.get("equipe_a") or dados.get("equipe_a_nome") or "",
        "equipe_b": dados.get("equipe_b") or dados.get("equipe_b_nome") or "",
        "equipe_nome": dados.get("equipe_nome") or "",
        "equipe_adversaria": dados.get("equipe_adversaria") or "",

        "saque_atual": dados.get("saque_atual") or "",
        "saque_atual_nome": dados.get("saque_atual_nome") or "",
        "saque_inicial": dados.get("saque_inicial") or "",
        "saque_inicial_nome": dados.get("saque_inicial_nome") or "",

        "tempos_limite": _to_int(dados.get("tempos_limite", dados.get("tempos_por_set", 0))),
        "subs_limite": _to_int(dados.get("subs_limite", dados.get("substituicoes_por_set", 0))),
        "tempos_a": _to_int(dados.get("tempos_a", 0)),
        "tempos_b": _to_int(dados.get("tempos_b", 0)),
        "subs_a": _to_int(dados.get("subs_a", 0)),
        "subs_b": _to_int(dados.get("subs_b", 0)),
        "tempos_restantes": _to_int(dados.get("tempos_restantes", 0)),
        "subs_restantes": _to_int(dados.get("subs_restantes", 0)),

        "rotacao_a": _normalizar_lista(rotacao_a),
        "rotacao_b": _normalizar_lista(rotacao_b),
        "rotacao": {
            "equipe_a": _normalizar_lista(rotacao_a),
            "equipe_b": _normalizar_lista(rotacao_b),
            "propria": _normalizar_lista(dados.get("rotacao_propria") or rotacao.get("propria")),
        },

        "lado": dados.get("lado") or "",
        "lado_quadra": dados.get("lado_quadra") or "",
        "placar_proprio": _to_int(dados.get("placar_proprio", 0)),
        "placar_adversario": _to_int(dados.get("placar_adversario", 0)),
        "sets_proprios": _to_int(dados.get("sets_proprios", 0)),
        "sets_adversario": _to_int(dados.get("sets_adversario", 0)),

        "banco": dados.get("banco") or [],
        "scout": dados.get("scout") or {},
        "eventos": dados.get("eventos") or [],
        "historico": dados.get("historico") or [],
        "solicitacoes": dados.get("solicitacoes") or [],
        "ultima_acao": dados.get("ultima_acao") or "-",
    }

    return _json_safe(payload)


def emitir_estado_partida(partida_id, dados=None):
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

    payload = _json_safe({
        "partida_id": sala,
        "solicitacao": solicitacao,
        "mensagem": (
            (solicitacao or {}).get("mensagem")
            if isinstance(solicitacao, dict)
            else solicitacao
        ),
    })

    socketio.emit(
        "solicitacao_treinador",
        payload,
        room=sala,
    )


def emitir_resposta_solicitacao(partida_id, dados=None):
    sala = _room(partida_id)
    if not sala:
        return

    payload = dict(dados or {})
    payload.setdefault("partida_id", sala)
    payload = _json_safe(payload)

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
    equipe_nome = str(data.get("equipe_nome") or "").strip()

    if not partida_id:
        return

    sala = _room(partida_id)
    join_room(sala)

    if not competicao:
        return

    try:
        estado = {}

        if equipe_nome:
            try:
                from banco import montar_contexto_treinador
                estado = montar_contexto_treinador(int(partida_id), competicao, equipe_nome) or {}
            except Exception as e:
                print(f"Erro ao montar contexto treinador socket {partida_id}: {e}")
                estado = {}

        if not estado:
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