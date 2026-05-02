from datetime import date, datetime
from flask import request
from flask_socketio import join_room
from extensions import socketio

# =========================
# CACHE ULTRA RÁPIDO
# =========================
_ESTADO_PARTIDAS = {}

PLACAR_GERAL_ROOM = "placar_geral_ao_vivo"
_ULTIMO_PLACAR_GERAL = None
_ULTIMO_PLACAR_APONTADOR = {}


# =========================
# HELPERS
# =========================
def _room(partida_id):
    return str(partida_id or "").strip()


def _normalizar_apontador(apontador):
    return str(apontador or "").strip()


def _room_placar_apontador(apontador):
    apontador = _normalizar_apontador(apontador)
    return f"placar_apontador:{apontador}" if apontador else ""


def _to_int(valor, padrao=0):
    try:
        if valor is None or valor == "":
            return padrao
        return int(valor)
    except Exception:
        return padrao


def _to_bool(valor, padrao=False):
    if isinstance(valor, bool):
        return valor
    if valor is None or valor == "":
        return padrao
    if isinstance(valor, (int, float)):
        return bool(valor)
    texto = str(valor).strip().lower()
    if texto in {"1", "true", "sim", "yes", "y", "on"}:
        return True
    if texto in {"0", "false", "nao", "não", "no", "n", "off"}:
        return False
    return padrao


def _normalizar_lista(valor):
    if isinstance(valor, list):
        return valor
    if isinstance(valor, tuple):
        return list(valor)
    return []


def _normalizar_dict(valor):
    return valor if isinstance(valor, dict) else {}


def _primeiro_valor(dados, chaves, padrao=None):
    for chave in chaves:
        if chave in dados and dados.get(chave) is not None and dados.get(chave) != "":
            return dados.get(chave)
    return padrao


def _json_safe(valor):
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()

    if isinstance(valor, dict):
        return {str(k): _json_safe(v) for k, v in valor.items()}

    if isinstance(valor, list):
        return [_json_safe(v) for v in valor]

    if isinstance(valor, tuple):
        return [_json_safe(v) for v in valor]

    return valor


# =========================
# CACHE
# =========================
def obter_estado_cache(partida_id):
    return _ESTADO_PARTIDAS.get(_room(partida_id))


def atualizar_estado_cache(partida_id, dados):
    sala = _room(partida_id)
    if not sala:
        return
    _ESTADO_PARTIDAS[sala] = _normalizar_payload(partida_id, dados)


def limpar_estado_cache(partida_id):
    _ESTADO_PARTIDAS.pop(_room(partida_id), None)


# Mantém compatibilidade com apontadores.py / placar ao vivo.
def obter_ultimo_placar_apontador(apontador):
    apontador = _normalizar_apontador(apontador)
    return _ULTIMO_PLACAR_APONTADOR.get(apontador)


# =========================
# NORMALIZAÇÃO
# =========================
def _normalizar_payload(partida_id, dados=None):
    dados = dict(dados or {})

    pontos_a = _to_int(_primeiro_valor(dados, ["pontos_a", "placar_a"], 0), 0)
    pontos_b = _to_int(_primeiro_valor(dados, ["pontos_b", "placar_b"], 0), 0)

    payload = {
        **dados,
        "ok": bool(dados.get("ok", True)),
        "partida_id": str(partida_id),
        "competicao": dados.get("competicao") or "",

        # =========================
        # PLACAR
        # =========================
        "pontos_a": pontos_a,
        "pontos_b": pontos_b,
        "placar_a": pontos_a,
        "placar_b": pontos_b,

        # =========================
        # SETS
        # =========================
        "sets_a": _to_int(dados.get("sets_a"), 0),
        "sets_b": _to_int(dados.get("sets_b"), 0),
        "set_atual": _to_int(dados.get("set_atual", 1), 1),

        # =========================
        # EQUIPES
        # =========================
        "equipe_a": dados.get("equipe_a") or dados.get("nome_a") or dados.get("time_a") or "",
        "equipe_b": dados.get("equipe_b") or dados.get("nome_b") or dados.get("time_b") or "",

        # =========================
        # SAQUE / ROTAÇÃO
        # =========================
        "saque_atual": dados.get("saque_atual") or "",
        "rotacao_a": _normalizar_lista(dados.get("rotacao_a")),
        "rotacao_b": _normalizar_lista(dados.get("rotacao_b")),

        # =========================
        # TEMPOS E SUBSTITUIÇÕES
        # =========================
        # No seu sistema, tempos_a/tempos_b representam tempos USADOS no set.
        "tempos_a": _to_int(dados.get("tempos_a"), 0),
        "tempos_b": _to_int(dados.get("tempos_b"), 0),
        "limite_tempos": _to_int(dados.get("limite_tempos", 2), 2),

        "subs_a": _to_int(dados.get("subs_a"), 0),
        "subs_b": _to_int(dados.get("subs_b"), 0),
        "limite_substituicoes": _to_int(dados.get("limite_substituicoes", 6), 6),

        # =========================
        # SANÇÕES / CARTÕES
        # =========================
        "sancoes_a": _normalizar_lista(dados.get("sancoes_a")),
        "sancoes_b": _normalizar_lista(dados.get("sancoes_b")),
        "cartoes_verdes_a": _normalizar_lista(dados.get("cartoes_verdes_a")),
        "cartoes_verdes_b": _normalizar_lista(dados.get("cartoes_verdes_b")),
        "status_jogadores_a": _normalizar_dict(dados.get("status_jogadores_a")),
        "status_jogadores_b": _normalizar_dict(dados.get("status_jogadores_b")),

        # =========================
        # REGRAS DA COMPETIÇÃO
        # =========================
        "pontos_set": _to_int(
            _primeiro_valor(dados, ["pontos_set", "ponto_alvo_set", "pontos_para_vencer_set"], 25),
            25,
        ),
        "ponto_alvo_set": _to_int(
            _primeiro_valor(dados, ["ponto_alvo_set", "pontos_set", "pontos_para_vencer_set"], 25),
            25,
        ),
        "pontos_para_vencer_set": _to_int(
            _primeiro_valor(dados, ["pontos_para_vencer_set", "pontos_set", "ponto_alvo_set"], 25),
            25,
        ),
        "pontos_tiebreak": _to_int(dados.get("pontos_tiebreak", 15), 15),
        "diferenca_minima": _to_int(dados.get("diferenca_minima", 2), 2),
        "sets_para_vencer": _to_int(dados.get("sets_para_vencer", 2), 2),
        "sets_max": _to_int(dados.get("sets_max", 3), 3),
        "sets_tipo": dados.get("sets_tipo") or "",

        # =========================
        # FASE / FINALIZAÇÃO
        # =========================
        "fase_partida": dados.get("fase_partida") or "jogo",
        "status_jogo": dados.get("status_jogo") or "em_andamento",
        "fim_set": _to_bool(dados.get("fim_set"), False),
        "set_finalizado": _to_bool(dados.get("set_finalizado"), False),
        "fim_jogo": _to_bool(dados.get("fim_jogo"), False),
        "partida_finalizada": _to_bool(dados.get("partida_finalizada"), False),
        "vencedor_set": dados.get("vencedor_set") or "",
        "vencedor_partida": dados.get("vencedor_partida") or "",

        # =========================
        # HISTÓRICO / SCOUT / TELÃO
        # =========================
        "historico": _normalizar_lista(dados.get("historico")),
        "scout": _normalizar_dict(dados.get("scout")),
        "atletas": dados.get("atletas") or {},
        "eventos": _normalizar_lista(dados.get("eventos")),
        "evolucao_pontos": _normalizar_lista(dados.get("evolucao_pontos")),
        "ultima_acao": dados.get("ultima_acao") or "-",

        # =========================
        # IDENTIFICAÇÃO
        # =========================
        "apontador": dados.get("apontador") or dados.get("apontador_login") or dados.get("operador_login") or "",
    }

    return _json_safe(payload)


# =========================
# EMISSÃO PRINCIPAL
# =========================
def emitir_estado_partida(partida_id, dados=None):
    sala = _room(partida_id)
    if not sala:
        return

    payload = _normalizar_payload(partida_id, dados)

    # Salva estado global para reconexão e para telas que entram depois.
    _ESTADO_PARTIDAS[sala] = payload

    # Compatibilidade com telas antigas e novas.
    socketio.emit("estado_partida", payload, room=sala)
    socketio.emit("estado_jogo_atualizado", payload, room=sala)


# =========================
# TREINADOR → APONTADOR
# =========================
def emitir_solicitacao_treinador(partida_id, dados):
    payload = {
        "partida_id": str(partida_id),
        **(dados or {}),
    }
    socketio.emit("solicitacao_treinador", _json_safe(payload), room=_room(partida_id))


# =========================
# APONTADOR → TREINADOR
# =========================
def emitir_resposta_solicitacao(partida_id, dados):
    payload = {
        "partida_id": str(partida_id),
        **(dados or {}),
    }
    socketio.emit("resposta_solicitacao", _json_safe(payload), room=_room(partida_id))


def _emitir_pedido_treinador_socket(partida_id, tipo, dados=None):
    """
    Pedido rápido via SocketIO.
    Não substitui as rotas HTTP que salvam no banco; serve para deixar o
    apontador e o treinador dinâmicos mesmo quando a tela já está aberta.
    """
    dados = dict(dados or {})
    equipe = str(dados.get("equipe") or dados.get("lado") or "").strip().upper()
    equipe_nome = str(dados.get("equipe_nome") or "").strip()

    texto_tipo = "tempo" if tipo == "tempo" else "substituição"
    mensagem = dados.get("mensagem") or (
        f"{equipe_nome} solicitou {texto_tipo}"
        if equipe_nome
        else f"Equipe {equipe or '-'} solicitou {texto_tipo}"
    )

    payload = {
        "partida_id": str(partida_id),
        "tipo": tipo,
        "equipe": equipe,
        "equipe_nome": equipe_nome,
        "mensagem": mensagem,
        "status": "pendente",
    }

    socketio.emit("solicitacao_treinador", _json_safe(payload), room=_room(partida_id))

    # Confirma para quem pediu, sem derrubar se não houver request.sid.
    try:
        socketio.emit("resposta_solicitacao", _json_safe(payload), room=request.sid)
    except Exception:
        pass


@socketio.on("pedido_tempo")
def pedido_tempo_socket(data):
    data = data or {}
    partida_id = str(data.get("partida_id") or "").strip()
    if not partida_id:
        return
    _emitir_pedido_treinador_socket(partida_id, "tempo", data)


@socketio.on("pedido_substituicao")
def pedido_substituicao_socket(data):
    data = data or {}
    partida_id = str(data.get("partida_id") or "").strip()
    if not partida_id:
        return
    _emitir_pedido_treinador_socket(partida_id, "substituicao", data)


# =========================
# PLACAR
# =========================
def emitir_placar_geral(partida_id, dados=None):
    global _ULTIMO_PLACAR_GERAL

    payload = _normalizar_payload(partida_id, dados)
    _ULTIMO_PLACAR_GERAL = payload

    socketio.emit("placar_geral_atualizado", payload, room=PLACAR_GERAL_ROOM)


def emitir_placar_apontador(apontador, partida_id, dados=None):
    apontador = _normalizar_apontador(apontador)
    sala = _room_placar_apontador(apontador)

    if not sala:
        return

    payload = _normalizar_payload(partida_id, dados)

    # Salva para reconexão.
    _ULTIMO_PLACAR_APONTADOR[apontador] = payload

    socketio.emit("placar_apontador_atualizado", payload, room=sala)


# =========================
# SOCKET EVENTS
# =========================
@socketio.on("connect")
def on_connect():
    return True


@socketio.on("disconnect")
def on_disconnect():
    return True


@socketio.on("entrar_partida")
def entrar_partida(data):
    partida_id = str((data or {}).get("partida_id") or "").strip()

    if not partida_id:
        return

    sala = _room(partida_id)
    join_room(sala)

    estado = _ESTADO_PARTIDAS.get(sala)

    if estado:
        payload = _normalizar_payload(partida_id, estado)
        socketio.emit("estado_partida", payload, room=request.sid)
        socketio.emit("estado_jogo_atualizado", payload, room=request.sid)


@socketio.on("entrar_placar_geral")
def entrar_placar_geral(data=None):
    join_room(PLACAR_GERAL_ROOM)

    if _ULTIMO_PLACAR_GERAL:
        socketio.emit("placar_geral_atualizado", _ULTIMO_PLACAR_GERAL, room=request.sid)


@socketio.on("entrar_placar_apontador")
def entrar_placar_apontador(data=None):
    apontador = _normalizar_apontador((data or {}).get("apontador"))
    sala = _room_placar_apontador(apontador)

    if not sala:
        return

    join_room(sala)

    ultimo = _ULTIMO_PLACAR_APONTADOR.get(apontador)

    if ultimo:
        socketio.emit("placar_apontador_atualizado", ultimo, room=request.sid)
