from datetime import date, datetime
import time

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
_INVERSAO_PLACAR_APONTADOR = {}


# =========================
# HELPERS
# =========================
def _room(partida_id):
    return str(partida_id or "").strip()


def _room_arbitros(partida_id):
    return _room(partida_id)


def _rooms_partida(partida_id, competicao=None):
    base = _room(partida_id)
    comp = str(competicao or "").strip()

    if not base:
        return []

    salas = [
        base,
        f"partida:{base}",
        f"partida_{base}",
        f"arbitros:{base}",
        f"arbitros_{base}",
    ]

    if comp:
        salas.extend([
            f"partida:{comp}:{base}",
            f"partida_{comp}_{base}",
            f"arbitros:{comp}:{base}",
            f"arbitros_{comp}_{base}",
        ])

    return list(dict.fromkeys([s for s in salas if s]))


def _emitir_salas(evento, payload, partida_id, **kwargs):
    payload = _json_safe(payload)
    competicao = payload.get("competicao") if isinstance(payload, dict) else None

    for sala in _rooms_partida(partida_id, competicao):
        socketio.emit(evento, payload, room=sala, **kwargs)


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


def _numero_rotacao_seguro(valor):
    """Converte qualquer item de rotação para número/string simples.

    Evita que objetos {numero,nome} cheguem ao HTML como [object Object].
    """
    if valor is None:
        return ""
    if isinstance(valor, dict):
        for chave in ("numero", "camisa", "numero_camisa", "atleta_numero", "n", "id"):
            if valor.get(chave) not in (None, ""):
                return _numero_rotacao_seguro(valor.get(chave))
        return ""
    texto = str(valor or "").strip()
    return "" if texto == "[object Object]" else texto


def _normalizar_rotacao_payload(valor):
    if not isinstance(valor, (list, tuple)):
        return []
    saida = [_numero_rotacao_seguro(item) for item in list(valor)[:6]]
    while len(saida) < 6:
        saida.append("")
    return saida[:6]



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



def _normalizar_url_escudo(valor):
    valor = str(valor or "").strip()
    if not valor:
        return "/static/img/escudo_padrao.svg"
    if valor.startswith(("http://", "https://", "/static/", "data:")):
        return valor
    valor = valor.replace("\\", "/")
    if valor.startswith("static/"):
        return "/" + valor
    if valor.startswith("uploads/"):
        return "/static/" + valor
    if "/uploads/" in valor:
        return "/static/uploads/" + valor.split("/uploads/", 1)[1]
    return "/static/uploads/escudos/" + valor.lstrip("/")

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


def obter_ultimo_placar_apontador(apontador):
    apontador = _normalizar_apontador(apontador)
    return _ULTIMO_PLACAR_APONTADOR.get(apontador)


# =========================
# NORMALIZAÇÃO
# =========================
def _normalizar_payload(partida_id, dados=None):
    dados = dict(dados or {})

    pontos_a = _to_int(
        _primeiro_valor(dados, ["pontos_a", "placar_a"], 0),
        0,
    )

    pontos_b = _to_int(
        _primeiro_valor(dados, ["pontos_b", "placar_b"], 0),
        0,
    )

    payload = {
        **dados,
        "ok": bool(dados.get("ok", True)),
        "partida_id": str(partida_id),
        "competicao": dados.get("competicao") or "",

        "pontos_a": pontos_a,
        "pontos_b": pontos_b,
        "placar_a": pontos_a,
        "placar_b": pontos_b,

        "sets_a": _to_int(dados.get("sets_a"), 0),
        "sets_b": _to_int(dados.get("sets_b"), 0),
        "set_atual": _to_int(dados.get("set_atual", 1), 1),

        "equipe_a": dados.get("equipe_a") or dados.get("nome_a") or dados.get("time_a") or "",
        "equipe_b": dados.get("equipe_b") or dados.get("nome_b") or dados.get("time_b") or "",
        "equipe_a_operacional": dados.get("equipe_a_operacional") or dados.get("equipe_a") or dados.get("nome_a") or dados.get("time_a") or "",
        "equipe_b_operacional": dados.get("equipe_b_operacional") or dados.get("equipe_b") or dados.get("nome_b") or dados.get("time_b") or "",

        "escudo_a": _normalizar_url_escudo(dados.get("escudo_a") or dados.get("equipe_a_escudo") or dados.get("escudoA")),
        "escudo_b": _normalizar_url_escudo(dados.get("escudo_b") or dados.get("equipe_b_escudo") or dados.get("escudoB")),
        "escudo_a_operacional": _normalizar_url_escudo(dados.get("escudo_a_operacional") or dados.get("escudo_a") or dados.get("equipe_a_escudo") or dados.get("escudoA")),
        "escudo_b_operacional": _normalizar_url_escudo(dados.get("escudo_b_operacional") or dados.get("escudo_b") or dados.get("equipe_b_escudo") or dados.get("escudoB")),
        "equipe_a_escudo": _normalizar_url_escudo(dados.get("equipe_a_escudo") or dados.get("escudo_a") or dados.get("escudoA")),
        "equipe_b_escudo": _normalizar_url_escudo(dados.get("equipe_b_escudo") or dados.get("escudo_b") or dados.get("escudoB")),
        "cor_a": dados.get("cor_a_operacional") or dados.get("cor_a") or dados.get("equipe_a_cor") or "#2E6BE6",
        "cor_b": dados.get("cor_b_operacional") or dados.get("cor_b") or dados.get("equipe_b_cor") or "#E53935",
        "cor_a_operacional": dados.get("cor_a_operacional") or dados.get("cor_a") or dados.get("equipe_a_cor") or "#2E6BE6",
        "cor_b_operacional": dados.get("cor_b_operacional") or dados.get("cor_b") or dados.get("equipe_b_cor") or "#E53935",
        "equipe_a_cor": dados.get("equipe_a_cor") or dados.get("cor_a_operacional") or dados.get("cor_a") or "#2E6BE6",
        "equipe_b_cor": dados.get("equipe_b_cor") or dados.get("cor_b_operacional") or dados.get("cor_b") or "#E53935",

        "saque_atual": dados.get("saque_atual") or "",
        "sacador_nome": dados.get("sacador_nome") or dados.get("nome_sacador") or "",
        "sacador_numero": dados.get("sacador_numero") or dados.get("numero_sacador") or "",

        "rotacao_a": _normalizar_rotacao_payload(dados.get("rotacao_a")),
        "rotacao_b": _normalizar_rotacao_payload(dados.get("rotacao_b")),

        "tempos_a": _to_int(dados.get("tempos_a"), 0),
        "tempos_b": _to_int(dados.get("tempos_b"), 0),
        "limite_tempos": _to_int(dados.get("limite_tempos", 2), 2),

        "subs_a": _to_int(dados.get("subs_a"), 0),
        "subs_b": _to_int(dados.get("subs_b"), 0),
        "limite_substituicoes": _to_int(dados.get("limite_substituicoes", 6), 6),

        "sancoes_a": _normalizar_lista(dados.get("sancoes_a")),
        "sancoes_b": _normalizar_lista(dados.get("sancoes_b")),
        "cartoes_verdes_a": _normalizar_lista(dados.get("cartoes_verdes_a")),
        "cartoes_verdes_b": _normalizar_lista(dados.get("cartoes_verdes_b")),
        "status_jogadores_a": _normalizar_dict(dados.get("status_jogadores_a")),
        "status_jogadores_b": _normalizar_dict(dados.get("status_jogadores_b")),

        "pontos_set": _to_int(
            _primeiro_valor(
                dados,
                ["pontos_set", "ponto_alvo_set", "pontos_para_vencer_set"],
                25,
            ),
            25,
        ),
        "ponto_alvo_set": _to_int(
            _primeiro_valor(
                dados,
                ["ponto_alvo_set", "pontos_set", "pontos_para_vencer_set"],
                25,
            ),
            25,
        ),
        "pontos_para_vencer_set": _to_int(
            _primeiro_valor(
                dados,
                ["pontos_para_vencer_set", "pontos_set", "ponto_alvo_set"],
                25,
            ),
            25,
        ),
        "pontos_tiebreak": _to_int(dados.get("pontos_tiebreak", 15), 15),
        "diferenca_minima": _to_int(dados.get("diferenca_minima", 2), 2),
        "sets_para_vencer": _to_int(dados.get("sets_para_vencer", 2), 2),
        "sets_max": _to_int(dados.get("sets_max", 3), 3),
        "sets_tipo": dados.get("sets_tipo") or "",

        "fase_partida": dados.get("fase_partida") or "jogo",
        "status_jogo": dados.get("status_jogo") or "em_andamento",
        "fim_set": _to_bool(dados.get("fim_set"), False),
        "set_finalizado": _to_bool(dados.get("set_finalizado"), False),
        "fim_jogo": _to_bool(dados.get("fim_jogo"), False),
        "partida_finalizada": _to_bool(dados.get("partida_finalizada"), False),
        "vencedor_set": dados.get("vencedor_set") or "",
        "vencedor_partida": dados.get("vencedor_partida") or "",

        "historico": _normalizar_lista(dados.get("historico")),
        "scout": _normalizar_dict(dados.get("scout")),
        "atletas": dados.get("atletas") or {},
        "eventos": _normalizar_lista(dados.get("eventos")),
        "evolucao_pontos": _normalizar_lista(dados.get("evolucao_pontos")),
        "ultima_acao": dados.get("ultima_acao") or "-",

        "apontador": dados.get("apontador") or dados.get("apontador_login") or dados.get("operador_login") or "",
        "lados_invertidos_apontador": _to_bool(
            dados.get(
                "lados_invertidos_apontador",
                dados.get("lados_invertidos", dados.get("quadra_invertida")),
            ),
            False,
        ),
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

    _ESTADO_PARTIDAS[sala] = payload

    _emitir_salas("estado_partida", payload, partida_id)
    _emitir_salas("estado_jogo_atualizado", payload, partida_id)
    _emitir_salas("estado_arbitros", payload, partida_id)

    # NOVO APP TEMPO REAL
    _emitir_salas("estado_partida_tempo_real", payload, partida_id)

    ultima_acao = str(payload.get("ultima_acao") or "").strip()

    if ultima_acao and ultima_acao != "-":
        _emitir_salas(
            "ultima_acao_arbitros",
            {
                "partida_id": str(partida_id),
                "texto": ultima_acao,
                "descricao": ultima_acao,
            },
            partida_id,
        )

    saque_atual = str(payload.get("saque_atual") or "").strip().upper()

    if saque_atual in {"A", "B"}:
        _emitir_salas(
            "saque_arbitros",
            {
                "partida_id": str(partida_id),
                "equipe": saque_atual,
                "equipe_nome": payload.get("equipe_a") if saque_atual == "A" else payload.get("equipe_b"),
                "saque_atual": saque_atual,
                "sacador_nome": payload.get("sacador_nome") or "",
                "sacador_numero": payload.get("sacador_numero") or "",
            },
            partida_id,
        )


# =========================
# TREINADOR → APONTADOR
# =========================
def emitir_solicitacao_treinador(partida_id, dados):
    sala = _room(partida_id)

    if not sala:
        return

    dados = dict(dados or {})

    tipo = str(dados.get("tipo") or "").strip().lower()
    equipe = str(dados.get("equipe") or dados.get("lado") or "").strip().upper()
    equipe_nome = str(dados.get("equipe_nome") or "").strip()

    texto_tipo = (
        "tempo"
        if tipo == "tempo"
        else "substituição"
        if tipo in {"substituicao", "substituição"}
        else "solicitação"
    )

    payload = {
        "id_solicitacao": dados.get("id_solicitacao"),
        "partida_id": str(partida_id),
        "competicao": str(dados.get("competicao") or ""),
        "tipo": "substituicao" if tipo == "substituição" else tipo,
        "equipe": equipe,
        "equipe_nome": equipe_nome,
        "mensagem": str(
            dados.get("mensagem")
            or (
                f"{equipe_nome} solicitou {texto_tipo}"
                if equipe_nome
                else f"Equipe {equipe or '-'} solicitou {texto_tipo}"
            )
        ).strip(),
        "status": str(dados.get("status") or "pendente").strip().lower(),
        "origem": str(dados.get("origem") or "treinador_http").strip(),
        "duracao": _to_int(dados.get("duracao") or dados.get("segundos") or 30, 30),
        "timestamp": time.time(),
        **dados,
    }

    payload = _json_safe(payload)

    eventos = (
        "solicitacao_treinador",
        "resposta_solicitacao",
        "solicitacao_arbitros",
        "notificacao_geral",
    )

    for evento in eventos:
        try:
            _emitir_salas(evento, payload, partida_id)
        except Exception as e:
            print(f"ERRO emitir {evento}:", e, flush=True)

    try:
        socketio.emit("solicitacao_arbitros", payload, room=str(partida_id))
        socketio.emit("notificacao_geral", payload, room=str(partida_id))
    except Exception as e:
        print("ERRO fallback socket:", e, flush=True)


# =========================
# APONTADOR → ÁRBITROS
# =========================
def emitir_tempo_executado(partida_id, dados=None):
    sala = _room(partida_id)

    if not sala:
        return

    dados = dict(dados or {})

    duracao = _to_int(
        dados.get("duracao") or dados.get("segundos") or dados.get("restante") or 30,
        30,
    )

    equipe = str(dados.get("equipe") or "").strip().upper()

    payload = {
        "partida_id": str(partida_id),
        "tipo": "tempo",
        "ativo": True,
        "status": str(dados.get("status") or "iniciado").strip().lower(),
        "equipe": equipe,
        "equipe_nome": str(dados.get("equipe_nome") or "").strip(),
        "duracao": duracao,
        "segundos": duracao,
        "restante": duracao,
        "mensagem": str(dados.get("mensagem") or f"Tempo autorizado - Equipe {equipe or '-'}").strip(),
        "origem": str(dados.get("origem") or "apontador").strip(),
        **dados,
    }

    payload = _json_safe(payload)

    _emitir_salas("cronometro_tempo", payload, partida_id)
    _emitir_salas("cronometro_arbitros", payload, partida_id)
    _emitir_salas("tempo_executado", payload, partida_id)
    _emitir_salas("notificacao_geral", {**payload, "tipo": "tempo_executado"}, partida_id)


def emitir_substituicao_executada(partida_id, dados=None):
    sala = _room(partida_id)

    if not sala:
        return

    dados = dict(dados or {})

    equipe = str(dados.get("equipe") or "").strip().upper()
    numero_sai = str(dados.get("numero_sai") or dados.get("sai") or "").strip()
    numero_entra = str(dados.get("numero_entra") or dados.get("entra") or "").strip()

    payload = {
        "partida_id": str(partida_id),
        "tipo": "substituicao",
        "equipe": equipe,
        "equipe_nome": str(dados.get("equipe_nome") or "").strip(),
        "numero_sai": numero_sai,
        "numero_entra": numero_entra,
        "status": str(dados.get("status") or "executada").strip().lower(),
        "origem": str(dados.get("origem") or "apontador").strip(),
        "mensagem": str(
            dados.get("mensagem")
            or f"Substituição executada - Equipe {equipe or '-'}: #{numero_sai} → #{numero_entra}"
        ).strip(),
        **dados,
    }

    payload = _json_safe(payload)

    _emitir_salas("substituicao_executada", payload, partida_id)
    _emitir_salas("substituicao_arbitros", payload, partida_id)
    _emitir_salas("notificacao_geral", {**payload, "tipo": "substituicao_executada"}, partida_id)


# =========================
# APONTADOR → TREINADOR
# =========================
def emitir_resposta_solicitacao(partida_id, dados):
    payload = {
        "partida_id": str(partida_id),
        **(dados or {}),
    }

    _emitir_salas("resposta_solicitacao", payload, partida_id)


def _emitir_pedido_treinador_socket(partida_id, tipo, dados=None):
    dados = dict(dados or {})
    dados["tipo"] = tipo
    dados["origem"] = dados.get("origem") or "treinador_socket"

    emitir_solicitacao_treinador(partida_id, dados)

    try:
        payload = {
            "partida_id": str(partida_id),
            **dados,
            "status": dados.get("status") or "pendente",
        }

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

    inv_key = (apontador, str(partida_id or ""))

    if inv_key in _INVERSAO_PLACAR_APONTADOR:
        payload["lados_invertidos_apontador"] = bool(_INVERSAO_PLACAR_APONTADOR[inv_key])

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
    data = data or {}

    partida_id = str(data.get("partida_id") or "").strip()
    competicao = str(data.get("competicao") or "").strip()

    if not partida_id:
        return

    sala = _room(partida_id)

    for r in _rooms_partida(partida_id, competicao):
        join_room(r)

    socketio.emit(
        "entrou_partida",
        {
            "ok": True,
            "partida_id": str(partida_id),
            "competicao": competicao,
            "room": sala,
        },
        room=request.sid,
    )

    estado = _ESTADO_PARTIDAS.get(sala)

    if estado:
        payload = _normalizar_payload(partida_id, estado)

        socketio.emit("estado_partida", payload, room=request.sid)
        socketio.emit("estado_jogo_atualizado", payload, room=request.sid)


@socketio.on("entrar_partida_tempo_real")
def entrar_partida_tempo_real(data):
    data = data or {}

    partida_id = str(data.get("partida_id") or "").strip()
    perfil = str(data.get("perfil") or "").strip()
    competicao = str(data.get("competicao") or "").strip()

    if not partida_id:
        return

    sala = _room(partida_id)

    for r in _rooms_partida(partida_id, competicao):
        join_room(r)

    socketio.emit(
        "entrou_partida_tempo_real",
        {
            "ok": True,
            "partida_id": partida_id,
            "perfil": perfil,
            "competicao": competicao,
            "room": sala,
        },
        room=request.sid,
    )

    estado = _ESTADO_PARTIDAS.get(sala)

    if estado:
        payload = _normalizar_payload(partida_id, estado)
    else:
        payload = _normalizar_payload(
            partida_id,
            {
                "equipe_a": "Equipe A",
                "equipe_b": "Equipe B",
                "pontos_a": 0,
                "pontos_b": 0,
                "set_atual": 1,
                "ultima_acao": "Conectado ao tempo real",
                "sacador_nome": "Aguardando",
                "status_jogo": "aguardando",
            },
        )

    socketio.emit("estado_partida_tempo_real", payload, room=request.sid)
    socketio.emit("estado_partida", payload, room=request.sid)
    socketio.emit("estado_jogo_atualizado", payload, room=request.sid)


@socketio.on("join_partida")
def join_partida(data):
    return entrar_partida(data)


@socketio.on("join")
def join_generico(data):
    data = data or {}

    room = str(data.get("room") or data.get("sala") or "").strip()
    partida_id = str(data.get("partida_id") or "").strip()
    competicao = str(data.get("competicao") or "").strip()

    if room:
        join_room(room)

    if partida_id:
        for r in _rooms_partida(partida_id, competicao):
            join_room(r)

    socketio.emit(
        "entrou_partida",
        {
            "ok": True,
            "room": room,
            "partida_id": partida_id,
            "competicao": competicao,
        },
        room=request.sid,
    )


@socketio.on("entrar_arbitro")
def entrar_arbitro(data):
    data = data or {}

    partida_id = str(data.get("partida_id") or "").strip()
    competicao = str(data.get("competicao") or "").strip()

    if not partida_id:
        return

    sala = _room_arbitros(partida_id)
    sala_estado = _room(partida_id)

    for r in _rooms_partida(partida_id, competicao):
        join_room(r)

    socketio.emit(
        "entrou_partida",
        {
            "ok": True,
            "partida_id": str(partida_id),
            "competicao": competicao,
            "room": sala,
            "arbitro": True,
        },
        room=request.sid,
    )

    estado = _ESTADO_PARTIDAS.get(sala_estado) or _ESTADO_PARTIDAS.get(_room(partida_id))

    if estado:
        payload = _normalizar_payload(partida_id, estado)

        socketio.emit("estado_arbitros", payload, room=request.sid)
        socketio.emit("estado_partida", payload, room=request.sid)
        socketio.emit("estado_jogo_atualizado", payload, room=request.sid)
        socketio.emit("estado_partida_tempo_real", payload, room=request.sid)


def emitir_ultima_acao_arbitros(partida_id, texto):
    _emitir_salas(
        "ultima_acao_arbitros",
        {
            "partida_id": str(partida_id),
            "texto": str(texto or ""),
            "descricao": str(texto or ""),
        },
        partida_id,
    )


def emitir_cronometro_arbitros(partida_id, dados=None):
    payload = {
        "partida_id": str(partida_id),
        **(dados or {}),
    }

    _emitir_salas("cronometro_arbitros", payload, partida_id)
    _emitir_salas("cronometro_tempo", payload, partida_id)




@socketio.on("estado_partida_local")
def estado_partida_local_socket(data):
    """
    Canal principal do modo rápido/offline-first.
    O apontador envia o estado já calculado no navegador; o servidor só guarda
    em cache e retransmite para treinador, árbitros e telão, sem consultar banco.
    """
    data = dict(data or {})
    partida_id = str(data.get("partida_id") or "").strip()

    if not partida_id:
        return

    payload = _normalizar_payload(partida_id, data)
    _ESTADO_PARTIDAS[_room(partida_id)] = payload

    eventos = (
        "estado_partida",
        "estado_jogo_atualizado",
        "estado_arbitros",
        "estado_partida_tempo_real",
        "placar_atualizado",
        "ponto_registrado",
    )

    for evento in eventos:
        _emitir_salas(evento, payload, partida_id, include_self=False)

    # Telão/placar profissional por PIN escuta especificamente esta sala.
    # O modo rápido/offline-first precisa alimentar também esse canal,
    # senão árbitro/treinador recebem estado e o telão fica atrasado/travado.
    apontador = _normalizar_apontador(
        payload.get("apontador")
        or payload.get("apontador_login")
        or payload.get("operador_login")
    )
    sala_placar = _room_placar_apontador(apontador)
    if sala_placar:
        _ULTIMO_PLACAR_APONTADOR[apontador] = payload
        socketio.emit("placar_apontador_atualizado", payload, room=sala_placar)

    ultima_acao = str(payload.get("ultima_acao") or "").strip()
    if ultima_acao and ultima_acao != "-":
        _emitir_salas(
            "ultima_acao_arbitros",
            {
                "partida_id": str(partida_id),
                "texto": ultima_acao,
                "descricao": ultima_acao,
                "ultima_acao": ultima_acao,
            },
            partida_id,
            include_self=False,
        )

    saque_atual = str(payload.get("saque_atual") or "").strip().upper()
    # O estado_partida_tempo_real já contém saque_atual/sacador.
    # Não dispara saque_arbitros a cada refresh/local-state, pois isso gerava
    # popups repetidos e aparentemente aleatórios nos árbitros.
    if payload.get("forcar_popup_saque") and saque_atual in {"A", "B"}:
        _emitir_salas(
            "saque_arbitros",
            {
                "partida_id": str(partida_id),
                "equipe": saque_atual,
                "equipe_nome": payload.get("equipe_a") if saque_atual == "A" else payload.get("equipe_b"),
                "saque_atual": saque_atual,
                "sacador_nome": payload.get("sacador_nome") or "",
                "sacador_numero": payload.get("sacador_numero") or "",
            },
            partida_id,
            include_self=False,
        )

    socketio.emit("estado_partida_local_ok", {"ok": True, "partida_id": partida_id}, room=request.sid)


@socketio.on("cronometro_tempo")
def cronometro_tempo_socket(data):
    data = dict(data or {})

    partida_id = str(data.get("partida_id") or "").strip()

    if not partida_id:
        return

    payload = {
        "partida_id": partida_id,
        "ativo": True,
        "status": str(data.get("status") or "iniciado").strip().lower(),
        "duracao": _to_int(data.get("duracao") or data.get("segundos") or 30, 30),
        "segundos": _to_int(data.get("segundos") or data.get("restante") or data.get("duracao") or 30, 30),
        "restante": _to_int(data.get("restante") or data.get("segundos") or data.get("duracao") or 30, 30),
        "equipe": str(data.get("equipe") or "").strip().upper(),
        "equipe_nome": str(data.get("equipe_nome") or "").strip(),
        "origem": "apontador",
    }

    _emitir_salas("cronometro_tempo", payload, partida_id, include_self=False)
    _emitir_salas("cronometro_arbitros", payload, partida_id, include_self=False)
    _emitir_salas("tempo_executado", payload, partida_id, include_self=False)


@socketio.on("inversao_lados_apontador")
def inversao_lados_apontador(data=None):
    data = dict(data or {})

    apontador = _normalizar_apontador(data.get("apontador"))
    partida_id = str(data.get("partida_id") or "").strip()

    if not apontador or not partida_id:
        return

    invertido = _to_bool(data.get("invertido", data.get("lados_invertidos")), False)
    sala = _room_placar_apontador(apontador)

    _INVERSAO_PLACAR_APONTADOR[(apontador, partida_id)] = invertido

    payload = {
        "partida_id": partida_id,
        "competicao": str(data.get("competicao") or ""),
        "apontador": apontador,
        "lados_invertidos_apontador": invertido,
    }

    ultimo = _ULTIMO_PLACAR_APONTADOR.get(apontador)

    if isinstance(ultimo, dict):
        ultimo = dict(ultimo)
        ultimo["lados_invertidos_apontador"] = invertido
        _ULTIMO_PLACAR_APONTADOR[apontador] = ultimo

    if sala:
        socketio.emit("inversao_lados_apontador", payload, room=sala)


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