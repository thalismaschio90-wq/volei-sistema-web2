from datetime import date, datetime

from flask import request
from flask_socketio import join_room

from extensions import socketio


# Cache simples em memória para o estado ao vivo da partida.
# Evita consulta no banco toda hora e deixa treinador/telão mais rápidos.
_ESTADO_PARTIDAS = {}

# Sala global do telão/placar ao vivo.
# Essa sala NÃO depende da partida. O telão fica aberto nela o evento inteiro.
PLACAR_GERAL_ROOM = "placar_geral_ao_vivo"

# Último estado exibido no telão.
_ULTIMO_PLACAR_GERAL = None

# Último estado por apontador/telão individual.
_ULTIMO_PLACAR_APONTADOR = {}


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


def _normalizar_lista(valor):
    if not valor:
        return []
    if isinstance(valor, list):
        return valor
    if isinstance(valor, tuple):
        return list(valor)
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


def _mesclar_com_cache(partida_id, dados=None):
    """
    Junta o estado novo com o último estado salvo em memória.

    Isso é importante porque algumas rotas mandam só parte do estado.
    Exemplo: uma substituição pode não mandar equipe_a/equipe_b ou set1_a/set1_b.
    Se emitirmos só o parcial, o telão pode perder nomes/placar/evolução.
    """
    sala = _room(partida_id)
    antigo = _ESTADO_PARTIDAS.get(sala) or {}
    novo = dict(dados or {})

    mesclado = dict(antigo)
    mesclado.update(novo)

    # Mantém inversão se o estado parcial não trouxe essa informação.
    if "invertido" not in novo and "invertido" in antigo:
        mesclado["invertido"] = antigo.get("invertido")

    return mesclado


def _normalizar_payload(partida_id, dados=None):
    dados = dict(dados or {})

    pontos_a = _to_int(dados.get("pontos_a", dados.get("placar_a", 0)))
    pontos_b = _to_int(dados.get("pontos_b", dados.get("placar_b", 0)))

    sets_a = _to_int(dados.get("sets_a", 0))
    sets_b = _to_int(dados.get("sets_b", 0))

    set_atual = _to_int(dados.get("set_atual", 1), 1)

    rotacao = dados.get("rotacao") or {}
    if not isinstance(rotacao, dict):
        rotacao = {}

    rotacao_a = dados.get("rotacao_a") or rotacao.get("equipe_a") or []
    rotacao_b = dados.get("rotacao_b") or rotacao.get("equipe_b") or []
    rotacao_propria = dados.get("rotacao_propria") or rotacao.get("propria") or []

    equipe_a = (
        dados.get("equipe_a")
        or dados.get("equipe_a_nome")
        or dados.get("nome_a")
        or ""
    )
    equipe_b = (
        dados.get("equipe_b")
        or dados.get("equipe_b_nome")
        or dados.get("nome_b")
        or ""
    )

    payload = {
        **dados,
        "ok": dados.get("ok", True),
        "partida_id": str(partida_id),

        "pontos_a": pontos_a,
        "pontos_b": pontos_b,
        "placar_a": pontos_a,
        "placar_b": pontos_b,

        "sets_a": sets_a,
        "sets_b": sets_b,
        "set_atual": set_atual,
        "descricao_set": dados.get("descricao_set") or f"{set_atual}º SET",

        # Evolução dos sets para o telão.
        "set1_a": _to_int(dados.get("set1_a", 0)),
        "set1_b": _to_int(dados.get("set1_b", 0)),
        "set2_a": _to_int(dados.get("set2_a", 0)),
        "set2_b": _to_int(dados.get("set2_b", 0)),
        "set3_a": _to_int(dados.get("set3_a", 0)),
        "set3_b": _to_int(dados.get("set3_b", 0)),
        "set4_a": _to_int(dados.get("set4_a", 0)),
        "set4_b": _to_int(dados.get("set4_b", 0)),
        "set5_a": _to_int(dados.get("set5_a", 0)),
        "set5_b": _to_int(dados.get("set5_b", 0)),

        "equipe_a": equipe_a,
        "equipe_b": equipe_b,
        "equipe_nome": dados.get("equipe_nome") or "",
        "equipe_adversaria": dados.get("equipe_adversaria") or "",

        "competicao": dados.get("competicao") or "",
        "status": dados.get("status") or "",
        "status_jogo": dados.get("status_jogo") or dados.get("status") or "",
        "fase_partida": dados.get("fase_partida") or "",
        "partida_finalizada": bool(dados.get("partida_finalizada", False)),

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
        "rotacao_propria": _normalizar_lista(rotacao_propria),
        "rotacao": {
            "equipe_a": _normalizar_lista(rotacao_a),
            "equipe_b": _normalizar_lista(rotacao_b),
            "propria": _normalizar_lista(rotacao_propria),
        },

        "lado": dados.get("lado") or "",
        "lado_quadra": dados.get("lado_quadra") or "",

        # Usado pelo telão para inverter visualmente os lados.
        "invertido": bool(dados.get("invertido", False)),

        "banco": dados.get("banco") or [],
        "scout": dados.get("scout") or {},
        "eventos": dados.get("eventos") or [],
        "historico": dados.get("historico") or [],
        "solicitacoes": dados.get("solicitacoes") or [],
        "ultima_acao": dados.get("ultima_acao") or "-",
    }

    return _json_safe(payload)


def obter_estado_cache(partida_id):
    """Usado por rotas leves se precisar devolver o último estado sem consultar banco."""
    return _ESTADO_PARTIDAS.get(_room(partida_id))


def limpar_estado_cache(partida_id):
    _ESTADO_PARTIDAS.pop(_room(partida_id), None)


def emitir_estado_partida(partida_id, dados=None):
    """Emite estado para a sala específica da partida."""
    sala = _room(partida_id)
    if not sala:
        return

    dados_mesclados = _mesclar_com_cache(partida_id, dados)
    payload = _normalizar_payload(partida_id, dados_mesclados)

    _ESTADO_PARTIDAS[sala] = payload

    socketio.emit(
        "estado_jogo_atualizado",
        payload,
        room=sala,
    )


def emitir_placar_geral(partida_id, dados=None):
    """
    Atualiza o telão fixo do torneio.

    Use quando:
    - o apontador entrar em uma partida;
    - marcar ponto;
    - desfazer ponto;
    - trocar set;
    - finalizar partida;
    - inverter lados.
    """
    global _ULTIMO_PLACAR_GERAL

    sala = _room(partida_id)
    if not sala:
        return

    dados_mesclados = _mesclar_com_cache(partida_id, dados)
    payload = _normalizar_payload(partida_id, dados_mesclados)

    _ULTIMO_PLACAR_GERAL = payload
    _ESTADO_PARTIDAS[sala] = payload

    socketio.emit(
        "placar_geral_atualizado",
        payload,
        room=PLACAR_GERAL_ROOM,
    )


def emitir_placar_apontador(apontador, partida_id, dados=None):
    """
    Atualiza o telão fixo vinculado a um apontador específico.

    Fluxo ideal para múltiplas quadras/projetores:
    Apontador X abre qualquer partida -> somente o telão do Apontador X muda.
    """
    apontador = _normalizar_apontador(apontador)
    sala_placar = _room_placar_apontador(apontador)
    sala_partida = _room(partida_id)

    if not apontador or not sala_placar or not sala_partida:
        return

    dados_mesclados = _mesclar_com_cache(partida_id, dados)
    dados_mesclados["apontador"] = apontador

    payload = _normalizar_payload(partida_id, dados_mesclados)
    payload["apontador"] = apontador

    _ULTIMO_PLACAR_APONTADOR[apontador] = payload
    _ESTADO_PARTIDAS[sala_partida] = payload

    socketio.emit(
        "placar_apontador_atualizado",
        payload,
        room=sala_placar,
    )


def obter_ultimo_placar_apontador(apontador):
    apontador = _normalizar_apontador(apontador)
    if not apontador:
        return None
    return _ULTIMO_PLACAR_APONTADOR.get(apontador)


def obter_ultimo_placar_geral():
    return _ULTIMO_PLACAR_GERAL


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
    if not partida_id:
        return

    sala = _room(partida_id)
    join_room(sala)

    # Não consulta banco aqui para não travar o pool.
    estado_cache = _ESTADO_PARTIDAS.get(sala)
    if estado_cache:
        socketio.emit(
            "estado_jogo_atualizado",
            estado_cache,
            room=request.sid,
        )


@socketio.on("entrar_placar_geral")
def entrar_placar_geral(data=None):
    """
    Telão entra aqui.
    Ele fica sempre na sala global.
    Se já tiver jogo em andamento, recebe o último estado imediatamente.
    """
    join_room(PLACAR_GERAL_ROOM)

    if _ULTIMO_PLACAR_GERAL:
        socketio.emit(
            "placar_geral_atualizado",
            _ULTIMO_PLACAR_GERAL,
            room=request.sid,
        )


@socketio.on("entrar_placar_apontador")
def entrar_placar_apontador(data=None):
    """
    Telão individual entra aqui.
    Cada apontador tem sua própria sala de placar.
    """
    data = data or {}
    apontador = _normalizar_apontador(data.get("apontador"))
    sala = _room_placar_apontador(apontador)

    if not apontador or not sala:
        return

    join_room(sala)

    ultimo = _ULTIMO_PLACAR_APONTADOR.get(apontador)
    if ultimo:
        socketio.emit(
            "placar_apontador_atualizado",
            ultimo,
            room=request.sid,
        )
