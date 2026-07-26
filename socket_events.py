from datetime import date, datetime
import time
import os
import copy
import threading

from flask import request, session
from flask_socketio import join_room
from extensions import socketio


# =========================
# CACHE ULTRA RÁPIDO
# =========================
_ESTADO_PARTIDAS = {}
_ESTADO_PARTIDAS_LOCK = threading.RLock()

PLACAR_GERAL_ROOM = "placar_geral_ao_vivo"
_ULTIMO_PLACAR_GERAL = None
_ULTIMO_PLACAR_APONTADOR = {}
_INVERSAO_PLACAR_APONTADOR = {}

# Controle para não reenviar payload gigante a cada ponto.
# 0 = quase nunca envia completo automaticamente; valores maiores enviam um
# estado completo no máximo a cada N segundos, mantendo o placar leve em tempo real.
try:
    SOCKET_FULL_STATE_INTERVAL = float(os.environ.get("SOCKET_FULL_STATE_INTERVAL", "30") or 30)
except Exception:
    SOCKET_FULL_STATE_INTERVAL = 30

_ULTIMO_ESTADO_COMPLETO_EMITIDO = {}
_ESTADO_PARTIDAS_VERSAO = {}


def _env_bool(nome, padrao=False):
    texto = str(os.environ.get(nome, "1" if padrao else "0") or "").strip().lower()
    return texto in {"1", "true", "sim", "s", "yes", "on"}



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
        return ["", "", "", "", "", ""]

    saida = []

    for item in list(valor)[:6]:
        if isinstance(item, dict):
            numero = (
                item.get("numero")
                or item.get("camisa")
                or item.get("numero_camisa")
                or item.get("atleta_numero")
                or item.get("n")
                or ""
            )
        else:
            numero = item

        texto = str(numero or "").strip()
        saida.append("" if texto == "[object Object]" else texto)

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





def _normalizar_sets_tipo_socket(valor):
    texto = str(valor or "").strip().lower().replace("-", "_").replace(" ", "_")
    if texto in {"set_unico", "único", "unico", "1_set", "melhor_de_1", "md1"}:
        return "set_unico"
    if texto in {"melhor_de_5", "md5", "5"}:
        return "melhor_de_5"
    return "melhor_de_3"


def _aplicar_placar_exibicao_socket(payload):
    sets_tipo = _normalizar_sets_tipo_socket(payload.get("sets_tipo") or payload.get("tipo_sets") or payload.get("formato_sets"))
    set_unico = sets_tipo == "set_unico" or _to_int(payload.get("sets_max"), 3) == 1

    if set_unico:
        a = _to_int(_primeiro_valor(payload, ["set1_a", "pontos_a", "placar_a"], 0), 0)
        b = _to_int(_primeiro_valor(payload, ["set1_b", "pontos_b", "placar_b"], 0), 0)
        tipo = "pontos"
        rotulo = "PONTOS"
    else:
        a = _to_int(payload.get("sets_a"), 0)
        b = _to_int(payload.get("sets_b"), 0)
        tipo = "sets"
        rotulo = "SETS"

    payload["set_unico"] = bool(set_unico)
    payload["placar_exibicao_a"] = a
    payload["placar_exibicao_b"] = b
    payload["placar_exibicao_tipo"] = tipo
    payload["placar_exibicao_rotulo"] = rotulo
    payload["placar_exibicao"] = f"{a} x {b}"
    return payload


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



def _login_socket_payload(data):
    data = data or {}
    return _normalizar_apontador(
        data.get("operador_login")
        or data.get("apontador_login")
        or data.get("apontador")
        or session.get("usuario")
        or session.get("usuario_login")
        or session.get("login")
        or session.get("apontador_login")
    )


def _validar_operador_socket(partida_id, competicao, data):
    """Impede que outro socket/apontador sobrescreva o cache da partida.

    Jogo avulso é operado por PIN/sessão própria e usa partida_id avulso:<codigo>.
    Ele não existe na tabela de partidas da competição, então não pode passar
    pela trava operacional normal. Sem este bypass o socket do jogo rápido
    conecta, mas o servidor rejeita o estado e árbitros/telão ficam atrasados.
    """
    data = data or {}
    partida_txt = str(partida_id or "").strip().lower()
    if partida_txt.startswith("avulso:") or data.get("modo_avulso") or str(data.get("competicao") or "").strip().upper() == "JOGO AVULSO":
        return True, "Jogo avulso liberado."

    login = _login_socket_payload(data)
    if not login:
        return False, "Sessão do apontador não identificada."

    try:
        from banco import validar_operador_partida, heartbeat_partida_operacional
        ok, msg, _partida = validar_operador_partida(partida_id, competicao, login, renovar=True)
        if ok:
            heartbeat_partida_operacional(partida_id, competicao, login, getattr(request, "sid", None))
        return ok, msg
    except Exception as e:
        print("ERRO validar operador socket:", repr(e), flush=True)
        return False, "Não foi possível validar a trava operacional da partida."


# =========================
# CACHE
# =========================
def obter_estado_cache(partida_id):
    # Nunca entrega a referência interna. Uma substituição que alterasse um
    # dict/lista aninhado podia modificar o cache inteiro sem intenção.
    with _ESTADO_PARTIDAS_LOCK:
        estado = _ESTADO_PARTIDAS.get(_room(partida_id))
        return copy.deepcopy(estado) if estado is not None else None


def obter_estado_versao(partida_id):
    """Versão monotônica do estado vivo da partida neste processo.

    Permite ao visualizador detectar cada ação sem consultar MAX(id) no banco
    a cada polling. A versão volta a zero em reinício, quando o cliente força
    uma leitura completa normalmente.
    """
    with _ESTADO_PARTIDAS_LOCK:
        return int(_ESTADO_PARTIDAS_VERSAO.get(_room(partida_id), 0) or 0)


def atualizar_estado_cache(partida_id, dados):
    sala = _room(partida_id)

    if not sala:
        return


    normalizado = _normalizar_payload(partida_id, copy.deepcopy(dados or {}))
    with _ESTADO_PARTIDAS_LOCK:
        _ESTADO_PARTIDAS[sala] = normalizado
        _ESTADO_PARTIDAS_VERSAO[sala] = int(_ESTADO_PARTIDAS_VERSAO.get(sala, 0) or 0) + 1


def limpar_estado_cache(partida_id):
    with _ESTADO_PARTIDAS_LOCK:
        sala = _room(partida_id)
        _ESTADO_PARTIDAS.pop(sala, None)
        _ESTADO_PARTIDAS_VERSAO.pop(sala, None)


def obter_ultimo_placar_apontador(apontador):
    apontador = _normalizar_apontador(apontador)
    return _ULTIMO_PLACAR_APONTADOR.get(apontador)


# =========================
# NORMALIZAÇÃO
# =========================
def _nome_equipe_igual_socket(a, b):
    return str(a or "").strip().casefold() == str(b or "").strip().casefold()


def _adicionar_placar_publico_socket(payload):
    """Adiciona a ordem pública sem alterar o estado operacional original."""
    cadastro_a = str(payload.get("equipe_a_cadastro") or payload.get("equipe_a") or "").strip()
    cadastro_b = str(payload.get("equipe_b_cadastro") or payload.get("equipe_b") or "").strip()
    operacional_a = str(payload.get("equipe_a_operacional") or payload.get("equipe_a") or cadastro_a).strip()
    operacional_b = str(payload.get("equipe_b_operacional") or payload.get("equipe_b") or cadastro_b).strip()
    pontos_a = _to_int(payload.get("pontos_a"), 0)
    pontos_b = _to_int(payload.get("pontos_b"), 0)

    invertido = bool(
        cadastro_a and cadastro_b and operacional_a and operacional_b
        and _nome_equipe_igual_socket(operacional_a, cadastro_b)
        and _nome_equipe_igual_socket(operacional_b, cadastro_a)
    )
    publico_a, publico_b = (pontos_b, pontos_a) if invertido else (pontos_a, pontos_b)
    payload["equipe_a_cadastro"] = cadastro_a or operacional_a
    payload["equipe_b_cadastro"] = cadastro_b or operacional_b
    payload["pontos_publicos_a"] = publico_a
    payload["pontos_publicos_b"] = publico_b
    payload["placar_publico_a"] = publico_a
    payload["placar_publico_b"] = publico_b
    payload["lados_operacionais_invertidos"] = invertido
    return payload


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

        # Quadra padronizada para visualizador/tabela/apontador.
        # O ID é a referência real; os nomes abaixo são só exibição.
        "quadra_id": dados.get("quadra_id") or "",
        "quadra_nome": dados.get("quadra_nome") or dados.get("quadra_exibicao") or dados.get("quadra_label") or "",
        "quadra_exibicao": dados.get("quadra_exibicao") or dados.get("quadra_label") or dados.get("quadra_nome") or "",
        "quadra_label": dados.get("quadra_label") or dados.get("quadra_exibicao") or dados.get("quadra_nome") or "",

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

    payload = _adicionar_placar_publico_socket(payload)
    payload = _aplicar_placar_exibicao_socket(payload)
    return _json_safe(payload)



# =========================
# PAYLOAD LEVE PARA TEMPO REAL
# =========================
def _payload_placar_rapido(payload):
    """Payload pequeno para eventos frequentes.

    Mantém os campos que o placar/apontador/telão normalmente precisam a cada
    ponto, mas não carrega listas pesadas como atletas, eventos, histórico,
    evolução de pontos e scout completo.
    """
    if not isinstance(payload, dict):
        return {}

    chaves = [
        "ok", "partida_id", "competicao",
        "pontos_a", "pontos_b", "placar_a", "placar_b",
        "sets_a", "sets_b", "set_atual", "sets_tipo", "set_unico",
        "placar_exibicao_a", "placar_exibicao_b", "placar_exibicao_tipo",
        "placar_exibicao_rotulo", "placar_exibicao",
        "equipe_a", "equipe_b", "equipe_a_cadastro", "equipe_b_cadastro",
        "equipe_a_operacional", "equipe_b_operacional",
        "pontos_publicos_a", "pontos_publicos_b", "placar_publico_a", "placar_publico_b",
        "lados_operacionais_invertidos",
        "escudo_a", "escudo_b", "escudo_a_operacional", "escudo_b_operacional",
        "equipe_a_escudo", "equipe_b_escudo",
        "cor_a", "cor_b", "cor_a_operacional", "cor_b_operacional",
        "equipe_a_cor", "equipe_b_cor",
        "quadra_id", "quadra_nome", "quadra_exibicao", "quadra_label",
        "saque_atual", "sacador_nome", "sacador_numero",
        "rotacao_a", "rotacao_b",
        "tempos_a", "tempos_b", "limite_tempos",
        "subs_a", "subs_b", "limite_substituicoes",
        "status_jogadores_a", "status_jogadores_b",
        "pontos_set", "ponto_alvo_set", "pontos_para_vencer_set",
        "pontos_tiebreak", "diferenca_minima", "sets_para_vencer", "sets_max",
        "fase_partida", "status_jogo", "fim_set", "set_finalizado", "fim_jogo",
        "partida_finalizada", "vencedor_set", "vencedor_partida",
        "ultima_acao", "apontador", "lados_invertidos_apontador",
    ]

    leve = {k: payload.get(k) for k in chaves if k in payload}
    leve["payload_leve"] = True
    return _json_safe(leve)


def _deve_emitir_estado_completo(partida_id, payload):
    if _env_bool("SOCKET_FULL_STATE_EVERY_POINT", False):
        return True

    if payload.get("fim_set") or payload.get("fim_jogo") or payload.get("partida_finalizada"):
        return True

    status = str(payload.get("status_jogo") or payload.get("fase_partida") or "").strip().lower()
    if status in {"finalizada", "finalizado", "encerrada", "encerrado", "entre_sets", "tiebreak_sorteio"}:
        return True

    intervalo = SOCKET_FULL_STATE_INTERVAL
    if intervalo <= 0:
        return False

    chave = _room(partida_id)
    agora = time.time()
    ultimo = _ULTIMO_ESTADO_COMPLETO_EMITIDO.get(chave, 0)
    if (agora - ultimo) >= intervalo:
        _ULTIMO_ESTADO_COMPLETO_EMITIDO[chave] = agora
        return True

    return False

# =========================
# EMISSÃO PRINCIPAL
# =========================
def emitir_estado_partida(partida_id, dados=None):
    sala = _room(partida_id)

    if not sala:
        return

    payload = _normalizar_payload(partida_id, dados)

    # Guarda sempre o estado completo em memória para quem entrar/recarregar a tela.
    with _ESTADO_PARTIDAS_LOCK:
        _ESTADO_PARTIDAS[sala] = copy.deepcopy(payload)
        _ESTADO_PARTIDAS_VERSAO[sala] = int(_ESTADO_PARTIDAS_VERSAO.get(sala, 0) or 0) + 1
        payload["estado_versao"] = _ESTADO_PARTIDAS_VERSAO[sala]

    payload_leve = _payload_placar_rapido(payload)

    # Evento novo e leve para placares/telão/apontador. É o caminho preferido.
    _emitir_salas("placar_rapido", payload_leve, partida_id)

    # Mantém compatibilidade com telas antigas, mas enviando pacote pequeno.
    # O estado completo continua disponível no cache e é enviado ao entrar na sala.
    _emitir_salas("estado_partida", payload_leve, partida_id)
    _emitir_salas("estado_jogo_atualizado", payload_leve, partida_id)
    _emitir_salas("estado_arbitros", payload_leve, partida_id)
    _emitir_salas("estado_partida_tempo_real", payload_leve, partida_id)

    # Estado completo só periodicamente ou em momentos importantes.
    if _deve_emitir_estado_completo(partida_id, payload):
        _emitir_salas("estado_partida_completo", payload, partida_id)

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

    # ==========================================================
    # Atualiza também o placar profissional automaticamente
    # ==========================================================
    apontador = _normalizar_apontador(
        payload.get("apontador")
        or payload.get("apontador_login")
        or payload.get("operador_login")
    )

    if apontador:
        _ULTIMO_PLACAR_APONTADOR[apontador] = payload

        socketio.emit(
            "placar_apontador_atualizado",
            _payload_placar_rapido(payload),
            room=_room_placar_apontador(apontador),
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

    socketio.emit("placar_geral_atualizado", _payload_placar_rapido(payload), room=PLACAR_GERAL_ROOM)


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

    socketio.emit("placar_apontador_atualizado", _payload_placar_rapido(payload), room=sala)


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




@socketio.on("estado_avulso_local")
def estado_avulso_local_socket(data):
    """Canal leve específico do Jogo Rápido/Avulso.

    Recebe o estado calculado no navegador do apontador e retransmite
    imediatamente para árbitros e telão, sem depender do polling HTTP.
    """
    data = dict(data or {})
    partida_id = str(data.get("partida_id") or "").strip()
    if not partida_id:
        return

    data["modo_avulso"] = True
    data["competicao"] = data.get("competicao") or "JOGO AVULSO"
    payload = _normalizar_payload(partida_id, data)

    # Proteção contra regressão de set: após a troca local para o próximo set,
    # uma aba/reconexão atrasada não pode recolocar o cache no set anterior e
    # retransmitir 0x0/1º set para apontador, árbitros e telão.
    cache_anterior = _ESTADO_PARTIDAS.get(_room(partida_id)) or {}
    set_cache = _to_int(cache_anterior.get("set_atual"), 1)
    set_recebido = _to_int(payload.get("set_atual"), 1)
    permite_regressao = _to_bool(data.get("permitir_regressao_set"), False)
    if cache_anterior and set_recebido < set_cache and not permite_regressao:
        socketio.emit("estado_partida_local_ok", {
            "ok": True,
            "ignorado_por_estado_antigo": True,
            "partida_id": partida_id,
            "set_atual": set_cache,
        }, room=request.sid)
        return

    _ESTADO_PARTIDAS[_room(partida_id)] = payload

    eventos = (
        "placar_rapido",
        "estado_partida",
        "estado_jogo_atualizado",
        "estado_arbitros",
        "estado_partida_tempo_real",
        "estado_avulso",
        "placar_avulso",
        "jogo_avulso_estado",
        "placar_atualizado",
        "ponto_registrado",
    )
    for evento in eventos:
        _emitir_salas(evento, payload, partida_id, include_self=False)

    ultima_acao = str(payload.get("ultima_acao") or "").strip()
    if ultima_acao and ultima_acao != "-":
        _emitir_salas("ultima_acao_arbitros", {
            "partida_id": str(partida_id),
            "texto": ultima_acao,
            "descricao": ultima_acao,
            "ultima_acao": ultima_acao,
        }, partida_id, include_self=False)

    # Avisos centralizados do jogo rápido.
    # O apontador envia `aviso_arbitragem` no estado; cada tela decide se deve exibir
    # conforme `destinos`: apontador, primeiro, segundo ou todos.
    aviso = payload.get("aviso_arbitragem")
    if isinstance(aviso, dict):
        aviso = dict(aviso)
        aviso.setdefault("partida_id", str(partida_id))
        aviso.setdefault("competicao", payload.get("competicao") or "JOGO AVULSO")
        for evento_aviso in ("aviso_arbitragem", "avisos_arbitragem", "arbitragem_aviso"):
            _emitir_salas(evento_aviso, aviso, partida_id, include_self=False)

        tipo_aviso = str(aviso.get("tipo") or "").strip().lower()
        if tipo_aviso in {"tempo", "tempo_fim"}:
            dados_tempo = {
                "partida_id": str(partida_id),
                "competicao": payload.get("competicao") or "JOGO AVULSO",
                "equipe": aviso.get("equipe"),
                "equipe_nome": aviso.get("texto") or aviso.get("equipe_nome"),
                "segundos": aviso.get("segundos") or 30,
                "restante": 0 if tipo_aviso == "tempo_fim" else (aviso.get("segundos") or 30),
                "status": "finalizado" if tipo_aviso == "tempo_fim" else "iniciado",
                "origem": "apontador",
                "tipo": "tempo_executado",
                "restantes_tempos": aviso.get("restantes_tempos"),
            }
            for evento_tempo in ("cronometro_arbitros", "cronometro_tempo", "tempo_executado", "tempo_apontador", "tempo_oficial"):
                _emitir_salas(evento_tempo, dados_tempo, partida_id, include_self=False)

    tempo_ativo = payload.get("tempo_ativo")
    if isinstance(tempo_ativo, dict) and str(tempo_ativo.get("origem") or "apontador").lower() == "apontador":
        dados_tempo = dict(tempo_ativo)
        dados_tempo.setdefault("partida_id", str(partida_id))
        dados_tempo.setdefault("competicao", payload.get("competicao") or "JOGO AVULSO")
        dados_tempo.setdefault("tipo", "tempo_executado")
        dados_tempo.setdefault("origem", "apontador")
        for evento_tempo in ("cronometro_arbitros", "cronometro_tempo", "tempo_executado", "tempo_apontador", "tempo_oficial"):
            _emitir_salas(evento_tempo, dados_tempo, partida_id, include_self=False)

    socketio.emit("estado_avulso_local_ok", {"ok": True, "partida_id": partida_id}, room=request.sid)


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

    competicao = str(data.get("competicao") or "").strip()
    ok_lock, msg_lock = _validar_operador_socket(partida_id, competicao, data)
    if not ok_lock:
        socketio.emit("estado_partida_local_ok", {
            "ok": False,
            "bloqueada": True,
            "partida_id": partida_id,
            "mensagem": msg_lock,
        }, room=request.sid)
        return

    payload = _normalizar_payload(partida_id, data)

    # O navegador pode reconectar carregando o HTML/snapshot antigo. Ele não
    # pode sobrescrever o cache vivo e fazer telão/árbitros/apontador voltarem.
    sala_cache = _room(partida_id)
    with _ESTADO_PARTIDAS_LOCK:
        atual = copy.deepcopy(_ESTADO_PARTIDAS.get(sala_cache) or {})

        def _progresso(d):
            return (
                _to_int(d.get("sets_a"), 0) + _to_int(d.get("sets_b"), 0),
                max(1, _to_int(d.get("set_atual"), 1)),
            )

        prog_atual = _progresso(atual)
        prog_novo = _progresso(payload)
        total_atual = _to_int(atual.get("pontos_a"), 0) + _to_int(atual.get("pontos_b"), 0)
        total_novo = _to_int(payload.get("pontos_a"), 0) + _to_int(payload.get("pontos_b"), 0)
        origem = str(payload.get("origem") or "").strip().lower()
        permite_reducao = "desfazer" in origem

        atrasado = bool(atual) and not permite_reducao and (
            prog_novo < prog_atual
            or (prog_novo == prog_atual and total_novo < total_atual)
        )

        if atrasado:
            # Conserva todo o estado vivo, mas confirma ao emissor para ele
            # solicitar a hidratação oficial em vez de insistir no snapshot velho.
            socketio.emit("estado_partida_local_ok", {
                "ok": False,
                "snapshot_atrasado": True,
                "partida_id": partida_id,
                "estado_atual": atual,
                "mensagem": "Snapshot local mais antigo que o estado vivo; atualização ignorada.",
            }, room=request.sid)
            return

        _ESTADO_PARTIDAS[sala_cache] = copy.deepcopy(payload)

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
        socketio.emit(
            "placar_apontador_atualizado",
            _payload_placar_rapido(ultimo),
            room=request.sid,
        )