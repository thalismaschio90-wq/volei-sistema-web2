from datetime import date, datetime
import time
import os
import copy
import threading

from flask import request, session
from flask_socketio import join_room
from extensions import socketio
from realtime.publisher import publicar_nas_salas
from realtime.event_priority import (
    PRIORIDADE_BAIXA,
    PRIORIDADE_CRITICA,
    PRIORIDADE_NORMAL,
    event_dispatcher,
)
from realtime.inbound_state import aceitar_e_salvar_estado
from realtime.delta import criar_delta_estado, delta_compensa
from realtime.delta_metrics import delta_metrics_store
from realtime.rooms import (
    normalizar_id_partida,
    sala_arbitros,
    sala_placar_apontador,
    sala_delta,
    sala_legacy,
    salas_partida,
)
from realtime.state_store import estado_partidas_store
from realtime.live_state import estado_partida_vivo
from realtime.event_history import historico_delta_store
from realtime.recovery import recuperar_estado
from realtime.presence import presence_store
from realtime.synchronization import (
    emitir_para_cliente,
    inscrever_em_salas,
    montar_confirmacao,
    normalizar_entrada,
    obter_estado_inicial,
)


# =========================
# CACHE ULTRA RÁPIDO
# =========================
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
_ULTIMA_VERSAO_PUBLICADA = {}
_PUBLICACAO_VERSAO_LOCK = threading.RLock()


def _env_bool(nome, padrao=False):
    texto = str(os.environ.get(nome, "1" if padrao else "0") or "").strip().lower()
    return texto in {"1", "true", "sim", "s", "yes", "on"}



SOCKET_DELTA_ENABLED = _env_bool("SOCKET_DELTA_ENABLED", True)
SOCKET_LEGACY_STATE_EVENTS = _env_bool("SOCKET_LEGACY_STATE_EVENTS", True)
SOCKET_LEGACY_REQUIRE_DELTA_HEALTHY = _env_bool("SOCKET_LEGACY_REQUIRE_DELTA_HEALTHY", False)
try:
    SOCKET_DELTA_MIN_SAVING_PERCENT = float(
        os.environ.get("SOCKET_DELTA_MIN_SAVING_PERCENT", "10") or 10
    )
except Exception:
    SOCKET_DELTA_MIN_SAVING_PERCENT = 10.0


# =========================
# HELPERS
# =========================
def _room(partida_id):
    return normalizar_id_partida(partida_id)


def _room_arbitros(partida_id):
    return sala_arbitros(partida_id)


def _rooms_partida(partida_id, competicao=None):
    return salas_partida(partida_id, competicao)


def _emitir_salas(
    evento,
    payload,
    partida_id,
    *,
    prioridade=None,
    deduplicar_ms=0.0,
    **kwargs,
):
    publicar_nas_salas(
        socketio,
        evento,
        payload,
        partida_id,
        normalizar=_json_safe,
        prioridade=prioridade,
        deduplicar_ms=deduplicar_ms,
        **kwargs,
    )


def _emitir_sala_capacidade(
    evento,
    payload,
    sala,
    *,
    prioridade=None,
    deduplicar_ms=0.0,
):
    if not sala:
        return
    event_dispatcher.publicar(
        socketio,
        evento,
        _json_safe(payload),
        sala=sala,
        prioridade=prioridade,
        deduplicar_ms=deduplicar_ms,
    )

def _normalizar_apontador(apontador):
    return str(apontador or "").strip()


def _room_placar_apontador(apontador):
    apontador = _normalizar_apontador(apontador)
    return sala_placar_apontador(apontador)


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
    return estado_partida_vivo.obter(partida_id)


def obter_estado_versao(partida_id):
    """Versão monotônica do estado vivo neste processo."""
    return estado_partida_vivo.versao(partida_id)


def atualizar_estado_cache(partida_id, dados):
    sala = _room(partida_id)
    if not sala:
        return None
    normalizado = _normalizar_payload(partida_id, copy.deepcopy(dados or {}))
    salvo = estado_partida_vivo.salvar(sala, normalizado, atualizar_origem=isinstance(dados, dict))
    return copy.deepcopy(salvo.estado) if salvo else None


def limpar_estado_cache(partida_id):
    estado_partida_vivo.remover(partida_id)
    historico_delta_store.remover(partida_id)
    chave = _room(partida_id)
    if chave:
        with _PUBLICACAO_VERSAO_LOCK:
            _ULTIMA_VERSAO_PUBLICADA.pop(chave, None)
            _ULTIMO_ESTADO_COMPLETO_EMITIDO.pop(chave, None)


def obter_ultimo_placar_apontador(apontador):
    apontador = _normalizar_apontador(apontador)
    estado = _ULTIMO_PLACAR_APONTADOR.get(apontador)
    return copy.deepcopy(estado) if estado is not None else None


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
        "ok", "partida_id", "competicao", "estado_versao", "estado_atualizado_em",
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


def _reservar_versao_publicacao(partida_id, versao):
    """Garante no máximo uma publicação por versão em cada processo.

    A rota pode salvar o estado e chamar mais de um helper de publicação. A
    versão oficial é monotônica; portanto, republicar a mesma versão apenas
    multiplica serialização e mensagens Socket.IO sem acrescentar informação.
    """
    chave = _room(partida_id)
    if not chave:
        return False
    try:
        versao_num = int(versao or 0)
    except (TypeError, ValueError):
        versao_num = 0
    if versao_num <= 0:
        return True
    with _PUBLICACAO_VERSAO_LOCK:
        anterior = int(_ULTIMA_VERSAO_PUBLICADA.get(chave, 0) or 0)
        if versao_num <= anterior:
            return False
        _ULTIMA_VERSAO_PUBLICADA[chave] = versao_num
        return True


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

    # A fachada viva evita uma segunda gravação quando a rota já salvou o
    # mesmo snapshot imediatamente antes de publicar. Assim uma ação gera uma
    # única versão oficial, usada por apontador, árbitros, telão e público.
    publicacao = estado_partida_vivo.preparar_publicacao(sala, payload)
    if publicacao is None:
        return
    anterior = publicacao.anterior
    salvo = publicacao.atual
    payload = salvo.estado

    # Uma versão já publicada não precisa gerar novamente placar, eventos
    # legados, snapshot e avisos auxiliares. A entrada de novos clientes usa o
    # fluxo próprio de snapshot/recovery, portanto não depende deste broadcast.
    if not _reservar_versao_publicacao(partida_id, salvo.versao):
        return payload

    payload_leve = _payload_placar_rapido(payload)

    if SOCKET_DELTA_ENABLED and publicacao.alterado and anterior is not None:
        delta = criar_delta_estado(
            partida_id,
            anterior.estado,
            payload,
            versao_base=anterior.versao,
            versao=salvo.versao,
        )
        _delta_economico = delta_compensa(
            delta,
            economia_minima_percentual=SOCKET_DELTA_MIN_SAVING_PERCENT,
        )
        delta_metrics_store.registrar_delta_servidor(
            emitido=not delta.vazio,
            bytes_delta=delta.bytes_delta,
            bytes_estado=delta.bytes_estado,
            economia_percentual=delta.economia_percentual,
        )
        if not delta.vazio:
            historico_delta_store.registrar(partida_id, delta.payload())
            # Clientes modernos ficam em uma sala exclusiva. O delta é emitido
            # mesmo quando a economia é pequena para garantir que recebam todas
            # as transições sem depender dos eventos legados.
            _emitir_sala_capacidade(
                "estado_partida_delta",
                delta.payload(),
                sala_delta(partida_id),
                prioridade=PRIORIDADE_CRITICA,
            )

    # Evento pequeno e estável para os receptores atuais.
    _emitir_salas("placar_rapido", payload_leve, partida_id, prioridade=PRIORIDADE_CRITICA, deduplicar_ms=20)
    delta_metrics_store.registrar_publicacao("placar")

    # Compatibilidade temporária. Se a proteção de homologação estiver ativa,
    # os eventos antigos permanecem ligados até os clientes comprovarem saúde.
    eventos_legados_ativos = SOCKET_LEGACY_STATE_EVENTS or (
        SOCKET_LEGACY_REQUIRE_DELTA_HEALTHY and not delta_metrics_store.esta_homologado()
    )
    if eventos_legados_ativos:
        delta_metrics_store.registrar_publicacao("legacy")
        sala_compatibilidade = sala_legacy(partida_id)
        _emitir_sala_capacidade("estado_partida", payload_leve, sala_compatibilidade, prioridade=PRIORIDADE_NORMAL, deduplicar_ms=20)
        _emitir_sala_capacidade("estado_jogo_atualizado", payload_leve, sala_compatibilidade, prioridade=PRIORIDADE_NORMAL, deduplicar_ms=20)
        _emitir_sala_capacidade("estado_arbitros", payload_leve, sala_compatibilidade, prioridade=PRIORIDADE_NORMAL, deduplicar_ms=20)
        _emitir_sala_capacidade("estado_partida_tempo_real", payload_leve, sala_compatibilidade, prioridade=PRIORIDADE_NORMAL, deduplicar_ms=20)

    # Estado completo só periodicamente ou em momentos importantes.
    if _deve_emitir_estado_completo(partida_id, payload):
        _emitir_salas("estado_partida_completo", payload, partida_id, prioridade=PRIORIDADE_NORMAL)
        delta_metrics_store.registrar_publicacao("snapshot")

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
            prioridade=PRIORIDADE_NORMAL,
            deduplicar_ms=80,
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
            prioridade=PRIORIDADE_CRITICA,
            deduplicar_ms=40,
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

        event_dispatcher.publicar(
            socketio,
            "placar_apontador_atualizado",
            _payload_placar_rapido(payload),
            sala=_room_placar_apontador(apontador),
            prioridade=PRIORIDADE_CRITICA,
            deduplicar_ms=20,
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
    presence_store.registrar(getattr(request, "sid", ""), {"perfil": "conectado"})
    return True


@socketio.on("disconnect")
def on_disconnect():
    presence_store.remover(getattr(request, "sid", ""))
    return True


@socketio.on("cliente_heartbeat")
def cliente_heartbeat(data):
    dados = data if isinstance(data, dict) else {}
    item = presence_store.registrar(getattr(request, "sid", ""), dados)
    partida_id = normalizar_id_partida(item.get("partida_id"))
    versao_servidor = estado_partida_vivo.versao(partida_id) if partida_id else 0
    resposta = {
        "ok": True,
        "heartbeat_id": str(dados.get("heartbeat_id") or ""),
        "servidor_em_ms": int(time.time() * 1000),
        "partida_id": partida_id,
        "perfil": item.get("perfil") or "",
        "estado_versao": versao_servidor,
        "latencia_recebimento_ms": item.get("latencia_ms") or 0,
    }
    socketio.emit("cliente_heartbeat_ok", resposta, room=request.sid)
    return resposta




@socketio.on("recuperar_eventos_partida")
def recuperar_eventos_partida(data):
    dados = data if isinstance(data, dict) else {}
    partida_id = normalizar_id_partida(dados.get("partida_id"))
    if not partida_id:
        emitir_para_cliente(socketio, request.sid, ["recuperacao_partida"], {
            "ok": False,
            "modo": "invalido",
            "motivo": "partida_id_obrigatorio",
        })
        return

    try:
        versao_cliente = int(dados.get("ultima_versao") or dados.get("estado_versao") or 0)
    except (TypeError, ValueError):
        versao_cliente = 0
    try:
        limite = max(1, min(int(dados.get("limite") or os.getenv("REALTIME_RECOVERY_BATCH_LIMIT", "100")), 500))
    except (TypeError, ValueError):
        limite = 100

    resultado = recuperar_estado(
        partida_id,
        versao_cliente,
        state_store=estado_partidas_store,
        history_store=historico_delta_store,
        limite=limite,
    )
    emitir_para_cliente(socketio, request.sid, ["recuperacao_partida"], resultado.payload())

@socketio.on("entrar_partida")
def entrar_partida(data):
    entrada = normalizar_entrada(data)
    presence_store.registrar(getattr(request, "sid", ""), {**(data if isinstance(data, dict) else {}), "partida_id": entrada.partida_id, "perfil": entrada.perfil})
    if not entrada.partida_id:
        return

    inscrever_em_salas(entrada, join_room)
    sala = _room(entrada.partida_id)
    emitir_para_cliente(
        socketio,
        request.sid,
        ["entrou_partida"],
        montar_confirmacao(entrada, room=sala),
    )

    estado = obter_estado_inicial(estado_partidas_store, sala)
    if estado:
        payload = _normalizar_payload(entrada.partida_id, estado)
        emitir_para_cliente(
            socketio,
            request.sid,
            ["estado_partida", "estado_jogo_atualizado"],
            payload,
        )


@socketio.on("entrar_partida_tempo_real")
def entrar_partida_tempo_real(data):
    entrada = normalizar_entrada(data)
    presence_store.registrar(getattr(request, "sid", ""), {**(data if isinstance(data, dict) else {}), "partida_id": entrada.partida_id, "perfil": entrada.perfil})
    if not entrada.partida_id:
        return

    inscrever_em_salas(entrada, join_room)
    sala = _room(entrada.partida_id)
    emitir_para_cliente(
        socketio,
        request.sid,
        ["entrou_partida_tempo_real"],
        montar_confirmacao(entrada, room=sala),
    )

    estado = obter_estado_inicial(estado_partidas_store, sala)
    if estado:
        payload = _normalizar_payload(entrada.partida_id, estado)
    else:
        payload = _normalizar_payload(
            entrada.partida_id,
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

    emitir_para_cliente(
        socketio,
        request.sid,
        ["estado_partida_tempo_real", "estado_partida", "estado_jogo_atualizado"],
        payload,
    )


@socketio.on("join_partida")
def join_partida(data):
    return entrar_partida(data)


@socketio.on("join")
def join_generico(data):
    entrada = normalizar_entrada(data)
    if not entrada.valida:
        return

    inscrever_em_salas(entrada, join_room)
    emitir_para_cliente(
        socketio,
        request.sid,
        ["entrou_partida"],
        montar_confirmacao(entrada),
    )


@socketio.on("entrar_arbitro")
def entrar_arbitro(data):
    entrada = normalizar_entrada(data)
    presence_store.registrar(getattr(request, "sid", ""), {**(data if isinstance(data, dict) else {}), "partida_id": entrada.partida_id, "perfil": entrada.perfil})
    if not entrada.partida_id:
        return

    inscrever_em_salas(entrada, join_room)
    sala = _room_arbitros(entrada.partida_id)
    sala_estado = _room(entrada.partida_id)
    emitir_para_cliente(
        socketio,
        request.sid,
        ["entrou_partida"],
        montar_confirmacao(entrada, room=sala, arbitro=True),
    )

    estado = obter_estado_inicial(
        estado_partidas_store,
        sala_estado,
        chaves_alternativas=[entrada.partida_id],
    )
    if estado:
        payload = _normalizar_payload(entrada.partida_id, estado)
        emitir_para_cliente(
            socketio,
            request.sid,
            [
                "estado_arbitros",
                "estado_partida",
                "estado_jogo_atualizado",
                "estado_partida_tempo_real",
            ],
            payload,
        )


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

    resultado_estado = aceitar_e_salvar_estado(
        store=estado_partidas_store,
        partida_id=partida_id,
        novo=payload,
        dados_originais=data,
    )
    if not resultado_estado.aceito:
        socketio.emit("estado_partida_local_ok", {
            "ok": False,
            "snapshot_atrasado": resultado_estado.snapshot_atrasado,
            "conflito_versao": resultado_estado.conflito_versao,
            "ignorado_por_estado_antigo": True,
            "partida_id": partida_id,
            "estado_versao": resultado_estado.versao_atual,
            "estado_atual": resultado_estado.estado,
            "mensagem": "Estado local antigo; sincronize com a versão oficial antes de reenviar.",
        }, room=request.sid)
        return
    payload = resultado_estado.estado

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

    socketio.emit("estado_avulso_local_ok", {"ok": True, "partida_id": partida_id, "estado_versao": payload.get("estado_versao", 0)}, room=request.sid)


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

    # A aceitação é atômica: a versão e o progresso são verificados sob a
    # mesma trava usada para gravar, impedindo corrida entre duas abas.
    sala_cache = _room(partida_id)
    resultado_estado = aceitar_e_salvar_estado(
        store=estado_partidas_store,
        partida_id=sala_cache,
        novo=payload,
        dados_originais=data,
    )
    if not resultado_estado.aceito:
        socketio.emit("estado_partida_local_ok", {
            "ok": False,
            "snapshot_atrasado": resultado_estado.snapshot_atrasado,
            "conflito_versao": resultado_estado.conflito_versao,
            "partida_id": partida_id,
            "estado_versao": resultado_estado.versao_atual,
            "estado_atual": resultado_estado.estado,
            "mensagem": "Estado local mais antigo que o estado vivo; atualização ignorada.",
        }, room=request.sid)
        return
    payload = resultado_estado.estado

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

    socketio.emit("estado_partida_local_ok", {"ok": True, "partida_id": partida_id, "estado_versao": payload.get("estado_versao", 0)}, room=request.sid)


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