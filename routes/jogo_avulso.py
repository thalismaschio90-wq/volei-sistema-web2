from flask import Blueprint, render_template, request, redirect, session, url_for, flash, jsonify, make_response
from routes.utils import exigir_perfil
from socket_events import emitir_estado_partida, obter_estado_cache, atualizar_estado_cache

try:
    from banco import apontador_pode_criar_jogo_avulso
except Exception:
    def apontador_pode_criar_jogo_avulso(cpf):
        return False

import time
import uuid

jogo_avulso_bp = Blueprint("jogo_avulso", __name__)

_JOGOS_AVULSOS = {}


def _json_no_cache(payload, status=200):
    resposta = jsonify(payload)
    resposta.status_code = status
    resposta.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"
    return resposta


def _cpf_apontador():
    return session.get("usuario") or session.get("usuario_login") or session.get("login") or ""


def _tem_permissao_jogo_avulso():
    cpf = _cpf_apontador()
    if not cpf:
        return False
    try:
        return bool(apontador_pode_criar_jogo_avulso(cpf))
    except Exception as e:
        print("ERRO permissao jogo avulso:", e, flush=True)
        return False


def _normalizar_int(valor, padrao=0, minimo=None, maximo=None):
    try:
        n = int(valor)
    except Exception:
        n = padrao
    if minimo is not None:
        n = max(minimo, n)
    if maximo is not None:
        n = min(maximo, n)
    return n


def _novo_codigo():
    return f"AV{uuid.uuid4().hex[:8].upper()}"


def _partida_id(codigo):
    return f"avulso:{str(codigo or '').strip().upper()}"


def _buscar_jogo(codigo):
    codigo = str(codigo or "").strip().upper()
    jogo = _JOGOS_AVULSOS.get(codigo)
    if jogo:
        return jogo
    estado = obter_estado_cache(_partida_id(codigo)) or {}
    if estado:
        jogo = {
            "codigo": codigo,
            "estado": estado,
            "criado_em": time.time(),
            "apontador": estado.get("apontador") or "",
        }
        _JOGOS_AVULSOS[codigo] = jogo
        return jogo
    return None


def _salvar_estado(codigo, estado):
    codigo = str(codigo or "").strip().upper()
    partida_id = _partida_id(codigo)
    estado = dict(estado or {})
    estado["codigo"] = codigo
    estado["partida_id"] = partida_id
    estado["competicao"] = "JOGO AVULSO"
    estado["modo_avulso"] = True
    estado["atualizado_em"] = time.time()
    _JOGOS_AVULSOS.setdefault(codigo, {
        "codigo": codigo,
        "criado_em": time.time(),
        "apontador": estado.get("apontador") or _cpf_apontador(),
    })["estado"] = estado
    atualizar_estado_cache(partida_id, estado)
    emitir_estado_partida(partida_id, estado)
    return estado


@jogo_avulso_bp.route("/apontador/jogo-avulso/novo", methods=["GET", "POST"])
@exigir_perfil("apontador")
def novo_jogo_avulso():
    if not _tem_permissao_jogo_avulso():
        flash("Jogo rápido não liberado para este apontador. Fale com o administrador do sistema.", "erro")
        return redirect(url_for("apontadores.painel_apontador"))

    if request.method == "GET":
        return render_template("jogo_avulso_novo.html")

    codigo = _novo_codigo()
    sets_tipo = (request.form.get("sets_tipo") or "set_unico").strip()
    if sets_tipo not in {"set_unico", "melhor_de_3", "melhor_de_5"}:
        sets_tipo = "set_unico"

    sets_max = 1 if sets_tipo == "set_unico" else (3 if sets_tipo == "melhor_de_3" else 5)
    sets_para_vencer = 1 if sets_tipo == "set_unico" else (2 if sets_tipo == "melhor_de_3" else 3)

    equipe_a = (request.form.get("equipe_a") or "Equipe A").strip()[:60] or "Equipe A"
    equipe_b = (request.form.get("equipe_b") or "Equipe B").strip()[:60] or "Equipe B"

    estado = {
        "ok": True,
        "codigo": codigo,
        "partida_id": _partida_id(codigo),
        "competicao": "JOGO AVULSO",
        "modo_avulso": True,
        "apontador": _cpf_apontador(),
        "equipe_a": equipe_a,
        "equipe_b": equipe_b,
        "sets_tipo": sets_tipo,
        "sets_max": sets_max,
        "sets_para_vencer": sets_para_vencer,
        "pontos_set": _normalizar_int(request.form.get("pontos_set"), 21, 1, 99),
        "ponto_alvo_set": _normalizar_int(request.form.get("pontos_set"), 21, 1, 99),
        "pontos_tiebreak": _normalizar_int(request.form.get("pontos_tiebreak"), 15, 1, 99),
        "diferenca_minima": _normalizar_int(request.form.get("diferenca_minima"), 2, 1, 20),
        "limite_tempos": _normalizar_int(request.form.get("tempos_por_set"), 2, 0, 9),
        "limite_substituicoes": _normalizar_int(request.form.get("substituicoes_por_set"), 6, 0, 30),
        "pontos_a": 0,
        "pontos_b": 0,
        "placar_a": 0,
        "placar_b": 0,
        "sets_a": 0,
        "sets_b": 0,
        "set_atual": 1,
        "saque_atual": (request.form.get("saque_inicial") or "A").strip().upper() if (request.form.get("saque_inicial") or "A").strip().upper() in {"A", "B"} else "A",
        "fase_partida": "papeleta",
        "status_jogo": "papeleta",
        "rotacao_a": [],
        "rotacao_b": [],
        "banco_a": [],
        "banco_b": [],
        "numeros_a": [],
        "numeros_b": [],
        "tempos_a": 0,
        "tempos_b": 0,
        "subs_a": 0,
        "subs_b": 0,
        "historico": [],
        "eventos": [],
        "evolucao_pontos": [],
        "scout": {},
        "ultima_acao": "Jogo rápido criado",
    }

    _salvar_estado(codigo, estado)
    return redirect(url_for("jogo_avulso.papeleta_jogo_avulso", codigo=codigo))


@jogo_avulso_bp.route("/apontador/jogo-avulso/<codigo>/papeleta", methods=["GET"])
@exigir_perfil("apontador")
def papeleta_jogo_avulso(codigo):
    if not _tem_permissao_jogo_avulso():
        flash("Jogo rápido não liberado para este apontador.", "erro")
        return redirect(url_for("apontadores.painel_apontador"))

    jogo = _buscar_jogo(codigo)
    if not jogo:
        flash("Jogo avulso não encontrado ou expirado.", "erro")
        return redirect(url_for("apontadores.painel_apontador"))

    return render_template("jogo_avulso_papeleta.html", codigo=str(codigo).upper(), estado=jogo.get("estado") or {})


@jogo_avulso_bp.route("/apontador/jogo-avulso/<codigo>/iniciar", methods=["POST"])
@exigir_perfil("apontador")
def iniciar_jogo_avulso(codigo):
    if not _tem_permissao_jogo_avulso():
        flash("Jogo rápido não liberado para este apontador.", "erro")
        return redirect(url_for("apontadores.painel_apontador"))

    jogo = _buscar_jogo(codigo)
    if not jogo:
        flash("Jogo avulso não encontrado ou expirado.", "erro")
        return redirect(url_for("apontadores.painel_apontador"))

    estado = dict(jogo.get("estado") or {})

    def numeros_lado(lado):
        pos = []
        for i in [1, 2, 3, 4, 5, 6]:
            n = (request.form.get(f"{lado}_{i}") or "").strip()
            if n and n.isdigit():
                pos.append(n)
        banco_txt = (request.form.get(f"banco_{lado}") or "").replace(";", ",").replace(" ", ",")
        banco = []
        for pedaco in banco_txt.split(","):
            pedaco = pedaco.strip()
            if pedaco and pedaco.isdigit() and pedaco not in pos and pedaco not in banco:
                banco.append(pedaco)
        return pos, banco

    titulares_a, banco_a = numeros_lado("A")
    titulares_b, banco_b = numeros_lado("B")

    if len(titulares_a) != 6 or len(titulares_b) != 6:
        flash("Preencha as 6 posições das duas equipes com números.", "erro")
        return redirect(url_for("jogo_avulso.papeleta_jogo_avulso", codigo=codigo))

    # A tela usa a ordem visual [4,3,2,5,6,1], mas o formulário salva por posição real.
    # Para operar a rotação, guardamos na ordem de quadra: IV, III, II, V, VI, I.
    rotacao_a = [request.form.get(f"A_{i}", "").strip() for i in [4, 3, 2, 5, 6, 1]]
    rotacao_b = [request.form.get(f"B_{i}", "").strip() for i in [4, 3, 2, 5, 6, 1]]

    estado.update({
        "fase_partida": "jogo",
        "status_jogo": "em_andamento",
        "rotacao_a": rotacao_a,
        "rotacao_b": rotacao_b,
        "papeleta_a": {str(i): request.form.get(f"A_{i}", "").strip() for i in [1, 2, 3, 4, 5, 6]},
        "papeleta_b": {str(i): request.form.get(f"B_{i}", "").strip() for i in [1, 2, 3, 4, 5, 6]},
        "banco_a": banco_a,
        "banco_b": banco_b,
        "numeros_a": sorted(list(dict.fromkeys(titulares_a + banco_a)), key=lambda x: int(x)),
        "numeros_b": sorted(list(dict.fromkeys(titulares_b + banco_b)), key=lambda x: int(x)),
        "historico": [{"descricao": "Jogo iniciado", "tipo": "inicio", "set": 1, "ts": time.time()}],
        "ultima_acao": "Jogo iniciado",
    })

    _salvar_estado(codigo, estado)
    return redirect(url_for("jogo_avulso.operacao_jogo_avulso", codigo=codigo))


@jogo_avulso_bp.route("/apontador/jogo-avulso/<codigo>/operacao")
@exigir_perfil("apontador")
def operacao_jogo_avulso(codigo):
    if not _tem_permissao_jogo_avulso():
        flash("Jogo rápido não liberado para este apontador.", "erro")
        return redirect(url_for("apontadores.painel_apontador"))

    jogo = _buscar_jogo(codigo)
    if not jogo:
        flash("Jogo avulso não encontrado ou expirado.", "erro")
        return redirect(url_for("apontadores.painel_apontador"))

    resposta = make_response(render_template("jogo_avulso_operacao.html", codigo=str(codigo).upper(), estado=jogo.get("estado") or {}))
    resposta.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resposta


@jogo_avulso_bp.route("/telao-avulso/<codigo>")
def telao_jogo_avulso(codigo):
    jogo = _buscar_jogo(codigo)
    return render_template("jogo_avulso_telao.html", codigo=str(codigo).upper(), estado=(jogo or {}).get("estado") or {})


@jogo_avulso_bp.route("/arbitro1-avulso/<codigo>")
def arbitro1_jogo_avulso(codigo):
    jogo = _buscar_jogo(codigo)
    return render_template("jogo_avulso_arbitro.html", codigo=str(codigo).upper(), estado=(jogo or {}).get("estado") or {}, arbitro="1º Árbitro")


@jogo_avulso_bp.route("/arbitro2-avulso/<codigo>")
def arbitro2_jogo_avulso(codigo):
    jogo = _buscar_jogo(codigo)
    return render_template("jogo_avulso_arbitro.html", codigo=str(codigo).upper(), estado=(jogo or {}).get("estado") or {}, arbitro="2º Árbitro")


@jogo_avulso_bp.route("/apontador/jogo-avulso/<codigo>/estado", methods=["GET", "POST"])
def estado_jogo_avulso(codigo):
    codigo = str(codigo or "").strip().upper()
    if request.method == "GET":
        jogo = _buscar_jogo(codigo)
        estado = (jogo or {}).get("estado") or obter_estado_cache(_partida_id(codigo)) or {}
        return _json_no_cache({"ok": True, "estado": estado})

    dados = request.get_json(silent=True) or {}
    estado = dados.get("estado") or dados
    estado = _salvar_estado(codigo, estado)
    return _json_no_cache({"ok": True, "estado": estado})
