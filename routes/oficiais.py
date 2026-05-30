from flask import Blueprint, jsonify, render_template, request, redirect, session, url_for, flash, make_response
from banco import (
    criar_tabelas_oficiais,
    buscar_oficial_por_cpf,
    cadastrar_oficial,
    vincular_oficial_competicao,
    listar_oficiais_competicao,
    listar_pins_operacionais_competicao,
    regenerar_pin_operacional_apontador,
    transferir_pin_operacional,
    criar_apontador,
    buscar_competicao_por_organizador,
    remover_apontador_da_competicao,
    buscar_partida_operacional,
    buscar_estado_jogo_partida,
    listar_papeleta,
    listar_atletas_aprovados_da_equipe,
)
from routes.utils import exigir_perfil, login_obrigatorio
from socket_events import obter_estado_cache

oficiais_bp = Blueprint("oficiais", __name__)



def _resposta_sem_cache(html):
    resp = make_response(html)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


def _arbitro_tem_pin_competicao(competicao):
    if not session.get("arbitro_pin_validado"):
        return False

    tipo = (session.get("arbitro_pin_tipo") or "").strip().lower()
    if tipo not in {"competicao", "operacional"}:
        return False

    return (session.get("arbitro_competicao") or "").strip() == (competicao or "").strip()


def _int_seguro(valor, padrao=0):
    try:
        if valor is None or valor == "":
            return padrao
        return int(valor)
    except Exception:
        return padrao


def _rotacao_fallback_por_papeleta(papeleta):
    return [
        papeleta.get(4, ""),
        papeleta.get(3, ""),
        papeleta.get(2, ""),
        papeleta.get(5, ""),
        papeleta.get(6, ""),
        papeleta.get(1, ""),
    ]


def _atletas_mapa(equipe, competicao):
    mapa = {}
    if not equipe:
        return mapa

    try:
        atletas = listar_atletas_aprovados_da_equipe(equipe, competicao) or []
    except Exception:
        atletas = []

    for atleta in atletas:
        numero = str(
            atleta.get("numero")
            or atleta.get("numero_camisa")
            or atleta.get("camisa")
            or ""
        ).strip()
        nome = str(atleta.get("nome") or atleta.get("atleta") or "").strip()
        if numero:
            mapa[numero] = {"numero": numero, "nome": nome}
    return mapa


def _normalizar_rotacao(rotacao, mapa_atletas):
    saida = []
    if not isinstance(rotacao, list):
        rotacao = []

    for item in rotacao[:6]:
        if isinstance(item, dict):
            numero = str(item.get("numero") or item.get("camisa") or "").strip()
            nome = str(item.get("nome") or "").strip()
        else:
            numero = str(item or "").strip()
            nome = ""

        if numero and not nome:
            nome = (mapa_atletas.get(numero) or {}).get("nome", "")

        saida.append({"numero": numero, "nome": nome})

    while len(saida) < 6:
        saida.append({"numero": "", "nome": ""})

    return saida


def _montar_estado_arbitro(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao) or {}
    cache_estado = obter_estado_cache(partida_id)
    estado = dict(cache_estado) if isinstance(cache_estado, dict) else {}

    if not estado:
        try:
            estado = buscar_estado_jogo_partida(partida_id, competicao) or {}
        except Exception:
            estado = {}

    equipe_a = (
        estado.get("equipe_a")
        or partida.get("equipe_a_operacional")
        or partida.get("equipe_a")
        or "Equipe A"
    )
    equipe_b = (
        estado.get("equipe_b")
        or partida.get("equipe_b_operacional")
        or partida.get("equipe_b")
        or "Equipe B"
    )

    set_atual = _int_seguro(estado.get("set_atual") or partida.get("set_atual"), 1)

    papeleta_a = {}
    papeleta_b = {}
    try:
        dados_a = listar_papeleta(partida_id, competicao, equipe_a, set_atual) or []
        papeleta_a = {row["posicao"]: row["numero"] for row in dados_a}
    except Exception:
        papeleta_a = {}
    try:
        dados_b = listar_papeleta(partida_id, competicao, equipe_b, set_atual) or []
        papeleta_b = {row["posicao"]: row["numero"] for row in dados_b}
    except Exception:
        papeleta_b = {}

    for posicao in range(1, 7):
        papeleta_a.setdefault(posicao, "")
        papeleta_b.setdefault(posicao, "")

    rotacao_a = estado.get("rotacao_a") or []
    rotacao_b = estado.get("rotacao_b") or []

    if not any(str(x.get("numero") if isinstance(x, dict) else x).strip() for x in rotacao_a):
        rotacao_a = _rotacao_fallback_por_papeleta(papeleta_a)
    if not any(str(x.get("numero") if isinstance(x, dict) else x).strip() for x in rotacao_b):
        rotacao_b = _rotacao_fallback_por_papeleta(papeleta_b)

    mapa_a = _atletas_mapa(equipe_a, competicao)
    mapa_b = _atletas_mapa(equipe_b, competicao)

    saque_atual = str(estado.get("saque_atual") or "").strip().upper()

    if saque_atual == "A":
        rotacao_saque = _normalizar_rotacao(rotacao_a, mapa_a)
        equipe_sacadora = equipe_a
    elif saque_atual == "B":
        rotacao_saque = _normalizar_rotacao(rotacao_b, mapa_b)
        equipe_sacadora = equipe_b
    else:
        rotacao_saque = []
        equipe_sacadora = ""

    sacador = {"numero": "-", "nome": ""}

    if rotacao_saque and len(rotacao_saque) >= 6:
        sacador = rotacao_saque[5] or sacador

    numero_sacador = sacador.get("numero") or "-"
    nome_sacador = sacador.get("nome") or ""

    return {
        "ok": True,
        "competicao": competicao,
        "partida_id": partida_id,
        "equipe_a": equipe_a,
        "equipe_b": equipe_b,
        "pontos_a": _int_seguro(estado.get("pontos_a") or estado.get("placar_a"), 0),
        "pontos_b": _int_seguro(estado.get("pontos_b") or estado.get("placar_b"), 0),
        "sets_a": _int_seguro(estado.get("sets_a"), 0),
        "sets_b": _int_seguro(estado.get("sets_b"), 0),
        "set_atual": set_atual,
        "saque_atual": saque_atual,
        "equipe_sacadora": equipe_sacadora,
        "numero_sacador": numero_sacador,
        "nome_sacador": nome_sacador,
        "rotacao_a": _normalizar_rotacao(rotacao_a, mapa_a),
        "rotacao_b": _normalizar_rotacao(rotacao_b, mapa_b),
        "historico": estado.get("historico") or [],
        "ultima_acao": estado.get("ultima_acao") or "-",
        "partida_finalizada": bool(estado.get("partida_finalizada")) or str(partida.get("status") or "").lower() in {"finalizada", "finalizado", "encerrada", "encerrado"},
    }


@oficiais_bp.route("/oficiais", methods=["GET", "POST"])
@exigir_perfil("organizador")
def oficiais():
    criar_tabelas_oficiais()

    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição vinculada ao organizador.", "erro")
        return redirect(url_for("painel.inicio"))

    nome_competicao = competicao["nome"]

    if request.method == "POST":
        cpf = request.form.get("cpf", "").strip()
        nome = request.form.get("nome", "").strip()
        funcao = request.form.get("funcao", "").strip()

        if not cpf:
            flash("Informe o CPF.", "erro")
            return redirect(url_for("oficiais.oficiais"))

        if not funcao:
            flash("Selecione a função.", "erro")
            return redirect(url_for("oficiais.oficiais"))

        oficial = buscar_oficial_por_cpf(cpf)

        if not oficial:
            if not nome:
                flash("Esse CPF ainda não está cadastrado. Informe o nome.", "erro")
                return redirect(url_for("oficiais.oficiais"))

            cadastrar_oficial(nome, cpf)

        if funcao == "apontador":
            criar_apontador(cpf)

        vincular_oficial_competicao(nome_competicao, cpf, funcao)

        flash("Oficial vinculado com sucesso.", "sucesso")
        return redirect(url_for("oficiais.oficiais"))

    oficiais_competicao = listar_oficiais_competicao(nome_competicao)
    pins_operacionais = listar_pins_operacionais_competicao(nome_competicao)

    return render_template(
        "oficiais.html",
        oficiais=oficiais_competicao,
        pins_operacionais=pins_operacionais,
        competicao=competicao,
        aba_ativa=request.args.get("aba", "oficiais")
    )


@oficiais_bp.route("/oficiais/remover-apontador/<cpf>", methods=["POST"])
@exigir_perfil("organizador")
def remover_apontador_competicao_view(cpf):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição vinculada ao organizador.", "erro")
        return redirect(url_for("painel.inicio"))

    remover_apontador_da_competicao(cpf, competicao["nome"])

    flash("Apontador removido apenas desta competição.", "sucesso")
    return redirect(url_for("oficiais.oficiais"))




@oficiais_bp.route("/oficiais/pin/<cpf>/regenerar", methods=["POST"])
@exigir_perfil("organizador")
def regenerar_pin_operacional_view(cpf):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição vinculada ao organizador.", "erro")
        return redirect(url_for("painel.inicio"))

    regenerar_pin_operacional_apontador(competicao["nome"], cpf)
    flash("PIN operacional regenerado com sucesso.", "sucesso")
    return redirect(url_for("oficiais.oficiais", aba="pins"))


@oficiais_bp.route("/oficiais/pin/transferir", methods=["POST"])
@exigir_perfil("organizador")
def transferir_pin_operacional_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição vinculada ao organizador.", "erro")
        return redirect(url_for("painel.inicio"))

    pin = request.form.get("pin", "").strip()
    novo_cpf = request.form.get("novo_cpf", "").strip()

    if not pin or not novo_cpf:
        flash("Informe o PIN e o CPF do novo apontador.", "erro")
        return redirect(url_for("oficiais.oficiais", aba="pins"))

    if transferir_pin_operacional(competicao["nome"], pin, novo_cpf):
        flash("PIN transferido para outro apontador com sucesso.", "sucesso")
    else:
        flash("Não foi possível transferir este PIN.", "erro")

    return redirect(url_for("oficiais.oficiais", aba="pins"))


@oficiais_bp.route("/oficiais/primeiro-arbitro/<competicao>/<int:partida_id>")
def primeiro_arbitro_view(competicao, partida_id):
    if not _arbitro_tem_pin_competicao(competicao):
        flash("Digite o PIN da quadra no Painel dos Árbitros antes de abrir esta tela.", "erro")
        return redirect(url_for("painel.painel_arbitros"))
    estado = _montar_estado_arbitro(competicao, partida_id)
    return _resposta_sem_cache(render_template(
        "primeiro_arbitro.html",
        competicao=competicao,
        partida_id=partida_id,
        estado=estado,
        tipo_arbitro="primeiro",
    ))


@oficiais_bp.route("/oficiais/segundo-arbitro/<competicao>/<int:partida_id>")
def segundo_arbitro_view(competicao, partida_id):
    if not _arbitro_tem_pin_competicao(competicao):
        flash("Digite o PIN da quadra no Painel dos Árbitros antes de abrir esta tela.", "erro")
        return redirect(url_for("painel.painel_arbitros"))
    estado = _montar_estado_arbitro(competicao, partida_id)
    return _resposta_sem_cache(render_template(
        "segundo_arbitro.html",
        competicao=competicao,
        partida_id=partida_id,
        estado=estado,
        tipo_arbitro="segundo",
    ))


@oficiais_bp.route("/oficiais/arbitro/estado/<competicao>/<int:partida_id>")
def estado_arbitro_view(competicao, partida_id):
    try:
        return jsonify(_montar_estado_arbitro(competicao, partida_id))
    except Exception as e:
        return jsonify({"ok": False, "mensagem": f"Erro ao carregar estado do árbitro: {e}"}), 500
