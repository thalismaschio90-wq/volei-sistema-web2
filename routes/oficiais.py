from flask import Blueprint, jsonify, render_template, request, redirect, session, url_for, flash
from banco import (
    criar_tabelas_oficiais,
    buscar_oficial_por_cpf,
    cadastrar_oficial,
    vincular_oficial_competicao,
    listar_oficiais_competicao,
    criar_apontador,
    buscar_competicao_por_organizador,
    remover_apontador_da_competicao,
    buscar_partida_operacional,
    buscar_estado_jogo_partida,
    listar_papeleta,
    listar_atletas_aprovados_da_equipe,
)
from routes.utils import exigir_perfil
from socket_events import obter_estado_cache

oficiais_bp = Blueprint("oficiais", __name__)


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
    estado = dict(obter_estado_cache(partida_id) or {})

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
        "saque_atual": str(estado.get("saque_atual") or "").strip().upper(),
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

    return render_template(
        "oficiais.html",
        oficiais=oficiais_competicao,
        competicao=competicao
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


@oficiais_bp.route("/oficiais/primeiro-arbitro/<competicao>/<int:partida_id>")
def primeiro_arbitro_view(competicao, partida_id):
    estado = _montar_estado_arbitro(competicao, partida_id)
    return render_template(
        "primeiro_arbitro.html",
        competicao=competicao,
        partida_id=partida_id,
        estado=estado,
        tipo_arbitro="primeiro",
    )


@oficiais_bp.route("/oficiais/segundo-arbitro/<competicao>/<int:partida_id>")
def segundo_arbitro_view(competicao, partida_id):
    estado = _montar_estado_arbitro(competicao, partida_id)
    return render_template(
        "segundo_arbitro.html",
        competicao=competicao,
        partida_id=partida_id,
        estado=estado,
        tipo_arbitro="segundo",
    )


@oficiais_bp.route("/oficiais/arbitro/estado/<competicao>/<int:partida_id>")
def estado_arbitro_view(competicao, partida_id):
    try:
        return jsonify(_montar_estado_arbitro(competicao, partida_id))
    except Exception as e:
        return jsonify({"ok": False, "mensagem": f"Erro ao carregar estado do árbitro: {e}"}), 500
