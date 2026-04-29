from flask import Blueprint, render_template, request, redirect, session, url_for, flash, jsonify, make_response

from banco import (
    criar_tabelas_oficiais,
    conectar,
    listar_competicoes_apontador,
    buscar_oficial_por_cpf,
    listar_partidas,
    buscar_partida_operacional,
    assumir_partida_operacional,
    abandonar_partida_operacional,
    listar_arbitros_competicao,
    salvar_pre_jogo_partida,
    listar_atletas_aprovados_da_equipe,
    atualizar_numero_atleta,
    equipe_ja_conferida,
    marcar_equipe_conferida,
    salvar_papeleta,
    listar_papeleta,
    inicializar_sets_partida,
    registrar_resultado_set,
    salvar_capitao_partida,
    inicializar_jogo_partida,
    buscar_estado_jogo_partida,
    registrar_ponto_partida,
    desfazer_ultima_acao_partida,
    registrar_tempo_partida,
    buscar_tempos_restantes_partida,
    registrar_substituicao_partida,
    registrar_substituicao_excepcional_partida,
    registrar_retardamento_partida,
    registrar_sancao_partida,
    registrar_cartao_verde_partida,
    resumir_fluxo_oficial_partida,
    papeleta_set_esta_completa, verificar_fim_de_set, finalizar_set_e_avancar,
    salvar_sorteio_tiebreak_partida,
    partida_encerrada,
    precisa_tiebreak,
    verificar_fim_partida, encerrar_partida,
    garantir_estado_partida,
    listar_eventos_partida,
)
from routes.utils import exigir_perfil
from socket_events import emitir_estado_partida, emitir_resposta_solicitacao

apontadores_bp = Blueprint("apontadores", __name__)


# =========================================================
# HELPERS
# =========================================================
def _json_no_cache(payload, status=200):
    resposta = jsonify(payload)
    resposta.status_code = status
    resposta.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"
    return resposta


def _montar_descricao_evento(ev):
    descricao = (ev.get("descricao") or "").strip()
    if descricao:
        return descricao

    partes = []
    tipo_evento = str(ev.get("tipo_evento") or ev.get("tipo") or "").strip()
    equipe = str(ev.get("equipe") or "").strip()
    fundamento = str(ev.get("fundamento") or "").strip()
    resultado = str(ev.get("resultado") or "").strip()
    detalhe = str(ev.get("detalhe") or ev.get("detalhes") or "").strip()
    numero = str(ev.get("numero") or "").strip()
    atleta_nome = str(ev.get("atleta_nome") or "").strip()

    if tipo_evento:
        partes.append(tipo_evento.replace("_", " ").title())
    if equipe:
        partes.append(f"Equipe {equipe}")
    if fundamento:
        partes.append(fundamento.replace("_", " "))
    if resultado:
        partes.append(resultado.replace("_", " "))
    if detalhe:
        partes.append(detalhe.replace("_", " "))
    if numero:
        partes.append(f"#{numero}")
    if atleta_nome:
        partes.append(atleta_nome)

    return " • ".join([p for p in partes if p]) or "Ação registrada"


def _buscar_historico_resumido(partida_id, competicao, limite=5):
    try:
        eventos = listar_eventos_partida(partida_id, competicao, limite=limite) or []
    except TypeError:
        eventos = listar_eventos_partida(partida_id, competicao) or []
        eventos = eventos[:limite]
    except Exception:
        return [], "-"

    historico = [{"descricao": _montar_descricao_evento(ev)} for ev in eventos]
    ultima_acao = historico[0]["descricao"] if historico else "-"
    return historico[:limite], ultima_acao


def _buscar_papeletas_set_atual(partida_id, competicao, partida, estado=None):
    equipe_a = partida.get("equipe_a_operacional")
    equipe_b = partida.get("equipe_b_operacional")
    set_atual = int(partida.get("set_atual") or (estado or {}).get("set_atual") or 1)

    papeleta_a = {}
    papeleta_b = {}

    try:
        if equipe_a:
            dados_a = listar_papeleta(partida_id, competicao, equipe_a, set_atual) or []
            papeleta_a = {row["posicao"]: row["numero"] for row in dados_a}

        if equipe_b:
            dados_b = listar_papeleta(partida_id, competicao, equipe_b, set_atual) or []
            papeleta_b = {row["posicao"]: row["numero"] for row in dados_b}
    except Exception:
        papeleta_a = {}
        papeleta_b = {}

    for i in range(1, 7):
        papeleta_a.setdefault(i, "")
        papeleta_b.setdefault(i, "")

    return equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b


def _rotacao_fallback_por_papeleta(papeleta):
    return [
        papeleta.get(4, ""),
        papeleta.get(3, ""),
        papeleta.get(2, ""),
        papeleta.get(5, ""),
        papeleta.get(6, ""),
        papeleta.get(1, ""),
    ]


# =========================================================
# CONSULTAS BÁSICAS
# =========================================================
def listar_apontadores():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    o.nome,
                    o.cpf,
                    a.ativo,
                    a.primeiro_acesso
                FROM apontadores_acesso a
                JOIN oficiais o ON o.cpf = a.cpf
                ORDER BY o.nome
            """)
            return cur.fetchall()


@apontadores_bp.route("/apontadores")
@exigir_perfil("superadmin")
def apontadores():
    criar_tabelas_oficiais()
    lista = listar_apontadores()

    return render_template(
        "apontadores.html",
        apontadores=lista
    )


@apontadores_bp.route("/apontador")
@exigir_perfil("apontador")
def painel_apontador():
    criar_tabelas_oficiais()

    cpf = session.get("usuario")

    if not cpf:
        flash("CPF do apontador não encontrado na sessão.", "erro")
        return render_template("painel_apontador.html")

    oficial = buscar_oficial_por_cpf(cpf)
    if not oficial:
        flash("Não foi possível localizar o apontador pelo CPF informado.", "erro")
        return render_template("painel_apontador.html")

    competicoes = listar_competicoes_apontador(cpf)

    if not competicoes:
        return render_template("painel_apontador.html")

    if len(competicoes) == 1:
        return render_template(
            "painel_apontador.html",
            competicao_unica=competicoes[0]
        )

    return render_template(
        "painel_apontador.html",
        competicoes=competicoes
    )


@apontadores_bp.route("/apontador/entrar/<competicao>")
@exigir_perfil("apontador")
def entrar_competicao_apontador(competicao):
    session["competicao_apontador"] = competicao

    partidas = listar_partidas(competicao)
    partidas = sorted(partidas, key=lambda x: (x.get("ordem") or 0, x.get("id") or 0))

    return render_template(
        "painel_apontador.html",
        modo_partidas=True,
        competicao_nome=competicao,
        partidas=partidas
    )


# =========================================================
# PRÉ-JOGO
# =========================================================
@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def abrir_pre_jogo_apontador(competicao, partida_id):
    cpf = session.get("usuario")
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    bloqueada_por_outro = (
        partida.get("operador_login")
        and partida.get("operador_login") != cpf
        and (partida.get("status_operacao") or "livre").lower() in {"reservado", "pre_jogo", "em_andamento"}
    )

    arbitros = listar_arbitros_competicao(competicao)

    equipe_a_conferida = False
    equipe_b_conferida = False

    equipe_a_operacional = partida.get("equipe_a_operacional")
    equipe_b_operacional = partida.get("equipe_b_operacional")

    if equipe_a_operacional:
        equipe_a_conferida = equipe_ja_conferida(competicao, equipe_a_operacional)

    if equipe_b_operacional:
        equipe_b_conferida = equipe_ja_conferida(competicao, equipe_b_operacional)

    precisa_conferencia = False
    if equipe_a_operacional and equipe_b_operacional:
        precisa_conferencia = (not equipe_a_conferida) or (not equipe_b_conferida)

    fluxo = resumir_fluxo_oficial_partida(partida_id, competicao, partida=partida)

    return render_template(
        "pre_jogo_apontador.html",
        competicao_nome=competicao,
        partida=partida,
        fluxo=fluxo,
        arbitros=arbitros,
        bloqueada_por_outro=bloqueada_por_outro,
        equipe_a_conferida=equipe_a_conferida,
        equipe_b_conferida=equipe_b_conferida,
        precisa_conferencia=precisa_conferencia,
        capitao_a_nome=partida.get("capitao_a_nome"),
        capitao_a_numero=partida.get("capitao_a_numero"),
        capitao_b_nome=partida.get("capitao_b_nome"),
        capitao_b_numero=partida.get("capitao_b_numero"),
        pre_jogo_bloqueado=(fluxo.get("fase_partida") != "pre_jogo"),
        tie_break_pendente=bool(fluxo.get("tiebreak_pendente")),
    )


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/assumir", methods=["POST"])
@exigir_perfil("apontador")
def assumir_partida_view(competicao, partida_id):
    cpf = session.get("usuario")
    oficial = buscar_oficial_por_cpf(cpf)

    if not oficial:
        flash("Apontador não localizado.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    ok, msg = assumir_partida_operacional(
        partida_id,
        competicao,
        cpf,
        oficial["nome"]
    )

    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/abandonar", methods=["POST"])
@exigir_perfil("apontador")
def abandonar_partida_view(competicao, partida_id):
    cpf = session.get("usuario")
    ok, msg = abandonar_partida_operacional(partida_id, competicao, cpf)
    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_pre_jogo_view(competicao, partida_id):
    cpf = session.get("usuario")

    arbitro_1_cpf = request.form.get("arbitro_1_cpf", "").strip()
    arbitro_2_cpf = request.form.get("arbitro_2_cpf", "").strip()
    vencedor_sorteio = request.form.get("sorteio_vencedor", "").strip()
    escolha_sorteio = request.form.get("sorteio_escolha", "").strip()
    lado_esquerdo = request.form.get("lado_esquerdo", "").strip()
    saque_inicial = request.form.get("saque_inicial", "").strip()

    ok, msg = salvar_pre_jogo_partida(
        partida_id=partida_id,
        competicao=competicao,
        operador_login=cpf,
        arbitro_1_cpf=arbitro_1_cpf,
        arbitro_2_cpf=arbitro_2_cpf,
        sorteio_vencedor=vencedor_sorteio,
        sorteio_escolha=escolha_sorteio,
        saque_inicial=saque_inicial,
        lado_esquerdo=lado_esquerdo,
    )

    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))


@apontadores_bp.route("/apontador/tiebreak/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def abrir_tiebreak_view(competicao, partida_id):
    cpf = session.get("usuario")
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode fazer o sorteio do tie-break.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    fluxo = resumir_fluxo_oficial_partida(partida_id, competicao, partida=partida) or {}
    if fluxo.get("fase_partida") != "tiebreak_sorteio":
        flash("O sorteio do tie-break não está liberado neste momento.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    return render_template(
        "tiebreak_sorteio_apontador.html",
        competicao_nome=competicao,
        partida=partida,
        fluxo=fluxo,
    )


@apontadores_bp.route("/apontador/tiebreak/<competicao>/<int:partida_id>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_tiebreak_view(competicao, partida_id):
    cpf = session.get("usuario")

    vencedor_sorteio = request.form.get("sorteio_vencedor", "").strip()
    escolha_sorteio = request.form.get("sorteio_escolha", "").strip()
    saque_tiebreak = request.form.get("saque_tiebreak", "").strip()
    lado_esquerdo_tiebreak = request.form.get("lado_esquerdo_tiebreak", "").strip()

    ok, msg = salvar_sorteio_tiebreak_partida(
        partida_id=partida_id,
        competicao=competicao,
        operador_login=cpf,
        sorteio_vencedor=vencedor_sorteio,
        sorteio_escolha=escolha_sorteio,
        saque_tiebreak=saque_tiebreak,
        lado_esquerdo_tiebreak=lado_esquerdo_tiebreak,
    )

    flash(msg, "sucesso" if ok else "erro")
    if ok:
        return redirect(url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id))
    return redirect(url_for("apontadores.abrir_tiebreak_view", competicao=competicao, partida_id=partida_id))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/conferencia/<lado>")
@exigir_perfil("apontador")
def conferencia_equipe_view(competicao, partida_id, lado):
    cpf = session.get("usuario")
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode fazer a conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    if lado not in {"A", "B"}:
        flash("Lado inválido para conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    equipe = partida.get("equipe_a_operacional") if lado == "A" else partida.get("equipe_b_operacional")
    if not equipe:
        flash("Salve primeiro o sorteio para definir as equipes operacionais.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    atletas = listar_atletas_aprovados_da_equipe(equipe, competicao)

    return render_template(
        "conferencia_equipe.html",
        competicao_nome=competicao,
        partida=partida,
        lado=lado,
        equipe_nome=equipe,
        atletas=atletas,
    )


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/conferencia/<lado>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_conferencia_equipe_view(competicao, partida_id, lado):
    cpf = session.get("usuario")
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode salvar a conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    if lado not in {"A", "B"}:
        flash("Lado inválido para conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    equipe = partida.get("equipe_a_operacional") if lado == "A" else partida.get("equipe_b_operacional")
    if not equipe:
        flash("Equipe não definida para conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    ids = request.form.getlist("atleta_id")
    houve_erro = False

    for atleta_id in ids:
        numero = request.form.get(f"numero_{atleta_id}", "").strip()
        ok, msg = atualizar_numero_atleta(atleta_id, numero)
        if not ok:
            houve_erro = True
            flash(msg, "erro")

    if not houve_erro:
        marcar_equipe_conferida(competicao, equipe)
        flash("Conferência salva com sucesso.", "sucesso")

    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/capitao/<lado>")
@exigir_perfil("apontador")
def definir_capitao_view(competicao, partida_id, lado):
    cpf = session.get("usuario")
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode definir o capitão.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    if lado not in {"A", "B"}:
        flash("Lado inválido para capitão.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    equipe = partida.get("equipe_a_operacional") if lado == "A" else partida.get("equipe_b_operacional")
    if not equipe:
        flash("Equipe operacional ainda não definida.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    atletas = listar_atletas_aprovados_da_equipe(equipe, competicao)
    atletas = [a for a in atletas if a.get("numero") not in (None, "")]
    atleta_atual_id = partida.get("capitao_a_id") if lado == "A" else partida.get("capitao_b_id")

    return render_template(
        "definir_capitao.html",
        competicao_nome=competicao,
        partida=partida,
        lado=lado,
        equipe_nome=equipe,
        atletas=atletas,
        atleta_atual_id=atleta_atual_id,
    )


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/capitao/<lado>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_capitao_view(competicao, partida_id, lado):
    cpf = session.get("usuario")
    atleta_id = request.form.get("atleta_id", "").strip()

    ok, msg = salvar_capitao_partida(partida_id, competicao, cpf, lado, atleta_id)
    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))


# =========================================================
# PAPELETA
# =========================================================
@apontadores_bp.route("/apontador/papeleta/<competicao>/<int:partida_id>", methods=["GET"])
@exigir_perfil("apontador")
def papeleta_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    inicializar_sets_partida(partida_id, competicao)
    partida = buscar_partida_operacional(partida_id, competicao)
    fluxo = resumir_fluxo_oficial_partida(partida_id, competicao, partida=partida) or {}

    if fluxo.get("fase_partida") == "encerrado":
        flash("A partida já está finalizada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if fluxo.get("fase_partida") == "pre_jogo":
        flash("Finalize primeiro o pré-jogo para acessar a papeleta.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    if fluxo.get("fase_partida") == "tiebreak_sorteio":
        flash("Antes do tie-break, faça o sorteio específico do set decisivo.", "erro")
        return redirect(url_for("apontadores.abrir_tiebreak_view", competicao=competicao, partida_id=partida_id))

    if fluxo.get("fase_partida") == "jogo":
        return redirect(url_for("apontadores.jogo_view", competicao=competicao, partida_id=partida_id))

    equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b = _buscar_papeletas_set_atual(
        partida_id, competicao, partida
    )

    atletas_a = listar_atletas_aprovados_da_equipe(equipe_a, competicao) if equipe_a else []
    atletas_b = listar_atletas_aprovados_da_equipe(equipe_b, competicao) if equipe_b else []

    atletas_a = [a for a in atletas_a if a.get("numero")]
    atletas_b = [a for a in atletas_b if a.get("numero")]

    return render_template(
        "papeleta_apontador.html",
        competicao_nome=competicao,
        partida=partida,
        equipe_a=equipe_a,
        equipe_b=equipe_b,
        atletas_a=atletas_a,
        atletas_b=atletas_b,
        papeleta_a=papeleta_a,
        papeleta_b=papeleta_b,
        fluxo=fluxo,
    )


@apontadores_bp.route("/apontador/papeleta/<competicao>/<int:partida_id>", methods=["POST"])
@exigir_perfil("apontador")
def salvar_papeleta_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    equipe_a = partida.get("equipe_a_operacional")
    equipe_b = partida.get("equipe_b_operacional")
    set_atual = int(partida.get("set_atual") or 1)

    atletas_cache = {}

    def montar_dados(lado, equipe):
        if not equipe:
            return {}

        atletas = atletas_cache.get(equipe)
        if atletas is None:
            atletas = listar_atletas_aprovados_da_equipe(equipe, competicao)
            atletas_cache[equipe] = atletas

        mapa_atletas_por_numero = {
            int(a.get("numero") or 0): a
            for a in atletas
            if a.get("numero") not in (None, "")
        }

        dados = {}
        for pos in [1, 2, 3, 4, 5, 6]:
            valor = request.form.get(f"{lado}_{pos}", "").strip()
            if not valor:
                continue

            try:
                numero = int(valor)
            except ValueError:
                continue

            atleta = mapa_atletas_por_numero.get(numero)
            if atleta:
                dados[pos] = atleta

        return dados

    dados_a = montar_dados("A", equipe_a)
    dados_b = montar_dados("B", equipe_b)

    if len(dados_a) != 6 or len(dados_b) != 6:
        flash("Preencha as 6 posições das duas equipes.", "erro")
        return redirect(url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id))

    salvar_papeleta(partida_id, competicao, equipe_a, set_atual, dados_a)
    salvar_papeleta(partida_id, competicao, equipe_b, set_atual, dados_b)

    partida_atual = buscar_partida_operacional(partida_id, competicao)
    fluxo = resumir_fluxo_oficial_partida(partida_id, competicao, partida=partida_atual) or {}
    flash("Papeleta salva com sucesso.", "sucesso")

    if fluxo.get("papeleta_a_completa") and fluxo.get("papeleta_b_completa"):
        return redirect(url_for("apontadores.jogo_view", competicao=competicao, partida_id=partida_id))

    return redirect(url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id))


# =========================================================
# JOGO
# =========================================================
@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>", methods=["GET"])
@exigir_perfil("apontador")
def jogo_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    status_jogo = (partida.get("status_jogo") or "").strip().lower()

    # Só inicializa se ainda não estiver em jogo.
    if status_jogo not in {"em_andamento", "entre_sets", "finalizada"}:
        partida = inicializar_jogo_partida(partida_id, competicao) or partida

    if (partida.get("status_jogo") or "").strip().lower() == "finalizada":
        flash("A partida já está finalizada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    estado = buscar_estado_jogo_partida(partida_id, competicao) or {}

    equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b = _buscar_papeletas_set_atual(
        partida_id,
        competicao,
        partida,
        estado
    )

    try:
        atletas_a = listar_atletas_aprovados_da_equipe(equipe_a, competicao) if equipe_a else []
        atletas_b = listar_atletas_aprovados_da_equipe(equipe_b, competicao) if equipe_b else []
    except Exception:
        atletas_a = []
        atletas_b = []

    atletas_a = [a for a in atletas_a if a.get("numero")]
    atletas_b = [a for a in atletas_b if a.get("numero")]

    if not estado.get("rotacao_a"):
        estado["rotacao_a"] = _rotacao_fallback_por_papeleta(papeleta_a)

    if not estado.get("rotacao_b"):
        estado["rotacao_b"] = _rotacao_fallback_por_papeleta(papeleta_b)

    # Histórico inicial apenas para abrir a tela já preenchida.
    try:
        historico_inicial, ultima_acao = _buscar_historico_resumido(partida_id, competicao, limite=5)
    except Exception:
        historico_inicial, ultima_acao = [], estado.get("ultima_acao") or "-"

    estado["historico"] = historico_inicial
    estado["ultima_acao"] = ultima_acao or estado.get("ultima_acao") or "-"

    resposta = make_response(render_template(
        "jogo_apontador.html",
        competicao_nome=competicao,
        partida=partida,
        estado=estado,
        papeleta_a=papeleta_a,
        papeleta_b=papeleta_b,
        atletas_a=atletas_a,
        atletas_b=atletas_b,
        modo_operacao=(partida.get("modo_operacao") or "simples"),
    ))

    resposta.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"

    return resposta


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/ponto", methods=["POST"])
@exigir_perfil("apontador")
def ponto_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}

        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        fundamento = (request.form.get("fundamento") or corpo.get("fundamento") or "").strip().lower()
        resultado = (request.form.get("resultado") or corpo.get("resultado") or "").strip().lower()
        tipo_lance = (request.form.get("tipo_lance") or corpo.get("tipo_lance") or "").strip().lower()
        detalhe_lance = (request.form.get("detalhe_lance") or corpo.get("detalhe_lance") or "").strip().lower()
        tipo_erro = (request.form.get("tipo_erro") or corpo.get("tipo_erro") or "").strip().lower()
        atleta_numero = str(request.form.get("atleta_numero") or corpo.get("atleta_numero") or "").strip()
        atleta_nome = (request.form.get("atleta_nome") or corpo.get("atleta_nome") or "").strip()
        atleta_label = (request.form.get("atleta_label") or corpo.get("atleta_label") or "").strip()

        if not tipo_lance:
            return _json_no_cache({"ok": False, "mensagem": "Selecione se foi ponto, erro ou falta."}, 400)

        if tipo_lance not in {"ponto", "erro", "falta"}:
            return _json_no_cache({"ok": False, "mensagem": "Tipo de lance inválido."}, 400)

        if not detalhe_lance and not resultado and not fundamento:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o detalhe da jogada."}, 400)

        detalhe_final = (detalhe_lance or tipo_erro or resultado or fundamento).strip().lower()

        detalhes_validos = {
            "ponto": {"ataque", "bloqueio", "ace"},
            "erro": {"erro_saque", "erro_geral"},
            "falta": {"rede", "invasao", "rotacao", "conducao", "dois_toques"},
        }

        if detalhe_final not in detalhes_validos[tipo_lance]:
            return _json_no_cache({"ok": False, "mensagem": "Detalhe da jogada inválido."}, 400)

        exige_atleta = detalhe_final in {"ataque", "bloqueio", "ace"}

        if exige_atleta and not atleta_numero:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o atleta da jogada."}, 400)

        if not exige_atleta:
            atleta_numero = ""
            atleta_nome = ""
            atleta_label = ""

        fundamento_final = detalhe_final
        resultado_final = tipo_lance

        detalhes_evento = {
            "fundamento": fundamento_final,
            "resultado": resultado_final,
            "tipo_lance": tipo_lance,
            "detalhe_lance": detalhe_final,
            "tipo_erro": tipo_erro,
            "atleta_numero": atleta_numero,
            "atleta_nome": atleta_nome,
            "atleta_label": atleta_label,
        }

        ok, retorno = registrar_ponto_partida(
            partida_id=partida_id,
            competicao=competicao,
            equipe=equipe,
            tipo="ponto",
            detalhes=detalhes_evento
        )

        if not ok:
            mensagem = retorno if isinstance(retorno, str) else "Não foi possível registrar o ponto."
            return _json_no_cache({"ok": False, "mensagem": mensagem}, 400)

        estado = retorno if isinstance(retorno, dict) else {}

        if "ultima_acao" not in estado:
            estado["ultima_acao"] = "Ponto registrado"

        try:
            emitir_estado_partida(partida_id, estado)
        except Exception as e:
            print("ERRO emitir_estado_partida PONTO:", e)

        return _json_no_cache({
            "ok": True,
            "mensagem": "Ponto registrado com sucesso.",
            **estado
        })

    except Exception as e:
        print("ERRO ponto_view:", e)
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar ponto: {e}"}, 500)
    

@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/desfazer", methods=["POST"])
@exigir_perfil("apontador")
def desfazer_acao_view(competicao, partida_id):
    try:
        ok, retorno = desfazer_ultima_acao_partida(partida_id, competicao)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)
        return _json_no_cache({"ok": True, **retorno})
    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao desfazer ação: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/tempo", methods=["POST"])
@exigir_perfil("apontador")
def registrar_tempo_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        ok, retorno = registrar_tempo_partida(partida_id, competicao, equipe)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = retorno if isinstance(retorno, dict) else {}

        try:
            emitir_estado_partida(partida_id, estado)
        except Exception as e:
            print("ERRO emitir_estado_partida TEMPO:", e)

        try:
            emitir_resposta_solicitacao(partida_id, {
                "tipo": "tempo",
                "equipe": equipe,
                "status": "atendida",
                "mensagem": f"Tempo da equipe {equipe} autorizado pelo apontador."
            })
        except Exception as e:
            print("ERRO emitir_resposta_solicitacao TEMPO:", e)

        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar tempo: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/substituicao", methods=["POST"])
@exigir_perfil("apontador")
def registrar_substituicao_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        numero_sai = str(request.form.get("numero_sai") or corpo.get("numero_sai") or "").strip()
        numero_entra = str(request.form.get("numero_entra") or corpo.get("numero_entra") or "").strip()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        if not numero_sai or not numero_entra:
            return _json_no_cache({"ok": False, "mensagem": "Selecione quem sai e quem entra."}, 400)

        ok, retorno = registrar_substituicao_partida(partida_id, competicao, equipe, numero_sai, numero_entra)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = retorno if isinstance(retorno, dict) else {}

        try:
            emitir_estado_partida(partida_id, estado)
        except Exception as e:
            print("ERRO emitir_estado_partida SUBSTITUICAO:", e)

        try:
            emitir_resposta_solicitacao(partida_id, {
                "tipo": "substituicao",
                "equipe": equipe,
                "status": "atendida",
                "mensagem": f"Substituição da equipe {equipe} autorizada pelo apontador."
            })
        except Exception as e:
            print("ERRO emitir_resposta_solicitacao SUBSTITUICAO:", e)

        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar substituição: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/substituicao-excepcional", methods=["POST"])
@exigir_perfil("apontador")
def registrar_substituicao_excepcional_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        numero_sai = str(request.form.get("numero_sai") or corpo.get("numero_sai") or "").strip()
        numero_entra = str(request.form.get("numero_entra") or corpo.get("numero_entra") or "").strip()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        if not numero_sai or not numero_entra:
            return _json_no_cache({"ok": False, "mensagem": "Selecione quem sai e quem entra."}, 400)

        ok, retorno = registrar_substituicao_excepcional_partida(partida_id, competicao, equipe, numero_sai, numero_entra)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        return _json_no_cache({"ok": True, **retorno})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar substituição excepcional: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/retardamento", methods=["POST"])
@exigir_perfil("apontador")
def registrar_retardamento_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        ok, retorno = registrar_retardamento_partida(partida_id, competicao, equipe)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        return _json_no_cache({"ok": True, **retorno})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar retardamento: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/sancao", methods=["POST"])
@exigir_perfil("apontador")
def registrar_sancao_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}

        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        tipo_pessoa = (request.form.get("tipo_pessoa") or corpo.get("tipo_pessoa") or "").strip().lower()
        alvo = (request.form.get("alvo") or corpo.get("alvo") or "").strip()
        sancao = (request.form.get("sancao") or corpo.get("sancao") or "").strip().lower()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        if tipo_pessoa not in {"jogador", "comissao"}:
            return _json_no_cache({"ok": False, "mensagem": "Tipo de pessoa inválido."}, 400)

        if not alvo:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o alvo da sanção."}, 400)

        if not sancao:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o tipo de sanção."}, 400)

        ok, retorno = registrar_sancao_partida(partida_id, competicao, equipe, tipo_pessoa, alvo, sancao)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        return _json_no_cache({"ok": True, **retorno})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar sanção: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/cartao-verde", methods=["POST"])
@exigir_perfil("apontador")
def registrar_cartao_verde_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}

        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        tipo_pessoa = (request.form.get("tipo_pessoa") or corpo.get("tipo_pessoa") or "").strip().lower()
        alvo = (request.form.get("alvo") or corpo.get("alvo") or "").strip()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        if tipo_pessoa not in {"jogador", "comissao"}:
            return _json_no_cache({"ok": False, "mensagem": "Tipo de pessoa inválido."}, 400)

        if not alvo:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o alvo do cartão verde."}, 400)

        ok, retorno = registrar_cartao_verde_partida(partida_id, competicao, equipe, tipo_pessoa, alvo)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        return _json_no_cache({"ok": True, **retorno})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar cartão verde: {e}"}, 500)


@apontadores_bp.route("/apontador/estado/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def estado_jogo_view(competicao, partida_id):
    try:
        partida = buscar_partida_operacional(partida_id, competicao)

        if not partida:
            return _json_no_cache({"ok": False, "mensagem": "Partida não encontrada"}, 404)

        garantir_estado_partida(partida_id, competicao)
        estado = buscar_estado_jogo_partida(partida_id, competicao) or {}

        equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b = _buscar_papeletas_set_atual(
            partida_id, competicao, partida, estado
        )

        historico, ultima_acao = _buscar_historico_resumido(partida_id, competicao, limite=5)

        pontos_a = int(estado.get("pontos_a") or estado.get("placar_a") or 0)
        pontos_b = int(estado.get("pontos_b") or estado.get("placar_b") or 0)

        rotacao_a = list(estado.get("rotacao_a") or [])
        rotacao_b = list(estado.get("rotacao_b") or [])

        if not any(str(x).strip() for x in rotacao_a):
            rotacao_a = _rotacao_fallback_por_papeleta(papeleta_a)

        if not any(str(x).strip() for x in rotacao_b):
            rotacao_b = _rotacao_fallback_por_papeleta(papeleta_b)

        tempos_a = estado.get("tempos_a")
        tempos_b = estado.get("tempos_b")
        if tempos_a is None or tempos_b is None:
            tempos = buscar_tempos_restantes_partida(partida_id, competicao)
            tempos_a = tempos.get("tempos_a")
            tempos_b = tempos.get("tempos_b")

        return _json_no_cache({
            "ok": True,
            "pontos_a": pontos_a,
            "pontos_b": pontos_b,
            "placar_a": pontos_a,
            "placar_b": pontos_b,
            "sets_a": int(estado.get("sets_a") or 0),
            "sets_b": int(estado.get("sets_b") or 0),
            "set_atual": int(estado.get("set_atual") or 1),
            "saque_atual": estado.get("saque_atual") or "",
            "tempos_a": tempos_a,
            "tempos_b": tempos_b,
            "subs_a": int(estado.get("subs_a") or 0),
            "subs_b": int(estado.get("subs_b") or 0),
            "limite_substituicoes": int(estado.get("limite_substituicoes") or 0),
            "rotacao_a": rotacao_a,
            "rotacao_b": rotacao_b,
            "rotacao": {
                "equipe_a": rotacao_a,
                "equipe_b": rotacao_b
            },
            "status_jogadores_a": estado.get("status_jogadores_a") or {},
            "status_jogadores_b": estado.get("status_jogadores_b") or {},
            "sancoes_a": estado.get("sancoes_a") or [],
            "sancoes_b": estado.get("sancoes_b") or [],
            "cartoes_verdes_a": estado.get("cartoes_verdes_a") or [],
            "cartoes_verdes_b": estado.get("cartoes_verdes_b") or [],
            "historico": historico,
            "ultima_acao": ultima_acao,
            "partida_finalizada": str(estado.get("fase_partida") or "").lower() == "encerrado"
        })

    except Exception as e:
        print("ERRO estado_jogo_view:", e)
        return _json_no_cache({
            "ok": False,
            "mensagem": "Erro interno ao carregar estado do jogo."
        }, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/encerrar", methods=["POST"])
@exigir_perfil("apontador")
def encerrar_partida_view(competicao, partida_id):
    try:
        observacoes = ""
        corpo = request.get_json(silent=True) or {}
        if request.is_json:
            observacoes = (corpo.get("observacoes") or "").strip()
        else:
            observacoes = (request.form.get("observacoes") or "").strip()

        estado = buscar_estado_jogo_partida(partida_id, competicao)
        if not estado:
            return _json_no_cache({"ok": False, "mensagem": "Estado não encontrado."}, 404)

        encerrar_partida(partida_id, competicao, observacoes)
        estado = buscar_estado_jogo_partida(partida_id, competicao) or {}

        return _json_no_cache({
            "ok": True,
            "mensagem": "Partida encerrada com sucesso.",
            "encerrado": True,
            "estado": estado,
            "partida_finalizada": True
        })
    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao encerrar partida: {e}"}, 500)


@apontadores_bp.route("/apontador/observacoes/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def observacoes_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    return render_template(
        "observacoes.html",
        partida=partida,
        competicao_nome=competicao
    )


@apontadores_bp.route("/apontador/observacoes/<competicao>/<int:partida_id>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_observacoes_view(competicao, partida_id):
    observacoes = request.form.get("observacoes")
    encerrar_partida(partida_id, competicao, observacoes)
    return redirect("/")

# FIX: garantir fundamento/resultado corretos para falta e erro_saque
