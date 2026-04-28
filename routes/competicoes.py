from flask import Blueprint, render_template, session, redirect, url_for, request, flash

from banco import (
    listar_competicoes,
    buscar_competicao_por_organizador,
    competicao_existe,
    criar_competicao_com_organizador,
    excluir_competicao,
    atualizar_dados_competicao,
    atualizar_estrutura_competicao,
    atualizar_regras_jogo,
    atualizar_pontuacao_desempate,
    redefinir_senha_organizador,
    competicao_esta_travada,
    destravar_competicao,
)

from routes.utils import exigir_perfil, perfil_atual

competicoes_bp = Blueprint("competicoes", __name__)


def _competicao_do_organizador_logado():
    usuario = session.get("usuario")
    if not usuario:
        return None
    return buscar_competicao_por_organizador(usuario)


def _to_int(valor, padrao=0, minimo=None):
    try:
        numero = int(valor)
    except (TypeError, ValueError):
        numero = padrao

    if minimo is not None and numero < minimo:
        numero = minimo

    return numero


@competicoes_bp.route("/competicoes")
@exigir_perfil("superadmin", "organizador")
def listar_competicoes_view():
    perfil = perfil_atual()

    if perfil == "superadmin":
        competicoes = listar_competicoes()
        credenciais = session.pop("credenciais_novas", None)
        senha_redefinida = session.pop("senha_redefinida_organizador", None)

        return render_template(
            "competicoes.html",
            competicoes=competicoes,
            credenciais=credenciais,
            senha_redefinida=senha_redefinida,
        )

    if perfil == "organizador":
        competicao = _competicao_do_organizador_logado()

        if not competicao:
            flash("Nenhuma competição vinculada a este organizador.", "erro")
            return redirect(url_for("painel.inicio"))

        return render_template(
            "editar_competicao.html",
            competicao=competicao,
            competicao_travada=competicao_esta_travada(competicao["nome"]),
        )

    return redirect(url_for("painel.inicio"))


@competicoes_bp.route("/competicoes/nova", methods=["GET", "POST"])
@exigir_perfil("superadmin")
def nova_competicao():
    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        data = request.form.get("data", "").strip()
        status = request.form.get("status", "").strip() or "Em preparação"
        modo_operacao = request.form.get("modo_operacao", "simples").strip() or "simples"

        tempos_por_set = _to_int(request.form.get("tempos_por_set"), padrao=2, minimo=0)
        substituicoes_por_set = _to_int(request.form.get("substituicoes_por_set"), padrao=6, minimo=0)

        if not nome:
            flash("Informe o nome da competição.", "erro")
            return render_template("nova_competicao.html")

        if competicao_existe(nome):
            flash("Já existe uma competição com esse nome.", "erro")
            return render_template("nova_competicao.html")

        credenciais = criar_competicao_com_organizador(
            nome,
            data,
            status,
            modo_operacao=modo_operacao,
            tempos_por_set=tempos_por_set,
            substituicoes_por_set=substituicoes_por_set,
        )

        session["credenciais_novas"] = {
            "competicao": nome,
            "login": credenciais["login"],
            "senha": credenciais["senha"],
        }

        flash("Competição criada com sucesso.", "sucesso")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    return render_template("nova_competicao.html")


@competicoes_bp.route("/competicoes/salvar", methods=["POST"])
@exigir_perfil("organizador")
def salvar_competicao_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. Os dados não podem mais ser alterados.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    nome_atual = comp["nome"]

    dados = {
        "nome": request.form.get("nome", "").strip(),
        "cidade": request.form.get("cidade", "").strip(),
        "data": request.form.get("data", "").strip(),
        "ginasio": request.form.get("ginasio", "").strip(),
        "categoria": request.form.get("categoria", "").strip(),
        "sexo": request.form.get("sexo", "").strip(),
        "divisao": request.form.get("divisao", "").strip(),
        "status": request.form.get("status", "").strip() or comp.get("status", "Em preparação"),
    }

    if not dados["nome"]:
        flash("Informe o nome da competição.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    if dados["nome"] != nome_atual and competicao_existe(dados["nome"]):
        flash("Já existe uma competição com esse nome.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    atualizar_dados_competicao(nome_atual, dados)

    session["competicao"] = dados["nome"]

    flash("Dados da competição salvos com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/<nome>/excluir", methods=["POST"])
@exigir_perfil("superadmin")
def excluir_competicao_view(nome):
    confirmacao = (request.form.get("confirmacao_exclusao") or "").strip().upper()

    if confirmacao and confirmacao != "EXCLUIR":
        flash("Confirmação inválida. Digite EXCLUIR para confirmar a exclusão.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    try:
        sucesso = excluir_competicao(nome)

        if sucesso:
            flash(
                "Competição excluída com sucesso. SUPERADMIN foi preservado e os apontadores foram mantidos sem vínculo com a competição removida.",
                "sucesso",
            )
        else:
            flash("Não foi possível excluir a competição.", "erro")

    except Exception as e:
        flash(f"Erro ao excluir competição: {str(e)}", "erro")

    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/<nome>/resetar-senha", methods=["POST"])
@exigir_perfil("superadmin")
def resetar_senha_organizador_view(nome):
    competicoes = listar_competicoes()
    comp = next((c for c in competicoes if c["nome"] == nome), None)

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    resultado = redefinir_senha_organizador(comp["organizador_login"])

    session["senha_redefinida_organizador"] = {
        "competicao": nome,
        "login": resultado["login"],
        "senha": resultado["senha"],
    }

    flash("Senha do organizador redefinida com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/regras", methods=["GET", "POST"])
@exigir_perfil("organizador")
def salvar_regras_jogo_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if request.method == "GET":
        return redirect(url_for("competicoes.listar_competicoes_view"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. As regras do jogo não podem mais ser alteradas.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    dados = {
        "sets_tipo": request.form.get("sets_tipo", "melhor_de_3"),
        "pontos_set": _to_int(request.form.get("pontos_set"), padrao=25, minimo=1),
        "tem_tiebreak": request.form.get("tem_tiebreak") == "on",
        "pontos_tiebreak": _to_int(request.form.get("pontos_tiebreak"), padrao=15, minimo=1),
        "diferenca_minima": _to_int(request.form.get("diferenca_minima"), padrao=2, minimo=1),
        "tempos_por_set": _to_int(request.form.get("tempos_por_set"), padrao=2, minimo=0),
        "substituicoes_por_set": _to_int(request.form.get("substituicoes_por_set"), padrao=6, minimo=0),
    }

    atualizar_regras_jogo(comp["nome"], dados)

    flash("Regras do jogo salvas.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/estrutura", methods=["POST"])
@exigir_perfil("organizador")
def salvar_estrutura_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. A estrutura não pode mais ser alterada.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    data_limite_inscricao = request.form.get("data_limite_inscricao", "").strip()
    hora_limite_inscricao = request.form.get("hora_limite_inscricao", "").strip()

    dados = {
        "qtd_equipes": _to_int(request.form.get("qtd_equipes"), padrao=0, minimo=0),
        "formato": request.form.get("formato", "").strip(),
        "tem_grupos": request.form.get("tem_grupos") == "on",
        "qtd_grupos": _to_int(request.form.get("qtd_grupos"), padrao=0, minimo=0),
        "qtd_quadras": _to_int(request.form.get("qtd_quadras"), padrao=1, minimo=1),
        "modo_operacao": request.form.get("modo_operacao", "simples").strip() or "simples",
        "data_limite_inscricao": data_limite_inscricao,
        "hora_limite_inscricao": hora_limite_inscricao,
        "limite_atletas": _to_int(request.form.get("limite_atletas"), padrao=0, minimo=0),
        "permitir_edicao_pos_prazo": request.form.get("permitir_edicao_pos_prazo") == "on",
        "bloquear_apos_inicio": not bool(data_limite_inscricao),
    }

    atualizar_estrutura_competicao(comp["nome"], dados)

    flash("Estrutura da competição salva.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/pontuacao", methods=["POST"])
@exigir_perfil("organizador")
def salvar_pontuacao_desempate_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. A pontuação e os critérios de desempate não podem mais ser alterados.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    sets_tipo = comp.get("sets_tipo", "melhor_de_3")
    dados = {}

    if sets_tipo == "set_unico":
        dados["vitoria_set_unico"] = _to_int(request.form.get("vitoria_set_unico"), padrao=2)
        dados["derrota_set_unico"] = _to_int(request.form.get("derrota_set_unico"), padrao=0)

    elif sets_tipo == "melhor_de_3":
        dados["vitoria_2x0"] = _to_int(request.form.get("vitoria_2x0"), padrao=3)
        dados["vitoria_2x1"] = _to_int(request.form.get("vitoria_2x1"), padrao=2)
        dados["derrota_1x2"] = _to_int(request.form.get("derrota_1x2"), padrao=1)
        dados["derrota_0x2"] = _to_int(request.form.get("derrota_0x2"), padrao=0)

    elif sets_tipo == "melhor_de_5":
        dados["vitoria_3x0"] = _to_int(request.form.get("vitoria_3x0"), padrao=3)
        dados["vitoria_3x1"] = _to_int(request.form.get("vitoria_3x1"), padrao=3)
        dados["vitoria_3x2"] = _to_int(request.form.get("vitoria_3x2"), padrao=2)
        dados["derrota_2x3"] = _to_int(request.form.get("derrota_2x3"), padrao=1)
        dados["derrota_1x3"] = _to_int(request.form.get("derrota_1x3"), padrao=0)
        dados["derrota_0x3"] = _to_int(request.form.get("derrota_0x3"), padrao=0)

    criterios_ordenados = request.form.get("criterios_ordenados", "").strip()
    if not criterios_ordenados:
        criterios_ordenados = (
            "vitorias,pontos,saldo_sets,sets_pro,sets_contra,"
            "saldo_pontos,pontos_pro,pontos_contra,confronto_direto,"
            "coef_sets,coef_pontos,fair_play,sorteio"
        )

    dados["criterios_desempate"] = criterios_ordenados

    atualizar_pontuacao_desempate(comp["nome"], dados)

    flash("Pontuação e critérios de desempate salvos.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/destravar", methods=["POST"])
@exigir_perfil("organizador")
def destravar_competicao_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    confirmacao = (request.form.get("confirmacao_destravar") or "").strip().upper()
    if confirmacao != "DESTRAVAR":
        flash("Confirmação inválida. Digite DESTRAVAR para liberar a competição.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    destravar_competicao(comp["nome"])
    flash("Competição destravada com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))