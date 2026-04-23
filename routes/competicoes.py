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
    criar_campos_regras_operacionais_competicoes,
    criar_campos_travamento_competicoes,
    competicao_esta_travada,
    destravar_competicao,
)
from routes.utils import exigir_perfil, perfil_atual

competicoes_bp = Blueprint("competicoes", __name__)

criar_campos_regras_operacionais_competicoes()
criar_campos_travamento_competicoes()


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
        competicao = buscar_competicao_por_organizador(session.get("usuario"))

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

        try:
            tempos_por_set = int(request.form.get("tempos_por_set") or 2)
        except ValueError:
            tempos_por_set = 2

        try:
            substituicoes_por_set = int(request.form.get("substituicoes_por_set") or 6)
        except ValueError:
            substituicoes_por_set = 6

        if tempos_por_set < 0:
            tempos_por_set = 0
        if substituicoes_por_set < 0:
            substituicoes_por_set = 0

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


@competicoes_bp.route("/competicoes/<nome>/excluir", methods=["POST"])
@exigir_perfil("superadmin")
def excluir_competicao_view(nome):
    """
    Regra oficial de exclusão completa da competição:

    - Apaga todos os dados vinculados à competição
    - Mantém somente:
        * superadmin
        * apontadores
    - Apontadores não são apagados
    - Apontadores apenas perdem o vínculo com a competição excluída
    - Organizador, equipes e demais usuários da competição são apagados
    """
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
    comp = buscar_competicao_por_organizador(session.get("usuario"))

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    # 👉 SE FOR GET → só redireciona (não quebra mais)
    if request.method == "GET":
        return redirect(url_for("competicoes.listar_competicoes_view"))

    try:
        pontos_set = int(request.form.get("pontos_set") or 25)
    except ValueError:
        pontos_set = 25

    try:
        pontos_tiebreak = int(request.form.get("pontos_tiebreak") or 15)
    except ValueError:
        pontos_tiebreak = 15

    try:
        diferenca_minima = int(request.form.get("diferenca_minima") or 2)
    except ValueError:
        diferenca_minima = 2

    try:
        tempos_por_set = int(request.form.get("tempos_por_set") or 2)
    except ValueError:
        tempos_por_set = 2

    try:
        substituicoes_por_set = int(request.form.get("substituicoes_por_set") or 6)
    except ValueError:
        substituicoes_por_set = 6

    if tempos_por_set < 0:
        tempos_por_set = 0
    if substituicoes_por_set < 0:
        substituicoes_por_set = 0

    dados = {
        "sets_tipo": request.form.get("sets_tipo", "melhor_de_3"),
        "pontos_set": pontos_set,
        "tem_tiebreak": request.form.get("tem_tiebreak") == "on",
        "pontos_tiebreak": pontos_tiebreak,
        "diferenca_minima": diferenca_minima,
        "tempos_por_set": tempos_por_set,
        "substituicoes_por_set": substituicoes_por_set,
    }

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. As regras do jogo não podem mais ser alteradas.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    atualizar_regras_jogo(comp["nome"], dados)

    flash("Regras do jogo salvas.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/estrutura", methods=["POST"])
@exigir_perfil("organizador")
def salvar_estrutura_view():
    comp = buscar_competicao_por_organizador(session.get("usuario"))

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    try:
        qtd_equipes = int(request.form.get("qtd_equipes") or 0)
    except ValueError:
        qtd_equipes = 0

    try:
        qtd_grupos = int(request.form.get("qtd_grupos") or 0)
    except ValueError:
        qtd_grupos = 0

    try:
        qtd_quadras = int(request.form.get("qtd_quadras") or 1)
    except ValueError:
        qtd_quadras = 1

    try:
        limite_atletas = int(request.form.get("limite_atletas") or 0)
    except ValueError:
        limite_atletas = 0

    dados = {
        "qtd_equipes": qtd_equipes,
        "formato": request.form.get("formato", "").strip(),
        "tem_grupos": request.form.get("tem_grupos") == "on",
        "qtd_grupos": qtd_grupos,
        "qtd_quadras": qtd_quadras,
        "modo_operacao": request.form.get("modo_operacao", "simples").strip(),
        "data_limite_inscricao": request.form.get("data_limite_inscricao", "").strip(),
        "hora_limite_inscricao": request.form.get("hora_limite_inscricao", "").strip(),
        "limite_atletas": limite_atletas,
        "permitir_edicao_pos_prazo": request.form.get("permitir_edicao_pos_prazo") == "on",
        "bloquear_apos_inicio": not bool(request.form.get("data_limite_inscricao", "").strip()),
    }

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. A estrutura não pode mais ser alterada.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    atualizar_estrutura_competicao(comp["nome"], dados)

    flash("Estrutura da competição salva.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/pontuacao", methods=["POST"])
@exigir_perfil("organizador")
def salvar_pontuacao_desempate_view():
    comp = buscar_competicao_por_organizador(session.get("usuario"))

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    sets_tipo = comp.get("sets_tipo", "melhor_de_3")
    dados = {}

    if sets_tipo == "set_unico":
        for campo, padrao in [
            ("vitoria_set_unico", 2),
            ("derrota_set_unico", 0),
        ]:
            try:
                dados[campo] = int(request.form.get(campo) or padrao)
            except ValueError:
                dados[campo] = padrao

    elif sets_tipo == "melhor_de_3":
        for campo, padrao in [
            ("vitoria_2x0", 3),
            ("vitoria_2x1", 2),
            ("derrota_1x2", 1),
            ("derrota_0x2", 0),
        ]:
            try:
                dados[campo] = int(request.form.get(campo) or padrao)
            except ValueError:
                dados[campo] = padrao

    elif sets_tipo == "melhor_de_5":
        for campo, padrao in [
            ("vitoria_3x0", 3),
            ("vitoria_3x1", 3),
            ("vitoria_3x2", 2),
            ("derrota_2x3", 1),
            ("derrota_1x3", 0),
            ("derrota_0x3", 0),
        ]:
            try:
                dados[campo] = int(request.form.get(campo) or padrao)
            except ValueError:
                dados[campo] = padrao

    criterios_ordenados = request.form.get("criterios_ordenados", "").strip()

    if not criterios_ordenados:
        criterios_ordenados = (
            "vitorias,pontos,saldo_sets,sets_pro,sets_contra,"
            "saldo_pontos,pontos_pro,pontos_contra,confronto_direto,"
            "coef_sets,coef_pontos,fair_play,sorteio"
        )

    dados["criterios_desempate"] = criterios_ordenados

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. A pontuação e os critérios de desempate não podem mais ser alterados.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    atualizar_pontuacao_desempate(comp["nome"], dados)

    flash("Pontuação e critérios de desempate salvos.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/destravar", methods=["POST"])
@exigir_perfil("organizador")
def destravar_competicao_view():
    comp = buscar_competicao_por_organizador(session.get("usuario"))

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