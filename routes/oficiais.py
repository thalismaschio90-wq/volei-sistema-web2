from flask import Blueprint, render_template, request, redirect, session, url_for, flash
from banco import (
    criar_tabelas_oficiais,
    buscar_oficial_por_cpf,
    cadastrar_oficial,
    vincular_oficial_competicao,
    listar_oficiais_competicao,
    criar_apontador,
    buscar_competicao_por_organizador,
    remover_apontador_da_competicao
)
from routes.utils import exigir_perfil

oficiais_bp = Blueprint("oficiais", __name__)


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
