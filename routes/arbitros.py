from flask import Blueprint, render_template, request, redirect, session, url_for, flash
from banco import (
    buscar_competicao_por_organizador,
    listar_mesarios_da_competicao,
    mesario_existe_na_competicao,
    criar_mesario_com_credenciais,
    redefinir_senha_do_mesario,
    excluir_mesario,
)
from routes.utils import exigir_perfil

arbitros_bp = Blueprint("arbitros", __name__)


@arbitros_bp.route("/arbitros")
@exigir_perfil("organizador")
def listar_arbitros():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    arbitros = listar_mesarios_da_competicao(competicao["nome"])

    credenciais = session.pop("credenciais_novo_mesario", None)
    senha_redefinida = session.pop("senha_redefinida_mesario", None)

    return render_template(
        "arbitros.html",
        competicao=competicao,
        arbitros=arbitros,
        credenciais=credenciais,
        senha_redefinida=senha_redefinida
    )


@arbitros_bp.route("/arbitros/novo", methods=["GET", "POST"])
@exigir_perfil("organizador")
def novo_arbitro():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if request.method == "POST":
        nome = request.form.get("nome")

        if mesario_existe_na_competicao(nome, competicao["nome"]):
            flash("Já existe um árbitro com esse nome.", "erro")
            return redirect(url_for("arbitros.novo_arbitro"))

        credenciais = criar_mesario_com_credenciais(nome, competicao["nome"])

        session["credenciais_novo_mesario"] = {
            "nome": nome,
            "login": credenciais["login"],
            "senha": credenciais["senha"]
        }

        flash("Árbitro criado com sucesso!", "sucesso")
        return redirect(url_for("arbitros.listar_arbitros"))

    return render_template("novo_arbitro.html", competicao=competicao)


@arbitros_bp.route("/arbitros/<nome>/redefinir", methods=["POST"])
@exigir_perfil("organizador")
def redefinir_senha_arbitro(nome):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    resultado = redefinir_senha_do_mesario(nome, competicao["nome"])

    session["senha_redefinida_mesario"] = {
        "nome": nome,
        "login": resultado["login"],
        "senha": resultado["senha"]
    }

    flash("Senha redefinida!", "sucesso")
    return redirect(url_for("arbitros.listar_arbitros"))


@arbitros_bp.route("/arbitros/<nome>/excluir", methods=["POST"])
@exigir_perfil("organizador")
def excluir_arbitro(nome):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    excluir_mesario(nome, competicao["nome"])

    flash("Árbitro removido!", "sucesso")
    return redirect(url_for("arbitros.listar_arbitros"))