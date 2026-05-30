from flask import Blueprint, redirect, url_for, flash

from routes.utils import exigir_perfil

arbitros_bp = Blueprint("arbitros", __name__)


@arbitros_bp.route("/arbitros")
@exigir_perfil("organizador")
def listar_arbitros():
    flash("Árbitros e PINs operacionais agora ficam em Arbitragem e oficiais.", "sucesso")
    return redirect(url_for("oficiais.oficiais", aba="pins"))


@arbitros_bp.route("/arbitros/novo", methods=["GET", "POST"])
@exigir_perfil("organizador")
def novo_arbitro():
    flash("Não é mais necessário criar login de árbitro. Use os PINs operacionais.", "erro")
    return redirect(url_for("oficiais.oficiais", aba="pins"))


@arbitros_bp.route("/arbitros/<nome>/redefinir", methods=["POST"])
@exigir_perfil("organizador")
def redefinir_senha_arbitro(nome):
    flash("Os novos acessos de árbitro são feitos por PIN, sem login e senha.", "erro")
    return redirect(url_for("oficiais.oficiais", aba="pins"))


@arbitros_bp.route("/arbitros/<nome>/excluir", methods=["POST"])
@exigir_perfil("organizador")
def excluir_arbitro(nome):
    flash("Os novos acessos de árbitro são feitos por PIN, sem cadastro de usuário.", "erro")
    return redirect(url_for("oficiais.oficiais", aba="pins"))
