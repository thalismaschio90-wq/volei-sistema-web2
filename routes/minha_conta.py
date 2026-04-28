from flask import Blueprint, render_template, session
from routes.utils import exigir_login

minha_conta_bp = Blueprint("minha_conta", __name__)


@minha_conta_bp.route("/minha-conta")
@exigir_login()
def minha_conta():
    return render_template(
        "minha_conta.html",
        usuario=session.get("usuario"),
        nome=session.get("nome"),
        perfil=session.get("perfil")
    )