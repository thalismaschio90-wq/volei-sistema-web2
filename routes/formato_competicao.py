from flask import Blueprint, redirect, url_for, request, flash

from routes.utils import exigir_perfil

formato_competicao_bp = Blueprint("formato_competicao", __name__)


@formato_competicao_bp.route("/formato-competicao", methods=["GET", "POST"])
@exigir_perfil("organizador")
def formato_competicao_view():
    """
    Compatibilidade com links antigos.

    O formato/fases da competição agora fica dentro de Minha competição,
    na aba "Fases da competição".
    """
    if request.method == "POST":
        flash("O formato da competição agora é salvo dentro de Minha competição, na aba Fases da competição.", "erro")

    return redirect(url_for("competicoes.listar_competicoes_view"))
