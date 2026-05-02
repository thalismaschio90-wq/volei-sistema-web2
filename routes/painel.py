from flask import Blueprint, render_template, session, redirect, url_for
from banco import (
    contar_competicoes,
    contar_equipes,
    contar_partidas,
    listar_competicoes_do_organizador,
    listar_competicoes_apontador
)
from routes.utils import login_obrigatorio

painel_bp = Blueprint("painel", __name__)


@painel_bp.route("/inicio")
@login_obrigatorio
def inicio():
    perfil = session.get("perfil")

    # =========================
    # SUPER ADMIN
    # =========================
    if perfil == "superadmin":
        return render_template(
            "painel_superadmin.html",
            total_competicoes=contar_competicoes(),
            total_equipes=contar_equipes(),
            total_partidas=contar_partidas()
        )

    # =========================
    # ORGANIZADOR
    # =========================
    elif perfil == "organizador":
        competicoes = listar_competicoes_do_organizador(session.get("usuario"))

        return render_template(
            "painel_organizador.html",
            competicoes=competicoes
        )

    # =========================
    # APONTADOR
    # =========================
    elif perfil == "apontador":
        cpf = session.get("usuario")
        competicoes = listar_competicoes_apontador(cpf)

        # Se não tem nenhuma competição
        if not competicoes:
            return render_template(
                "painel_apontador.html",
                competicoes=[],
                mensagem="Você não está vinculado a nenhuma competição ativa."
            )

        # Se tem só uma, entra direto na competição do apontador
        if len(competicoes) == 1:
            session["competicao_atual"] = competicoes[0]["competicao"]
            return redirect(
                url_for(
                    "apontadores.entrar_competicao_apontador",
                    competicao=competicoes[0]["competicao"]
                )
            )

        # Se tem mais de uma, mostra escolha no painel do apontador
        return render_template(
            "painel_apontador.html",
            competicoes=competicoes
        )

    # =========================
    # EQUIPE
    # =========================
    elif perfil == "equipe":
        return redirect(url_for("equipes.minhas_partidas_view"))

    return redirect(url_for("auth.login"))