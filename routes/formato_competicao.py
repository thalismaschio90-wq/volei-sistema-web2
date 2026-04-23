from flask import Blueprint, render_template, request, redirect, session, url_for, flash

from banco import (
    buscar_competicao_por_organizador,
    buscar_configuracao_avancada_competicao,
    atualizar_configuracao_avancada_competicao,
    inicializar_configuracao_avancada_competicao,
    competicao_esta_travada,
)

from routes.utils import exigir_perfil

formato_competicao_bp = Blueprint("formato_competicao", __name__)


@formato_competicao_bp.route("/formato-competicao", methods=["GET", "POST"])
@exigir_perfil("organizador")
def formato_competicao_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição vinculada ao organizador.", "erro")
        return redirect(url_for("painel.inicio"))

    inicializar_configuracao_avancada_competicao(competicao["nome"])

    if request.method == "POST":
        if competicao_esta_travada(competicao["nome"]):
            flash("A competição está travada. O formato não pode mais ser alterado.", "erro")
            return redirect(url_for("formato_competicao.formato_competicao_view"))

        tipo_confronto = request.form.get("tipo_confronto", "grupo_interno").strip()
        tipo_classificacao = request.form.get("tipo_classificacao", "grupo").strip()
        cruzamentos_grupos = request.form.get("cruzamentos_grupos", "").strip()
        qtd_classificados = request.form.get("qtd_classificados", "0").strip()
        formato_finais = request.form.get("formato_finais", "quartas").strip()
        possui_bye = request.form.get("possui_bye", "nao").strip() == "sim"
        qtd_bye = request.form.get("qtd_bye", "0").strip()
        data_limite_inscricao = request.form.get("data_limite_inscricao") or None
        hora_limite_inscricao = request.form.get("hora_limite_inscricao") or None
        bloquear_apos_inicio_jogos = request.form.get("bloquear_apos_inicio_jogos", "nao").strip() == "sim"

        try:
            qtd_classificados = int(qtd_classificados)
        except ValueError:
            qtd_classificados = 0

        try:
            qtd_bye = int(qtd_bye)
        except ValueError:
            qtd_bye = 0

        fases_config = {
            "tipo_confronto": tipo_confronto,
            "tipo_classificacao": tipo_classificacao,
            "cruzamentos_grupos": cruzamentos_grupos,
            "grupos": {
                "tipo_jogo": request.form.get("grupos_tipo_jogo", "set_unico").strip(),
                "pontos": int(request.form.get("grupos_pontos", 25) or 25),
                "tem_tiebreak": request.form.get("grupos_tem_tiebreak", "nao").strip() == "sim",
                "pontos_tiebreak": int(request.form.get("grupos_pontos_tiebreak", 15) or 15),
            },
            "grupos_especificos": {
                "A": {"tipo_jogo": request.form.get("grupo_A_tipo", "").strip(), "pontos": request.form.get("grupo_A_pontos", "").strip()},
                "B": {"tipo_jogo": request.form.get("grupo_B_tipo", "").strip(), "pontos": request.form.get("grupo_B_pontos", "").strip()},
                "C": {"tipo_jogo": request.form.get("grupo_C_tipo", "").strip(), "pontos": request.form.get("grupo_C_pontos", "").strip()},
                "D": {"tipo_jogo": request.form.get("grupo_D_tipo", "").strip(), "pontos": request.form.get("grupo_D_pontos", "").strip()},
            },
            "quartas": {
                "tipo_jogo": request.form.get("quartas_tipo_jogo", "melhor_de_3").strip(),
                "pontos": int(request.form.get("quartas_pontos", 21) or 21),
                "tem_tiebreak": request.form.get("quartas_tem_tiebreak", "sim").strip() == "sim",
                "pontos_tiebreak": int(request.form.get("quartas_pontos_tiebreak", 15) or 15),
            },
            "semifinal": {
                "tipo_jogo": request.form.get("semifinal_tipo_jogo", "melhor_de_3").strip(),
                "pontos": int(request.form.get("semifinal_pontos", 21) or 21),
                "tem_tiebreak": request.form.get("semifinal_tem_tiebreak", "sim").strip() == "sim",
                "pontos_tiebreak": int(request.form.get("semifinal_pontos_tiebreak", 15) or 15),
            },
            "final": {
                "tipo_jogo": request.form.get("final_tipo_jogo", "melhor_de_3").strip(),
                "pontos": int(request.form.get("final_pontos", 25) or 25),
                "tem_tiebreak": request.form.get("final_tem_tiebreak", "sim").strip() == "sim",
                "pontos_tiebreak": int(request.form.get("final_pontos_tiebreak", 15) or 15),
            },
        }

        atualizar_configuracao_avancada_competicao(
            nome_competicao=competicao["nome"],
            tipo_classificacao=tipo_classificacao,
            qtd_classificados=qtd_classificados,
            formato_finais=formato_finais,
            possui_bye=possui_bye,
            qtd_bye=qtd_bye,
            fases_config=fases_config,
            tipo_confronto=tipo_confronto,
            cruzamentos_grupos=cruzamentos_grupos,
            data_limite_inscricao=data_limite_inscricao,
            hora_limite_inscricao=hora_limite_inscricao,
            bloquear_apos_inicio=bloquear_apos_inicio_jogos,
        )

        flash("Formato da competição salvo com sucesso.", "sucesso")
        return redirect(url_for("formato_competicao.formato_competicao_view"))

    config = buscar_configuracao_avancada_competicao(competicao["nome"])
    if not config:
        flash("Não foi possível carregar a configuração avançada.", "erro")
        return redirect(url_for("painel.inicio"))

    fases = config.get("fases_config") or {}

    return render_template(
        "formato_competicao.html",
        competicao=competicao,
        config=config,
        fases=fases,
    )
