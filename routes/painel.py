from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify
from banco import (
    conectar,
    contar_competicoes,
    contar_equipes,
    contar_partidas,
    listar_competicoes_do_organizador,
    listar_competicoes_apontador,
    buscar_usuario_por_login,
    atualizar_senha_usuario,
    buscar_apontador,
    definir_senha_apontador
)
from routes.utils import login_obrigatorio

painel_bp = Blueprint("painel", __name__)


STATUS_ATIVOS_ARBITRO = (
    "pre_jogo",
    "papeleta",
    "papeleta_pronta",
    "em_andamento",
    "andamento",
    "ao_vivo",
    "jogo",
    "iniciada",
    "iniciado",
    "entre_sets",
    "tiebreak_sorteio",
)

STATUS_FINALIZADOS_ARBITRO = (
    "finalizada",
    "finalizado",
    "encerrada",
    "encerrado",
)


def _perfil_normalizado():
    return (session.get("perfil") or "").strip().lower()


def _usuario_logado():
    return (session.get("usuario") or "").strip()


def _usuario_tem_perfil_arbitro():
    """
    No sistema atual, os árbitros criados pelo organizador entram como perfil 'mesario'.
    Mantemos essa compatibilidade para não quebrar cadastros antigos.
    """
    return _perfil_normalizado() in {"mesario", "arbitro"}


def _competicao_arbitro_logado():
    competicao = (session.get("competicao_vinculada") or "").strip()

    if competicao:
        return competicao

    usuario = buscar_usuario_por_login(_usuario_logado())
    if usuario:
        competicao = (usuario.get("competicao_vinculada") or "").strip()
        if competicao:
            session["competicao_vinculada"] = competicao
            return competicao

    return ""


def _buscar_partida_ativa_para_painel_arbitro(competicao):
    """
    Busca a partida que deve aparecer nos tablets fixos dos árbitros.
    Ela entra na fila assim que o apontador salva o pré-jogo/sorteio.
    """
    if not competicao:
        return None

    ativos = list(STATUS_ATIVOS_ARBITRO)
    finalizados = list(STATUS_FINALIZADOS_ARBITRO)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    competicao,
                    ordem,
                    quadra,
                    grupo,
                    equipe_a,
                    equipe_b,
                    equipe_a_operacional,
                    equipe_b_operacional,
                    status,
                    status_operacao,
                    status_jogo,
                    fase_partida,
                    set_atual,
                    pontos_a,
                    pontos_b,
                    sets_a,
                    sets_b,
                    pre_jogo_finalizado,
                    arbitro_1_nome,
                    arbitro_2_nome,
                    operador_nome,
                    operador_login
                FROM partidas
                WHERE competicao = %s
                  AND LOWER(COALESCE(status, '')) <> ALL(%s)
                  AND LOWER(COALESCE(status_operacao, '')) <> ALL(%s)
                  AND (
                        COALESCE(pre_jogo_finalizado, FALSE) = TRUE
                     OR LOWER(COALESCE(status, '')) = ANY(%s)
                     OR LOWER(COALESCE(status_operacao, '')) = ANY(%s)
                     OR LOWER(COALESCE(status_jogo, '')) = ANY(%s)
                     OR LOWER(COALESCE(fase_partida, '')) = ANY(%s)
                  )
                ORDER BY
                    CASE
                        WHEN LOWER(COALESCE(status_jogo, '')) IN ('em_andamento', 'andamento', 'ao_vivo', 'jogo') THEN 1
                        WHEN LOWER(COALESCE(status_operacao, '')) IN ('em_andamento', 'andamento', 'ao_vivo', 'jogo') THEN 2
                        WHEN LOWER(COALESCE(status, '')) IN ('em_andamento', 'andamento', 'ao_vivo', 'jogo', 'iniciada', 'iniciado') THEN 3
                        WHEN COALESCE(pre_jogo_finalizado, FALSE) = TRUE THEN 4
                        ELSE 9
                    END,
                    COALESCE(ordem, 999999),
                    id
                LIMIT 1
                """,
                (competicao, finalizados, finalizados, ativos, ativos, ativos, ativos),
            )
            return cur.fetchone()


@painel_bp.route("/inicio")
@login_obrigatorio
def inicio():
    perfil = _perfil_normalizado()

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
        competicoes = listar_competicoes_do_organizador(
            session.get("usuario")
        )

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

        if not competicoes:
            return render_template(
                "painel_apontador.html",
                competicoes=[],
                mensagem="Você não está vinculado a nenhuma competição ativa."
            )

        if len(competicoes) == 1:

            session["competicao_atual"] = competicoes[0]["competicao"]

            return redirect(
                url_for(
                    "apontadores.entrar_competicao_apontador",
                    competicao=competicoes[0]["competicao"]
                )
            )

        return render_template(
            "painel_apontador.html",
            competicoes=competicoes
        )

    # =========================
    # ÁRBITROS
    # =========================
    elif perfil in {"mesario", "arbitro"}:

        return redirect(
            url_for("painel.painel_arbitros")
        )

    # =========================
    # EQUIPE
    # =========================
    elif perfil == "equipe":

        return redirect(
            url_for("equipes.minhas_partidas_view")
        )

    # =========================
    # FALLBACK
    # =========================
    return redirect(
        url_for("auth.login")
    )


@painel_bp.route("/painel-arbitros")
@login_obrigatorio
def painel_arbitros():
    if not _usuario_tem_perfil_arbitro():
        flash("Você não tem permissão para acessar o painel dos árbitros.", "erro")
        return redirect(url_for("painel.inicio"))

    competicao = _competicao_arbitro_logado()

    return render_template(
        "painel_arbitro.html",
        competicao=competicao,
        nome=session.get("nome") or session.get("usuario")
    )


@painel_bp.route("/painel-arbitro-1")
@login_obrigatorio
def painel_arbitro_1():
    if not _usuario_tem_perfil_arbitro():
        flash("Você não tem permissão para acessar o painel do 1º árbitro.", "erro")
        return redirect(url_for("painel.inicio"))

    return render_template(
        "painel_arbitro_automatico.html",
        tipo="primeiro",
        titulo="Painel do 1º Árbitro",
        subtitulo="Tablet fixo do árbitro principal. A partida aparecerá automaticamente após o pré-jogo.",
        endpoint_status=url_for("painel.proxima_partida_arbitro_1"),
        voltar_url=url_for("painel.painel_arbitros"),
    )


@painel_bp.route("/painel-arbitro-2")
@login_obrigatorio
def painel_arbitro_2():
    if not _usuario_tem_perfil_arbitro():
        flash("Você não tem permissão para acessar o painel do 2º árbitro.", "erro")
        return redirect(url_for("painel.inicio"))

    return render_template(
        "painel_arbitro_automatico.html",
        tipo="segundo",
        titulo="Painel do 2º Árbitro",
        subtitulo="Tablet fixo do segundo árbitro. A partida aparecerá automaticamente após o pré-jogo.",
        endpoint_status=url_for("painel.proxima_partida_arbitro_2"),
        voltar_url=url_for("painel.painel_arbitros"),
    )


def _resposta_proxima_partida_arbitro(tipo):
    if not _usuario_tem_perfil_arbitro():
        return jsonify({"ok": False, "erro": "sem_permissao"}), 403

    competicao = _competicao_arbitro_logado()
    partida = _buscar_partida_ativa_para_painel_arbitro(competicao)

    if not partida:
        return jsonify({
            "ok": True,
            "tem_partida": False,
            "competicao": competicao,
            "mensagem": "Aguardando o apontador salvar o pré-jogo."
        })

    rota = "oficiais.primeiro_arbitro_view" if tipo == "primeiro" else "oficiais.segundo_arbitro_view"
    url = url_for(rota, competicao=partida["competicao"], partida_id=partida["id"])

    return jsonify({
        "ok": True,
        "tem_partida": True,
        "url": url,
        "partida": {
            "id": partida.get("id"),
            "competicao": partida.get("competicao"),
            "ordem": partida.get("ordem"),
            "quadra": partida.get("quadra"),
            "grupo": partida.get("grupo"),
            "equipe_a": partida.get("equipe_a_operacional") or partida.get("equipe_a"),
            "equipe_b": partida.get("equipe_b_operacional") or partida.get("equipe_b"),
            "status": partida.get("status"),
            "status_operacao": partida.get("status_operacao"),
            "operador": partida.get("operador_nome") or partida.get("operador_login") or "",
        }
    })


@painel_bp.route("/painel-arbitro-1/proxima")
@login_obrigatorio
def proxima_partida_arbitro_1():
    return _resposta_proxima_partida_arbitro("primeiro")


@painel_bp.route("/painel-arbitro-2/proxima")
@login_obrigatorio
def proxima_partida_arbitro_2():
    return _resposta_proxima_partida_arbitro("segundo")


@painel_bp.route("/minha-conta")
@login_obrigatorio
def minha_conta():
    login = session.get("usuario")
    perfil = _perfil_normalizado()

    if perfil == "apontador":
        apontador = buscar_apontador(login)

        if not apontador:
            flash("Apontador não encontrado.", "erro")
            return redirect(url_for("painel.inicio"))

        return render_template(
            "minha_conta.html",
            usuario=apontador.get("cpf"),
            nome=apontador.get("nome") or session.get("nome") or apontador.get("cpf"),
            perfil="apontador"
        )

    usuario_db = buscar_usuario_por_login(login)

    if not usuario_db:
        flash("Usuário não encontrado.", "erro")
        return redirect(url_for("painel.inicio"))

    return render_template(
        "minha_conta.html",
        usuario=usuario_db.get("login"),
        nome=usuario_db.get("nome"),
        perfil="árbitro" if perfil == "mesario" else usuario_db.get("perfil")
    )


@painel_bp.route("/minha-conta/alterar-senha", methods=["POST"])
@login_obrigatorio
def alterar_senha_minha_conta():
    login = session.get("usuario")
    perfil = _perfil_normalizado()

    senha_atual = (request.form.get("senha_atual") or "").strip()
    nova_senha = (request.form.get("nova_senha") or "").strip()
    confirmar_senha = (request.form.get("confirmar_senha") or "").strip()

    if not senha_atual or not nova_senha or not confirmar_senha:
        flash("Preencha todos os campos.", "erro")
        return redirect(url_for("painel.minha_conta"))

    if nova_senha != confirmar_senha:
        flash("A confirmação da senha não confere.", "erro")
        return redirect(url_for("painel.minha_conta"))

    if len(nova_senha) < 4:
        flash("A nova senha deve ter pelo menos 4 caracteres.", "erro")
        return redirect(url_for("painel.minha_conta"))

    if perfil == "apontador":
        apontador = buscar_apontador(login)

        if not apontador:
            flash("Apontador não encontrado.", "erro")
            return redirect(url_for("painel.inicio"))

        senha_salva = apontador.get("senha")

        if senha_salva and senha_atual != senha_salva:
            flash("Senha atual incorreta.", "erro")
            return redirect(url_for("painel.minha_conta"))

        definir_senha_apontador(login, nova_senha)

        flash("Senha alterada com sucesso!", "sucesso")
        return redirect(url_for("painel.minha_conta"))

    usuario_db = buscar_usuario_por_login(login)

    if not usuario_db:
        flash("Usuário não encontrado.", "erro")
        return redirect(url_for("painel.inicio"))

    if senha_atual != usuario_db.get("senha"):
        flash("Senha atual incorreta.", "erro")
        return redirect(url_for("painel.minha_conta"))

    atualizar_senha_usuario(login, nova_senha)

    flash("Senha alterada com sucesso!", "sucesso")
    return redirect(url_for("painel.minha_conta"))
