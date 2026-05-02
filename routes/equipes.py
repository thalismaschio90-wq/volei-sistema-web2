print(">>> CARREGOU O ARQUIVO EQUIPES.PY CERTO <<<")
from flask import Blueprint, render_template, request, redirect, session, url_for, flash
from banco import (
    buscar_competicao_por_organizador,
    listar_equipes_da_competicao,
    equipe_existe_na_competicao,
    criar_equipe_com_credenciais,
    redefinir_senha_da_equipe,
    excluir_equipe,
    buscar_config_conferencia_atletas,
    listar_atletas_para_conferencia,
    criar_campos_conferencia_atletas,
    conectar,

    # ATLETAS - EQUIPE
    cadastrar_atleta,
    listar_atletas_da_equipe,
    excluir_atleta,
    atualizar_numero_atleta,
    controle_inscricao_para_equipe,

    # ATLETAS - ORGANIZADOR
    listar_atletas_da_competicao,
    atualizar_status_atleta,

    # EQUIPE - GERENCIAMENTO
    buscar_equipe_por_nome_e_competicao,
    buscar_equipe_por_login,
    atualizar_nome_equipe,
    atualizar_quadro_tecnico_equipe,
    salvar_liberacao_extra_equipe,

    # USUÁRIO
    buscar_usuario_por_login,
    competicao_esta_travada,
    validar_edicao_atletas_equipe,
    equipe_tem_partida_iniciada,
    listar_partidas,
)
from routes.utils import exigir_perfil

equipes_bp = Blueprint("equipes", __name__)


# =========================
# ORGANIZADOR - EQUIPES
# =========================
@equipes_bp.route("/equipes")
@exigir_perfil("organizador")
def listar_equipes_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição vinculada ao organizador.", "erro")
        return redirect(url_for("painel.inicio"))

    credenciais = session.pop("credenciais_nova_equipe", None)
    senha_redefinida = session.pop("senha_redefinida_equipe", None)

    equipes = listar_equipes_da_competicao(competicao["nome"])

    return render_template(
        "equipes.html",
        competicao=competicao,
        equipes=equipes,
        credenciais=credenciais,
        senha_redefinida=senha_redefinida
    )


@equipes_bp.route("/equipes/nova", methods=["GET", "POST"])
@exigir_perfil("organizador")
def nova_equipe():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição vinculada ao organizador.", "erro")
        return redirect(url_for("painel.inicio"))

    if request.method == "POST":
        nome = request.form.get("nome", "").strip()

        if not nome:
            flash("Informe o nome da equipe.", "erro")
            return render_template("nova_equipe.html", competicao=competicao)

        if equipe_existe_na_competicao(nome, competicao["nome"]):
            flash("Já existe uma equipe com esse nome nesta competição.", "erro")
            return render_template("nova_equipe.html", competicao=competicao)

        if competicao_esta_travada(competicao["nome"]):
            flash("A competição está travada. Não é possível criar equipes.", "erro")
            return render_template("nova_equipe.html", competicao=competicao)

        credenciais = criar_equipe_com_credenciais(nome, competicao["nome"])

        session["credenciais_nova_equipe"] = {
            "nome": nome,
            "login": credenciais["login"],
            "senha": credenciais["senha"]
        }

        flash("Equipe criada com sucesso.", "sucesso")
        return redirect(url_for("equipes.listar_equipes_view"))

    return render_template("nova_equipe.html", competicao=competicao)


@equipes_bp.route("/equipes/<nome>/redefinir-senha", methods=["POST"])
@exigir_perfil("organizador")
def redefinir_senha_equipe_view(nome):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    resultado = redefinir_senha_da_equipe(nome, competicao["nome"])

    if not resultado:
        flash("Erro ao redefinir senha.", "erro")
        return redirect(url_for("equipes.listar_equipes_view"))

    session["senha_redefinida_equipe"] = {
        "nome": nome,
        "login": resultado["login"],
        "senha": resultado["senha"]
    }

    flash("Senha da equipe redefinida com sucesso.", "sucesso")
    return redirect(url_for("equipes.listar_equipes_view"))


@equipes_bp.route("/equipes/<nome>/excluir", methods=["POST"])
@exigir_perfil("organizador")
def excluir_equipe_view(nome):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if competicao_esta_travada(competicao["nome"]):
        flash("A competição está travada. Não é possível excluir equipes.", "erro")
        return redirect(url_for("equipes.listar_equipes_view"))

    sucesso = excluir_equipe(nome, competicao["nome"])

    if sucesso:
        flash("Equipe excluída com sucesso.", "sucesso")
    else:
        flash("Erro ao excluir equipe.", "erro")

    return redirect(url_for("equipes.listar_equipes_view"))


# =========================
# ORGANIZADOR - GERENCIAR EQUIPE
# =========================
@equipes_bp.route("/equipes/<path:nome>/gerenciar", methods=["GET", "POST"])
@exigir_perfil("organizador")
def gerenciar_equipe_view(nome):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição vinculada ao organizador.", "erro")
        return redirect(url_for("painel.inicio"))

    nome_competicao = competicao["nome"]

    equipe = buscar_equipe_por_nome_e_competicao(nome, nome_competicao)
    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("equipes.listar_equipes_view"))

    erro = None
    sucesso = None

    if request.method == "POST":
        acao = request.form.get("acao", "").strip()

        if acao == "salvar":
            novo_nome = request.form.get("nome", "").strip()

            if not novo_nome:
                erro = "Informe o nome da equipe."

            elif (
                novo_nome.lower() != equipe["nome"].lower()
                and equipe_existe_na_competicao(novo_nome, nome_competicao)
            ):
                erro = "Já existe uma equipe com esse nome nesta competição."

            elif competicao_esta_travada(nome_competicao):
                erro = "A competição está travada. O nome da equipe não pode mais ser alterado."

            else:
                atualizar_nome_equipe(equipe["nome"], nome_competicao, novo_nome)
                sucesso = "Nome da equipe atualizado com sucesso."
                nome = novo_nome

        elif acao == "salvar_tecnico":
            ok_edicao, mensagem_edicao = validar_edicao_atletas_equipe(
                nome_competicao,
                equipe["nome"]
            )

            if not ok_edicao:
                erro = mensagem_edicao
            else:
                atualizar_quadro_tecnico_equipe(
                    equipe["nome"],
                    nome_competicao,
                    request.form.get("treinador", "").strip(),
                    request.form.get("auxiliar_tecnico", "").strip(),
                    request.form.get("preparador_fisico", "").strip(),
                    request.form.get("medico", "").strip(),
                )
                sucesso = "Quadro técnico atualizado com sucesso."

        elif acao == "salvar_liberacao":
            if competicao_esta_travada(nome_competicao):
                erro = "A competição está travada. Não é possível alterar permissões especiais agora."
            else:
                liberado = request.form.get("liberacao_extra_inscricao") == "on"
                data_extra = request.form.get("liberacao_extra_data", "").strip() or None
                hora_extra = request.form.get("liberacao_extra_hora", "").strip() or None

                salvar_liberacao_extra_equipe(
                    equipe["nome"],
                    nome_competicao,
                    liberado,
                    data_extra,
                    hora_extra,
                )

                sucesso = "Permissão especial atualizada com sucesso."

        elif acao == "resetar_senha":
            resultado = redefinir_senha_da_equipe(equipe["nome"], nome_competicao)

            if resultado:
                session["senha_redefinida_equipe"] = {
                    "nome": equipe["nome"],
                    "login": resultado["login"],
                    "senha": resultado["senha"],
                }
                return redirect(url_for("equipes.listar_equipes_view"))

            erro = "Não foi possível redefinir a senha."

        elif acao == "excluir":
            if competicao_esta_travada(nome_competicao):
                erro = "A competição está travada. Não é possível excluir equipes."
            else:
                ok = excluir_equipe(equipe["nome"], nome_competicao)

                if ok:
                    flash("Equipe excluída com sucesso.", "sucesso")
                    return redirect(url_for("equipes.listar_equipes_view"))

                erro = "Não foi possível excluir a equipe."

        else:
            erro = "Ação inválida."

        equipe = buscar_equipe_por_nome_e_competicao(nome, nome_competicao)

        if not equipe:
            flash("Equipe não encontrada após a atualização.", "erro")
            return redirect(url_for("equipes.listar_equipes_view"))

    atletas = listar_atletas_da_equipe(equipe["nome"], nome_competicao)

    return render_template(
    "gerenciar_equipe.html",
    equipe=equipe,
    atletas=atletas,
    erro=erro,
    sucesso=sucesso,
    competicao=competicao,
)


# =========================
# EQUIPE - MINHA EQUIPE
# =========================
@equipes_bp.route("/minha-equipe", methods=["GET", "POST"])
@exigir_perfil("equipe")
def minha_equipe():
    usuario = session.get("usuario")
    equipe = buscar_equipe_por_login(usuario)

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    erro = None
    sucesso = None

    if request.method == "POST":
        ok_edicao, mensagem_edicao = validar_edicao_atletas_equipe(equipe["competicao"], equipe["nome"])
        if not ok_edicao:
            erro = mensagem_edicao
        else:
            atualizar_quadro_tecnico_equipe(
            equipe["nome"],
            equipe["competicao"],
            request.form.get("treinador", "").strip(),
            request.form.get("auxiliar_tecnico", "").strip(),
            request.form.get("preparador_fisico", "").strip(),
            request.form.get("medico", "").strip(),
            )
            sucesso = "Quadro técnico atualizado com sucesso."
            equipe = buscar_equipe_por_login(usuario)

    return render_template(
        "minha_equipe.html",
        equipe=equipe,
        erro=erro,
        sucesso=sucesso
    )




# =========================
# EQUIPE - VISUALIZADOR DE PARTIDAS
# =========================
def _fase_label_partida_equipe(fase):
    fase = (fase or "grupos").strip().lower()
    mapa = {
        "grupos": "Classificatória",
        "grupo": "Classificatória",
        "classificatorias": "Classificatória",
        "classificatória": "Classificatória",
        "quartas": "Quartas de final",
        "quartas de final": "Quartas de final",
        "semifinal": "Semifinal",
        "semifinais": "Semifinal",
        "final": "Final",
        "finais": "Final",
    }
    return mapa.get(fase, fase.replace("_", " ").title())


def _ordem_fase_partida_equipe(fase):
    fase = (fase or "grupos").strip().lower()
    if fase in {"grupos", "grupo", "classificatorias", "classificatória"}:
        return 1
    if fase in {"quartas", "quartas de final"}:
        return 2
    if fase in {"semifinal", "semifinais"}:
        return 3
    if fase in {"final", "finais"}:
        return 4
    return 9


def _status_visual_partida_equipe(partida):
    status = (
        partida.get("status")
        or partida.get("fase_partida")
        or partida.get("status_jogo")
        or "agendada"
    )
    status = str(status or "agendada").strip().lower()

    mapa = {
        "": "AGENDADA",
        "pendente": "AGENDADA",
        "aguardando": "AGENDADA",
        "agendada": "AGENDADA",
        "pre_jogo": "PRÉ-JOGO",
        "pre-jogo": "PRÉ-JOGO",
        "em andamento": "AO VIVO",
        "em_andamento": "AO VIVO",
        "andamento": "AO VIVO",
        "ao vivo": "AO VIVO",
        "ao_vivo": "AO VIVO",
        "finalizada": "FINALIZADA",
        "finalizado": "FINALIZADA",
        "encerrado": "FINALIZADA",
        "encerrada": "FINALIZADA",
    }
    return mapa.get(status, status.replace("_", " ").upper())


def _partida_ao_vivo_equipe(partida):
    status = _status_visual_partida_equipe(partida)
    return status == "AO VIVO"


def _partida_finalizada_equipe(partida):
    status = _status_visual_partida_equipe(partida)
    return status == "FINALIZADA"


def _parciais_partida_equipe(partida):
    parciais = []
    for i in range(1, 6):
        a = partida.get(f"set{i}_a")
        b = partida.get(f"set{i}_b")
        if a is not None and b is not None:
            try:
                parciais.append(f"{int(a)}x{int(b)}")
            except (TypeError, ValueError):
                parciais.append(f"{a}x{b}")
    return " / ".join(parciais) if parciais else "-"


def _preparar_partidas_para_equipe(equipe):
    nome_equipe = (equipe.get("nome") or "").strip()
    competicao = (equipe.get("competicao") or "").strip()

    if not nome_equipe or not competicao:
        return []

    partidas = listar_partidas(competicao)
    resultado = []

    for p in partidas:
        partida = dict(p)
        equipe_a = (partida.get("equipe_a") or "").strip()
        equipe_b = (partida.get("equipe_b") or "").strip()
        fase = (partida.get("fase") or "grupos").strip().lower()

        minha_partida = (
            equipe_a.lower() == nome_equipe.lower()
            or equipe_b.lower() == nome_equipe.lower()
        )

        # Visualizador geral da equipe:
        # mostra TODAS as partidas da competição, não só as partidas da equipe.
        # O campo minha_partida continua marcado para destacar quando o jogo é dela.
        partida["fase_label"] = _fase_label_partida_equipe(fase)
        partida["fase_ordem"] = _ordem_fase_partida_equipe(fase)
        partida["status_visual"] = _status_visual_partida_equipe(partida)
        partida["ao_vivo"] = _partida_ao_vivo_equipe(partida)
        partida["finalizada"] = _partida_finalizada_equipe(partida)
        partida["parciais_formatadas"] = _parciais_partida_equipe(partida)
        partida["minha_partida"] = minha_partida
        partida["placar_ao_vivo_a"] = int(partida.get("pontos_a") or partida.get("placar_a") or 0)
        partida["placar_ao_vivo_b"] = int(partida.get("pontos_b") or partida.get("placar_b") or 0)
        resultado.append(partida)

    return sorted(
        resultado,
        key=lambda p: (
            p.get("fase_ordem") or 9,
            p.get("rodada") or 999999,
            p.get("ordem") or 999999,
            p.get("id") or 999999,
        )
    )


@equipes_bp.route("/minhas-partidas")
@exigir_perfil("equipe")
def minhas_partidas_view():
    usuario = session.get("usuario")
    equipe = buscar_equipe_por_login(usuario)

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    partidas = _preparar_partidas_para_equipe(equipe)

    return render_template(
        "minhas_partidas.html",
        equipe=equipe,
        partidas=partidas,
    )


# =========================
# EQUIPE - ATLETAS
# =========================
def _montar_contexto_atletas_equipe(equipe, erro=None, sucesso=None, modo_tela="lista", carregar_atletas=True):
    """
    Monta os dados da tela de atletas sem fazer consultas desnecessárias.

    Antes a tela de cadastro carregava TODOS os atletas, filtrava aprovados
    e ainda consultava jogos iniciados, mesmo quando o usuário só queria abrir
    o formulário. Isso deixava a inscrição pesada e travando.
    """
    controle_inscricao = controle_inscricao_para_equipe(equipe["competicao"], equipe["nome"])

    atletas_liberados = bool(controle_inscricao.get("aberta", True))
    mensagem_atletas = controle_inscricao.get("motivo") or ""

    atletas = []
    atletas_aprovados = []

    if carregar_atletas:
        atletas = listar_atletas_da_equipe(equipe["nome"], equipe["competicao"])
        atletas_aprovados = [
            a for a in atletas
            if (a.get("status") or "").lower() == "aprovado"
        ]

    return {
        "equipe": equipe,
        "atletas": atletas,
        "atletas_aprovados": atletas_aprovados,
        "controle_inscricao": controle_inscricao,
        "atletas_edicao_liberada": atletas_liberados,
        "mensagem_edicao_atletas": mensagem_atletas,
        "equipe_ja_iniciou_jogos": False,
        "erro": erro,
        "sucesso": sucesso,
        "modo_tela": modo_tela,
    }


@equipes_bp.route("/meus-atletas", methods=["GET", "POST"])
@exigir_perfil("equipe")
def meus_atletas_view():
    usuario = session.get("usuario")
    equipe = buscar_equipe_por_login(usuario)

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    erro = None
    sucesso = None

    if request.method == "POST":
        acao = request.form.get("acao", "").strip()

        if acao == "salvar_numero":
            ok, msg = atualizar_numero_atleta(
                int(request.form.get("id_atleta")),
                request.form.get("numero", "").strip()
            )
            if ok:
                sucesso = msg
            else:
                erro = msg

    contexto = _montar_contexto_atletas_equipe(equipe, erro=erro, sucesso=sucesso, modo_tela="lista")
    return render_template("meus_atletas.html", **contexto)


@equipes_bp.route("/cadastrar-atleta", methods=["GET", "POST"])
@exigir_perfil("equipe")
def cadastrar_atleta_pagina_view():
    usuario = session.get("usuario")
    equipe = buscar_equipe_por_login(usuario)

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    erro = None

    if request.method == "POST":
        # Deixa a função cadastrar_atleta validar CPF, prazo, limite e número.
        # Assim evitamos consulta duplicada antes de salvar.
        resultado = cadastrar_atleta(
            request.form.get("nome", "").strip(),
            request.form.get("cpf", "").strip(),
            request.form.get("data_nascimento", "").strip(),
            request.form.get("numero", "").strip(),
            equipe["nome"],
            equipe["competicao"]
        )

        if isinstance(resultado, tuple):
            ok, msg = resultado
        else:
            ok = bool(resultado)
            msg = None

        if ok:
            flash(msg or "Atleta cadastrado com sucesso.", "sucesso")
            return redirect(url_for("equipes.cadastrar_atleta_pagina_view"))

        erro = msg or "Não foi possível cadastrar o atleta. Verifique CPF duplicado, número repetido, limite de atletas ou bloqueio de inscrição."

    contexto = _montar_contexto_atletas_equipe(
        equipe,
        erro=erro,
        sucesso=None,
        modo_tela="cadastro",
        carregar_atletas=False
    )
    return render_template("meus_atletas.html", **contexto)


# =========================
# ATLETAS - EQUIPE
# =========================
@equipes_bp.route("/atletas/cadastrar", methods=["POST"])
@exigir_perfil("equipe")
def cadastrar_atleta_view():
    nome = request.form.get("nome", "").strip()
    cpf = request.form.get("cpf", "").strip()
    data_nascimento = request.form.get("data_nascimento", "").strip()
    numero = request.form.get("numero", "").strip()

    usuario = session.get("usuario")
    dados_usuario = buscar_usuario_por_login(usuario)

    if not dados_usuario:
        flash("Usuário da equipe não encontrado.", "erro")
        return redirect(url_for("painel.inicio"))

    equipe = dados_usuario["equipe"]
    competicao = dados_usuario["competicao_vinculada"]

    controle_inscricao = controle_inscricao_para_equipe(competicao, equipe)
    if not controle_inscricao.get("aberta", True):
        flash(controle_inscricao.get("motivo") or "Inscrição bloqueada.", "erro")
        return redirect(url_for("equipes.cadastrar_atleta_pagina_view"))

    resultado = cadastrar_atleta(nome, cpf, data_nascimento, numero, equipe, competicao)

    if isinstance(resultado, tuple):
        ok, msg = resultado
    else:
        ok = bool(resultado)
        msg = None

    if not ok:
        flash(msg or "Não foi possível cadastrar o atleta. Verifique CPF duplicado, número repetido, limite de atletas ou bloqueio de inscrição.", "erro")
    else:
        flash(msg or "Atleta cadastrado com sucesso!", "sucesso")

    return redirect(url_for("equipes.cadastrar_atleta_pagina_view"))


@equipes_bp.route("/atletas/<int:id_atleta>/excluir", methods=["POST"])
@exigir_perfil("equipe")
def excluir_atleta_view(id_atleta):
    usuario = session.get("usuario")
    equipe = buscar_equipe_por_login(usuario)

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    controle_inscricao = controle_inscricao_para_equipe(equipe["competicao"], equipe["nome"])
    atletas_liberados, mensagem_atletas = validar_edicao_atletas_equipe(equipe["competicao"], equipe["nome"])
    if not controle_inscricao.get("aberta", True):
        flash(controle_inscricao.get("motivo") or "Inscrição bloqueada.", "erro")
        return redirect(url_for("equipes.meus_atletas_view"))

    ok, msg = excluir_atleta(id_atleta)
    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("equipes.meus_atletas_view"))


# =========================
# ORGANIZADOR - ATLETAS
# =========================
@equipes_bp.route("/atletas")
@exigir_perfil("organizador")
def listar_atletas_organizador():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição vinculada ao organizador.", "erro")
        return redirect(url_for("painel.inicio"))

    atletas = listar_atletas_da_competicao(competicao["nome"])

    return render_template(
        "atletas_organizador.html",
        atletas=atletas,
        competicao=competicao
    )


@equipes_bp.route("/atletas/<int:id>/aprovar", methods=["POST"])
@exigir_perfil("organizador")
def aprovar_atleta(id):
    ok, msg = atualizar_status_atleta(id, "aprovado")
    flash(msg if ok else msg, "sucesso" if ok else "erro")
    return redirect(url_for("equipes.listar_atletas_organizador"))


@equipes_bp.route("/atletas/<int:id>/reprovar", methods=["POST"])
@exigir_perfil("organizador")
def reprovar_atleta(id):
    ok, msg = atualizar_status_atleta(id, "reprovado")
    flash(msg if ok else msg, "sucesso" if ok else "erro")
    return redirect(url_for("equipes.listar_atletas_organizador"))


@equipes_bp.route("/atletas/<int:id>/excluir-organizador", methods=["POST"])
@exigir_perfil("organizador")
def excluir_atleta_organizador(id):
    ok, msg = excluir_atleta(id)
    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("equipes.listar_atletas_organizador"))


@equipes_bp.route("/conferencia-atletas")
@exigir_perfil("equipe")
def conferencia_atletas():
    criar_campos_conferencia_atletas()

    usuario = session.get("usuario")
    equipe = buscar_equipe_por_login(usuario)

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    competicao = equipe["competicao"]
    comp = buscar_config_conferencia_atletas(competicao)

    if not comp or not comp.get("conferencia_liberada"):
        flash("Conferência de atletas ainda não liberada pela organização.", "erro")
        return redirect(url_for("painel.inicio"))

    if comp.get("conferencia_encerrada"):
        flash("Conferência de atletas encerrada pela organização.", "erro")
        return redirect(url_for("painel.inicio"))

    atletas = listar_atletas_para_conferencia(competicao)

    equipes = {}
    for a in atletas:
        nome_equipe = a.get("equipe") or "Sem equipe"
        equipes.setdefault(nome_equipe, []).append(a)

    return render_template(
        "conferencia_atletas.html",
        equipes=equipes,
        prazo=comp.get("conferencia_prazo"),
        link=comp.get("conferencia_link"),
        encerrado=comp.get("conferencia_encerrada"),
        competicao=comp
    )


@equipes_bp.route("/conferencia-atletas/config/<competicao>", methods=["POST"])
@exigir_perfil("organizador")
def salvar_config_conferencia(competicao):
    prazo = request.form.get("prazo", "").strip()
    link = request.form.get("link", "").strip()
    criar_campos_conferencia_atletas()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE competicoes
                SET conferencia_prazo = %s,
                    conferencia_link = %s
                WHERE nome = %s
            """, (prazo, link, competicao))
        conn.commit()

    flash("Configuração da conferência salva com sucesso.", "sucesso")
    return redirect(url_for("equipes.listar_atletas_organizador"))


@equipes_bp.route("/conferencia-atletas/liberar/<competicao>", methods=["POST"])
@exigir_perfil("organizador")
def liberar_conferencia(competicao):
    criar_campos_conferencia_atletas()
    
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE competicoes
                SET conferencia_liberada = TRUE,
                    conferencia_encerrada = FALSE
                WHERE nome = %s
            """, (competicao,))
        conn.commit()

    flash("Conferência de atletas liberada para as equipes.", "sucesso")
    return redirect(url_for("equipes.listar_atletas_organizador"))


@equipes_bp.route("/conferencia-atletas/encerrar/<competicao>", methods=["POST"])
@exigir_perfil("organizador")
def encerrar_conferencia(competicao):
    criar_campos_conferencia_atletas()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE competicoes
                SET conferencia_encerrada = TRUE
                WHERE nome = %s
            """, (competicao,))
        conn.commit()

    flash("Conferência de atletas encerrada.", "sucesso")
    return redirect(url_for("equipes.listar_atletas_organizador"))