print(">>> CARREGOU O ARQUIVO EQUIPES.PY CERTO <<<")
from flask import Blueprint, render_template, request, redirect, session, url_for, flash, current_app
from banco import (
    buscar_competicao_por_organizador,
    buscar_competicao_por_nome,
    listar_equipes_da_competicao,
    equipe_existe_na_competicao,
    criar_equipe_com_credenciais,
    criar_nova_equipe_com_credenciais,
    buscar_equipes_globais_por_nome,
    buscar_atleta_global_por_cpf,
    vincular_equipe_existente_competicao,
    listar_competicoes_da_equipe_por_login,
    salvar_perfil_equipe_por_login,
    perfil_equipe_incompleto_por_login,
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
    atualizar_atleta_equipe,
    controle_inscricao_para_equipe,

    # ATLETAS - ORGANIZADOR
    listar_atletas_da_competicao,
    atualizar_status_atleta,
    aprovar_todos_atletas_pendentes,

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
    listar_grupos,
    listar_equipes_por_grupo,
    atualizar_escudo_equipe_por_login,
    escudo_padrao_equipe,
)
from routes.utils import exigir_perfil, aplicar_placar_exibicao_partida
from routes.tabela import (
    _calcular_classificacao,
    _obter_regras_classificacao,
    _criterios_efetivos_ate_sorteio,
    _colunas_classificacao_por_criterios,
    _mapa_escudos_equipes,
)
from PIL import Image, ImageOps, UnidentifiedImageError
from io import BytesIO
import base64


equipes_bp = Blueprint("equipes", __name__)


_EXTENSOES_ESCUDO_PERMITIDAS = {"png", "jpg", "jpeg", "webp"}


def _extensao_arquivo(nome_arquivo):
    nome_arquivo = nome_arquivo or ""
    if "." not in nome_arquivo:
        return ""
    return nome_arquivo.rsplit(".", 1)[1].lower().strip()


def _salvar_upload_escudo(arquivo, login):
    """
    Processa o escudo e devolve uma data URL base64 para salvar direto no Neon.

    Importante:
    - não salva mais em /static/uploads, pois o Render pode apagar arquivos em deploy/restart;
    - corrige orientação EXIF de celular;
    - padroniza em JPG 512x512;
    - remove transparência com fundo branco.
    """
    if not arquivo or not getattr(arquivo, "filename", ""):
        return None, "Selecione uma imagem para enviar."

    extensao = _extensao_arquivo(arquivo.filename)
    if extensao not in _EXTENSOES_ESCUDO_PERMITIDAS:
        return None, "Formato inválido. Envie PNG, JPG, JPEG ou WebP."

    try:
        arquivo.stream.seek(0)
        imagem = Image.open(arquivo.stream)
        imagem = ImageOps.exif_transpose(imagem)
        imagem.load()

        if imagem.mode in ("RGBA", "LA") or (imagem.mode == "P" and "transparency" in imagem.info):
            imagem = imagem.convert("RGBA")
            fundo = Image.new("RGBA", imagem.size, (255, 255, 255, 255))
            fundo.alpha_composite(imagem)
            imagem = fundo.convert("RGB")
        else:
            imagem = imagem.convert("RGB")

        largura, altura = imagem.size
        if largura <= 0 or altura <= 0:
            return None, "Imagem inválida. Envie outra imagem."

        lado = min(largura, altura)
        esquerda = max((largura - lado) // 2, 0)
        topo = max((altura - lado) // 2, 0)
        imagem = imagem.crop((esquerda, topo, esquerda + lado, topo + lado))

        filtro = getattr(Image, "Resampling", Image).LANCZOS
        imagem = imagem.resize((512, 512), filtro)

        buffer = BytesIO()
        imagem.save(
            buffer,
            format="JPEG",
            quality=82,
            optimize=True,
            progressive=True,
        )

        encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
        data_url = f"data:image/jpeg;base64,{encoded}"

    except UnidentifiedImageError:
        return None, "Não foi possível ler essa imagem. Envie uma imagem válida."
    except Exception as e:
        print("ERRO PROCESSAR ESCUDO:", repr(e))
        return None, "Não foi possível processar a imagem. Tente outra imagem."

    return data_url, None


def _equipe_logada_com_competicao():
    usuario = session.get("usuario")

    if not usuario:
        return None

    competicao_atual = (session.get("competicao_equipe_atual") or "").strip()

    if not competicao_atual:
        return None

    return buscar_equipe_por_login(usuario, competicao_atual)



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

    equipes_encontradas = []
    nome_busca = ""

    if request.method == "POST":
        acao = request.form.get("acao", "").strip()
        nome_busca = request.form.get("nome", "").strip()
        login_equipe = request.form.get("login_equipe", "").strip()

        if competicao_esta_travada(competicao["nome"]):
            flash("A competição está travada. Não é possível adicionar ou vincular equipes.", "erro")
            return render_template(
                "nova_equipe.html",
                competicao=competicao,
                nome_busca=nome_busca,
                equipes_encontradas=[],
            )

        if acao == "vincular":
            if not login_equipe:
                flash("Equipe inválida para vínculo.", "erro")
                return redirect(url_for("equipes.nova_equipe"))

            resultado = vincular_equipe_existente_competicao(login_equipe, competicao["nome"])

            if not resultado:
                flash("Não foi possível encontrar essa equipe no cadastro global.", "erro")
                return redirect(url_for("equipes.nova_equipe"))

            session["credenciais_nova_equipe"] = {
                "nome": resultado.get("nome"),
                "login": resultado.get("login"),
                "senha": resultado.get("senha"),
                "ja_existia": True,
                "ja_vinculada": resultado.get("ja_vinculada", False),
            }

            if resultado.get("ja_vinculada"):
                flash("Essa equipe já estava vinculada a esta competição. O login e senha foram mantidos.", "sucesso")
            else:
                flash("Equipe existente vinculada à competição com sucesso. O login e senha foram mantidos.", "sucesso")

            return redirect(url_for("equipes.listar_equipes_view"))

        if not nome_busca:
            flash("Informe o nome da equipe.", "erro")
            return render_template(
                "nova_equipe.html",
                competicao=competicao,
                nome_busca=nome_busca,
                equipes_encontradas=[],
            )

        if acao == "buscar":
            equipes_encontradas = buscar_equipes_globais_por_nome(nome_busca)

            if not equipes_encontradas:
                flash("Nenhuma equipe encontrada com esse nome. Você pode criar uma nova equipe.", "aviso")

            return render_template(
                "nova_equipe.html",
                competicao=competicao,
                nome_busca=nome_busca,
                equipes_encontradas=equipes_encontradas,
            )

        if acao == "criar":
            credenciais = criar_nova_equipe_com_credenciais(nome_busca, competicao["nome"])

            if not credenciais:
                flash("Não foi possível criar a equipe.", "erro")
                return render_template(
                    "nova_equipe.html",
                    competicao=competicao,
                    nome_busca=nome_busca,
                    equipes_encontradas=buscar_equipes_globais_por_nome(nome_busca),
                )

            session["credenciais_nova_equipe"] = {
                "nome": credenciais.get("nome") or nome_busca,
                "login": credenciais["login"],
                "senha": credenciais["senha"],
                "ja_existia": False,
                "ja_vinculada": False,
            }

            flash("Nova equipe criada com sucesso. A equipe completará cidade, responsável e telefone no primeiro login.", "sucesso")
            return redirect(url_for("equipes.listar_equipes_view"))

        # Compatibilidade: se algum botão antigo postar sem acao, faz busca.
        equipes_encontradas = buscar_equipes_globais_por_nome(nome_busca)
        return render_template(
            "nova_equipe.html",
            competicao=competicao,
            nome_busca=nome_busca,
            equipes_encontradas=equipes_encontradas,
        )

    return render_template(
        "nova_equipe.html",
        competicao=competicao,
        nome_busca=nome_busca,
        equipes_encontradas=equipes_encontradas,
    )


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


@equipes_bp.route("/equipes/<path:nome>/excluir", methods=["POST"])
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
# EQUIPE - PERFIL GLOBAL
# =========================

@equipes_bp.route("/minha-equipe/escudo", methods=["POST"])
@exigir_perfil("equipe")
def atualizar_escudo_equipe_view():
    usuario = session.get("usuario")
    if not usuario:
        flash("Sessão expirada. Faça login novamente.", "erro")
        return redirect(url_for("auth.login"))

    remover = request.form.get("remover_escudo") == "1"

    if remover:
        ok = atualizar_escudo_equipe_por_login(usuario, "")
        flash("Escudo removido com sucesso." if ok else "Não foi possível remover o escudo.", "sucesso" if ok else "erro")
        return redirect(request.referrer or url_for("equipes.painel_equipe_inicio_view"))

    escudo_url, erro = _salvar_upload_escudo(request.files.get("escudo"), usuario)
    if erro:
        flash(erro, "erro")
        return redirect(request.referrer or url_for("equipes.painel_equipe_inicio_view"))

    ok = atualizar_escudo_equipe_por_login(usuario, escudo_url)
    flash("Escudo atualizado com sucesso." if ok else "Não foi possível salvar o escudo.", "sucesso" if ok else "erro")
    return redirect(request.referrer or url_for("equipes.painel_equipe_inicio_view"))


@equipes_bp.route("/perfil-equipe", methods=["GET", "POST"])
@exigir_perfil("equipe")
def perfil_equipe_view():
    usuario = session.get("usuario")
    equipe = buscar_equipe_por_login(usuario, None)

    if not equipe:
        flash("Equipe não encontrada. Faça login novamente.", "erro")
        return redirect(url_for("auth.logout"))

    # Essa tela continua existindo para o PRIMEIRO ACESSO da equipe.
    # Depois que cidade, responsável e telefone já estiverem preenchidos,
    # ela não fica mais como uma página normal do menu; Minha Conta passa
    # a ser a tela única para dados, login e escudo.
    if request.method == "GET" and not perfil_equipe_incompleto_por_login(usuario):
        return redirect("/minha-conta")

    if request.method == "POST":
        cidade = request.form.get("cidade", "").strip()
        responsavel = request.form.get("responsavel", "").strip()
        telefone = request.form.get("telefone", "").strip()
        email = request.form.get("email", "").strip()
        instagram = request.form.get("instagram", "").strip()

        if not cidade or not responsavel or not telefone:
            flash("Preencha cidade, responsável e telefone para continuar.", "erro")
            equipe = dict(equipe)
            equipe["cidade"] = cidade
            equipe["responsavel"] = responsavel
            equipe["telefone"] = telefone
            equipe["email"] = email
            equipe["instagram"] = instagram
            return render_template("perfil_equipe.html", equipe=equipe, escudo_padrao=escudo_padrao_equipe())

        salvar_perfil_equipe_por_login(
            usuario,
            cidade,
            responsavel,
            telefone,
            email,
            instagram,
        )

        flash("Perfil da equipe atualizado com sucesso.", "sucesso")
        return redirect(url_for("equipes.painel_equipe_inicio_view"))

    return render_template("perfil_equipe.html", equipe=equipe, escudo_padrao=escudo_padrao_equipe())


# =========================
# EQUIPE - MINHA EQUIPE
# =========================
@equipes_bp.route("/minha-equipe", methods=["GET", "POST"])
@exigir_perfil("equipe")
def minha_equipe():
    usuario = session.get("usuario")
    equipe = _equipe_logada_com_competicao()

    if not equipe:
        flash("Equipe não encontrada ou sem competição selecionada.", "erro")
        return redirect(url_for("auth.logout"))

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
            equipe = _equipe_logada_com_competicao()

    return render_template(
        "minha_equipe.html",
        equipe=equipe,
        erro=erro,
        sucesso=sucesso,
        escudo_padrao=escudo_padrao_equipe(),
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


def _escudo_padrao_url_equipe():
    return escudo_padrao_equipe() or "/static/img/escudo_padrao.svg"


def _buscar_escudo_equipe_mapa(mapa_escudos, nome_equipe):
    nome = str(nome_equipe or "").strip()
    if not nome:
        return _escudo_padrao_url_equipe()

    return (
        (mapa_escudos or {}).get(nome)
        or (mapa_escudos or {}).get(nome.lower())
        or (mapa_escudos or {}).get(nome.upper())
        or _escudo_padrao_url_equipe()
    )


def _montar_grupos_classificacao_equipe(nome_competicao):
    grupos = []

    for grupo in listar_grupos(nome_competicao) or []:
        grupos.append({
            "grupo": grupo,
            "equipes": listar_equipes_por_grupo(grupo["id"]) or [],
        })

    return grupos


def _montar_classificacao_para_equipe(nome_competicao, partidas_preparadas, mapa_escudos):
    competicao = buscar_competicao_por_nome(nome_competicao) or {"nome": nome_competicao}
    grupos = _montar_grupos_classificacao_equipe(nome_competicao)

    if not grupos:
        return {
            "competicao": competicao,
            "grupos": [],
            "classificacao": {},
            "criterios_classificacao": [],
            "colunas_classificacao": [],
        }

    classificacao = _calcular_classificacao(
        partidas_preparadas,
        grupos,
        competicao,
        mapa_escudos,
    )

    regras_classificacao = _obter_regras_classificacao(competicao)
    criterios_classificacao = _criterios_efetivos_ate_sorteio(
        regras_classificacao.get("criterios")
    )
    colunas_classificacao = _colunas_classificacao_por_criterios(criterios_classificacao)

    return {
        "competicao": competicao,
        "grupos": grupos,
        "classificacao": classificacao,
        "criterios_classificacao": criterios_classificacao,
        "colunas_classificacao": colunas_classificacao,
    }


def _preparar_partidas_para_equipe(equipe, competicao=None, mapa_escudos=None):
    nome_equipe = (equipe.get("nome") or "").strip()
    nome_competicao = (equipe.get("competicao") or "").strip()

    if not nome_equipe or not nome_competicao:
        return []

    competicao = competicao or buscar_competicao_por_nome(nome_competicao) or {"nome": nome_competicao}
    equipes_competicao = listar_equipes_da_competicao(nome_competicao) or []
    mapa_escudos = mapa_escudos or _mapa_escudos_equipes(equipes_competicao)

    partidas = listar_partidas(nome_competicao)
    resultado = []

    for p in partidas:
        partida = aplicar_placar_exibicao_partida(dict(p or {}), competicao)
        equipe_a = (partida.get("equipe_a") or "").strip()
        equipe_b = (partida.get("equipe_b") or "").strip()
        fase = (partida.get("fase") or "grupos").strip().lower()

        minha_partida = (
            equipe_a.lower() == nome_equipe.lower()
            or equipe_b.lower() == nome_equipe.lower()
        )

        partida["fase_label"] = _fase_label_partida_equipe(fase)
        partida["fase_ordem"] = _ordem_fase_partida_equipe(fase)
        partida["status_visual"] = _status_visual_partida_equipe(partida)
        partida["ao_vivo"] = _partida_ao_vivo_equipe(partida)
        partida["finalizada"] = _partida_finalizada_equipe(partida)
        partida["parciais_formatadas"] = _parciais_partida_equipe(partida)
        partida["minha_partida"] = minha_partida
        partida["escudo_a"] = _buscar_escudo_equipe_mapa(mapa_escudos, equipe_a)
        partida["escudo_b"] = _buscar_escudo_equipe_mapa(mapa_escudos, equipe_b)
        partida["placar_ao_vivo_a"] = int(partida.get("pontos_a") or partida.get("placar_a") or 0)
        partida["placar_ao_vivo_b"] = int(partida.get("pontos_b") or partida.get("placar_b") or 0)

        if partida.get("ao_vivo") and partida.get("set_unico"):
            partida["placar_exibicao_a"] = partida["placar_ao_vivo_a"]
            partida["placar_exibicao_b"] = partida["placar_ao_vivo_b"]
            partida["placar_exibicao"] = f'{partida["placar_exibicao_a"]} x {partida["placar_exibicao_b"]}'

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


@equipes_bp.route("/painel-equipe/selecionar-competicao", methods=["POST"])
@exigir_perfil("equipe")
def selecionar_competicao_equipe_view():
    usuario = session.get("usuario")
    competicao = request.form.get("competicao", "").strip()

    competicoes = listar_competicoes_da_equipe_por_login(usuario) or []
    nomes_liberados = {
        (c.get("nome") or c.get("competicao") or c.get("nome_competicao") or "").strip()
        for c in competicoes
    }

    if not competicao or competicao not in nomes_liberados:
        flash("Competição inválida para esta equipe.", "erro")
        return redirect(url_for("equipes.painel_equipe_inicio_view"))

    session["competicao_equipe_atual"] = competicao

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE usuarios
                    SET competicao_vinculada = %s
                    WHERE login = %s
                      AND perfil = 'equipe'
                """, (competicao, usuario))
            conn.commit()
    except Exception as e:
        print("AVISO selecionar_competicao_equipe:", e)

    flash("Competição selecionada com sucesso.", "sucesso")
    return redirect(url_for("equipes.painel_equipe_inicio_view"))


@equipes_bp.route("/minhas-partidas")
@exigir_perfil("equipe")
def minhas_partidas_view():
    usuario = session.get("usuario")
    equipe = _equipe_logada_com_competicao()

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    nome_competicao = (equipe.get("competicao") or "").strip()
    competicao = buscar_competicao_por_nome(nome_competicao) or {"nome": nome_competicao}
    equipes_competicao = listar_equipes_da_competicao(nome_competicao) or []
    mapa_escudos = _mapa_escudos_equipes(equipes_competicao)
    partidas = _preparar_partidas_para_equipe(equipe, competicao, mapa_escudos)
    dados_classificacao = _montar_classificacao_para_equipe(
        nome_competicao,
        partidas,
        mapa_escudos,
    )

    return render_template(
        "minhas_partidas.html",
        equipe=equipe,
        partidas=partidas,
        competicao=dados_classificacao.get("competicao") or competicao,
        grupos=dados_classificacao.get("grupos") or [],
        classificacao=dados_classificacao.get("classificacao") or {},
        criterios_classificacao=dados_classificacao.get("criterios_classificacao") or [],
        colunas_classificacao=dados_classificacao.get("colunas_classificacao") or [],
    )


# =========================
# EQUIPE - INÍCIO / DASHBOARD
# =========================
def _proxima_partida_da_equipe(partidas):
    """
    Pega a primeira partida da própria equipe que ainda não foi finalizada.
    Se não houver jogo próprio pendente, retorna None.
    """
    for partida in partidas:
        if partida.get("minha_partida") and not partida.get("finalizada"):
            return partida
    return None


@equipes_bp.route("/painel-equipe/inicio")
@exigir_perfil("equipe")
def painel_equipe_inicio_view():
    usuario = session.get("usuario")

    if perfil_equipe_incompleto_por_login(usuario):
        return redirect(url_for("equipes.perfil_equipe_view"))

    competicoes_equipe = listar_competicoes_da_equipe_por_login(usuario) or []

    if not competicoes_equipe:
        equipe_global = buscar_equipe_por_login(usuario, None)

        return render_template(
            "painel_equipe_competicoes.html",
            equipe=equipe_global or {
                "nome": session.get("equipe") or session.get("nome") or usuario,
                "login": usuario,
            },
            competicoes=[],
            mensagem="Sua equipe ainda não está vinculada a nenhuma competição.",
        )

    if not session.get("competicao_equipe_atual"):
        return render_template(
            "painel_equipe_competicoes.html",
            equipe=buscar_equipe_por_login(usuario, None),
            competicoes=competicoes_equipe,
            mensagem=None,
        )

    equipe = _equipe_logada_com_competicao()

    if not equipe:
        session.pop("competicao_equipe_atual", None)
        flash("Não foi possível carregar essa competição para a equipe. Escolha novamente.", "erro")
        return redirect(url_for("equipes.painel_equipe_inicio_view"))

    atletas = listar_atletas_da_equipe(equipe["nome"], equipe["competicao"])
    controle_inscricao = controle_inscricao_para_equipe(equipe["competicao"], equipe["nome"])
    partidas = _preparar_partidas_para_equipe(equipe)

    total_atletas = len(atletas)
    atletas_aprovados = [
        a for a in atletas
        if (a.get("status") or "").strip().lower() == "aprovado"
    ]
    atletas_pendentes = [
        a for a in atletas
        if (a.get("status") or "").strip().lower()
        in {"", "pendente", "aguardando", "em análise", "em analise", "em_analise"}
    ]
    atletas_reprovados = [
        a for a in atletas
        if (a.get("status") or "").strip().lower() == "reprovado"
    ]

    limite_atletas = 12
    try:
        if controle_inscricao and controle_inscricao.get("limite_atletas"):
            limite_atletas = int(controle_inscricao.get("limite_atletas"))
    except Exception:
        limite_atletas = 12

    percentual_atletas = 0
    if limite_atletas > 0:
        percentual_atletas = min(100, round((total_atletas / limite_atletas) * 100))

    minhas_partidas = [p for p in partidas if p.get("minha_partida")]
    proxima_partida = _proxima_partida_da_equipe(partidas)

    status_equipe = "Equipe em andamento"
    status_classe = "tag-info"

    if total_atletas >= limite_atletas and len(atletas_pendentes) == 0 and len(atletas_reprovados) == 0:
        status_equipe = "Equipe completa"
        status_classe = "tag-aprovado"
    elif len(atletas_pendentes) > 0:
        status_equipe = "Aguardando conferência"
        status_classe = "tag-pendente"
    elif len(atletas_reprovados) > 0:
        status_equipe = "Possui atleta reprovado"
        status_classe = "tag-reprovado"

    return render_template(
        "painel_equipe_inicio.html",
        equipe=equipe,
        competicoes_equipe=competicoes_equipe,
        atletas=atletas,
        total_atletas=total_atletas,
        limite_atletas=limite_atletas,
        percentual_atletas=percentual_atletas,
        atletas_aprovados=len(atletas_aprovados),
        atletas_pendentes=len(atletas_pendentes),
        atletas_reprovados=len(atletas_reprovados),
        controle_inscricao=controle_inscricao,
        partidas=partidas,
        minhas_partidas=minhas_partidas,
        proxima_partida=proxima_partida,
        status_equipe=status_equipe,
        status_classe=status_classe,
        escudo_padrao=escudo_padrao_equipe(),
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
    equipe = _equipe_logada_com_competicao()

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    erro = None
    sucesso = None

    if request.method == "POST":
        acao = request.form.get("acao", "").strip()

        if acao == "salvar_numero":
            ok, msg = atualizar_numero_atleta(
                request.form.get("id_atleta"),
                request.form.get("numero", "").strip()
            )
            flash(msg, "sucesso" if ok else "erro")
            return redirect(url_for("equipes.meus_atletas_view"))

    contexto = _montar_contexto_atletas_equipe(equipe, erro=erro, sucesso=sucesso, modo_tela="lista")
    return render_template("meus_atletas.html", **contexto)


@equipes_bp.route("/cadastrar-atleta", methods=["GET", "POST"])
@exigir_perfil("equipe")
def cadastrar_atleta_pagina_view():
    usuario = session.get("usuario")
    equipe = _equipe_logada_com_competicao()

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    erro = None
    atleta_encontrado = None
    cpf_busca = ""

    if request.method == "POST":
        acao = (request.form.get("acao") or "cadastrar").strip()
        cpf_busca = request.form.get("cpf", "").strip()

        if acao == "buscar_cpf":
            if not cpf_busca:
                erro = "Informe o CPF para buscar o atleta."
            else:
                atleta_encontrado = buscar_atleta_global_por_cpf(cpf_busca)
                if atleta_encontrado:
                    flash("Atleta encontrado no banco. Confira os dados e informe o número para vincular nesta competição.", "sucesso")
                else:
                    flash("CPF não encontrado no banco. Complete os dados para cadastrar um novo atleta.", "aviso")

        else:
            # Deixa a função cadastrar_atleta validar CPF, prazo, limite e número.
            # Se o CPF já existir em outra competição, ela reaproveita os dados enviados
            # e cria um novo registro apenas para a competição atual.
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

            erro = msg or "Não foi possível cadastrar o atleta. Verifique CPF duplicado nesta competição, número repetido, limite de atletas ou bloqueio de inscrição."

            # Em caso de erro, tenta manter os dados na tela.
            atleta_encontrado = {
                "nome": request.form.get("nome", "").strip(),
                "cpf": request.form.get("cpf", "").strip(),
                "data_nascimento": request.form.get("data_nascimento", "").strip(),
            }

    contexto = _montar_contexto_atletas_equipe(
        equipe,
        erro=erro,
        sucesso=None,
        modo_tela="cadastro",
        carregar_atletas=False
    )
    contexto["atleta_encontrado"] = atleta_encontrado
    contexto["cpf_busca"] = cpf_busca
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


@equipes_bp.route("/atletas/<int:id_atleta>/editar", methods=["POST"])
@exigir_perfil("equipe")
def editar_atleta_view(id_atleta):
    equipe = _equipe_logada_com_competicao()

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    controle_inscricao = controle_inscricao_para_equipe(equipe["competicao"], equipe["nome"])
    if not controle_inscricao.get("aberta", True):
        flash(controle_inscricao.get("motivo") or "Inscrição bloqueada.", "erro")
        return redirect(url_for("equipes.meus_atletas_view"))

    ok, msg = atualizar_atleta_equipe(
        id_atleta=id_atleta,
        equipe=equipe["nome"],
        competicao=equipe["competicao"],
        nome=request.form.get("nome", "").strip(),
        cpf=request.form.get("cpf", "").strip(),
        data_nascimento=request.form.get("data_nascimento", "").strip(),
    )
    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("equipes.meus_atletas_view"))


@equipes_bp.route("/atletas/<int:id_atleta>/excluir", methods=["POST"])
@exigir_perfil("equipe")
def excluir_atleta_view(id_atleta):
    usuario = session.get("usuario")
    equipe = _equipe_logada_com_competicao()

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


@equipes_bp.route("/atletas/aprovar-todos-pendentes", methods=["POST"])
@exigir_perfil("organizador")
def aprovar_todos_pendentes():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))
    if not competicao:
        flash("Nenhuma competição vinculada ao organizador.", "erro")
        return redirect(url_for("painel.inicio"))

    ok, msg = aprovar_todos_atletas_pendentes(competicao["nome"])
    flash(msg, "sucesso" if ok else "erro")
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
    equipe = _equipe_logada_com_competicao()

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
    aprovacao_automatica = request.form.get("aprovacao_automatica_atletas") == "on"
    criar_campos_conferencia_atletas()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE competicoes
                SET conferencia_prazo = %s,
                    conferencia_link = %s,
                    aprovacao_automatica_atletas = %s
                WHERE nome = %s
            """, (prazo, link, aprovacao_automatica, competicao))
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