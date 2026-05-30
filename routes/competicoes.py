from flask import Blueprint, render_template, session, redirect, url_for, request, flash

from banco import (
    listar_competicoes,
    buscar_competicao_por_organizador,
    competicao_existe,
    criar_competicao_com_organizador,
    excluir_competicao,
    atualizar_dados_competicao,
    atualizar_estrutura_competicao,
    atualizar_regras_jogo,
    atualizar_pontuacao_desempate,
    redefinir_senha_organizador,
    competicao_esta_travada,
    destravar_competicao,
    listar_quadras_competicao,
    garantir_quadras_competicao,
    salvar_quadras_competicao,
    buscar_configuracao_avancada_competicao,
    atualizar_configuracao_avancada_competicao,
    inicializar_configuracao_avancada_competicao,
)

from routes.utils import exigir_perfil, perfil_atual

competicoes_bp = Blueprint("competicoes", __name__)


def _competicao_do_organizador_logado():
    usuario = session.get("usuario")
    if not usuario:
        return None
    return buscar_competicao_por_organizador(usuario)


def _to_int(valor, padrao=0, minimo=None):
    try:
        numero = int(valor)
    except (TypeError, ValueError):
        numero = padrao

    if minimo is not None and numero < minimo:
        numero = minimo

    return numero


def _to_int_ou_none(valor):
    try:
        numero = int(valor)
        return numero if numero > 0 else None
    except (TypeError, ValueError):
        return None


CRITERIOS_CLASSIFICACAO_PADRAO = [
    "pontos",
    "vitorias",
    "saldo_sets",
    "sets_average",
    "saldo_pontos",
    "pontos_average",
    "confronto_direto",
    "sorteio",
]

CRITERIOS_CLASSIFICACAO_PERMITIDOS = {
    "pontos",
    "vitorias",
    "sets_average",
    "pontos_average",
    "saldo_sets",
    "saldo_pontos",
    "sets_pro",
    "sets_contra",
    "pontos_pro",
    "pontos_contra",
    "confronto_direto",
    "coef_sets",
    "coef_pontos",
    "fair_play",
    "menor_wo",
    "sorteio",
}


def _normalizar_criterios_classificacao_form(valor):
    criterios = []
    vistos = set()

    for item in str(valor or "").split(","):
        criterio = item.strip().lower().replace("-", "_").replace(" ", "_")
        if criterio in CRITERIOS_CLASSIFICACAO_PERMITIDOS and criterio not in vistos:
            criterios.append(criterio)
            vistos.add(criterio)

    if not criterios:
        criterios = list(CRITERIOS_CLASSIFICACAO_PADRAO)

    # REGRA OFICIAL:
    # Sorteio é sempre o último critério efetivo.
    # Se o organizador colocar qualquer critério abaixo dele na tela,
    # esses critérios são ignorados/removidos no salvamento.
    if "sorteio" in criterios:
        indice_sorteio = criterios.index("sorteio")
        criterios = criterios[:indice_sorteio + 1]

    return ",".join(criterios)


def _coletar_quadras_form():
    ids = request.form.getlist("quadra_id[]")
    nomes = request.form.getlist("quadra_nome[]")
    locais = request.form.getlist("quadra_local[]")
    ordens = request.form.getlist("quadra_ordem[]")

    quadras = []
    total = max(len(nomes), len(locais), len(ordens), len(ids))

    for idx in range(total):
        quadra_id = _to_int_ou_none(ids[idx] if idx < len(ids) else None)
        nome = (nomes[idx] if idx < len(nomes) else "").strip()
        local = (locais[idx] if idx < len(locais) else "").strip()
        ordem = _to_int(ordens[idx] if idx < len(ordens) else None, padrao=idx + 1, minimo=1)

        # Uma linha totalmente vazia não entra. Isso permite remover quadras novas sem salvar sujeira.
        if not nome and not local and not quadra_id:
            continue

        if not nome:
            nome = f"Quadra {ordem}"

        chave_ativa = f"quadra_ativa_{quadra_id}" if quadra_id else f"quadra_ativa_nova_{idx}"
        ativa = request.form.get(chave_ativa) == "on"

        quadras.append({
            "id": quadra_id,
            "nome": nome,
            "local": local,
            "ordem": ordem,
            "ativa": ativa,
        })

    return sorted(quadras, key=lambda q: q.get("ordem") or 9999)


@competicoes_bp.route("/competicoes")
@exigir_perfil("superadmin", "organizador")
def listar_competicoes_view():
    perfil = perfil_atual()

    if perfil == "superadmin":
        competicoes = listar_competicoes()
        credenciais = session.pop("credenciais_novas", None)
        senha_redefinida = session.pop("senha_redefinida_organizador", None)

        return render_template(
            "competicoes.html",
            competicoes=competicoes,
            credenciais=credenciais,
            senha_redefinida=senha_redefinida,
        )

    if perfil == "organizador":
        competicao = _competicao_do_organizador_logado()

        if not competicao:
            flash("Nenhuma competição vinculada a este organizador.", "erro")
            return redirect(url_for("painel.inicio"))

        quadras = garantir_quadras_competicao(
            competicao["nome"],
            _to_int(competicao.get("qtd_quadras"), padrao=1, minimo=1),
        )

        inicializar_configuracao_avancada_competicao(competicao["nome"])
        config = buscar_configuracao_avancada_competicao(competicao["nome"]) or {}
        fases = config.get("fases_config") or {}

        return render_template(
            "editar_competicao.html",
            competicao=competicao,
            quadras=quadras,
            config=config,
            fases=fases,
            competicao_travada=competicao_esta_travada(competicao["nome"]),
        )

    return redirect(url_for("painel.inicio"))


@competicoes_bp.route("/competicoes/nova", methods=["GET", "POST"])
@exigir_perfil("superadmin")
def nova_competicao():
    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        data = request.form.get("data", "").strip()
        status = request.form.get("status", "").strip() or "Em preparação"
        modo_operacao = request.form.get("modo_operacao", "simples").strip() or "simples"

        tempos_por_set = _to_int(request.form.get("tempos_por_set"), padrao=2, minimo=0)
        substituicoes_por_set = _to_int(request.form.get("substituicoes_por_set"), padrao=6, minimo=0)

        if not nome:
            flash("Informe o nome da competição.", "erro")
            return render_template("nova_competicao.html")

        if competicao_existe(nome):
            flash("Já existe uma competição com esse nome.", "erro")
            return render_template("nova_competicao.html")

        credenciais = criar_competicao_com_organizador(
            nome,
            data,
            status,
            modo_operacao=modo_operacao,
            tempos_por_set=tempos_por_set,
            substituicoes_por_set=substituicoes_por_set,
        )

        session["credenciais_novas"] = {
            "competicao": nome,
            "login": credenciais["login"],
            "senha": credenciais["senha"],
        }

        flash("Competição criada com sucesso.", "sucesso")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    return render_template("nova_competicao.html")


@competicoes_bp.route("/competicoes/salvar", methods=["POST"])
@exigir_perfil("organizador")
def salvar_competicao_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. Os dados não podem mais ser alterados.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    nome_atual = comp["nome"]

    dados = {
        "nome": request.form.get("nome", "").strip(),
        "cidade": request.form.get("cidade", "").strip(),
        "data": request.form.get("data", "").strip(),
        "ginasio": request.form.get("ginasio", "").strip(),
        "categoria": request.form.get("categoria", "").strip(),
        "sexo": request.form.get("sexo", "").strip(),
        "divisao": request.form.get("divisao", "").strip(),
        "status": request.form.get("status", "").strip() or comp.get("status", "Em preparação"),
    }

    if not dados["nome"]:
        flash("Informe o nome da competição.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    if dados["nome"] != nome_atual and competicao_existe(dados["nome"]):
        flash("Já existe uma competição com esse nome.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    atualizar_dados_competicao(nome_atual, dados)

    session["competicao"] = dados["nome"]

    flash("Dados da competição salvos com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/<nome>/excluir", methods=["POST"])
@exigir_perfil("superadmin")
def excluir_competicao_view(nome):
    confirmacao = (request.form.get("confirmacao_exclusao") or "").strip().upper()

    if confirmacao and confirmacao != "EXCLUIR":
        flash("Confirmação inválida. Digite EXCLUIR para confirmar a exclusão.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    try:
        sucesso = excluir_competicao(nome)

        if sucesso:
            flash(
                "Competição excluída com sucesso. SUPERADMIN foi preservado e os apontadores foram mantidos sem vínculo com a competição removida.",
                "sucesso",
            )
        else:
            flash("Não foi possível excluir a competição.", "erro")

    except Exception as e:
        flash(f"Erro ao excluir competição: {str(e)}", "erro")

    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/<nome>/resetar-senha", methods=["POST"])
@exigir_perfil("superadmin")
def resetar_senha_organizador_view(nome):
    competicoes = listar_competicoes()
    comp = next((c for c in competicoes if c["nome"] == nome), None)

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    resultado = redefinir_senha_organizador(comp["organizador_login"])

    session["senha_redefinida_organizador"] = {
        "competicao": nome,
        "login": resultado["login"],
        "senha": resultado["senha"],
    }

    flash("Senha do organizador redefinida com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/regras", methods=["GET", "POST"])
@exigir_perfil("organizador")
def salvar_regras_jogo_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if request.method == "GET":
        return redirect(url_for("competicoes.listar_competicoes_view"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. As regras do jogo não podem mais ser alteradas.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    dados = {
        "sets_tipo": request.form.get("sets_tipo", "melhor_de_3"),
        "pontos_set": _to_int(request.form.get("pontos_set"), padrao=25, minimo=1),
        "tem_tiebreak": request.form.get("tem_tiebreak") == "on",
        "pontos_tiebreak": _to_int(request.form.get("pontos_tiebreak"), padrao=15, minimo=1),
        "diferenca_minima": _to_int(request.form.get("diferenca_minima"), padrao=2, minimo=1),
        "tempos_por_set": _to_int(request.form.get("tempos_por_set"), padrao=2, minimo=0),
        "substituicoes_por_set": _to_int(request.form.get("substituicoes_por_set"), padrao=6, minimo=0),
    }

    atualizar_regras_jogo(comp["nome"], dados)

    flash("Regras do jogo salvas.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/estrutura", methods=["POST"])
@exigir_perfil("organizador")
def salvar_estrutura_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. A estrutura não pode mais ser alterada.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    data_limite_inscricao = request.form.get("data_limite_inscricao", "").strip()
    hora_limite_inscricao = request.form.get("hora_limite_inscricao", "").strip()

    dados = {
        "qtd_equipes": _to_int(request.form.get("qtd_equipes"), padrao=0, minimo=0),
        "tem_grupos": request.form.get("tem_grupos") == "on",
        "qtd_grupos": _to_int(request.form.get("qtd_grupos"), padrao=0, minimo=0),
        "data_limite_inscricao": data_limite_inscricao,
        "hora_limite_inscricao": hora_limite_inscricao,
        "limite_atletas": _to_int(request.form.get("limite_atletas"), padrao=0, minimo=0),
        "permitir_edicao_pos_prazo": request.form.get("permitir_edicao_pos_prazo") == "on",
        "bloquear_apos_inicio": not bool(data_limite_inscricao),
    }

    atualizar_estrutura_competicao(comp["nome"], dados)

    flash("Estrutura da competição salva com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view", tab="estrutura"))



@competicoes_bp.route("/competicoes/quadras", methods=["POST"])
@exigir_perfil("organizador")
def salvar_quadras_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. As quadras não podem mais ser alteradas.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    quadras = _coletar_quadras_form()

    if not quadras:
        flash("Cadastre pelo menos uma quadra ativa para a competição.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    qtd_quadras = len(quadras)
    salvar_quadras_competicao(comp["nome"], quadras)
    atualizar_estrutura_competicao(comp["nome"], {"qtd_quadras": qtd_quadras})

    flash("Quadras da competição salvas com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))


@competicoes_bp.route("/competicoes/pontuacao", methods=["POST"])
@exigir_perfil("organizador")
def salvar_pontuacao_desempate_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. A pontuação e os critérios de classificação não podem mais ser alterados.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    sets_tipo = comp.get("sets_tipo", "melhor_de_3")
    dados = {}

    if sets_tipo == "set_unico":
        dados["vitoria_set_unico"] = _to_int(request.form.get("vitoria_set_unico"), padrao=2)
        dados["derrota_set_unico"] = _to_int(request.form.get("derrota_set_unico"), padrao=0)

    elif sets_tipo == "melhor_de_3":
        dados["vitoria_2x0"] = _to_int(request.form.get("vitoria_2x0"), padrao=3)
        dados["vitoria_2x1"] = _to_int(request.form.get("vitoria_2x1"), padrao=2)
        dados["derrota_1x2"] = _to_int(request.form.get("derrota_1x2"), padrao=1)
        dados["derrota_0x2"] = _to_int(request.form.get("derrota_0x2"), padrao=0)

    elif sets_tipo == "melhor_de_5":
        dados["vitoria_3x0"] = _to_int(request.form.get("vitoria_3x0"), padrao=3)
        dados["vitoria_3x1"] = _to_int(request.form.get("vitoria_3x1"), padrao=3)
        dados["vitoria_3x2"] = _to_int(request.form.get("vitoria_3x2"), padrao=2)
        dados["derrota_2x3"] = _to_int(request.form.get("derrota_2x3"), padrao=1)
        dados["derrota_1x3"] = _to_int(request.form.get("derrota_1x3"), padrao=0)
        dados["derrota_0x3"] = _to_int(request.form.get("derrota_0x3"), padrao=0)

    criterios_ordenados = _normalizar_criterios_classificacao_form(
        request.form.get("criterios_ordenados", "")
    )
    dados["criterios_desempate"] = criterios_ordenados

    atualizar_pontuacao_desempate(comp["nome"], dados)

    flash("Pontuação e critérios de classificação salvos.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))



@competicoes_bp.route("/competicoes/fases", methods=["POST"])
@exigir_perfil("organizador")
def salvar_fases_competicao_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. As fases não podem mais ser alteradas.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view", tab="fases"))

    tipo_confronto = request.form.get("tipo_confronto", "grupo_interno").strip() or "grupo_interno"
    tipo_classificacao = request.form.get("tipo_classificacao", "grupo").strip() or "grupo"
    cruzamentos_grupos = request.form.get("cruzamentos_grupos", "").strip()
    formato_finais = request.form.get("formato_finais", "quartas").strip() or "quartas"
    possui_bye = request.form.get("possui_bye", "nao").strip() == "sim"

    qtd_classificados = _to_int(request.form.get("qtd_classificados"), padrao=0, minimo=0)
    qtd_bye = _to_int(request.form.get("qtd_bye"), padrao=0, minimo=0)

    fase_nomes = request.form.getlist("fase_nome[]")
    fase_series = request.form.getlist("fase_serie[]")
    fase_tipos = request.form.getlist("fase_tipo[]")
    fase_origens = request.form.getlist("fase_origem[]")

    fases_personalizadas = []
    total = max(len(fase_nomes), len(fase_series), len(fase_tipos), len(fase_origens))

    for idx in range(total):
        nome = (fase_nomes[idx] if idx < len(fase_nomes) else "").strip()
        serie = (fase_series[idx] if idx < len(fase_series) else "geral").strip() or "geral"
        tipo = (fase_tipos[idx] if idx < len(fase_tipos) else "mata_mata").strip() or "mata_mata"
        origem = (fase_origens[idx] if idx < len(fase_origens) else "").strip()

        if not nome and not origem:
            continue

        fases_personalizadas.append({
            "nome": nome or f"Fase {idx + 1}",
            "serie": serie,
            "tipo": tipo,
            "origem": origem,
            "ordem": idx + 1,
        })

    config_atual = buscar_configuracao_avancada_competicao(comp["nome"]) or {}
    fases_config = config_atual.get("fases_config") or {}

    fases_config.update({
        "tipo_confronto": tipo_confronto,
        "tipo_classificacao": tipo_classificacao,
        "cruzamentos_grupos": cruzamentos_grupos,
        "formato_finais": formato_finais,
        "fases_personalizadas": fases_personalizadas,
    })

    atualizar_configuracao_avancada_competicao(
        nome_competicao=comp["nome"],
        tipo_classificacao=tipo_classificacao,
        qtd_classificados=qtd_classificados,
        formato_finais=formato_finais,
        possui_bye=possui_bye,
        qtd_bye=qtd_bye,
        fases_config=fases_config,
        tipo_confronto=tipo_confronto,
        cruzamentos_grupos=cruzamentos_grupos,
        data_limite_inscricao=comp.get("data_limite_inscricao"),
        hora_limite_inscricao=comp.get("hora_limite_inscricao"),
        bloquear_apos_inicio=comp.get("bloquear_apos_inicio", False),
    )

    flash("Classificação e avanço salvos com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view", tab="fases"))



def _bool_select_regras_avancadas(valor):
    valor = (valor or "padrao").strip().lower()
    if valor == "sim":
        return True
    if valor == "nao":
        return False
    return None


@competicoes_bp.route("/competicoes/regras-avancadas", methods=["POST"])
@exigir_perfil("organizador")
def salvar_regras_avancadas_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(comp["nome"]):
        flash("A competição está travada. As regras avançadas não podem mais ser alteradas.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view", tab="regras-avancadas"))

    config_atual = buscar_configuracao_avancada_competicao(comp["nome"]) or {}
    fases_config = config_atual.get("fases_config") or {}

    regras_grupos = {}
    for grupo in ["A", "B", "C", "D", "E", "F", "G", "H"]:
        ativo = request.form.get(f"grupo_{grupo}_ativo") == "on"
        sets_tipo = request.form.get(f"grupo_{grupo}_sets_tipo", "set_unico").strip() or "set_unico"
        pontos_set = _to_int(request.form.get(f"grupo_{grupo}_pontos_set"), padrao=0, minimo=0)
        tem_tiebreak = _bool_select_regras_avancadas(request.form.get(f"grupo_{grupo}_tem_tiebreak"))
        pontos_tiebreak = _to_int(request.form.get(f"grupo_{grupo}_pontos_tiebreak"), padrao=0, minimo=0)

        if ativo or pontos_set or pontos_tiebreak or tem_tiebreak is not None:
            item = {
                "ativo": ativo,
                "sets_tipo": sets_tipo,
            }
            if pontos_set:
                item["pontos_set"] = pontos_set
            if tem_tiebreak is not None:
                item["tem_tiebreak"] = tem_tiebreak
            if pontos_tiebreak:
                item["pontos_tiebreak"] = pontos_tiebreak
            regras_grupos[grupo] = item

    regras_fases = {}
    for fase_id in ["oitavas", "quartas", "semifinal", "final"]:
        ativo = request.form.get(f"fase_{fase_id}_ativo") == "on"
        sets_tipo = request.form.get(f"fase_{fase_id}_sets_tipo", "set_unico").strip() or "set_unico"
        pontos_set = _to_int(request.form.get(f"fase_{fase_id}_pontos_set"), padrao=0, minimo=0)
        tem_tiebreak = _bool_select_regras_avancadas(request.form.get(f"fase_{fase_id}_tem_tiebreak"))
        pontos_tiebreak = _to_int(request.form.get(f"fase_{fase_id}_pontos_tiebreak"), padrao=0, minimo=0)

        if ativo or pontos_set or pontos_tiebreak or tem_tiebreak is not None:
            item = {
                "ativo": ativo,
                "sets_tipo": sets_tipo,
            }
            if pontos_set:
                item["pontos_set"] = pontos_set
            if tem_tiebreak is not None:
                item["tem_tiebreak"] = tem_tiebreak
            if pontos_tiebreak:
                item["pontos_tiebreak"] = pontos_tiebreak
            regras_fases[fase_id] = item

    series = {}
    for serie_id in ["ouro", "prata", "bronze"]:
        ativa = request.form.get(f"serie_{serie_id}_ativa") == "on"
        faixa = request.form.get(f"serie_{serie_id}_faixa", "").strip()
        if ativa or faixa:
            series[serie_id] = {
                "ativa": ativa,
                "faixa": faixa,
            }

    repescagem = {
        "ativa": request.form.get("repescagem_ativa") == "on",
        "descricao": request.form.get("repescagem_descricao", "").strip(),
    }

    fase_nomes = request.form.getlist("fase_avancada_nome[]")
    fase_series = request.form.getlist("fase_avancada_serie[]")
    fase_tipos = request.form.getlist("fase_avancada_tipo[]")
    fase_origens = request.form.getlist("fase_avancada_origem[]")

    fases_personalizadas = []
    total = max(len(fase_nomes), len(fase_series), len(fase_tipos), len(fase_origens))

    for idx in range(total):
        nome = (fase_nomes[idx] if idx < len(fase_nomes) else "").strip()
        serie = (fase_series[idx] if idx < len(fase_series) else "geral").strip() or "geral"
        tipo = (fase_tipos[idx] if idx < len(fase_tipos) else "mata_mata").strip() or "mata_mata"
        origem = (fase_origens[idx] if idx < len(fase_origens) else "").strip()

        if not nome and not origem:
            continue

        fases_personalizadas.append({
            "nome": nome or f"Fase avançada {idx + 1}",
            "serie": serie,
            "tipo": tipo,
            "origem": origem,
            "ordem": idx + 1,
        })

    fases_config["regras_avancadas"] = {
        "grupos": regras_grupos,
        "fases": regras_fases,
        "series": series,
        "repescagem": repescagem,
        "fases_personalizadas": fases_personalizadas,
    }

    atualizar_configuracao_avancada_competicao(
        nome_competicao=comp["nome"],
        tipo_classificacao=config_atual.get("tipo_classificacao") or comp.get("tipo_classificacao") or "grupo",
        qtd_classificados=config_atual.get("qtd_classificados") or comp.get("qtd_classificados") or 0,
        formato_finais=config_atual.get("formato_finais") or comp.get("formato_finais") or "quartas",
        possui_bye=config_atual.get("possui_bye") if config_atual.get("possui_bye") is not None else comp.get("possui_bye", False),
        qtd_bye=config_atual.get("qtd_bye") or comp.get("qtd_bye") or 0,
        fases_config=fases_config,
        tipo_confronto=config_atual.get("tipo_confronto") or comp.get("tipo_confronto") or "grupo_interno",
        cruzamentos_grupos=config_atual.get("cruzamentos_grupos") or comp.get("cruzamentos_grupos") or "",
        data_limite_inscricao=comp.get("data_limite_inscricao"),
        hora_limite_inscricao=comp.get("hora_limite_inscricao"),
        bloquear_apos_inicio=comp.get("bloquear_apos_inicio", False),
    )

    flash("Regras avançadas salvas com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view", tab="regras-avancadas"))




@competicoes_bp.route("/competicoes/destravar", methods=["POST"])
@exigir_perfil("organizador")
def destravar_competicao_view():
    comp = _competicao_do_organizador_logado()

    if not comp:
        flash("Competição não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    confirmacao = (request.form.get("confirmacao_destravar") or "").strip().upper()
    if confirmacao != "DESTRAVAR":
        flash("Confirmação inválida. Digite DESTRAVAR para liberar a competição.", "erro")
        return redirect(url_for("competicoes.listar_competicoes_view"))

    destravar_competicao(comp["nome"])
    flash("Competição destravada com sucesso.", "sucesso")
    return redirect(url_for("competicoes.listar_competicoes_view"))