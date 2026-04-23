from flask import Blueprint, render_template, request, redirect, session, url_for, flash

from banco import (
    buscar_competicao_por_organizador,
    listar_equipes_da_competicao,
    criar_grupo,
    listar_grupos,
    adicionar_equipe_no_grupo,
    listar_equipes_por_grupo,
    criar_partida,
    listar_partidas,
    limpar_partidas,
    limpar_partidas_por_fase,
    remover_equipe_do_grupo,
    excluir_grupo as excluir_grupo_banco,
    competicao_esta_travada,
)

from routes.utils import exigir_perfil

tabela_bp = Blueprint("tabela", __name__)


# =========================================================
# HELPERS
# =========================================================
def _fases_disponiveis(competicao):
    formato_finais = (competicao.get("formato_finais") or "").strip().lower()

    tem_quartas = formato_finais == "quartas"
    tem_semifinais = formato_finais in {"quartas", "semifinal"}
    tem_finais = True

    return {
        "tem_quartas": tem_quartas,
        "tem_semifinais": tem_semifinais,
        "tem_finais": tem_finais,
    }


def _fase_subaba_para_banco(fase_subaba):
    mapa = {
        "classificatorias": "grupos",
        "quartas": "quartas",
        "semifinais": "semifinal",
        "finais": "final",
    }
    return mapa.get(fase_subaba, "grupos")


def _filtrar_partidas_por_fase(partidas, fase_subaba):
    if fase_subaba == "classificatorias":
        return [p for p in partidas if (p.get("fase") or "grupos") == "grupos"]

    if fase_subaba == "quartas":
        return [p for p in partidas if (p.get("fase") or "") == "quartas"]

    if fase_subaba == "semifinais":
        return [p for p in partidas if (p.get("fase") or "") in {"semifinal", "semifinais"}]

    if fase_subaba == "finais":
        return [p for p in partidas if (p.get("fase") or "") == "final"]

    return partidas


# =========================================================
# VISUALIZADOR PÚBLICO
# =========================================================
@tabela_bp.route("/visualizador/<competicao_nome>")
def visualizador_publico(competicao_nome):
    grupos_raw = listar_grupos(competicao_nome)
    partidas = listar_partidas(competicao_nome)

    grupos = []
    for g in grupos_raw:
        equipes_grupo = listar_equipes_por_grupo(g["id"])
        grupos.append({
            "grupo": g,
            "equipes": equipes_grupo
        })

    partidas_ordenadas = sorted(
        partidas,
        key=lambda p: (
            p.get("ordem") or 0,
            p.get("grupo") or "",
            p.get("equipe_a") or "",
            p.get("equipe_b") or "",
        )
    )

    # Mantido provisoriamente vazio só para não travar o módulo da tabela
    classificacao = {}

    return render_template(
        "visualizador_publico.html",
        competicao_nome=competicao_nome,
        grupos=grupos,
        classificacao=classificacao,
        partidas=partidas_ordenadas,
    )


# =========================================================
# TELA PRINCIPAL
# =========================================================
@tabela_bp.route("/tabela")
@exigir_perfil("organizador")
def tabela_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    aba = (request.args.get("aba") or "geracao").strip().lower()
    if aba not in {"geracao", "partidas", "classificacao", "visualizador"}:
        aba = "geracao"

    fase_subaba = (request.args.get("fase") or "classificatorias").strip().lower()
    if fase_subaba not in {"classificatorias", "quartas", "semifinais", "finais"}:
        fase_subaba = "classificatorias"

    grupos_raw = listar_grupos(competicao["nome"])
    equipes = listar_equipes_da_competicao(competicao["nome"])
    partidas = listar_partidas(competicao["nome"])
    partidas_fase = _filtrar_partidas_por_fase(partidas, fase_subaba)

    grupos = []
    for g in grupos_raw:
        equipes_grupo = listar_equipes_por_grupo(g["id"])
        grupos.append({
            "grupo": g,
            "equipes": equipes_grupo
        })

    fases = _fases_disponiveis(competicao)

    # Mantido provisoriamente vazio até religar a função de classificação no banco
    classificacao = {}

    return render_template(
        "tabela.html",
        competicao=competicao,
        grupos=grupos,
        equipes=equipes,
        partidas=partidas,
        partidas_fase=partidas_fase,
        classificacao=classificacao,
        aba_ativa=aba,
        fase_ativa=fase_subaba,
        competicao_travada=competicao_esta_travada(competicao["nome"]),
        **fases,
    )


# =========================================================
# CRIAR GRUPO
# =========================================================
@tabela_bp.route("/tabela/criar-grupo", methods=["POST"])
@exigir_perfil("organizador")
def criar_grupo_view():
    nome = request.form.get("nome", "").strip().upper()
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if not nome:
        flash("Informe o nome do grupo.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if competicao_esta_travada(competicao["nome"]):
        flash("A competição está travada. Não é possível criar grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    criar_grupo(nome, competicao["nome"])

    flash("Grupo criado com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# ADICIONAR EQUIPE AO GRUPO
# =========================================================
@tabela_bp.route("/tabela/adicionar-equipe", methods=["POST"])
@exigir_perfil("organizador")
def adicionar_equipe_grupo():
    grupo_id = request.form.get("grupo_id")
    equipe = request.form.get("equipe")
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if not grupo_id or not equipe:
        flash("Preencha todos os campos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if competicao_esta_travada(competicao["nome"]):
        flash("A competição está travada. Não é possível alterar grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    adicionar_equipe_no_grupo(grupo_id, equipe, competicao["nome"])

    flash("Equipe adicionada ao grupo.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# REMOVER EQUIPE DO GRUPO
# =========================================================
@tabela_bp.route("/tabela/remover-equipe-grupo", methods=["POST"])
@exigir_perfil("organizador")
def remover_equipe_grupo_view():
    grupo_id = request.form.get("grupo_id")
    equipe = request.form.get("equipe")
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if not grupo_id or not equipe:
        flash("Dados inválidos para remover equipe do grupo.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if competicao_esta_travada(competicao["nome"]):
        flash("A competição está travada. Não é possível alterar grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    remover_equipe_do_grupo(grupo_id, equipe, competicao["nome"])

    flash("Equipe removida do grupo.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# EXCLUIR GRUPO
# =========================================================
@tabela_bp.route("/tabela/excluir-grupo/<int:grupo_id>", methods=["POST"])
@exigir_perfil("organizador")
def excluir_grupo_view(grupo_id):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(competicao["nome"]):
        flash("A competição está travada. Não é possível excluir grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    excluir_grupo_banco(grupo_id, competicao["nome"])

    flash("Grupo excluído com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# LIMPEZA DE PARTIDAS
# =========================================================
@tabela_bp.route("/tabela/limpar", methods=["POST"])
@exigir_perfil("organizador")
def limpar_tabela():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(competicao["nome"]):
        flash("A competição está travada. Não é possível limpar a tabela.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    limpar_partidas(competicao["nome"])

    flash("Tabela limpa com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


@tabela_bp.route("/tabela/limpar-fase", methods=["POST"])
@exigir_perfil("organizador")
def limpar_fase_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    fase_banco = _fase_subaba_para_banco(fase_subaba)

    if competicao_esta_travada(competicao["nome"]):
        flash("A competição está travada. Não é possível limpar partidas desta fase.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    limpar_partidas_por_fase(competicao["nome"], fase_banco)

    flash("Partidas da fase removidas com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))


# =========================================================
# CRIAR PARTIDA MANUAL
# =========================================================
@tabela_bp.route("/tabela/nova-partida", methods=["POST"])
@exigir_perfil("organizador")
def nova_partida():
    grupo = request.form.get("grupo")
    equipe_a = request.form.get("equipe_a")
    equipe_b = request.form.get("equipe_b")
    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()

    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if not grupo or not equipe_a or not equipe_b:
        flash("Preencha todos os campos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    if competicao_esta_travada(competicao["nome"]):
        flash("A competição está travada. Não é possível criar novas partidas.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    partidas = listar_partidas(competicao["nome"])
    ordem = len(partidas) + 1
    fase_banco = _fase_subaba_para_banco(fase_subaba)

    criar_partida(
        competicao["nome"],
        grupo,
        equipe_a,
        equipe_b,
        ordem,
        fase=fase_banco,
    )

    flash("Partida criada com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))


# =========================================================
# GERAR JOGOS AUTOMÁTICOS
# =========================================================
@tabela_bp.route("/tabela/gerar-automatico", methods=["POST"])
@exigir_perfil("organizador")
def gerar_automatico():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if competicao_esta_travada(competicao["nome"]):
        flash("A competição está travada. Não é possível gerar jogos automaticamente.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))

    grupos_raw = listar_grupos(competicao["nome"])

    # limpa jogos antigos
    limpar_partidas_por_fase(competicao["nome"], "grupos")

    ordem = 1

    # ================================
    # FUNÇÃO ROUND ROBIN COM RODADAS
    # ================================
    def gerar_rodadas(equipes):
        times = equipes[:]

        if len(times) % 2 == 1:
            times.append(None)  # folga

        n = len(times)
        rodadas = []

        for _ in range(n - 1):
            rodada = []
            for i in range(n // 2):
                t1 = times[i]
                t2 = times[n - 1 - i]

                if t1 is not None and t2 is not None:
                    rodada.append((t1, t2))

            rodadas.append(rodada)

            # rotação (mantém o primeiro fixo)
            times = [times[0]] + [times[-1]] + times[1:-1]

        return rodadas

    # ================================
    # GERAR RODADAS POR GRUPO
    # ================================
    rodadas_por_grupo = {}

    for g in grupos_raw:
        equipes = listar_equipes_por_grupo(g["id"])
        nomes = [e["equipe"] for e in equipes]

        rodadas = gerar_rodadas(nomes)
        rodadas_por_grupo[g["nome"]] = rodadas

    # ================================
    # INTERCALAR RODADAS ENTRE GRUPOS
    # ================================
    max_rodadas = max(len(r) for r in rodadas_por_grupo.values())

    ultimo_times_usados = set()

    for rodada_index in range(max_rodadas):
        jogos_da_rodada = []

        # pega jogos da mesma rodada de todos grupos
        for grupo_nome, rodadas in rodadas_por_grupo.items():
            if rodada_index < len(rodadas):
                for jogo in rodadas[rodada_index]:
                    jogos_da_rodada.append((grupo_nome, jogo))

        # ================================
        # ORDENAR PARA EVITAR REPETIÇÃO
        # ================================
        jogos_ordenados = []

        while jogos_da_rodada:
            melhor_jogo = None

            for j in jogos_da_rodada:
                t1, t2 = j[1]

                if t1 not in ultimo_times_usados and t2 not in ultimo_times_usados:
                    melhor_jogo = j
                    break

            if not melhor_jogo:
                melhor_jogo = jogos_da_rodada[0]

            jogos_ordenados.append(melhor_jogo)

            t1, t2 = melhor_jogo[1]
            ultimo_times_usados = {t1, t2}

            jogos_da_rodada.remove(melhor_jogo)

        # ================================
        # SALVAR NO BANCO
        # ================================
        for grupo_nome, (t1, t2) in jogos_ordenados:
            criar_partida(
                competicao["nome"],
                grupo_nome,
                t1,
                t2,
                ordem,
                fase="grupos",
                origem="automatica",
            )
            ordem += 1

    flash("Jogos gerados automaticamente com rodadas equilibradas.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))