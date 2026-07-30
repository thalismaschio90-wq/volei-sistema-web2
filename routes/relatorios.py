from flask import Blueprint, render_template, request, session, redirect, url_for, flash, jsonify

from routes.utils import exigir_perfil
relatorios_bp = Blueprint("relatorios", __name__)

RELATORIOS_ORGANIZADOR = [
    {"id": "historico_jogos", "titulo": "Histórico de jogos", "descricao": "Lista todas as partidas finalizadas da competição."},
    {"id": "ordem_jogos", "titulo": "Ordem dos jogos", "descricao": "Ordem completa dos jogos, com opção de gerar todos ou filtrar por quadra e mostrando o grupo de cada partida."},
    {"id": "ranking_atletas", "titulo": "Ranking geral de atletas", "descricao": "Atletas ordenados por pontos, ataques, bloqueios e aces."},
    {"id": "maior_pontuador", "titulo": "Maior pontuador", "descricao": "Ranking dos atletas com mais pontos na competição."},
    {"id": "melhor_sacador", "titulo": "Melhor sacador", "descricao": "Ranking dos atletas com mais aces."},
    {"id": "melhor_bloqueador", "titulo": "Melhor bloqueador", "descricao": "Ranking dos atletas com mais pontos de bloqueio."},
    {"id": "melhor_atacante", "titulo": "Melhor atacante", "descricao": "Ranking dos atletas com mais pontos de ataque."},
    {"id": "ranking_equipes", "titulo": "Ranking das equipes", "descricao": "Vitórias, derrotas, sets pró, sets contra e saldo."},
    {"id": "estatisticas_competicao", "titulo": "Estatísticas gerais", "descricao": "Totais gerais de pontos, fundamentos, erros e faltas."},
    {"id": "fichas_inscricao", "titulo": "Fichas de inscrição", "descricao": "Relação das equipes inscritas com dados cadastrais e atletas."},
    {"id": "relatorio_equipe", "titulo": "Relatório por equipe", "descricao": "Resumo completo da equipe selecionada."},
    {"id": "relatorio_partida", "titulo": "Relatório da partida", "descricao": "Resumo completo da partida selecionada."},
    {"id": "historico_partida", "titulo": "Histórico da partida", "descricao": "Linha do tempo dos eventos salvos da partida."},
    {"id": "atletas_partida", "titulo": "Estatísticas dos atletas da partida", "descricao": "Scout dos atletas da partida selecionada."},
]

RELATORIOS_EQUIPE = [
    {"id": "historico_jogos", "titulo": "Histórico dos meus jogos", "descricao": "Partidas finalizadas da sua equipe."},
    {"id": "relatorio_equipe", "titulo": "Relatório da minha equipe", "descricao": "Resumo da sua equipe na competição."},
    {"id": "ranking_atletas", "titulo": "Ranking dos meus atletas", "descricao": "Atletas da sua equipe ordenados por desempenho."},
    {"id": "relatorio_partida", "titulo": "Relatório da partida", "descricao": "Resumo de uma partida da sua equipe."},
    {"id": "historico_partida", "titulo": "Histórico da partida", "descricao": "Eventos de uma partida da sua equipe."},
    {"id": "atletas_partida", "titulo": "Estatísticas dos atletas da partida", "descricao": "Scout dos atletas da partida selecionada."},
]


from services.relatorios.geracao import (
    _minha_competicao_e_equipe,
    _todas_partidas,
    _partida_por_id,
    _listar_equipes_inscritas,
    _listar_quadras_partidas,
    _montar_relatorio,
)
from services.relatorios.pdf import _pdf_response
from services.relatorios.cache import gerar_com_cache
from services.relatorios.fila import fila_habilitada, enfileirar_relatorio, consultar_tarefa

@relatorios_bp.route("/relatorios")
@exigir_perfil("organizador", "equipe")
def relatorios_home():
    competicao, equipe, erro = _minha_competicao_e_equipe()
    if erro:
        flash(erro, "erro")
        return redirect(url_for("painel.inicio"))

    competicao_nome = competicao.get("nome")
    perfil = session.get("perfil")
    equipe_nome = equipe.get("nome") if equipe else None
    partidas = _todas_partidas(competicao_nome, equipe_nome=equipe_nome, somente_finalizadas=False)

    equipes = []
    if perfil == "organizador":
        equipes = _listar_equipes_inscritas(competicao_nome)

    quadras = _listar_quadras_partidas(partidas)

    return render_template(
        "relatorios.html",
        competicao=competicao,
        equipe=equipe,
        perfil=perfil,
        relatorios=RELATORIOS_EQUIPE if perfil == "equipe" else RELATORIOS_ORGANIZADOR,
        partidas=partidas,
        equipes=equipes,
        quadras=quadras,
    )


@relatorios_bp.route("/relatorios/<tipo>")
@exigir_perfil("organizador", "equipe")
def relatorios_visualizar(tipo):
    competicao, equipe, erro = _minha_competicao_e_equipe()
    if erro:
        flash(erro, "erro")
        return redirect(url_for("painel.inicio"))

    quadra_filtro = request.args.get("quadra", "")

    # Guarda o filtro selecionado na visualização para o botão "Gerar PDF"
    # do template antigo continuar gerando o PDF da mesma quadra.
    # Sem isso, o preview aparece filtrado, mas o PDF volta para todas as quadras.
    if tipo == "ordem_jogos":
        session["relatorio_ordem_jogos_quadra"] = quadra_filtro

    resultado = gerar_com_cache(
        tipo,
        competicao.get("nome"),
        lambda: _montar_relatorio(
            tipo,
            competicao.get("nome"),
            equipe_logada=equipe,
            equipe_filtro=request.args.get("equipe"),
            partida_id=request.args.get("partida_id"),
            quadra_filtro=quadra_filtro,
        ),
        ignorar_cache=request.args.get("recalcular") == "1",
        perfil=session.get("perfil"),
        equipe_logada=(equipe or {}).get("nome"),
        equipe_filtro=request.args.get("equipe"),
        partida_id=request.args.get("partida_id"),
        quadra_filtro=quadra_filtro,
    )
    titulo, linhas = resultado.titulo, resultado.linhas

    return render_template(
        "relatorio_preview.html",
        titulo=titulo,
        linhas=linhas,
        tipo=tipo,
        equipe_filtro=request.args.get("equipe", ""),
        partida_id=request.args.get("partida_id", ""),
        quadra_filtro=quadra_filtro,
    )


@relatorios_bp.route("/relatorios/<tipo>/pdf")
@exigir_perfil("organizador", "equipe")
def relatorios_pdf(tipo):
    competicao, equipe, erro = _minha_competicao_e_equipe()
    if erro:
        flash(erro, "erro")
        return redirect(url_for("painel.inicio"))

    quadra_filtro = request.args.get("quadra", "")

    # Quando o PDF é gerado a partir do preview, alguns templates antigos
    # mandam só equipe/partida_id e perdem ?quadra=. Para ordem dos jogos,
    # reaproveita a última quadra escolhida na visualização.
    if tipo == "ordem_jogos" and not quadra_filtro:
        quadra_filtro = session.get("relatorio_ordem_jogos_quadra", "")

    resultado = gerar_com_cache(
        tipo,
        competicao.get("nome"),
        lambda: _montar_relatorio(
            tipo,
            competicao.get("nome"),
            equipe_logada=equipe,
            equipe_filtro=request.args.get("equipe"),
            partida_id=request.args.get("partida_id"),
            quadra_filtro=quadra_filtro,
        ),
        ignorar_cache=request.args.get("recalcular") == "1",
        perfil=session.get("perfil"),
        equipe_logada=(equipe or {}).get("nome"),
        equipe_filtro=request.args.get("equipe"),
        partida_id=request.args.get("partida_id"),
        quadra_filtro=quadra_filtro,
    )
    titulo, linhas = resultado.titulo, resultado.linhas
    resp = _pdf_response(titulo, linhas, competicao.get("nome"))
    if resp is None:
        return redirect(url_for("relatorios.relatorios_home"))
    return resp


@relatorios_bp.post("/relatorios/<tipo>/gerar-assincrono")
@exigir_perfil("organizador", "equipe")
def relatorios_gerar_assincrono(tipo):
    competicao, equipe, erro = _minha_competicao_e_equipe()
    if erro:
        return jsonify({"ok": False, "mensagem": erro}), 400
    if not fila_habilitada():
        return jsonify({"ok": False, "mensagem": "A geração assíncrona de relatórios não está habilitada."}), 503
    solicitacao = {
        "tipo": tipo,
        "competicao": competicao.get("nome"),
        "perfil": session.get("perfil") or "",
        "equipe_logada_nome": (equipe or {}).get("nome") or "",
        "equipe_filtro": request.form.get("equipe") or request.args.get("equipe") or "",
        "partida_id": request.form.get("partida_id") or request.args.get("partida_id") or "",
        "quadra_filtro": request.form.get("quadra") or request.args.get("quadra") or "",
    }
    try:
        tarefa_id = enfileirar_relatorio(solicitacao)
    except Exception as exc:
        return jsonify({"ok": False, "mensagem": str(exc)}), 503
    return jsonify({
        "ok": True,
        "tarefa_id": tarefa_id,
        "status_url": url_for("relatorios.relatorios_status_tarefa", tarefa_id=tarefa_id),
    }), 202


@relatorios_bp.get("/relatorios/tarefas/<tarefa_id>")
@exigir_perfil("organizador", "equipe")
def relatorios_status_tarefa(tarefa_id):
    tarefa = consultar_tarefa(tarefa_id)
    codigo = 200 if not tarefa.falhou else (404 if tarefa.status == "nao_encontrada" else 500)
    return jsonify({
        "ok": not tarefa.falhou,
        "tarefa_id": tarefa.id,
        "status": tarefa.status,
        "pronto": tarefa.pronto,
        "falhou": tarefa.falhou,
        "mensagem": tarefa.mensagem,
        "resultado": tarefa.resultado,
    }), codigo
