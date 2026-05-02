from io import BytesIO
from datetime import datetime
from pathlib import Path

from flask import Blueprint, render_template, request, session, redirect, url_for, flash, send_file

from routes.utils import exigir_perfil
from banco import (
    buscar_competicao_por_organizador,
    buscar_equipe_por_login,
    listar_partidas,
    listar_eventos_partida,
    resumir_scout_equipe_partida,
)

relatorios_bp = Blueprint("relatorios", __name__)

STATUS_FINALIZADA = {"finalizado", "finalizada", "encerrado", "encerrada"}

RELATORIOS_ORGANIZADOR = [
    {"id": "historico_jogos", "titulo": "Histórico de jogos", "descricao": "Lista todas as partidas finalizadas da competição."},
    {"id": "ranking_atletas", "titulo": "Ranking geral de atletas", "descricao": "Atletas ordenados por pontos, ataques, bloqueios e aces."},
    {"id": "maior_pontuador", "titulo": "Maior pontuador", "descricao": "Ranking dos atletas com mais pontos na competição."},
    {"id": "melhor_sacador", "titulo": "Melhor sacador", "descricao": "Ranking dos atletas com mais aces."},
    {"id": "melhor_bloqueador", "titulo": "Melhor bloqueador", "descricao": "Ranking dos atletas com mais pontos de bloqueio."},
    {"id": "melhor_atacante", "titulo": "Melhor atacante", "descricao": "Ranking dos atletas com mais pontos de ataque."},
    {"id": "ranking_equipes", "titulo": "Ranking das equipes", "descricao": "Vitórias, derrotas, sets pró, sets contra e saldo."},
    {"id": "estatisticas_competicao", "titulo": "Estatísticas gerais", "descricao": "Totais gerais de pontos, fundamentos, erros e faltas."},
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


def _txt(valor, padrao="-"):
    valor = "" if valor is None else str(valor).strip()
    return valor or padrao


def _int(valor):
    try:
        return int(valor or 0)
    except Exception:
        return 0


def _status_finalizada(partida):
    status = _txt(partida.get("status") or partida.get("fase_partida") or partida.get("status_jogo"), "").lower()
    return status in STATUS_FINALIZADA


def _placar(partida):
    pontos_a = partida.get("pontos_a")
    pontos_b = partida.get("pontos_b")
    if pontos_a is not None and pontos_b is not None and (_int(pontos_a) or _int(pontos_b)):
        return f"{_int(pontos_a)} x {_int(pontos_b)}"
    return f"{_int(partida.get('sets_a'))} x {_int(partida.get('sets_b'))}"


def _parciais(partida):
    parciais = []
    for i in range(1, 6):
        a = partida.get(f"set{i}_a")
        b = partida.get(f"set{i}_b")
        if a is not None and b is not None:
            parciais.append(f"{_int(a)}x{_int(b)}")
    return " / ".join(parciais) if parciais else "-"


def _minha_competicao_e_equipe():
    perfil = session.get("perfil")
    usuario = session.get("usuario")

    if perfil == "organizador":
        competicao = buscar_competicao_por_organizador(usuario)
        if not competicao:
            return None, None, "Nenhuma competição vinculada ao organizador."
        return competicao, None, None

    if perfil == "equipe":
        equipe = buscar_equipe_por_login(usuario)
        if not equipe:
            return None, None, "Equipe não encontrada."
        competicao = {"nome": equipe.get("competicao")}
        return competicao, equipe, None

    return None, None, "Perfil sem permissão para relatórios."


def _todas_partidas(competicao_nome, equipe_nome=None, somente_finalizadas=True):
    partidas = listar_partidas(competicao_nome) or []
    saida = []
    equipe_nome_lower = (equipe_nome or "").strip().lower()

    for p in partidas:
        p = dict(p)
        if somente_finalizadas and not _status_finalizada(p):
            continue
        if equipe_nome_lower:
            ea = _txt(p.get("equipe_a"), "").lower()
            eb = _txt(p.get("equipe_b"), "").lower()
            if equipe_nome_lower not in {ea, eb}:
                continue
        saida.append(p)
    return saida


def _partida_por_id(competicao_nome, partida_id, equipe_nome=None):
    if not partida_id:
        return None
    for p in _todas_partidas(competicao_nome, equipe_nome=equipe_nome, somente_finalizadas=False):
        if _int(p.get("id")) == _int(partida_id):
            return p
    return None


def _lado_da_equipe(partida, equipe_nome):
    equipe_nome = (equipe_nome or "").strip().lower()
    if not equipe_nome:
        return None
    if _txt(partida.get("equipe_a"), "").lower() == equipe_nome:
        return "A"
    if _txt(partida.get("equipe_b"), "").lower() == equipe_nome:
        return "B"
    return None


def _nome_lado(partida, lado):
    return _txt(partida.get("equipe_a" if lado == "A" else "equipe_b"))


def _scout_lado(competicao_nome, partida, lado):
    try:
        return resumir_scout_equipe_partida(partida.get("id"), competicao_nome, lado) or {}
    except Exception as e:
        print("ERRO relatório scout:", repr(e), flush=True)
        return {"equipe": {}, "atletas_lista": [], "eventos": []}


def _linhas_titulo(titulo, competicao_nome):
    return [titulo.upper(), "", f"Competição: {competicao_nome}", f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}", ""]


def _agregar_equipes(competicao_nome, partidas):
    tabela = {}

    def garantir(nome):
        if nome not in tabela:
            tabela[nome] = {
                "Partidas": 0, "Vitórias": 0, "Derrotas": 0,
                "Sets Pró": 0, "Sets Contra": 0, "Saldo Sets": 0,
                "Pontos": 0, "Ataques": 0, "Bloqueios": 0, "Aces": 0,
                "Erros de saque": 0, "Erros de rotação": 0, "Faltas": 0, "Erros gerais": 0,
            }

    for p in partidas:
        ea, eb = _txt(p.get("equipe_a")), _txt(p.get("equipe_b"))
        garantir(ea); garantir(eb)
        sa, sb = _int(p.get("sets_a")), _int(p.get("sets_b"))
        tabela[ea]["Partidas"] += 1; tabela[eb]["Partidas"] += 1
        tabela[ea]["Sets Pró"] += sa; tabela[ea]["Sets Contra"] += sb
        tabela[eb]["Sets Pró"] += sb; tabela[eb]["Sets Contra"] += sa

        vencedor = _txt(p.get("vencedor"), "")
        if vencedor == ea:
            tabela[ea]["Vitórias"] += 1; tabela[eb]["Derrotas"] += 1
        elif vencedor == eb:
            tabela[eb]["Vitórias"] += 1; tabela[ea]["Derrotas"] += 1
        elif sa > sb:
            tabela[ea]["Vitórias"] += 1; tabela[eb]["Derrotas"] += 1
        elif sb > sa:
            tabela[eb]["Vitórias"] += 1; tabela[ea]["Derrotas"] += 1

        for nome, lado in [(ea, "A"), (eb, "B")]:
            scout = _scout_lado(competicao_nome, p, lado).get("equipe", {})
            tabela[nome]["Pontos"] += _int(scout.get("pontos"))
            tabela[nome]["Ataques"] += _int(scout.get("ataques"))
            tabela[nome]["Bloqueios"] += _int(scout.get("bloqueios"))
            tabela[nome]["Aces"] += _int(scout.get("aces"))
            tabela[nome]["Erros de saque"] += _int(scout.get("erros_saque"))
            tabela[nome]["Erros de rotação"] += _int(scout.get("erros_rotacao"))
            tabela[nome]["Faltas"] += _int(scout.get("faltas"))
            tabela[nome]["Erros gerais"] += _int(scout.get("erros_gerais"))

    for dados in tabela.values():
        dados["Saldo Sets"] = dados["Sets Pró"] - dados["Sets Contra"]

    return sorted(tabela.items(), key=lambda x: (x[1]["Vitórias"], x[1]["Saldo Sets"], x[1]["Sets Pró"]), reverse=True)


def _agregar_atletas(competicao_nome, partidas, equipe_nome=None):
    atletas = {}

    def add_atleta(equipe, dados):
        nome = _txt(dados.get("nome"), "Sem identificação")
        numero = _txt(dados.get("numero"), "")
        chave = f"{equipe}|||{numero}|||{nome}".lower()
        if chave not in atletas:
            atletas[chave] = {
                "Nome": nome,
                "Número": numero,
                "Equipe": equipe,
                "Jogos": 0,
                "Pontos": 0,
                "Ataques": 0,
                "Bloqueios": 0,
                "Aces": 0,
            }
        atletas[chave]["Jogos"] += 1
        atletas[chave]["Pontos"] += _int(dados.get("pontos"))
        atletas[chave]["Ataques"] += _int(dados.get("ataques"))
        atletas[chave]["Bloqueios"] += _int(dados.get("bloqueios"))
        atletas[chave]["Aces"] += _int(dados.get("aces"))

    filtro = (equipe_nome or "").strip().lower()
    for p in partidas:
        for lado in ["A", "B"]:
            equipe = _nome_lado(p, lado)
            if filtro and equipe.lower() != filtro:
                continue
            scout = _scout_lado(competicao_nome, p, lado)
            for atleta in scout.get("atletas_lista") or []:
                add_atleta(equipe, atleta)

    return sorted(atletas.values(), key=lambda x: (x["Pontos"], x["Ataques"], x["Bloqueios"], x["Aces"]), reverse=True)


def _montar_relatorio(tipo, competicao_nome, equipe_logada=None, equipe_filtro=None, partida_id=None):
    equipe_restrita = equipe_logada.get("nome") if equipe_logada else None
    equipe_alvo = equipe_restrita or equipe_filtro
    partidas_finalizadas = _todas_partidas(competicao_nome, equipe_nome=equipe_restrita, somente_finalizadas=True)

    if tipo == "historico_jogos":
        linhas = _linhas_titulo("Histórico de jogos", competicao_nome)
        if not partidas_finalizadas:
            linhas.append("Nenhuma partida finalizada encontrada.")
        for i, p in enumerate(partidas_finalizadas, start=1):
            linhas.append(f"{i}. {_txt(p.get('equipe_a'))} {_placar(p)} {_txt(p.get('equipe_b'))} | Sets: {_parciais(p)} | Vencedor: {_txt(p.get('vencedor'))}")
        return "Histórico de jogos", linhas

    if tipo == "ranking_equipes":
        linhas = _linhas_titulo("Ranking das equipes", competicao_nome)
        for pos, (nome, d) in enumerate(_agregar_equipes(competicao_nome, partidas_finalizadas), start=1):
            linhas.append(f"{pos}. {nome} | J={d['Partidas']} | V={d['Vitórias']} | D={d['Derrotas']} | Sets={d['Sets Pró']}x{d['Sets Contra']} | Saldo={d['Saldo Sets']}")
        return "Ranking das equipes", linhas

    if tipo in {"ranking_atletas", "maior_pontuador", "melhor_sacador", "melhor_bloqueador", "melhor_atacante"}:
        titulo_map = {
            "ranking_atletas": "Ranking geral de atletas",
            "maior_pontuador": "Maior pontuador",
            "melhor_sacador": "Melhor sacador",
            "melhor_bloqueador": "Melhor bloqueador",
            "melhor_atacante": "Melhor atacante",
        }
        chave_map = {
            "ranking_atletas": "Pontos",
            "maior_pontuador": "Pontos",
            "melhor_sacador": "Aces",
            "melhor_bloqueador": "Bloqueios",
            "melhor_atacante": "Ataques",
        }
        chave = chave_map[tipo]
        atletas = _agregar_atletas(competicao_nome, partidas_finalizadas, equipe_nome=equipe_restrita)
        atletas = sorted(
            atletas,
            key=lambda x: (x[chave], x["Pontos"], x["Ataques"], x["Bloqueios"], x["Aces"], x["Jogos"]),
            reverse=True,
        )

        # Relatórios de prêmio/destaque não devem mostrar todos os fundamentos.
        # Cada um mostra e ordena SOMENTE pelo fundamento dele.
        if tipo in {"maior_pontuador", "melhor_sacador", "melhor_bloqueador", "melhor_atacante"}:
            atletas = [a for a in atletas if _int(a.get(chave)) > 0]

        linhas = _linhas_titulo(titulo_map[tipo], competicao_nome)
        if not atletas:
            if tipo == "melhor_sacador":
                linhas.append("Nenhum ace registrado para definir o melhor sacador.")
            elif tipo == "melhor_bloqueador":
                linhas.append("Nenhum ponto de bloqueio registrado para definir o melhor bloqueador.")
            elif tipo == "melhor_atacante":
                linhas.append("Nenhum ponto de ataque registrado para definir o melhor atacante.")
            elif tipo == "maior_pontuador":
                linhas.append("Nenhum ponto registrado para definir o maior pontuador.")
            else:
                linhas.append("Nenhum atleta com scout encontrado.")

        rotulos = {
            "Pontos": "Pontos",
            "Ataques": "Ataques",
            "Bloqueios": "Bloqueios",
            "Aces": "Aces",
        }

        for pos, a in enumerate(atletas, start=1):
            numero = f"#{a['Número']} " if a.get("Número") else ""

            if tipo == "ranking_atletas":
                linhas.append(
                    f"{pos}. {numero}{a['Nome']} ({a['Equipe']}) | "
                    f"Pontos={a['Pontos']} | Ataques={a['Ataques']} | "
                    f"Bloqueios={a['Bloqueios']} | Aces={a['Aces']} | Jogos={a['Jogos']}"
                )
            elif tipo == "maior_pontuador":
                linhas.append(f"{pos}. {numero}{a['Nome']} ({a['Equipe']}) | Pontos={a['Pontos']} | Jogos={a['Jogos']}")
            else:
                linhas.append(f"{pos}. {numero}{a['Nome']} ({a['Equipe']}) | {rotulos[chave]}={a[chave]} | Jogos={a['Jogos']}")

        return titulo_map[tipo], linhas

    if tipo == "estatisticas_competicao":
        linhas = _linhas_titulo("Estatísticas gerais da competição", competicao_nome)
        ranking = _agregar_equipes(competicao_nome, partidas_finalizadas)
        totais = {"Partidas finalizadas": len(partidas_finalizadas), "Pontos": 0, "Ataques": 0, "Bloqueios": 0, "Aces": 0, "Erros de saque": 0, "Erros de rotação": 0, "Faltas": 0, "Erros gerais": 0}
        for _, d in ranking:
            for k in list(totais.keys()):
                if k != "Partidas finalizadas":
                    totais[k] += _int(d.get(k))
        for k, v in totais.items():
            linhas.append(f"{k}: {v}")
        return "Estatísticas gerais", linhas

    if tipo == "relatorio_equipe":
        if not equipe_alvo:
            return "Relatório da equipe", ["Selecione uma equipe para gerar este relatório."]
        partidas_eq = _todas_partidas(competicao_nome, equipe_nome=equipe_alvo, somente_finalizadas=True)
        dados = dict(_agregar_equipes(competicao_nome, partidas_eq)).get(equipe_alvo, {})
        linhas = _linhas_titulo(f"Relatório da equipe - {equipe_alvo}", competicao_nome)
        if not dados:
            linhas.append("Nenhuma partida finalizada encontrada para esta equipe.")
        for k, v in dados.items():
            linhas.append(f"{k}: {v}")
        return "Relatório da equipe", linhas

    if tipo in {"relatorio_partida", "historico_partida", "atletas_partida"}:
        partida = _partida_por_id(competicao_nome, partida_id, equipe_nome=equipe_restrita)
        if not partida:
            return "Relatório da partida", ["Selecione uma partida válida para gerar este relatório."]

        if tipo == "relatorio_partida":
            linhas = _linhas_titulo("Relatório da partida", competicao_nome)
            linhas += [
                f"Partida: {_txt(partida.get('equipe_a'))} x {_txt(partida.get('equipe_b'))}",
                f"Fase: {_txt(partida.get('fase'))}",
                f"Resultado: {_placar(partida)}",
                f"Parciais: {_parciais(partida)}",
                f"Vencedor: {_txt(partida.get('vencedor'))}",
                "",
            ]
            lados = [_lado_da_equipe(partida, equipe_restrita)] if equipe_restrita else ["A", "B"]
            for lado in [l for l in lados if l]:
                scout = _scout_lado(competicao_nome, partida, lado).get("equipe", {})
                linhas.append(f"ESTATÍSTICAS - {_nome_lado(partida, lado)}")
                linhas.append(f"Pontos: {_int(scout.get('pontos'))}")
                linhas.append(f"Ataques: {_int(scout.get('ataques'))}")
                linhas.append(f"Bloqueios: {_int(scout.get('bloqueios'))}")
                linhas.append(f"Aces: {_int(scout.get('aces'))}")
                linhas.append(f"Erros de saque: {_int(scout.get('erros_saque'))}")
                linhas.append(f"Erros de rotação: {_int(scout.get('erros_rotacao'))}")
                linhas.append(f"Faltas: {_int(scout.get('faltas'))}")
                linhas.append(f"Erros gerais: {_int(scout.get('erros_gerais'))}")
                linhas.append("")
            return "Relatório da partida", linhas

        if tipo == "historico_partida":
            linhas = _linhas_titulo("Histórico da partida", competicao_nome)
            linhas.append(f"Partida: {_txt(partida.get('equipe_a'))} x {_txt(partida.get('equipe_b'))}")
            linhas.append("")
            eventos = listar_eventos_partida(partida.get("id"), competicao_nome, limite=300) or []
            eventos = list(reversed(eventos))
            if not eventos:
                linhas.append("Sem eventos salvos para esta partida.")
            for ev in eventos:
                linhas.append(f"- Set {_txt(ev.get('set_numero'))} | {_txt(ev.get('descricao'))}")
            return "Histórico da partida", linhas

        linhas = _linhas_titulo("Estatísticas dos atletas da partida", competicao_nome)
        lados = [_lado_da_equipe(partida, equipe_restrita)] if equipe_restrita else ["A", "B"]
        for lado in [l for l in lados if l]:
            linhas.append(f"ATLETAS - {_nome_lado(partida, lado)}")
            atletas = _scout_lado(competicao_nome, partida, lado).get("atletas_lista") or []
            if not atletas:
                linhas.append("Sem scout de atletas registrado.")
            for a in atletas:
                numero = f"#{_txt(a.get('numero'), '')} " if _txt(a.get('numero'), '') else ""
                linhas.append(f"{numero}{_txt(a.get('nome'))}: Pontos={_int(a.get('pontos'))} | Ataques={_int(a.get('ataques'))} | Bloqueios={_int(a.get('bloqueios'))} | Aces={_int(a.get('aces'))}")
            linhas.append("")
        return "Estatísticas dos atletas", linhas

    return "Relatório", ["Tipo de relatório inválido."]


def _pdf_response(titulo, linhas, competicao_nome=None):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    except Exception:
        flash("Para gerar PDF, adicione reportlab no requirements.txt e faça deploy novamente.", "erro")
        return None

    def _registrar_fonte_moderna():
        fontes = [
            ("DejaVuSans", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
            ("LiberationSans", "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf"),
        ]
        fontes_bold = {
            "DejaVuSans": "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "LiberationSans": "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
        }
        for nome_fonte, caminho in fontes:
            try:
                if Path(caminho).exists():
                    pdfmetrics.registerFont(TTFont(nome_fonte, caminho))
                    bold = fontes_bold.get(nome_fonte)
                    if bold and Path(bold).exists():
                        pdfmetrics.registerFont(TTFont(f"{nome_fonte}-Bold", bold))
                    return nome_fonte
            except Exception:
                pass
        return "Helvetica"

    fonte = _registrar_fonte_moderna()
    fonte_bold = f"{fonte}-Bold" if fonte not in ("Helvetica", "Times-Roman") else "Helvetica-Bold"
    competicao_nome = _txt(competicao_nome or "", "Competição não informada")

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=15 * mm,
        leftMargin=15 * mm,
        topMargin=16 * mm,
        bottomMargin=14 * mm,
        title=f"{titulo} - {competicao_nome}",
    )

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(
        name="TituloModerno",
        parent=styles["Title"],
        fontName=fonte_bold,
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#111827"),
        spaceAfter=4,
    ))
    styles.add(ParagraphStyle(
        name="CompeticaoModerna",
        parent=styles["Normal"],
        fontName=fonte_bold,
        fontSize=11,
        leading=14,
        textColor=colors.HexColor("#2563eb"),
        spaceAfter=8,
    ))
    styles.add(ParagraphStyle(
        name="MetaModerna",
        parent=styles["Normal"],
        fontName=fonte,
        fontSize=8.5,
        leading=11,
        textColor=colors.HexColor("#6b7280"),
        spaceAfter=12,
    ))
    styles.add(ParagraphStyle(
        name="LinhaRelatorio",
        parent=styles["Normal"],
        fontName=fonte,
        fontSize=9.2,
        leading=12.5,
        textColor=colors.HexColor("#111827"),
    ))
    styles.add(ParagraphStyle(
        name="LinhaTituloSecao",
        parent=styles["Normal"],
        fontName=fonte_bold,
        fontSize=10.5,
        leading=14,
        textColor=colors.HexColor("#111827"),
        spaceBefore=8,
        spaceAfter=3,
    ))

    story = []
    story.append(Table(
        [[Paragraph(str(titulo), styles["TituloModerno"])]],
        colWidths=[180 * mm],
        style=TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f3f6fb")),
            ("BOX", (0, 0), (-1, -1), 0.7, colors.HexColor("#dbeafe")),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
            ("TOPPADDING", (0, 0), (-1, -1), 9),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ])
    ))
    story.append(Spacer(1, 7))
    story.append(Paragraph(f"Competição: {competicao_nome}", styles["CompeticaoModerna"]))
    story.append(Paragraph(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}", styles["MetaModerna"]))

    # Evita repetir no corpo linhas de cabeçalho antigas, porque o PDF moderno já tem título e competição no topo.
    pular_primeiras = {str(titulo).strip().upper(), f"COMPETIÇÃO: {competicao_nome}".upper()}
    for linha in linhas:
        texto = str(linha or "").strip()
        if not texto:
            story.append(Spacer(1, 5))
            continue
        if texto.upper() in pular_primeiras or texto.lower().startswith("gerado em:"):
            continue
        estilo = styles["LinhaTituloSecao"] if texto.isupper() and len(texto) <= 55 else styles["LinhaRelatorio"]
        story.append(Paragraph(texto.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"), estilo))

    def _rodape(canvas_obj, doc_obj):
        canvas_obj.saveState()
        canvas_obj.setFont(fonte, 8)
        canvas_obj.setFillColor(colors.HexColor("#6b7280"))
        canvas_obj.drawString(15 * mm, 9 * mm, f"{competicao_nome} - {titulo}")
        canvas_obj.drawRightString(195 * mm, 9 * mm, f"Página {doc_obj.page}")
        canvas_obj.restoreState()

    doc.build(story, onFirstPage=_rodape, onLaterPages=_rodape)
    buffer.seek(0)
    nome_base = f"{titulo}_{competicao_nome}".lower()
    for ch in [" ", "/", "\\", ":", "*", "?", '"', "<", ">", "|"]:
        nome_base = nome_base.replace(ch, "_")
    return send_file(buffer, as_attachment=True, download_name=f"{nome_base}.pdf", mimetype="application/pdf")


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
        nomes = set()
        for p in partidas:
            if p.get("equipe_a"):
                nomes.add(p.get("equipe_a"))
            if p.get("equipe_b"):
                nomes.add(p.get("equipe_b"))
        equipes = sorted(nomes)

    return render_template(
        "relatorios.html",
        competicao=competicao,
        equipe=equipe,
        perfil=perfil,
        relatorios=RELATORIOS_EQUIPE if perfil == "equipe" else RELATORIOS_ORGANIZADOR,
        partidas=partidas,
        equipes=equipes,
    )


@relatorios_bp.route("/relatorios/<tipo>")
@exigir_perfil("organizador", "equipe")
def relatorios_visualizar(tipo):
    competicao, equipe, erro = _minha_competicao_e_equipe()
    if erro:
        flash(erro, "erro")
        return redirect(url_for("painel.inicio"))

    titulo, linhas = _montar_relatorio(
        tipo,
        competicao.get("nome"),
        equipe_logada=equipe,
        equipe_filtro=request.args.get("equipe"),
        partida_id=request.args.get("partida_id"),
    )

    return render_template(
        "relatorio_preview.html",
        titulo=titulo,
        linhas=linhas,
        tipo=tipo,
        equipe_filtro=request.args.get("equipe", ""),
        partida_id=request.args.get("partida_id", ""),
    )


@relatorios_bp.route("/relatorios/<tipo>/pdf")
@exigir_perfil("organizador", "equipe")
def relatorios_pdf(tipo):
    competicao, equipe, erro = _minha_competicao_e_equipe()
    if erro:
        flash(erro, "erro")
        return redirect(url_for("painel.inicio"))

    titulo, linhas = _montar_relatorio(
        tipo,
        competicao.get("nome"),
        equipe_logada=equipe,
        equipe_filtro=request.args.get("equipe"),
        partida_id=request.args.get("partida_id"),
    )
    resp = _pdf_response(titulo, linhas, competicao.get("nome"))
    if resp is None:
        return redirect(url_for("relatorios.relatorios_home"))
    return resp
