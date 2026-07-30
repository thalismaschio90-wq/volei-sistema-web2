from io import BytesIO
from datetime import datetime
from pathlib import Path

from flask import current_app, flash, send_file

from services.relatorios.geracao import (
    _txt,
    _primeiro_valor,
    _listar_equipes_inscritas,
    _listar_atletas_inscritos,
)

def _pdf_response(titulo, linhas, competicao_nome=None):
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, KeepTogether
    except Exception:
        flash("Para gerar PDF, adicione reportlab no requirements.txt e faça deploy novamente.", "erro")
        return None

    import os
    import re
    import base64

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
        rightMargin=12 * mm,
        leftMargin=12 * mm,
        topMargin=12 * mm,
        bottomMargin=12 * mm,
        title=f"{titulo} - {competicao_nome}",
    )

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="VTPTitle", parent=styles["Title"], fontName=fonte_bold, fontSize=18, leading=21, textColor=colors.white, alignment=0, spaceAfter=0))
    styles.add(ParagraphStyle(name="VTPBrand", parent=styles["Normal"], fontName=fonte_bold, fontSize=10, leading=12, textColor=colors.HexColor("#f8fafc"), spaceAfter=2))
    styles.add(ParagraphStyle(name="VTPMeta", parent=styles["Normal"], fontName=fonte, fontSize=8.5, leading=11, textColor=colors.HexColor("#64748b")))
    styles.add(ParagraphStyle(name="Secao", parent=styles["Normal"], fontName=fonte_bold, fontSize=10.5, leading=13, textColor=colors.HexColor("#123852"), spaceBefore=8, spaceAfter=5))
    styles.add(ParagraphStyle(name="Texto", parent=styles["Normal"], fontName=fonte, fontSize=8.4, leading=11, textColor=colors.HexColor("#0f172a")))
    styles.add(ParagraphStyle(name="TextoBold", parent=styles["Normal"], fontName=fonte_bold, fontSize=8.4, leading=11, textColor=colors.HexColor("#0f172a")))
    styles.add(ParagraphStyle(name="Small", parent=styles["Normal"], fontName=fonte, fontSize=7.2, leading=9, textColor=colors.HexColor("#475569")))

    def esc(texto):
        return str(texto or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    def ptxt(texto, estilo="Texto"):
        return Paragraph(esc(texto), styles[estilo])

    def caminho_static(url_ou_path):
        valor = (url_ou_path or "").strip()
        if not valor:
            return None
        if valor.startswith("http://") or valor.startswith("https://"):
            return None
        if valor.startswith("/static/"):
            rel = valor.replace("/static/", "", 1)
            caminho = os.path.join(current_app.static_folder, rel)
        elif valor.startswith("static/"):
            caminho = os.path.join(current_app.root_path, valor)
        elif valor.startswith("/"):
            caminho = valor
        else:
            caminho = os.path.join(current_app.static_folder, valor)
        return caminho if caminho and os.path.exists(caminho) else None

    def img_safe(caminho, w, h):
        try:
            if caminho and os.path.exists(caminho):
                im = Image(caminho, width=w, height=h)
                im.hAlign = "CENTER"
                return im
        except Exception:
            return None
        return None

    def img_data_url(data_url, w, h):
        try:
            valor = str(data_url or "").strip()
            if not valor.startswith("data:image/") or "," not in valor:
                return None
            raw = base64.b64decode(valor.split(",", 1)[1])
            bio = BytesIO(raw)
            im = Image(bio, width=w, height=h)
            im.hAlign = "CENTER"
            return im
        except Exception:
            return None

    logo_path = caminho_static("/static/img/logo.png")
    logo = img_safe(logo_path, 17 * mm, 17 * mm)
    if logo is None:
        logo = ptxt("VT", "TextoBold")

    header = Table(
        [[logo, Paragraph('Volley<font color="#d4a62a">Table</font> Pro', styles["VTPTitle"]), Paragraph(esc(titulo), styles["VTPBrand"])]],
        colWidths=[21 * mm, 70 * mm, 85 * mm],
        style=TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#0b3557")),
            ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#0b3557")),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 9),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
        ])
    )

    story = [header, Spacer(1, 7)]
    story.append(Paragraph(f"Competição: <b>{esc(competicao_nome)}</b>", styles["VTPMeta"]))
    story.append(Paragraph(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}", styles["VTPMeta"]))
    story.append(Spacer(1, 7))

    def tabela(data, col_widths=None, repetir=True):
        if not data:
            return None
        data_p = [[ptxt(c, "TextoBold" if r == 0 else "Texto") for c in row] for r, row in enumerate(data)]
        t = Table(data_p, colWidths=col_widths, repeatRows=1 if repetir and len(data) > 1 else 0, hAlign="LEFT")
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#123852")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), fonte_bold),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#ffffff")),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
            ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#dbe5ef")),
            ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#cbd5e1")),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        return t

    def metric_cards(rows):
        table_data = []
        linha = []
        for label, value in rows:
            linha.append(Table([[ptxt(label, "Small")], [ptxt(value, "TextoBold")]], colWidths=[41 * mm], style=TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#dbe5ef")),
                ("LEFTPADDING", (0, 0), (-1, -1), 7),
                ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ])))
            if len(linha) == 4:
                table_data.append(linha); linha = []
        if linha:
            while len(linha) < 4:
                linha.append("")
            table_data.append(linha)
        if table_data:
            story.append(Table(table_data, colWidths=[44 * mm] * 4, style=TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")])) )
            story.append(Spacer(1, 7))

    def equipe_por_nome(nome):
        alvo = _txt(nome, "").lower()
        for e in _listar_equipes_inscritas(competicao_nome):
            if _txt(e.get("nome"), "").lower() == alvo:
                return e
        return None

    def bloco_equipe(nome):
        equipe = equipe_por_nome(nome)
        escudo_path = caminho_static((equipe or {}).get("escudo") or "")
        escudo = img_safe(escudo_path, 20 * mm, 20 * mm)
        if not escudo:
            escudo = ptxt("", "Texto")
        dados = [[escudo, Paragraph(f"<b>{esc(nome)}</b><br/><font size='8'>Equipe da competição</font>", styles["Texto"] )]]
        story.append(Table(dados, colWidths=[25 * mm, 151 * mm], style=TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#eef6ff")),
            ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#bfdbfe")),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("RIGHTPADDING", (0, 0), (-1, -1), 8),
            ("TOPPADDING", (0, 0), (-1, -1), 7),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ])))
        story.append(Spacer(1, 7))
        return equipe or {}

    def extrair_valor(linha, chave):
        m = re.search(rf"{re.escape(chave)}[:=]\s*([^|]+)", linha, flags=re.I)
        return m.group(1).strip() if m else "-"

    linhas_limpas = [str(l or "").strip() for l in linhas]
    linhas_corpo = [l for l in linhas_limpas if l and l.upper() != str(titulo).upper() and not l.lower().startswith("competição:") and not l.lower().startswith("gerado em:") and not set(l) <= {"="}]

    if "ficha" in titulo.lower():
        nome_equipe = None
        for l in linhas_corpo:
            if l.startswith("Equipe:"):
                nome_equipe = l.split(":", 1)[1].strip(); break
            if " - " in l and l.upper().startswith("FICHA"):
                nome_equipe = l.split(" - ", 1)[1].strip(); break
        if nome_equipe:
            equipe = bloco_equipe(nome_equipe)
            rows = [
                ("Responsável/Técnico", _primeiro_valor(equipe, "responsavel", "tecnico", "treinador", "nome_responsavel")),
                ("Telefone", _primeiro_valor(equipe, "telefone", "celular", "whatsapp", "contato")),
                ("E-mail", _primeiro_valor(equipe, "email", "e_mail", "login")),
                ("Cidade", _primeiro_valor(equipe, "cidade", "municipio")),
                ("Status", _primeiro_valor(equipe, "status", "status_vinculo", "status_inscricao", "situacao")),
                ("Atletas inscritos", str(len(_listar_atletas_inscritos(competicao_nome, nome_equipe)))),
            ]
            metric_cards(rows)
            atletas = _listar_atletas_inscritos(competicao_nome, nome_equipe)
            story.append(Paragraph("ATLETAS INSCRITOS", styles["Secao"]))
            dados = [["Foto", "Nº", "Nome completo", "CPF", "Nascimento", "Instagram"]]
            for pos, atleta in enumerate(atletas, start=1):
                foto_valor = _primeiro_valor(atleta, "foto_atleta", "foto", "foto_url", padrao="")
                foto = img_data_url(foto_valor, 15 * mm, 15 * mm) or ptxt("Sem foto", "Small")
                dados.append([
                    foto,
                    _primeiro_valor(atleta, "numero", "camisa", "n", padrao="-"),
                    _primeiro_valor(atleta, "nome", "nome_atleta", "atleta", "jogador", padrao="Sem identificação"),
                    _primeiro_valor(atleta, "cpf", "documento", "rg", padrao="-"),
                    _primeiro_valor(atleta, "data_nascimento", "nascimento", "dt_nascimento", padrao="-"),
                    _primeiro_valor(atleta, "instagram", padrao="-"),
                ])
            if len(dados) == 1:
                story.append(Paragraph("Nenhum atleta cadastrado/encontrado para esta equipe.", styles["Texto"]))
            else:
                story.append(tabela(dados, [18 * mm, 13 * mm, 58 * mm, 32 * mm, 28 * mm, 27 * mm]))
            story.append(Spacer(1, 18))
            story.append(Paragraph("Assinatura do responsável: ______________________________________________", styles["TextoBold"]))
        else:
            story.append(Paragraph("Selecione uma equipe para gerar a ficha de inscrição.", styles["Texto"]))

    elif "ranking das equipes" in titulo.lower() or "ranking da equipe" in titulo.lower():
        dados = [["Pos.", "Equipe", "Jogos", "Vitórias", "Derrotas", "Sets", "Saldo"]]
        for l in linhas_corpo:
            m = re.match(r"(\d+)\.\s*(.*?)\s*\|", l)
            if not m: continue
            dados.append([m.group(1), m.group(2), extrair_valor(l, "J"), extrair_valor(l, "V"), extrair_valor(l, "D"), extrair_valor(l, "Sets"), extrair_valor(l, "Saldo")])
        story.append(tabela(dados, [13*mm, 57*mm, 19*mm, 20*mm, 20*mm, 28*mm, 19*mm]) or Paragraph("Nenhum dado encontrado.", styles["Texto"]))

    elif "ranking" in titulo.lower() or "pontuador" in titulo.lower() or "sacador" in titulo.lower() or "bloqueador" in titulo.lower() or "atacante" in titulo.lower():
        dados = [["Pos.", "Nº", "Atleta", "Equipe", "Pontos", "Ataques", "Bloqueios", "Aces", "Jogos"]]
        for l in linhas_corpo:
            m = re.match(r"(\d+)\.\s*(#([^\s]+)\s*)?(.+?)(?:\s*\((.*?)\))?\s*\|", l)
            if not m: continue
            atleta = (m.group(4) or "").strip()
            equipe = (m.group(5) or "-").strip()
            dados.append([m.group(1), m.group(3) or "-", atleta, equipe, extrair_valor(l, "Pontos"), extrair_valor(l, "Ataques"), extrair_valor(l, "Bloqueios"), extrair_valor(l, "Aces"), extrair_valor(l, "Jogos")])
        story.append(tabela(dados, [12*mm, 13*mm, 47*mm, 35*mm, 17*mm, 17*mm, 18*mm, 14*mm, 13*mm]) or Paragraph("Nenhum dado encontrado.", styles["Texto"]))

    elif "ordem dos jogos" in titulo.lower():
        dados = [["Ordem real", "Grupo", "Nome da quadra", "Fase", "Partida", "Status"]]
        for l in linhas_corpo:
            m = re.match(r"(\d+)\.\s*", l)
            if not m:
                continue
            dados.append([
                extrair_valor(l, "Ordem"),
                extrair_valor(l, "Grupo"),
                extrair_valor(l, "Quadra"),
                extrair_valor(l, "Fase"),
                extrair_valor(l, "Partida"),
                extrair_valor(l, "Status"),
            ])
        story.append(tabela(dados, [19*mm, 23*mm, 34*mm, 25*mm, 64*mm, 21*mm]) or Paragraph("Nenhum jogo encontrado.", styles["Texto"]))

    elif "histórico de jogos" in titulo.lower():
        dados = [["Jogo", "Partida", "Parciais/Sets", "Vencedor"]]
        for l in linhas_corpo:
            m = re.match(r"(\d+)\.\s*(.*?)\s*\|", l)
            if not m: continue
            dados.append([m.group(1), m.group(2), extrair_valor(l, "Sets"), extrair_valor(l, "Vencedor")])
        story.append(tabela(dados, [14*mm, 88*mm, 44*mm, 30*mm]) or Paragraph("Nenhuma partida finalizada encontrada.", styles["Texto"]))

    elif "estatísticas gerais" in titulo.lower() or "relatório da equipe" in titulo.lower():
        # Relatório de equipe: destaca o escudo quando o nome estiver no título.
        m = re.search(r"-\s*(.+)$", " ".join(linhas_limpas[:1]) or titulo)
        if m:
            bloco_equipe(m.group(1).strip())
        dados = [["Indicador", "Valor"]]
        for l in linhas_corpo:
            if ":" in l and not l.upper().startswith("ESTAT"):
                k, v = l.split(":", 1)
                dados.append([k.strip(), v.strip()])
        story.append(tabela(dados, [85*mm, 35*mm]) or Paragraph("Nenhum dado encontrado.", styles["Texto"]))

    elif "relatório da partida" in titulo.lower():
        partida_nome = next((l.split(":", 1)[1].strip() for l in linhas_corpo if l.startswith("Partida:")), "-")
        story.append(Paragraph(f"<b>Partida:</b> {esc(partida_nome)}", styles["TextoBold"]))
        meta = [["Fase", "Resultado", "Parciais", "Vencedor"]]
        meta.append([next((l.split(":",1)[1].strip() for l in linhas_corpo if l.startswith("Fase:")), "-"), next((l.split(":",1)[1].strip() for l in linhas_corpo if l.startswith("Resultado:")), "-"), next((l.split(":",1)[1].strip() for l in linhas_corpo if l.startswith("Parciais:")), "-"), next((l.split(":",1)[1].strip() for l in linhas_corpo if l.startswith("Vencedor:")), "-")])
        story.append(Spacer(1, 5)); story.append(tabela(meta, [35*mm, 35*mm, 70*mm, 36*mm])); story.append(Spacer(1, 7))
        atual = None; dados = []
        for l in linhas_corpo:
            if l.startswith("ESTATÍSTICAS -"):
                if atual and dados:
                    story.append(Paragraph(atual, styles["Secao"])); story.append(tabela(dados, [70*mm, 30*mm])); story.append(Spacer(1, 6))
                atual = l; dados = [["Fundamento", "Total"]]
            elif atual and ":" in l:
                k, v = l.split(":", 1); dados.append([k.strip(), v.strip()])
        if atual and dados:
            story.append(Paragraph(atual, styles["Secao"])); story.append(tabela(dados, [70*mm, 30*mm]))

    elif "estatísticas dos atletas" in titulo.lower():
        atual = None; dados = []
        for l in linhas_corpo:
            if l.startswith("ATLETAS -"):
                if atual and dados:
                    story.append(Paragraph(atual, styles["Secao"])); story.append(tabela(dados, [14*mm, 62*mm, 22*mm, 22*mm, 25*mm, 20*mm])); story.append(Spacer(1, 6))
                atual = l; dados = [["Nº", "Atleta", "Pontos", "Ataques", "Bloqueios", "Aces"]]
            elif atual and ":" in l:
                nome, resto = l.split(":", 1)
                num = "-"
                m = re.match(r"#([^\s]+)\s+(.+)", nome.strip())
                if m:
                    num, nome = m.group(1), m.group(2)
                dados.append([num, nome.strip(), extrair_valor(resto, "Pontos"), extrair_valor(resto, "Ataques"), extrair_valor(resto, "Bloqueios"), extrair_valor(resto, "Aces")])
        if atual and dados:
            story.append(Paragraph(atual, styles["Secao"])); story.append(tabela(dados, [14*mm, 62*mm, 22*mm, 22*mm, 25*mm, 20*mm]))

    else:
        for linha in linhas_corpo:
            story.append(Paragraph(esc(linha), styles["Texto"]))

    def _rodape(canvas_obj, doc_obj):
        canvas_obj.saveState()
        canvas_obj.setFont(fonte, 7.5)
        canvas_obj.setFillColor(colors.HexColor("#64748b"))
        canvas_obj.drawString(12 * mm, 8 * mm, f"VolleyTable Pro • {competicao_nome}")
        canvas_obj.drawRightString(198 * mm, 8 * mm, f"Página {doc_obj.page}")
        canvas_obj.restoreState()

    doc.build(story, onFirstPage=_rodape, onLaterPages=_rodape)
    buffer.seek(0)
    nome_base = f"{titulo}_{competicao_nome}".lower()
    for ch in [" ", "/", "\\", ":", "*", "?", '"', "<", ">", "|"]:
        nome_base = nome_base.replace(ch, "_")
    return send_file(buffer, as_attachment=True, download_name=f"{nome_base}.pdf", mimetype="application/pdf")


