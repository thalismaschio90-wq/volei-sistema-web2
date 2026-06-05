from io import BytesIO
from datetime import datetime
from pathlib import Path

from flask import Blueprint, render_template, request, session, redirect, url_for, flash, send_file, current_app

import banco as banco_mod
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



def _como_dict(registro):
    if registro is None:
        return {}
    if isinstance(registro, dict):
        return dict(registro)
    try:
        return dict(registro)
    except Exception:
        return {k: getattr(registro, k) for k in dir(registro) if not k.startswith("_") and not callable(getattr(registro, k, None))}


def _primeiro_valor(dados, *chaves, padrao="-"):
    for chave in chaves:
        if chave in dados and str(dados.get(chave) or "").strip():
            return _txt(dados.get(chave), padrao)
    return padrao


def _chamar_banco(nomes_funcoes, *args):
    for nome in nomes_funcoes:
        func = getattr(banco_mod, nome, None)
        if not callable(func):
            continue
        tentativas = [args]
        if args:
            tentativas.append(args[:1])
        tentativas.append(tuple())
        for tentativa in tentativas:
            try:
                return func(*tentativa) or []
            except TypeError:
                continue
            except Exception as e:
                print(f"ERRO relatório fichas ({nome}):", repr(e), flush=True)
                break
    return []


def _listar_equipes_inscritas(competicao_nome):
    registros = _chamar_banco([
        "listar_equipes_competicao",
        "listar_equipes_por_competicao",
        "listar_equipes_da_competicao",
        "listar_equipes",
        "buscar_equipes_competicao",
    ], competicao_nome)

    equipes = []
    for item in registros or []:
        d = _como_dict(item)
        if not d:
            continue
        comp = _txt(d.get("competicao") or d.get("competicao_nome") or d.get("nome_competicao"), "")
        if comp and comp.lower() != _txt(competicao_nome, "").lower():
            continue
        nome = _primeiro_valor(d, "nome", "nome_equipe", "equipe", "time", padrao="")
        if nome:
            d["nome"] = nome
            equipes.append(d)

    if equipes:
        return sorted(equipes, key=lambda x: _txt(x.get("nome"), "").lower())

    # Fallback: se não existir função de equipes no banco.py, pega os nomes pelas partidas.
    nomes = set()
    for p in _todas_partidas(competicao_nome, somente_finalizadas=False):
        if p.get("equipe_a"):
            nomes.add(p.get("equipe_a"))
        if p.get("equipe_b"):
            nomes.add(p.get("equipe_b"))
    return [{"nome": n} for n in sorted(nomes)]


def _listar_atletas_inscritos(competicao_nome, equipe_nome):
    registros = _chamar_banco([
        "listar_atletas_equipe",
        "listar_atletas_por_equipe",
        "listar_atletas_da_equipe",
        "buscar_atletas_equipe",
        "listar_atletas",
        "listar_jogadores_equipe",
        "listar_jogadores_por_equipe",
    ], equipe_nome, competicao_nome)

    atletas = []
    for item in registros or []:
        d = _como_dict(item)
        if not d:
            continue
        comp = _txt(d.get("competicao") or d.get("competicao_nome") or d.get("nome_competicao"), "")
        eq = _txt(d.get("equipe") or d.get("nome_equipe") or d.get("time"), "")
        if comp and comp.lower() != _txt(competicao_nome, "").lower():
            continue
        if eq and eq.lower() != _txt(equipe_nome, "").lower():
            continue
        atletas.append(d)

    return sorted(atletas, key=lambda x: (_int(x.get("numero") or x.get("camisa")), _txt(x.get("nome"), "").lower()))


def _montar_fichas_inscricao(competicao_nome, equipe_logada=None, equipe_filtro=None):
    if equipe_logada:
        return "Ficha de inscrição", ["Este relatório está disponível somente para o organizador."]

    if not _txt(equipe_filtro, ""):
        return "Ficha de inscrição", ["Selecione uma equipe para gerar a ficha de inscrição."]

    linhas = _linhas_titulo("Ficha de inscrição", competicao_nome)
    equipes = _listar_equipes_inscritas(competicao_nome)
    equipes = [e for e in equipes if _txt(e.get("nome"), "").lower() == _txt(equipe_filtro, "").lower()]

    if not equipes:
        linhas.append("Equipe não encontrada no cadastro desta competição.")
        return "Ficha de inscrição", linhas

    for idx, equipe in enumerate(equipes, start=1):
        nome_equipe = _primeiro_valor(equipe, "nome", "nome_equipe", "equipe", "time", padrao="Equipe sem nome")
        atletas = _listar_atletas_inscritos(competicao_nome, nome_equipe)

        linhas.append("=" * 70)
        linhas.append(f"FICHA {idx} - {nome_equipe}")
        linhas.append("=" * 70)
        linhas.append(f"Equipe: {nome_equipe}")
        linhas.append(f"Responsável/Técnico: {_primeiro_valor(equipe, 'responsavel', 'tecnico', 'treinador', 'nome_responsavel')}")
        linhas.append(f"Telefone: {_primeiro_valor(equipe, 'telefone', 'celular', 'whatsapp', 'contato')}")
        linhas.append(f"E-mail: {_primeiro_valor(equipe, 'email', 'e_mail', 'login')}")
        linhas.append(f"Cidade: {_primeiro_valor(equipe, 'cidade', 'municipio')}")
        linhas.append(f"Categoria: {_primeiro_valor(equipe, 'categoria', 'naipe')}")
        linhas.append(f"Status da inscrição: {_primeiro_valor(equipe, 'status', 'status_inscricao', 'situacao')}")
        linhas.append(f"Atletas inscritos: {len(atletas)}")
        linhas.append("")
        linhas.append("ATLETAS")

        if not atletas:
            linhas.append("Nenhum atleta cadastrado/encontrado para esta equipe.")
        else:
            for pos, atleta in enumerate(atletas, start=1):
                numero = _primeiro_valor(atleta, "numero", "camisa", "n", padrao="-")
                nome = _primeiro_valor(atleta, "nome", "nome_atleta", "atleta", "jogador", padrao="Sem identificação")
                doc = _primeiro_valor(atleta, "documento", "cpf", "rg", padrao="-")
                nasc = _primeiro_valor(atleta, "nascimento", "data_nascimento", "dt_nascimento", padrao="-")
                posicao = _primeiro_valor(atleta, "posicao", "função", "funcao", padrao="-")
                linhas.append(f"{pos}. Nº {numero} | {nome} | Doc: {doc} | Nasc.: {nasc} | Posição: {posicao}")

        linhas.append("")
        linhas.append("Assinatura do responsável: ______________________________________________")
        linhas.append("")

    return "Fichas de inscrição", linhas



def _valor_partida(partida, *chaves, padrao="-"):
    for chave in chaves:
        valor = partida.get(chave)
        if valor is not None and str(valor).strip():
            return str(valor).strip()
    return padrao


def _quadra_partida(partida):
    """
    Nome exibido da quadra no relatório.

    Prioriza campos de NOME da quadra. Alguns cadastros salvam somente o
    número/id em `quadra`; quando for só número, mostra como "Quadra X" para
    não sair apenas "2" no PDF.
    """
    valor = _valor_partida(
        partida,
        "nome_quadra",
        "quadra_nome",
        "quadra_descricao",
        "local_quadra",
        "quadra",
        "court",
        padrao="Sem quadra",
    )
    if valor != "Sem quadra" and str(valor).strip().isdigit():
        return f"Quadra {str(valor).strip()}"
    return valor


def _grupo_partida(partida):
    return _valor_partida(partida, "grupo", "nome_grupo", "chave", "grupo_nome", "fase_grupo", padrao="-")


def _ordem_partida(partida):
    for chave in ("ordem", "ordem_jogo", "numero_jogo", "jogo", "sequencia"):
        try:
            if partida.get(chave) is not None and str(partida.get(chave)).strip():
                return _int(partida.get(chave))
        except Exception:
            pass
    return _int(partida.get("id"))


def _listar_quadras_partidas(partidas):
    quadras = []
    vistos = set()
    for p in partidas or []:
        q = _quadra_partida(dict(p))
        if not q or q == "Sem quadra":
            continue
        chave = q.lower()
        if chave not in vistos:
            vistos.add(chave)
            quadras.append(q)
    return sorted(quadras, key=lambda x: x.lower())


def _montar_ordem_jogos(competicao_nome, equipe_logada=None, quadra_filtro=None):
    equipe_restrita = equipe_logada.get("nome") if equipe_logada else None
    partidas = _todas_partidas(competicao_nome, equipe_nome=equipe_restrita, somente_finalizadas=False)

    filtro = (quadra_filtro or "").strip().lower()
    if filtro:
        partidas = [p for p in partidas if _quadra_partida(p).lower() == filtro]

    # ORDEM REAL DOS JOGOS:
    # nunca ordena por grupo nem por quadra, porque isso embaralha a sequência geral.
    # A ordem oficial é a ordem/sequência salva na partida; se não existir, usa o id.
    # Quando filtra por quadra, mantém a mesma ordem geral e apenas remove as outras quadras.
    partidas = sorted(
        partidas,
        key=lambda p: (
            _ordem_partida(p),
            _int(p.get("id")),
        )
    )

    titulo = "Ordem dos jogos"
    if quadra_filtro:
        titulo = f"Ordem dos jogos - {quadra_filtro}"

    linhas = _linhas_titulo(titulo, competicao_nome)

    if not partidas:
        if quadra_filtro:
            linhas.append(f"Nenhum jogo encontrado para a quadra {quadra_filtro}.")
        else:
            linhas.append("Nenhum jogo encontrado na competição.")
        return titulo, linhas

    for pos, p in enumerate(partidas, start=1):
        grupo = _grupo_partida(p)
        quadra = _quadra_partida(p)
        fase = _valor_partida(p, "fase", "fase_nome", "etapa", padrao="-")
        status = _valor_partida(p, "status", "fase_partida", "status_jogo", padrao="-")
        ordem = _ordem_partida(p) or pos
        equipe_a = _txt(p.get("equipe_a"), "Equipe A")
        equipe_b = _txt(p.get("equipe_b"), "Equipe B")

        # O número inicial também usa a ordem real para o PDF/preview não parecer
        # que a ordem foi recalculada por grupo/quadra.
        linhas.append(
            f"{ordem}. Ordem={ordem} | Grupo={grupo} | Quadra={quadra} | "
            f"Fase={fase} | Partida={equipe_a} x {equipe_b} | Status={status}"
        )

    return titulo, linhas

def _montar_relatorio(tipo, competicao_nome, equipe_logada=None, equipe_filtro=None, partida_id=None, quadra_filtro=None):
    equipe_restrita = equipe_logada.get("nome") if equipe_logada else None
    equipe_alvo = equipe_restrita or equipe_filtro
    partidas_finalizadas = _todas_partidas(competicao_nome, equipe_nome=equipe_restrita, somente_finalizadas=True)

    if tipo == "ordem_jogos":
        return _montar_ordem_jogos(competicao_nome, equipe_logada=equipe_logada, quadra_filtro=quadra_filtro)

    if tipo == "fichas_inscricao":
        return _montar_fichas_inscricao(competicao_nome, equipe_logada=equipe_logada, equipe_filtro=equipe_filtro)

    if tipo == "historico_jogos":
        linhas = _linhas_titulo("Histórico de jogos", competicao_nome)
        if not partidas_finalizadas:
            linhas.append("Nenhuma partida finalizada encontrada.")
        for i, p in enumerate(partidas_finalizadas, start=1):
            linhas.append(f"{i}. {_txt(p.get('equipe_a'))} {_placar(p)} {_txt(p.get('equipe_b'))} | Sets: {_parciais(p)} | Vencedor: {_txt(p.get('vencedor'))}")
        return "Histórico de jogos", linhas

    if tipo == "ranking_equipes":
        if equipe_filtro and not equipe_restrita:
            partidas_base = _todas_partidas(competicao_nome, equipe_nome=equipe_filtro, somente_finalizadas=True)
            linhas = _linhas_titulo(f"Ranking da equipe - {equipe_filtro}", competicao_nome)
            ranking = _agregar_equipes(competicao_nome, partidas_base)
            ranking = [(nome, d) for nome, d in ranking if nome.lower() == equipe_filtro.lower()]
            if not ranking:
                linhas.append("Esta equipe está cadastrada, mas ainda não possui partidas finalizadas.")
            for pos, (nome, d) in enumerate(ranking, start=1):
                linhas.append(f"{pos}. {nome} | J={d['Partidas']} | V={d['Vitórias']} | D={d['Derrotas']} | Sets={d['Sets Pró']}x{d['Sets Contra']} | Saldo={d['Saldo Sets']}")
            return "Ranking da equipe", linhas

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
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, KeepTogether
    except Exception:
        flash("Para gerar PDF, adicione reportlab no requirements.txt e faça deploy novamente.", "erro")
        return None

    import os
    import re

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
            dados = [["Ordem", "Nº camisa", "Nome completo", "CPF", "Data nascimento"]]
            for pos, atleta in enumerate(atletas, start=1):
                dados.append([
                    str(pos),
                    _primeiro_valor(atleta, "numero", "camisa", "n", padrao="-"),
                    _primeiro_valor(atleta, "nome", "nome_atleta", "atleta", "jogador", padrao="Sem identificação"),
                    _primeiro_valor(atleta, "cpf", "documento", "rg", padrao="-"),
                    _primeiro_valor(atleta, "data_nascimento", "nascimento", "dt_nascimento", padrao="-"),
                ])
            if len(dados) == 1:
                story.append(Paragraph("Nenhum atleta cadastrado/encontrado para esta equipe.", styles["Texto"]))
            else:
                story.append(tabela(dados, [15 * mm, 22 * mm, 74 * mm, 35 * mm, 30 * mm]))
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

    titulo, linhas = _montar_relatorio(
        tipo,
        competicao.get("nome"),
        equipe_logada=equipe,
        equipe_filtro=request.args.get("equipe"),
        partida_id=request.args.get("partida_id"),
        quadra_filtro=quadra_filtro,
    )

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

    titulo, linhas = _montar_relatorio(
        tipo,
        competicao.get("nome"),
        equipe_logada=equipe,
        equipe_filtro=request.args.get("equipe"),
        partida_id=request.args.get("partida_id"),
        quadra_filtro=quadra_filtro,
    )
    resp = _pdf_response(titulo, linhas, competicao.get("nome"))
    if resp is None:
        return redirect(url_for("relatorios.relatorios_home"))
    return resp
