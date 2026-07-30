from datetime import datetime

from core.request_cache import escopo_cache

import banco as banco_mod
from banco import (
    buscar_competicao_por_organizador,
    buscar_equipe_por_login,
    buscar_partida_por_id,
    listar_partidas,
    listar_eventos_partida,
    resumir_scout_equipe_partida,
)

STATUS_FINALIZADA = {"finalizado", "finalizada", "encerrado", "encerrada"}

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
    """Busca uma partida específica sem varrer todas as partidas da competição.

    Antes, qualquer relatório por jogo chamava _todas_partidas(), que carregava a
    lista completa da competição só para achar um ID. Em competições grandes isso
    deixava a abertura/geração de relatório lenta.
    """
    if not partida_id:
        return None

    try:
        partida = buscar_partida_por_id(partida_id, competicao_nome)
    except Exception as exc:
        print("AVISO relatorios._partida_por_id buscar_partida_por_id:", exc)
        partida = None

    if not partida:
        return None

    p = dict(partida)
    equipe_nome_lower = (equipe_nome or "").strip().lower()
    if equipe_nome_lower:
        ea = _txt(p.get("equipe_a"), "").lower()
        eb = _txt(p.get("equipe_b"), "").lower()
        if equipe_nome_lower not in {ea, eb}:
            return None

    return p


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
                instagram = _primeiro_valor(atleta, "instagram", padrao="-")
                foto_status = "com foto" if _primeiro_valor(atleta, "foto_atleta", "foto", "foto_url", padrao="") else "sem foto"
                linhas.append(f"{pos}. Nº {numero} | {nome} | Doc: {doc} | Nasc.: {nasc} | Instagram: {instagram} | Foto: {foto_status} | Posição: {posicao}")

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

def _montar_relatorio_sem_cache(tipo, competicao_nome, equipe_logada=None, equipe_filtro=None, partida_id=None, quadra_filtro=None):
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




def _montar_relatorio(tipo, competicao_nome, equipe_logada=None, equipe_filtro=None, partida_id=None, quadra_filtro=None):
    """Monta um relatório em um escopo de cache efêmero.

    Isso evita que os dois lados da mesma partida consultem novamente a mesma
    lista de eventos e também reaproveita leituras repetidas dentro da operação.
    """
    with escopo_cache():
        return _montar_relatorio_sem_cache(
            tipo,
            competicao_nome,
            equipe_logada=equipe_logada,
            equipe_filtro=equipe_filtro,
            partida_id=partida_id,
            quadra_filtro=quadra_filtro,
        )
