from flask import Blueprint, render_template, request, redirect, session, url_for, flash, jsonify, make_response
import threading
import time

from banco import (
    criar_tabelas_oficiais,
    conectar,
    listar_competicoes_apontador,
    excluir_apontador_global,
    buscar_oficial_por_cpf,
    listar_partidas,
    buscar_partida_operacional,
    assumir_partida_operacional,
    abandonar_partida_operacional,
    listar_arbitros_competicao,
    salvar_pre_jogo_partida,
    listar_atletas_aprovados_da_equipe,
    atualizar_numero_atleta,
    equipe_ja_conferida,
    marcar_equipe_conferida,
    salvar_papeleta,
    listar_papeleta,
    inicializar_sets_partida,
    registrar_resultado_set,
    salvar_capitao_partida,
    inicializar_jogo_partida,
    buscar_estado_jogo_partida,
    registrar_ponto_partida,
    registrar_wo_partida,
    desfazer_ultima_acao_partida,
    registrar_tempo_partida,
    buscar_tempos_restantes_partida,
    registrar_substituicao_partida,
    registrar_substituicao_excepcional_partida,
    registrar_retardamento_partida,
    registrar_sancao_partida,
    registrar_cartao_verde_partida,
    resumir_fluxo_oficial_partida,
    papeleta_set_esta_completa, verificar_fim_de_set, finalizar_set_e_avancar,
    salvar_sorteio_tiebreak_partida,
    partida_encerrada,
    precisa_tiebreak,
    verificar_fim_partida, encerrar_partida,
    garantir_estado_partida,
    listar_eventos_partida,
    aplicar_capitaes_padrao_partida,
)
from routes.utils import exigir_perfil
from socket_events import (
    emitir_estado_partida,
    emitir_placar_apontador,
    obter_estado_cache,
    atualizar_estado_cache,
)

apontadores_bp = Blueprint("apontadores", __name__)

_CACHE_ARBITROS_COMPETICAO = {}
_CACHE_ATLETAS_EQUIPE = {}


def _listar_arbitros_competicao_cache(competicao):
    chave = (competicao or "").strip()
    if chave not in _CACHE_ARBITROS_COMPETICAO:
        _CACHE_ARBITROS_COMPETICAO[chave] = listar_arbitros_competicao(competicao) or []
    return _CACHE_ARBITROS_COMPETICAO[chave]


def _listar_atletas_aprovados_cache(equipe, competicao):
    chave = ((competicao or "").strip(), (equipe or "").strip())
    if chave not in _CACHE_ATLETAS_EQUIPE:
        _CACHE_ATLETAS_EQUIPE[chave] = listar_atletas_aprovados_da_equipe(equipe, competicao) or []
    return _CACHE_ATLETAS_EQUIPE[chave]


def _limpar_cache_atletas(equipe=None, competicao=None):
    if not equipe or not competicao:
        _CACHE_ATLETAS_EQUIPE.clear()
        return
    _CACHE_ATLETAS_EQUIPE.pop(((competicao or "").strip(), (equipe or "").strip()), None)


# =========================================================
# HELPERS
# =========================================================
def _json_no_cache(payload, status=200):
    resposta = jsonify(payload)
    resposta.status_code = status
    resposta.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"
    return resposta



def _int_seguro(valor, padrao=0):
    try:
        if valor is None or valor == "":
            return padrao
        return int(valor)
    except Exception:
        return padrao


def _limites_operacionais(partida=None, estado=None):
    """
    Centraliza os limites usados pela tela do apontador.
    Prioridade: estado/cache -> partida/regras salvas -> padrão do vôlei.
    """
    partida = partida or {}
    estado = estado or {}

    limite_tempos = _int_seguro(
        estado.get("limite_tempos")
        or estado.get("tempos_limite")
        or partida.get("limite_tempos")
        or partida.get("tempos_limite")
        or partida.get("tempos_por_set")
        or 2,
        2,
    )

    limite_substituicoes = _int_seguro(
        estado.get("limite_substituicoes")
        or estado.get("substituicoes_limite")
        or partida.get("limite_substituicoes")
        or partida.get("substituicoes_limite")
        or partida.get("substituicoes_por_set")
        or 6,
        6,
    )

    return {
        "limite_tempos": max(0, limite_tempos),
        "limite_substituicoes": max(0, limite_substituicoes),
    }


def _contar_eventos_lado(partida_id, competicao, equipe, tipos, set_atual=None):
    """
    Conta ações já salvas para impedir pedidos infinitos.
    Conta por set quando o evento tiver set_numero; se eventos antigos não tiverem,
    ainda assim contabiliza para não liberar infinito.
    """
    equipe = (equipe or "").strip().upper()
    tipos = {str(t or "").strip().lower() for t in (tipos or []) if str(t or "").strip()}
    if equipe not in {"A", "B"} or not tipos:
        return 0

    try:
        eventos = listar_eventos_partida(partida_id, competicao, limite=1500) or []
    except TypeError:
        try:
            eventos = listar_eventos_partida(partida_id, competicao) or []
        except Exception:
            return 0
    except Exception:
        return 0

    total = 0
    for ev in eventos:
        ev_equipe = str(ev.get("equipe") or "").strip().upper()
        if ev_equipe != equipe:
            continue

        ev_set = ev.get("set_numero") or ev.get("set") or ev.get("set_atual")
        if set_atual and ev_set not in (None, ""):
            try:
                if int(ev_set) != int(set_atual):
                    continue
            except Exception:
                pass

        campos = {
            str(ev.get("tipo") or "").strip().lower(),
            str(ev.get("tipo_evento") or "").strip().lower(),
            str(ev.get("fundamento") or "").strip().lower(),
            str(ev.get("resultado") or "").strip().lower(),
            str(ev.get("detalhe") or "").strip().lower(),
            str(ev.get("detalhes") or "").strip().lower(),
        }
        campos = {c for c in campos if c}

        if campos.intersection(tipos):
            total += 1

    return total


def _contadores_operacionais(partida_id, competicao, partida=None, estado=None):
    estado = estado or {}
    partida = partida or {}
    set_atual = _int_seguro(estado.get("set_atual") or partida.get("set_atual") or 1, 1)

    tempos_a = _contar_eventos_lado(partida_id, competicao, "A", {"tempo", "pedido_tempo", "tempo_tecnico"}, set_atual)
    tempos_b = _contar_eventos_lado(partida_id, competicao, "B", {"tempo", "pedido_tempo", "tempo_tecnico"}, set_atual)
    subs_a = _contar_eventos_lado(partida_id, competicao, "A", {"substituicao", "substituição"}, set_atual)
    subs_b = _contar_eventos_lado(partida_id, competicao, "B", {"substituicao", "substituição"}, set_atual)

    return {
        "tempos_a": tempos_a,
        "tempos_b": tempos_b,
        "subs_a": subs_a,
        "subs_b": subs_b,
    }


def _aplicar_regras_e_contadores_estado(partida_id, competicao, estado=None, partida=None):
    estado = dict(estado or {})
    partida = partida or {}

    limites = _limites_operacionais(partida, estado)
    estado["limite_tempos"] = limites["limite_tempos"]
    estado["limite_substituicoes"] = limites["limite_substituicoes"]

    try:
        contadores = _contadores_operacionais(partida_id, competicao, partida=partida, estado=estado)
        # Estes campos representam USADOS no set atual.
        estado["tempos_a"] = contadores["tempos_a"]
        estado["tempos_b"] = contadores["tempos_b"]
        estado["subs_a"] = contadores["subs_a"]
        estado["subs_b"] = contadores["subs_b"]
    except Exception:
        estado.setdefault("tempos_a", 0)
        estado.setdefault("tempos_b", 0)
        estado.setdefault("subs_a", 0)
        estado.setdefault("subs_b", 0)

    # Regras de pontuação para set point/match point no frontend.
    if "pontos_set" not in estado:
        estado["pontos_set"] = (
            partida.get("pontos_set")
            or partida.get("ponto_alvo_set")
            or partida.get("pontos_para_vencer_set")
            or estado.get("ponto_alvo_set")
            or estado.get("pontos_para_vencer_set")
            or 25
        )
    if "pontos_tiebreak" not in estado:
        estado["pontos_tiebreak"] = partida.get("pontos_tiebreak") or estado.get("pontos_tiebreak") or 15
    if "diferenca_minima" not in estado:
        estado["diferenca_minima"] = partida.get("diferenca_minima") or estado.get("diferenca_minima") or 2
    if "sets_para_vencer" not in estado:
        estado["sets_para_vencer"] = partida.get("sets_para_vencer") or estado.get("sets_para_vencer") or 2
    if "sets_tipo" not in estado:
        estado["sets_tipo"] = partida.get("sets_tipo") or estado.get("sets_tipo") or "melhor_de_3"

    return estado


def _validar_limite_operacional(partida_id, competicao, equipe, tipo, partida=None, estado=None):
    equipe = (equipe or "").strip().upper()
    partida = partida or {}
    estado = estado or {}
    estado = _aplicar_regras_e_contadores_estado(partida_id, competicao, estado, partida)

    if tipo == "tempo":
        usados = _int_seguro(estado.get("tempos_a") if equipe == "A" else estado.get("tempos_b"), 0)
        limite = _int_seguro(estado.get("limite_tempos"), 2)
        if usados >= limite:
            return False, f"Limite de tempos atingido para a Equipe {equipe} neste set.", estado

    if tipo == "substituicao":
        usados = _int_seguro(estado.get("subs_a") if equipe == "A" else estado.get("subs_b"), 0)
        limite = _int_seguro(estado.get("limite_substituicoes"), 6)
        if usados >= limite:
            return False, f"Limite de substituições atingido para a Equipe {equipe} neste set.", estado

    return True, "", estado


def _montar_descricao_evento(ev):
    descricao = (ev.get("descricao") or "").strip()
    if descricao:
        return descricao

    partes = []
    tipo_evento = str(ev.get("tipo_evento") or ev.get("tipo") or "").strip()
    equipe = str(ev.get("equipe") or "").strip()
    fundamento = str(ev.get("fundamento") or "").strip()
    resultado = str(ev.get("resultado") or "").strip()
    detalhe = str(ev.get("detalhe") or ev.get("detalhes") or "").strip()
    numero = str(ev.get("numero") or "").strip()
    atleta_nome = str(ev.get("atleta_nome") or "").strip()

    if tipo_evento:
        partes.append(tipo_evento.replace("_", " ").title())
    if equipe:
        partes.append(f"Equipe {equipe}")
    if fundamento:
        partes.append(fundamento.replace("_", " "))
    if resultado:
        partes.append(resultado.replace("_", " "))
    if detalhe:
        partes.append(detalhe.replace("_", " "))
    if numero:
        partes.append(f"#{numero}")
    if atleta_nome:
        partes.append(atleta_nome)

    return " • ".join([p for p in partes if p]) or "Ação registrada"


def _buscar_historico_resumido(partida_id, competicao, limite=5):
    try:
        eventos = listar_eventos_partida(partida_id, competicao, limite=limite) or []
    except TypeError:
        eventos = listar_eventos_partida(partida_id, competicao) or []
        eventos = eventos[:limite]
    except Exception:
        return [], "-"

    historico = [{"descricao": _montar_descricao_evento(ev)} for ev in eventos]
    ultima_acao = historico[0]["descricao"] if historico else "-"
    return historico[:limite], ultima_acao


def _buscar_papeletas_set_atual(partida_id, competicao, partida, estado=None):
    equipe_a = partida.get("equipe_a_operacional")
    equipe_b = partida.get("equipe_b_operacional")
    set_atual = int(partida.get("set_atual") or (estado or {}).get("set_atual") or 1)

    papeleta_a = {}
    papeleta_b = {}

    try:
        if equipe_a:
            dados_a = listar_papeleta(partida_id, competicao, equipe_a, set_atual) or []
            papeleta_a = {row["posicao"]: row["numero"] for row in dados_a}

        if equipe_b:
            dados_b = listar_papeleta(partida_id, competicao, equipe_b, set_atual) or []
            papeleta_b = {row["posicao"]: row["numero"] for row in dados_b}
    except Exception:
        papeleta_a = {}
        papeleta_b = {}

    for i in range(1, 7):
        papeleta_a.setdefault(i, "")
        papeleta_b.setdefault(i, "")

    return equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b


def _rotacao_fallback_por_papeleta(papeleta):
    return [
        papeleta.get(4, ""),
        papeleta.get(3, ""),
        papeleta.get(2, ""),
        papeleta.get(5, ""),
        papeleta.get(6, ""),
        papeleta.get(1, ""),
    ]


def _montar_evolucao_pontos(partida_id, competicao):
    try:
        eventos = listar_eventos_partida(partida_id, competicao, limite=80) or []
    except TypeError:
        try:
            eventos = listar_eventos_partida(partida_id, competicao) or []
        except Exception:
            return []
    except Exception:
        return []

    def chave_ordem(ev):
        return (
            ev.get("id") or 0,
            str(ev.get("criado_em") or "")
        )

    eventos = sorted(eventos, key=chave_ordem)

    evolucao = []

    for ev in eventos:
        tipo_evento = str(ev.get("tipo") or ev.get("tipo_evento") or "").strip().lower()
        equipe = str(ev.get("equipe") or "").strip().upper()

        fundamento = str(ev.get("fundamento") or "").strip().lower()
        resultado = str(ev.get("resultado") or "").strip().lower()
        tipo_lance = str(
            ev.get("tipo_lance")
            or ev.get("detalhe")
            or ev.get("detalhes")
            or ""
        ).strip().lower()

        if equipe not in {"A", "B"}:
            continue

        if tipo_evento not in {"ponto", "pontuacao", "pontuação"}:
            continue

        eh_ponto_proprio = (
            resultado == "ponto"
            or tipo_lance == "ponto"
            or fundamento in {"ataque", "bloqueio", "ace"}
        )

        eh_erro_ou_falta = (
            resultado in {"erro", "falta"}
            or tipo_lance in {"erro", "falta"}
            or fundamento in {
                "erro_saque",
                "erro_geral",
                "rede",
                "invasao",
                "rotacao",
                "conducao",
                "dois_toques",
            }
        )

        if eh_erro_ou_falta:
            evolucao.append("B" if equipe == "A" else "A")
        elif eh_ponto_proprio:
            evolucao.append(equipe)

    return evolucao[-50:]


def _preparar_estado_para_placar(partida_id, competicao, estado=None, partida=None):
    """
    Garante que o payload enviado ao telão sempre tenha:
    - nomes das equipes
    - competição
    - partida_id
    - placar atual
    - evolução ponto a ponto em ordem real
    """
    estado = dict(estado or {})

    if partida is None:
        try:
            partida = buscar_partida_operacional(partida_id, competicao) or {}
        except Exception:
            partida = {}

    estado["competicao"] = estado.get("competicao") or competicao
    estado["partida_id"] = estado.get("partida_id") or partida_id

    estado["equipe_a"] = (
        estado.get("equipe_a")
        or estado.get("equipeA")
        or estado.get("equipe_a_nome")
        or estado.get("nome_equipe_a")
        or estado.get("nome_a")
        or estado.get("time_a")
        or partida.get("equipe_a")
        or partida.get("equipe_a_operacional")
        or ""
    )

    estado["equipe_b"] = (
        estado.get("equipe_b")
        or estado.get("equipeB")
        or estado.get("equipe_b_nome")
        or estado.get("nome_equipe_b")
        or estado.get("nome_b")
        or estado.get("time_b")
        or partida.get("equipe_b")
        or partida.get("equipe_b_operacional")
        or ""
    )

    if "pontos_a" not in estado:
        estado["pontos_a"] = estado.get("placar_a", 0)

    if "pontos_b" not in estado:
        estado["pontos_b"] = estado.get("placar_b", 0)

    if "placar_a" not in estado:
        estado["placar_a"] = estado.get("pontos_a", 0)

    if "placar_b" not in estado:
        estado["placar_b"] = estado.get("pontos_b", 0)

    if "evolucao_pontos" not in estado:
        estado["evolucao_pontos"] = _montar_evolucao_pontos(partida_id, competicao)

    return estado


def _emitir_estado_e_placar(partida_id, competicao, estado=None, partida=None, origem=""):
    estado = dict(estado or {})

    if partida is None:
        try:
            partida = buscar_partida_operacional(partida_id, competicao) or {}
        except Exception:
            partida = {}

    estado.setdefault("competicao", competicao)
    estado.setdefault("partida_id", partida_id)

    estado.setdefault("equipe_a", partida.get("equipe_a_operacional") or partida.get("equipe_a") or "")
    estado.setdefault("equipe_b", partida.get("equipe_b_operacional") or partida.get("equipe_b") or "")

    estado.setdefault("pontos_a", estado.get("placar_a", 0))
    estado.setdefault("pontos_b", estado.get("placar_b", 0))
    estado.setdefault("placar_a", estado.get("pontos_a", 0))
    estado.setdefault("placar_b", estado.get("pontos_b", 0))

    estado.setdefault("historico", estado.get("historico") or [])
    estado.setdefault("scout", estado.get("scout") or {})

    estado = _aplicar_regras_e_contadores_estado(partida_id, competicao, estado, partida)

    apontador_login = (
        session.get("usuario")
        or estado.get("apontador")
        or estado.get("apontador_login")
        or estado.get("operador_login")
        or partida.get("operador_login")
        or ""
    )

    if apontador_login:
        estado["apontador"] = apontador_login

    try:
        atualizar_estado_cache(partida_id, estado)
        emitir_estado_partida(partida_id, estado)
        if apontador_login:
            emitir_placar_apontador(apontador_login, partida_id, estado)
    except Exception as e:
        print(f"ERRO emitir estado/placar {origem}:", e, flush=True)

    return estado


# =========================================================
# CONSULTAS BÁSICAS
# =========================================================
def listar_apontadores():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    o.nome,
                    o.cpf,
                    a.ativo,
                    a.primeiro_acesso
                FROM apontadores_acesso a
                JOIN oficiais o ON o.cpf = a.cpf
                ORDER BY o.nome
            """)
            return cur.fetchall()


@apontadores_bp.route("/apontadores")
@exigir_perfil("superadmin")
def apontadores():
    criar_tabelas_oficiais()
    lista = listar_apontadores()

    return render_template(
        "apontadores.html",
        apontadores=lista
    )


@apontadores_bp.route("/apontadores/excluir/<cpf>", methods=["POST"])
@exigir_perfil("superadmin")
def excluir_apontador_global_view(cpf):
    excluir_apontador_global(cpf)
    flash("Apontador excluído permanentemente do sistema.", "sucesso")
    return redirect(url_for("apontadores.apontadores"))

@apontadores_bp.route("/apontador")
@exigir_perfil("apontador")
def painel_apontador():
    criar_tabelas_oficiais()

    cpf = session.get("usuario")

    if not cpf:
        flash("CPF do apontador não encontrado na sessão.", "erro")
        return render_template("painel_apontador.html")

    oficial = buscar_oficial_por_cpf(cpf)
    if not oficial:
        flash("Não foi possível localizar o apontador pelo CPF informado.", "erro")
        return render_template("painel_apontador.html")

    competicoes = listar_competicoes_apontador(cpf)

    if not competicoes:
        return render_template("painel_apontador.html")

    if len(competicoes) == 1:
        return render_template(
            "painel_apontador.html",
            competicao_unica=competicoes[0]
        )

    return render_template(
        "painel_apontador.html",
        competicoes=competicoes
    )


@apontadores_bp.route("/apontador/entrar/<competicao>")
@exigir_perfil("apontador")
def entrar_competicao_apontador(competicao):
    session["competicao_apontador"] = competicao

    partidas = listar_partidas(competicao)
    partidas = sorted(partidas, key=lambda x: (x.get("ordem") or 0, x.get("id") or 0))

    return render_template(
        "painel_apontador.html",
        modo_partidas=True,
        competicao_nome=competicao,
        partidas=partidas
    )


# =========================================================
# PRÉ-JOGO
# =========================================================
@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def abrir_pre_jogo_apontador(competicao, partida_id):
    cpf = session.get("usuario")
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("equipe_a_operacional") or partida.get("equipe_b_operacional"):
        try:
            partida = aplicar_capitaes_padrao_partida(partida_id, competicao) or partida
        except Exception:
            pass

    bloqueada_por_outro = (
        partida.get("operador_login")
        and partida.get("operador_login") != cpf
        and (partida.get("status_operacao") or "livre").lower() in {"reservado", "pre_jogo", "em_andamento"}
    )

    arbitros = _listar_arbitros_competicao_cache(competicao)

    equipe_a_conferida = False
    equipe_b_conferida = False

    equipe_a_operacional = partida.get("equipe_a_operacional")
    equipe_b_operacional = partida.get("equipe_b_operacional")

    if equipe_a_operacional:
        equipe_a_conferida = equipe_ja_conferida(competicao, equipe_a_operacional)

    if equipe_b_operacional:
        equipe_b_conferida = equipe_ja_conferida(competicao, equipe_b_operacional)

    precisa_conferencia = False
    if equipe_a_operacional and equipe_b_operacional:
        precisa_conferencia = (not equipe_a_conferida) or (not equipe_b_conferida)

    fluxo = {
        "fase_partida": partida.get("fase_partida") or "pre_jogo",
        "tiebreak_pendente": bool(partida.get("tiebreak_pendente")),
    }

    return render_template(
        "pre_jogo_apontador.html",
        competicao_nome=competicao,
        partida=partida,
        fluxo=fluxo,
        arbitros=arbitros,
        bloqueada_por_outro=bloqueada_por_outro,
        equipe_a_conferida=equipe_a_conferida,
        equipe_b_conferida=equipe_b_conferida,
        precisa_conferencia=precisa_conferencia,
        capitao_a_nome=partida.get("capitao_a_nome"),
        capitao_a_numero=partida.get("capitao_a_numero"),
        capitao_b_nome=partida.get("capitao_b_nome"),
        capitao_b_numero=partida.get("capitao_b_numero"),
        pre_jogo_bloqueado=(fluxo.get("fase_partida") != "pre_jogo"),
        tie_break_pendente=bool(fluxo.get("tiebreak_pendente")),
    )


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/assumir", methods=["POST"])
@exigir_perfil("apontador")
def assumir_partida_view(competicao, partida_id):
    cpf = session.get("usuario")
    oficial = buscar_oficial_por_cpf(cpf)

    if not oficial:
        flash("Apontador não localizado.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    ok, msg = assumir_partida_operacional(
        partida_id,
        competicao,
        cpf,
        oficial["nome"]
    )

    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id, rapido="1"))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/abandonar", methods=["POST"])
@exigir_perfil("apontador")
def abandonar_partida_view(competicao, partida_id):
    cpf = session.get("usuario")
    ok, msg = abandonar_partida_operacional(partida_id, competicao, cpf)
    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_pre_jogo_view(competicao, partida_id):
    cpf = session.get("usuario")

    arbitro_1_cpf = request.form.get("arbitro_1_cpf", "").strip()
    arbitro_2_cpf = request.form.get("arbitro_2_cpf", "").strip()
    vencedor_sorteio = request.form.get("sorteio_vencedor", "").strip()
    escolha_sorteio = request.form.get("sorteio_escolha", "").strip()
    lado_esquerdo = request.form.get("lado_esquerdo", "").strip()
    saque_inicial = request.form.get("saque_inicial", "").strip()

    ok, msg = salvar_pre_jogo_partida(
        partida_id=partida_id,
        competicao=competicao,
        operador_login=cpf,
        arbitro_1_cpf=arbitro_1_cpf,
        arbitro_2_cpf=arbitro_2_cpf,
        sorteio_vencedor=vencedor_sorteio,
        sorteio_escolha=escolha_sorteio,
        saque_inicial=saque_inicial,
        lado_esquerdo=lado_esquerdo,
    )

    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id, rapido="1"))


@apontadores_bp.route("/apontador/tiebreak/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def abrir_tiebreak_view(competicao, partida_id):
    cpf = session.get("usuario")
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode fazer o sorteio do tie-break.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    fluxo = resumir_fluxo_oficial_partida(partida_id, competicao, partida=partida) or {}
    if fluxo.get("fase_partida") != "tiebreak_sorteio":
        flash("O sorteio do tie-break não está liberado neste momento.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    return render_template(
        "tiebreak_sorteio_apontador.html",
        competicao_nome=competicao,
        partida=partida,
        fluxo=fluxo,
    )


@apontadores_bp.route("/apontador/tiebreak/<competicao>/<int:partida_id>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_tiebreak_view(competicao, partida_id):
    cpf = session.get("usuario")

    vencedor_sorteio = request.form.get("sorteio_vencedor", "").strip()
    escolha_sorteio = request.form.get("sorteio_escolha", "").strip()
    saque_tiebreak = request.form.get("saque_tiebreak", "").strip()
    lado_esquerdo_tiebreak = request.form.get("lado_esquerdo_tiebreak", "").strip()

    ok, msg = salvar_sorteio_tiebreak_partida(
        partida_id=partida_id,
        competicao=competicao,
        operador_login=cpf,
        sorteio_vencedor=vencedor_sorteio,
        sorteio_escolha=escolha_sorteio,
        saque_tiebreak=saque_tiebreak,
        lado_esquerdo_tiebreak=lado_esquerdo_tiebreak,
    )

    flash(msg, "sucesso" if ok else "erro")
    if ok:
        return redirect(url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id))
    return redirect(url_for("apontadores.abrir_tiebreak_view", competicao=competicao, partida_id=partida_id))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/conferencia/<lado>")
@exigir_perfil("apontador")
def conferencia_equipe_view(competicao, partida_id, lado):
    cpf = session.get("usuario")
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode fazer a conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    if lado not in {"A", "B"}:
        flash("Lado inválido para conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    equipe = partida.get("equipe_a_operacional") if lado == "A" else partida.get("equipe_b_operacional")
    if not equipe:
        flash("Salve primeiro o sorteio para definir as equipes operacionais.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    atletas = _listar_atletas_aprovados_cache(equipe, competicao)

    return render_template(
        "conferencia_equipe.html",
        competicao_nome=competicao,
        partida=partida,
        lado=lado,
        equipe_nome=equipe,
        atletas=atletas,
    )


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/conferencia/<lado>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_conferencia_equipe_view(competicao, partida_id, lado):
    cpf = session.get("usuario")
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode salvar a conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    if lado not in {"A", "B"}:
        flash("Lado inválido para conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    equipe = partida.get("equipe_a_operacional") if lado == "A" else partida.get("equipe_b_operacional")
    if not equipe:
        flash("Equipe não definida para conferência.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    ids = request.form.getlist("atleta_id")
    houve_erro = False

    for atleta_id in ids:
        numero = request.form.get(f"numero_{atleta_id}", "").strip()
        ok, msg = atualizar_numero_atleta(atleta_id, numero)
        if not ok:
            houve_erro = True
            flash(msg, "erro")

    if not houve_erro:
        _limpar_cache_atletas(equipe, competicao)
        marcar_equipe_conferida(competicao, equipe)
        flash("Conferência salva com sucesso.", "sucesso")

    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/capitao/<lado>")
@exigir_perfil("apontador")
def definir_capitao_view(competicao, partida_id, lado):
    cpf = session.get("usuario")
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if partida.get("operador_login") != cpf:
        flash("Somente o operador da partida pode definir o capitão.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    lado = (lado or "").strip().upper()
    if lado not in {"A", "B"}:
        flash("Lado inválido para capitão.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    equipe = partida.get("equipe_a_operacional") if lado == "A" else partida.get("equipe_b_operacional")
    if not equipe:
        flash("Equipe operacional ainda não definida.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    atletas = _listar_atletas_aprovados_cache(equipe, competicao)
    atletas = [a for a in atletas if a.get("numero") not in (None, "")]
    atleta_atual_id = partida.get("capitao_a_id") if lado == "A" else partida.get("capitao_b_id")

    return render_template(
        "definir_capitao.html",
        competicao_nome=competicao,
        partida=partida,
        lado=lado,
        equipe_nome=equipe,
        atletas=atletas,
        atleta_atual_id=atleta_atual_id,
    )


@apontadores_bp.route("/apontador/pre-jogo/<competicao>/<int:partida_id>/capitao/<lado>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_capitao_view(competicao, partida_id, lado):
    cpf = session.get("usuario")
    atleta_id = request.form.get("atleta_id", "").strip()

    ok, msg = salvar_capitao_partida(partida_id, competicao, cpf, lado, atleta_id)
    flash(msg, "sucesso" if ok else "erro")
    return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))


# =========================================================
# PAPELETA
# =========================================================
@apontadores_bp.route("/apontador/papeleta/<competicao>/<int:partida_id>", methods=["GET"])
@exigir_perfil("apontador")
def papeleta_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    fase = (partida.get("fase_partida") or "papeleta").strip().lower()

    if fase == "encerrado":
        flash("A partida já está finalizada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    if fase == "pre_jogo":
        flash("Finalize primeiro o pré-jogo para acessar a papeleta.", "erro")
        return redirect(url_for("apontadores.abrir_pre_jogo_apontador", competicao=competicao, partida_id=partida_id))

    if fase == "tiebreak_sorteio":
        flash("Antes do tie-break, faça o sorteio específico do set decisivo.", "erro")
        return redirect(url_for("apontadores.abrir_tiebreak_view", competicao=competicao, partida_id=partida_id))

    if fase == "jogo":
        return redirect(url_for("apontadores.jogo_view", competicao=competicao, partida_id=partida_id))

    equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b = _buscar_papeletas_set_atual(
        partida_id, competicao, partida
    )

    atletas_a = _listar_atletas_aprovados_cache(equipe_a, competicao) if equipe_a else []
    atletas_b = _listar_atletas_aprovados_cache(equipe_b, competicao) if equipe_b else []

    atletas_a = [a for a in atletas_a if a.get("numero")]
    atletas_b = [a for a in atletas_b if a.get("numero")]

    fluxo = {
        "fase_partida": fase,
        "papeleta_a_completa": all(papeleta_a.get(i) for i in range(1, 7)),
        "papeleta_b_completa": all(papeleta_b.get(i) for i in range(1, 7)),
        "set_atual": set_atual,
    }

    return render_template(
        "papeleta_apontador.html",
        competicao_nome=competicao,
        partida=partida,
        equipe_a=equipe_a,
        equipe_b=equipe_b,
        atletas_a=atletas_a,
        atletas_b=atletas_b,
        papeleta_a=papeleta_a,
        papeleta_b=papeleta_b,
        fluxo=fluxo,
    )


@apontadores_bp.route("/apontador/papeleta/<competicao>/<int:partida_id>", methods=["POST"])
@exigir_perfil("apontador")
def salvar_papeleta_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    equipe_a = partida.get("equipe_a_operacional") or partida.get("equipe_a")
    equipe_b = partida.get("equipe_b_operacional") or partida.get("equipe_b")
    set_atual = int(partida.get("set_atual") or 1)

    atletas_cache = {}

    def montar_dados(lado, equipe):
        if not equipe:
            return {}

        atletas = atletas_cache.get(equipe)
        if atletas is None:
            atletas = _listar_atletas_aprovados_cache(equipe, competicao)
            atletas_cache[equipe] = atletas

        mapa = {
            int(a.get("numero")): a
            for a in atletas
            if a.get("numero") not in (None, "")
        }

        dados = {}

        for pos in [1, 2, 3, 4, 5, 6]:
            valor = (request.form.get(f"{lado}_{pos}") or "").strip()
            if not valor:
                continue

            try:
                numero = int(valor)
            except Exception:
                continue

            atleta = mapa.get(numero)
            if atleta:
                dados[pos] = atleta

        return dados

    dados_a = montar_dados("A", equipe_a)
    dados_b = montar_dados("B", equipe_b)

    if len(dados_a) != 6 or len(dados_b) != 6:
        flash("Preencha as 6 posições das duas equipes.", "erro")
        return redirect(url_for("apontadores.papeleta_view", competicao=competicao, partida_id=partida_id))

    salvar_papeleta(partida_id, competicao, equipe_a, set_atual, dados_a)
    salvar_papeleta(partida_id, competicao, equipe_b, set_atual, dados_b)

    rotacao_a = [
        str(dados_a[4].get("numero") or ""),
        str(dados_a[3].get("numero") or ""),
        str(dados_a[2].get("numero") or ""),
        str(dados_a[5].get("numero") or ""),
        str(dados_a[6].get("numero") or ""),
        str(dados_a[1].get("numero") or ""),
    ]

    rotacao_b = [
        str(dados_b[4].get("numero") or ""),
        str(dados_b[3].get("numero") or ""),
        str(dados_b[2].get("numero") or ""),
        str(dados_b[5].get("numero") or ""),
        str(dados_b[6].get("numero") or ""),
        str(dados_b[1].get("numero") or ""),
    ]

    try:
        inicializar_jogo_partida(partida_id, competicao)
    except Exception as e:
        print("ERRO inicializar_jogo_partida:", repr(e), flush=True)

    estado = {
        "ok": True,
        "competicao": competicao,
        "partida_id": partida_id,
        "equipe_a": equipe_a or "",
        "equipe_b": equipe_b or "",
        "pontos_a": int(partida.get("pontos_a") or 0),
        "pontos_b": int(partida.get("pontos_b") or 0),
        "placar_a": int(partida.get("pontos_a") or 0),
        "placar_b": int(partida.get("pontos_b") or 0),
        "sets_a": int(partida.get("sets_a") or 0),
        "sets_b": int(partida.get("sets_b") or 0),
        "set_atual": set_atual,
        "saque_atual": partida.get("saque_atual") or partida.get("saque_inicial") or "",
        "rotacao_a": rotacao_a,
        "rotacao_b": rotacao_b,
        "historico": [{"descricao": "Jogo iniciado"}],
        "ultima_acao": "Jogo iniciado",
        "fase_partida": "jogo",
        "status_jogo": "em_andamento",
    }

    _emitir_estado_e_placar(partida_id, competicao, estado, partida=partida, origem="PAPELETA")

    flash("Papeleta salva com sucesso.", "sucesso")
    return redirect(url_for("apontadores.jogo_view", competicao=competicao, partida_id=partida_id))


# =========================================================
# JOGO
# =========================================================
@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>", methods=["GET"])
@exigir_perfil("apontador")
def jogo_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        flash("Partida não encontrada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    status_jogo = (partida.get("status_jogo") or "").strip().lower()

    # Só inicializa se ainda não estiver em jogo.
    if status_jogo not in {"em_andamento", "entre_sets", "finalizada"}:
        partida = inicializar_jogo_partida(partida_id, competicao) or partida

    if (partida.get("status_jogo") or "").strip().lower() == "finalizada":
        flash("A partida já está finalizada.", "erro")
        return redirect(url_for("apontadores.entrar_competicao_apontador", competicao=competicao))

    estado = buscar_estado_jogo_partida(partida_id, competicao) or {}

    equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b = _buscar_papeletas_set_atual(
        partida_id,
        competicao,
        partida,
        estado
    )

    try:
        atletas_a = _listar_atletas_aprovados_cache(equipe_a, competicao) if equipe_a else []
        atletas_b = _listar_atletas_aprovados_cache(equipe_b, competicao) if equipe_b else []
    except Exception:
        atletas_a = []
        atletas_b = []

    atletas_a = [a for a in atletas_a if a.get("numero")]
    atletas_b = [a for a in atletas_b if a.get("numero")]

    if not estado.get("rotacao_a"):
        estado["rotacao_a"] = _rotacao_fallback_por_papeleta(papeleta_a)

    if not estado.get("rotacao_b"):
        estado["rotacao_b"] = _rotacao_fallback_por_papeleta(papeleta_b)

    # Histórico inicial apenas para abrir a tela já preenchida.
    try:
        historico_inicial, ultima_acao = _buscar_historico_resumido(partida_id, competicao, limite=5)
    except Exception:
        historico_inicial, ultima_acao = [], estado.get("ultima_acao") or "-"

    estado["historico"] = historico_inicial
    estado["ultima_acao"] = ultima_acao or estado.get("ultima_acao") or "-"

    estado = _emitir_estado_e_placar(
        partida_id,
        competicao,
        estado,
        partida=partida,
        origem="JOGO_VIEW"
    )

    resposta = make_response(render_template(
        "jogo_apontador.html",
        competicao_nome=competicao,
        partida=partida,
        estado=estado,
        papeleta_a=papeleta_a,
        papeleta_b=papeleta_b,
        atletas_a=atletas_a,
        atletas_b=atletas_b,
        modo_operacao=(partida.get("modo_operacao") or "simples"),
    ))

    resposta.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"

    return resposta


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/ponto", methods=["POST"])
@exigir_perfil("apontador")
def ponto_view(competicao, partida_id):
    print("🔥 ENTROU NO PONTO_VIEW POST")

    try:
        import threading
        from socket_events import (
            obter_estado_cache,
            atualizar_estado_cache,
            emitir_estado_partida,
            emitir_placar_apontador
        )
        from banco import girar_rotacao_oficial, registrar_ponto_partida

        corpo = request.get_json(silent=True) or {}

        # =========================
        # 🔥 VALIDAÇÕES (INALTERADAS)
        # =========================
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        fundamento = (request.form.get("fundamento") or corpo.get("fundamento") or "").strip().lower()
        resultado = (request.form.get("resultado") or corpo.get("resultado") or "").strip().lower()
        tipo_lance = (request.form.get("tipo_lance") or corpo.get("tipo_lance") or "").strip().lower()
        detalhe_lance = (request.form.get("detalhe_lance") or corpo.get("detalhe_lance") or "").strip().lower()
        tipo_erro = (request.form.get("tipo_erro") or corpo.get("tipo_erro") or "").strip().lower()
        atleta_numero = str(request.form.get("atleta_numero") or corpo.get("atleta_numero") or "").strip()
        atleta_nome = (request.form.get("atleta_nome") or corpo.get("atleta_nome") or "").strip()
        atleta_label = (request.form.get("atleta_label") or corpo.get("atleta_label") or "").strip()

        if not tipo_lance:
            return _json_no_cache({"ok": False, "mensagem": "Selecione se foi ponto, erro ou falta."}, 400)

        if tipo_lance not in {"ponto", "erro", "falta"}:
            return _json_no_cache({"ok": False, "mensagem": "Tipo de lance inválido."}, 400)

        if not detalhe_lance and not resultado and not fundamento:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o detalhe da jogada."}, 400)

        detalhe_final = (detalhe_lance or tipo_erro or resultado or fundamento).strip().lower()

        detalhes_validos = {
            "ponto": {"ataque", "bloqueio", "ace"},
            "erro": {"erro_saque", "erro_geral"},
            "falta": {"rede", "invasao", "rotacao", "conducao", "dois_toques"},
        }

        if detalhe_final not in detalhes_validos[tipo_lance]:
            return _json_no_cache({"ok": False, "mensagem": "Detalhe da jogada inválido."}, 400)

        exige_atleta = detalhe_final in {"ataque", "bloqueio", "ace"}

        if exige_atleta and not atleta_numero:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o atleta da jogada."}, 400)

        if not exige_atleta:
            atleta_numero = ""
            atleta_nome = ""
            atleta_label = ""

        detalhes_evento = {
            "fundamento": detalhe_final,
            "resultado": tipo_lance,
            "tipo_lance": tipo_lance,
            "detalhe_lance": detalhe_final,
            "tipo_erro": tipo_erro,
            "atleta_numero": atleta_numero,
            "atleta_nome": atleta_nome,
            "atleta_label": atleta_label,
        }

        # =========================
        # ⚡ CACHE (ULTRA RÁPIDO)
        # =========================
        estado = obter_estado_cache(partida_id)

        if not estado:
            estado = buscar_estado_jogo_partida(partida_id, competicao) or {}

        # =========================
        # ⚡ ATUALIZA LOCAL (INSTANTÂNEO)
        # =========================
        estado["pontos_a"] = int(estado.get("pontos_a", 0))
        estado["pontos_b"] = int(estado.get("pontos_b", 0))

        if equipe == "A":
            estado["pontos_a"] += 1
        else:
            estado["pontos_b"] += 1

        saque = estado.get("saque_atual")

        if saque != equipe:
            estado["saque_atual"] = equipe

            if equipe == "A":
                estado["rotacao_a"] = girar_rotacao_oficial(estado.get("rotacao_a"))
            else:
                estado["rotacao_b"] = girar_rotacao_oficial(estado.get("rotacao_b"))

        # =========================
        # 🏁 FIM DE SET / PARTIDA NO CACHE (NÃO ESPERA BANCO)
        # =========================
        pontos_set = int(estado.get("pontos_set") or estado.get("ponto_alvo_set") or estado.get("pontos_para_vencer_set") or 21)
        diferenca_minima = int(estado.get("diferenca_minima") or 2)
        sets_para_vencer = int(estado.get("sets_para_vencer") or 2)
        estado["sets_a"] = int(estado.get("sets_a") or 0)
        estado["sets_b"] = int(estado.get("sets_b") or 0)
        estado["set_atual"] = int(estado.get("set_atual") or 1)

        fim_set = (
            (estado["pontos_a"] >= pontos_set or estado["pontos_b"] >= pontos_set)
            and abs(estado["pontos_a"] - estado["pontos_b"]) >= diferenca_minima
        )

        if fim_set:
            vencedor_set = "A" if estado["pontos_a"] > estado["pontos_b"] else "B"
            if vencedor_set == "A":
                estado["sets_a"] += 1
            else:
                estado["sets_b"] += 1

            fim_jogo = estado["sets_a"] >= sets_para_vencer or estado["sets_b"] >= sets_para_vencer
            estado["fim_set"] = True
            estado["set_finalizado"] = True
            estado["vencedor_set"] = vencedor_set

            if fim_jogo:
                estado["fim_jogo"] = True
                estado["partida_finalizada"] = True
                estado["fase_partida"] = "encerrado"
                estado["status_jogo"] = "finalizada"
                estado["vencedor_partida"] = "A" if estado["sets_a"] > estado["sets_b"] else "B"
                estado["ultima_acao"] = "Partida finalizada"
            else:
                estado["fim_jogo"] = False
                estado["partida_finalizada"] = False
                estado["fase_partida"] = "entre_sets"
                estado["status_jogo"] = "entre_sets"
                estado["ultima_acao"] = "Set finalizado"
        else:
            estado["fim_set"] = False
            estado["fim_jogo"] = False
            estado["partida_finalizada"] = False
            estado["fase_partida"] = "jogo"
            estado["status_jogo"] = "em_andamento"
            estado["ultima_acao"] = "Ponto registrado"

        historico = estado.get("historico") or []
        if not isinstance(historico, list):
            historico = []
        descricao_ponto = f"{equipe} • {tipo_lance} • {detalhe_final}" + (f" • {atleta_label or atleta_nome or atleta_numero}" if (atleta_label or atleta_nome or atleta_numero) else "")
        historico.insert(0, {"descricao": estado.get("ultima_acao") if fim_set else descricao_ponto})
        estado["historico"] = historico[:5]

        # =========================
        # ⚡ ATUALIZA CACHE
        # =========================
        atualizar_estado_cache(partida_id, estado)

        # =========================
        # ⚡ SOCKET IMEDIATO
        # =========================
        apontador = session.get("usuario") or ""
        estado["apontador"] = apontador

        emitir_estado_partida(partida_id, estado)
        emitir_placar_apontador(apontador, partida_id, estado)

        # =========================
        # 💾 BANCO EM BACKGROUND (SEM TRAVAR)
        # =========================
        def salvar():
            try:
                registrar_ponto_partida(
                    partida_id=partida_id,
                    competicao=competicao,
                    equipe=equipe,
                    tipo="ponto",
                    detalhes=detalhes_evento
                )
            except Exception as e:
                print("ERRO salvar ponto:", e)

        threading.Thread(target=salvar).start()

        return _json_no_cache({
            "ok": True,
            "mensagem": "Ponto registrado com sucesso.",
            **estado
        })

    except Exception as e:
        print("ERRO ponto_view:", e)
        return _json_no_cache({
            "ok": False,
            "mensagem": f"Erro ao registrar ponto: {e}"
        }, 500)
    

@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/wo", methods=["POST"])
@exigir_perfil("apontador")
def wo_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}

        equipe_vencedora = (
            request.form.get("equipe_vencedora")
            or corpo.get("equipe_vencedora")
            or ""
        ).strip().upper()

        if equipe_vencedora not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        ok, retorno = registrar_wo_partida(
            partida_id=partida_id,
            competicao=competicao,
            vencedor_lado=equipe_vencedora
        )

        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = retorno if isinstance(retorno, dict) else {}

        estado = _emitir_estado_e_placar(
            partida_id,
            competicao,
            estado,
            origem="WO"
        )

        return _json_no_cache({
            "ok": True,
            "mensagem": "Partida encerrada por WO.",
            **estado
        })

    except Exception as e:
        return _json_no_cache({
            "ok": False,
            "mensagem": f"Erro ao registrar WO: {e}"
        }, 500)
    

@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/desfazer", methods=["POST"])
@exigir_perfil("apontador")
def desfazer_acao_view(competicao, partida_id):
    try:
        ok, retorno = desfazer_ultima_acao_partida(partida_id, competicao)

        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = retorno if isinstance(retorno, dict) else {}

        estado["desfazer"] = True

        estado = _emitir_estado_e_placar(partida_id, competicao, estado, origem="DESFAZER")

        return _json_no_cache({
            "ok": True,
            **estado
        })

    except Exception as e:
        return _json_no_cache({
            "ok": False,
            "mensagem": f"Erro ao desfazer ação: {e}"
        }, 500)


def _descricao_acao(tipo, equipe='', payload=None):
    payload = payload or {}
    equipe_txt = f"Equipe {equipe}" if equipe else "Equipe"
    if tipo == "tempo":
        return f"Tempo solicitado - {equipe_txt}"
    if tipo == "substituicao":
        return f"{equipe_txt} • substituição • {payload.get('numero_sai', '')}>{payload.get('numero_entra', '')}"
    if tipo == "substituicao_excepcional":
        return f"{equipe_txt} • substituição excepcional • {payload.get('numero_sai', '')}>{payload.get('numero_entra', '')}"
    if tipo == "retardamento":
        return f"{equipe_txt} • retardamento"
    if tipo == "sancao":
        return f"{equipe_txt} • sanção • {payload.get('tipo_sancao') or payload.get('sancao') or ''}"
    if tipo == "cartao_verde":
        return f"{equipe_txt} • cartão verde"
    return payload.get('descricao') or "Ação registrada"


def _normalizar_estado_pos_acao(partida_id, competicao, retorno=None, origem="", acao=None):
    estado = retorno if isinstance(retorno, dict) else {}
    cache = obter_estado_cache(partida_id) or {}

    # Primeiro preserva o estado que já está na tela/cache para não zerar placar/rotação.
    base = dict(cache)
    base.update(estado)
    estado = base

    historico = estado.get("historico") or cache.get("historico") or []
    if not isinstance(historico, list):
        historico = []

    if acao:
        desc = acao.get("descricao") if isinstance(acao, dict) else str(acao)
        if desc and not (historico and isinstance(historico[0], dict) and historico[0].get("descricao") == desc):
            historico.insert(0, {"descricao": desc})

    if not historico and estado.get("ultima_acao"):
        historico = [{"descricao": estado.get("ultima_acao")}]

    estado["historico"] = historico[:5]
    estado["ultima_acao"] = estado.get("ultima_acao") or (
        estado["historico"][0].get("descricao") if estado["historico"] and isinstance(estado["historico"][0], dict) else "-"
    )

    try:
        partida = buscar_partida_operacional(partida_id, competicao) or {}
    except Exception:
        partida = {}

    return _emitir_estado_e_placar(partida_id, competicao, estado, partida=partida, origem=origem)


def _salvar_async(nome, funcao, *args, **kwargs):
    def executar():
        try:
            ok, retorno = funcao(*args, **kwargs)
            if not ok:
                print(f"ERRO async {nome}: {retorno}", flush=True)
        except Exception as e:
            print(f"ERRO async {nome}:", repr(e), flush=True)

    threading.Thread(target=executar, daemon=True).start()


def _acao_rapida(partida_id, competicao, tipo, equipe='', payload=None):
    payload = payload or {}
    equipe = (equipe or '').strip().upper()
    estado = dict(obter_estado_cache(partida_id) or {})

    if not estado:
        try:
            estado = buscar_estado_jogo_partida(partida_id, competicao) or {}
        except Exception:
            estado = {}

    descricao = payload.get("descricao") or _descricao_acao(tipo, equipe, payload)
    historico = estado.get("historico") or []
    if not isinstance(historico, list):
        historico = []
    historico.insert(0, {"descricao": descricao})
    estado["historico"] = historico[:5]
    estado["ultima_acao"] = descricao

    # Atualizações visuais imediatas sem depender do banco.
    if tipo == "tempo":
        campo = "tempos_a" if equipe == "A" else "tempos_b"
        try:
            estado[campo] = max(0, int(estado.get(campo, 0)) + 1)
        except Exception:
            estado[campo] = 1
    elif tipo == "substituicao":
        campo = "subs_a" if equipe == "A" else "subs_b"
        try:
            estado[campo] = int(estado.get(campo, 0)) + 1
        except Exception:
            estado[campo] = 1
        numero_sai = str(payload.get("numero_sai") or '').strip()
        numero_entra = str(payload.get("numero_entra") or '').strip()
        rot_key = "rotacao_a" if equipe == "A" else "rotacao_b"
        rot = list(estado.get(rot_key) or [])
        if numero_sai and numero_entra and len(rot) == 6:
            estado[rot_key] = [numero_entra if str(n) == numero_sai else n for n in rot]
    elif tipo == "cartao_verde":
        campo = "cartoes_verdes_a" if equipe == "A" else "cartoes_verdes_b"
        lista = estado.get(campo) or []
        if not isinstance(lista, list):
            lista = []
        lista.append({"tipo_pessoa": payload.get("tipo_pessoa"), "numero": payload.get("numero"), "nome": payload.get("nome")})
        estado[campo] = lista
    elif tipo == "sancao":
        campo = "sancoes_a" if equipe == "A" else "sancoes_b"
        lista = estado.get(campo) or []
        if not isinstance(lista, list):
            lista = []
        lista.append({"tipo_pessoa": payload.get("tipo_pessoa"), "numero": payload.get("numero"), "nome": payload.get("nome"), "tipo_sancao": payload.get("tipo_sancao")})
        estado[campo] = lista

    return _normalizar_estado_pos_acao(partida_id, competicao, estado, origem=f"{tipo.upper()}_RAPIDO", acao={"descricao": descricao})


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/tempo", methods=["POST"])
@exigir_perfil("apontador")
def registrar_tempo_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        partida = buscar_partida_operacional(partida_id, competicao) or {}
        estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
        permitido, mensagem_limite, estado_atual = _validar_limite_operacional(
            partida_id, competicao, equipe, "tempo", partida=partida, estado=estado_atual
        )
        if not permitido:
            estado_atual = _emitir_estado_e_placar(partida_id, competicao, estado_atual, partida=partida, origem="TEMPO_LIMITE")
            return _json_no_cache({"ok": False, "mensagem": mensagem_limite, **estado_atual}, 400)

        ok, retorno = registrar_tempo_partida(partida_id, competicao, equipe)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = _normalizar_estado_pos_acao(partida_id, competicao, retorno, origem="TEMPO")
        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar tempo: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/substituicao", methods=["POST"])
@exigir_perfil("apontador")
def registrar_substituicao_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        numero_sai = str(request.form.get("numero_sai") or corpo.get("numero_sai") or "").strip()
        numero_entra = str(request.form.get("numero_entra") or corpo.get("numero_entra") or "").strip()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        if not numero_sai or not numero_entra:
            return _json_no_cache({"ok": False, "mensagem": "Selecione quem sai e quem entra."}, 400)

        partida = buscar_partida_operacional(partida_id, competicao) or {}
        estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
        permitido, mensagem_limite, estado_atual = _validar_limite_operacional(
            partida_id, competicao, equipe, "substituicao", partida=partida, estado=estado_atual
        )
        if not permitido:
            estado_atual = _emitir_estado_e_placar(partida_id, competicao, estado_atual, partida=partida, origem="SUBSTITUICAO_LIMITE")
            return _json_no_cache({"ok": False, "mensagem": mensagem_limite, **estado_atual}, 400)

        ok, retorno = registrar_substituicao_partida(partida_id, competicao, equipe, numero_sai, numero_entra)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = _normalizar_estado_pos_acao(partida_id, competicao, retorno, origem="SUBSTITUICAO")
        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar substituição: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/substituicao-excepcional", methods=["POST"])
@exigir_perfil("apontador")
def registrar_substituicao_excepcional_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        numero_sai = str(request.form.get("numero_sai") or corpo.get("numero_sai") or "").strip()
        numero_entra = str(request.form.get("numero_entra") or corpo.get("numero_entra") or "").strip()
        motivo = str(request.form.get("motivo") or corpo.get("motivo") or "").strip()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        if not numero_sai or not numero_entra:
            return _json_no_cache({"ok": False, "mensagem": "Selecione quem sai e quem entra."}, 400)

        try:
            ok, retorno = registrar_substituicao_excepcional_partida(
                partida_id, competicao, equipe, numero_sai, numero_entra, motivo
            )
        except TypeError:
            ok, retorno = registrar_substituicao_excepcional_partida(
                partida_id, competicao, equipe, numero_sai, numero_entra
            )

        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = _normalizar_estado_pos_acao(partida_id, competicao, retorno, origem="SUBSTITUICAO_EXCEPCIONAL")
        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar substituição excepcional: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/retardamento", methods=["POST"])
@exigir_perfil("apontador")
def registrar_retardamento_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}
        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        ok, retorno = registrar_retardamento_partida(partida_id, competicao, equipe)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = _normalizar_estado_pos_acao(partida_id, competicao, retorno, origem="RETARDAMENTO")
        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar retardamento: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/sancao", methods=["POST"])
@exigir_perfil("apontador")
def registrar_sancao_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}

        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        tipo_pessoa = (request.form.get("tipo_pessoa") or corpo.get("tipo_pessoa") or "").strip().lower()
        alvo = (request.form.get("alvo") or corpo.get("alvo") or "").strip()
        sancao = (request.form.get("sancao") or corpo.get("sancao") or "").strip().lower()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        if tipo_pessoa not in {"jogador", "comissao"}:
            return _json_no_cache({"ok": False, "mensagem": "Tipo de pessoa inválido."}, 400)

        if not alvo:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o alvo da sanção."}, 400)

        if not sancao:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o tipo de sanção."}, 400)

        ok, retorno = registrar_sancao_partida(partida_id, competicao, equipe, tipo_pessoa, alvo, sancao)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = _normalizar_estado_pos_acao(partida_id, competicao, retorno, origem="SANCAO")
        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar sanção: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/cartao-verde", methods=["POST"])
@exigir_perfil("apontador")
def registrar_cartao_verde_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}

        equipe = (request.form.get("equipe") or corpo.get("equipe") or "").strip().upper()
        tipo_pessoa = (request.form.get("tipo_pessoa") or corpo.get("tipo_pessoa") or "").strip().lower()
        alvo = (request.form.get("alvo") or corpo.get("alvo") or "").strip()

        if equipe not in {"A", "B"}:
            return _json_no_cache({"ok": False, "mensagem": "Equipe inválida."}, 400)

        if tipo_pessoa not in {"jogador", "comissao"}:
            return _json_no_cache({"ok": False, "mensagem": "Tipo de pessoa inválido."}, 400)

        if not alvo:
            return _json_no_cache({"ok": False, "mensagem": "Selecione o alvo do cartão verde."}, 400)

        ok, retorno = registrar_cartao_verde_partida(partida_id, competicao, equipe, tipo_pessoa, alvo)
        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = _normalizar_estado_pos_acao(partida_id, competicao, retorno, origem="CARTAO_VERDE")
        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao registrar cartão verde: {e}"}, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/sincronizar", methods=["POST"])
@exigir_perfil("apontador")
def sincronizar_acao_view(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}
        tipo = (corpo.get("tipo") or "").strip().lower()
        equipe = (corpo.get("equipe") or "").strip().upper()

        if tipo == "tempo":
            partida = buscar_partida_operacional(partida_id, competicao) or {}
            estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
            permitido, mensagem_limite, estado_atual = _validar_limite_operacional(
                partida_id, competicao, equipe, "tempo", partida=partida, estado=estado_atual
            )
            if not permitido:
                estado_atual = _emitir_estado_e_placar(partida_id, competicao, estado_atual, partida=partida, origem="SINCRONIZAR_TEMPO_LIMITE")
                return _json_no_cache({"ok": False, "mensagem": mensagem_limite, **estado_atual}, 400)
            ok, retorno = registrar_tempo_partida(partida_id, competicao, equipe)

        elif tipo == "substituicao":
            partida = buscar_partida_operacional(partida_id, competicao) or {}
            estado_atual = obter_estado_cache(partida_id) or buscar_estado_jogo_partida(partida_id, competicao) or {}
            permitido, mensagem_limite, estado_atual = _validar_limite_operacional(
                partida_id, competicao, equipe, "substituicao", partida=partida, estado=estado_atual
            )
            if not permitido:
                estado_atual = _emitir_estado_e_placar(partida_id, competicao, estado_atual, partida=partida, origem="SINCRONIZAR_SUBSTITUICAO_LIMITE")
                return _json_no_cache({"ok": False, "mensagem": mensagem_limite, **estado_atual}, 400)
            ok, retorno = registrar_substituicao_partida(
                partida_id,
                competicao,
                equipe,
                str(corpo.get("numero_sai") or "").strip(),
                str(corpo.get("numero_entra") or "").strip()
            )

        elif tipo == "substituicao_excepcional":
            try:
                ok, retorno = registrar_substituicao_excepcional_partida(
                    partida_id,
                    competicao,
                    equipe,
                    str(corpo.get("numero_sai") or "").strip(),
                    str(corpo.get("numero_entra") or "").strip(),
                    str(corpo.get("motivo") or "").strip()
                )
            except TypeError:
                ok, retorno = registrar_substituicao_excepcional_partida(
                    partida_id,
                    competicao,
                    equipe,
                    str(corpo.get("numero_sai") or "").strip(),
                    str(corpo.get("numero_entra") or "").strip()
                )

        elif tipo == "retardamento":
            ok, retorno = registrar_retardamento_partida(partida_id, competicao, equipe)

        elif tipo == "sancao":
            ok, retorno = registrar_sancao_partida(
                partida_id,
                competicao,
                equipe,
                str(corpo.get("tipo_pessoa") or "").strip().lower(),
                str(corpo.get("alvo") or corpo.get("numero") or corpo.get("nome") or "").strip(),
                str(corpo.get("sancao") or corpo.get("tipo_sancao") or "").strip().lower()
            )

        elif tipo == "cartao_verde":
            ok, retorno = registrar_cartao_verde_partida(
                partida_id,
                competicao,
                equipe,
                str(corpo.get("tipo_pessoa") or "").strip().lower(),
                str(corpo.get("alvo") or corpo.get("numero") or corpo.get("nome") or "").strip()
            )

        else:
            return _json_no_cache({
                "ok": False,
                "mensagem": f"Ação inválida para sincronizar: {tipo}"
            }, 400)

        if not ok:
            return _json_no_cache({"ok": False, "mensagem": retorno}, 400)

        estado = _normalizar_estado_pos_acao(
            partida_id,
            competicao,
            retorno,
            origem=f"SINCRONIZAR_{tipo.upper()}"
        )

        return _json_no_cache({"ok": True, **estado})

    except Exception as e:
        return _json_no_cache({
            "ok": False,
            "mensagem": f"Erro ao sincronizar ação: {e}"
        }, 500)
    

@apontadores_bp.route("/apontador/estado/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def estado_jogo_view(competicao, partida_id):
    try:
        partida = buscar_partida_operacional(partida_id, competicao)

        if not partida:
            return _json_no_cache({"ok": False, "mensagem": "Partida não encontrada"}, 404)

        garantir_estado_partida(partida_id, competicao)
        estado = buscar_estado_jogo_partida(partida_id, competicao) or {}

        equipe_a, equipe_b, set_atual, papeleta_a, papeleta_b = _buscar_papeletas_set_atual(
            partida_id, competicao, partida, estado
        )

        historico, ultima_acao = _buscar_historico_resumido(partida_id, competicao, limite=5)

        pontos_a = int(estado.get("pontos_a") or estado.get("placar_a") or 0)
        pontos_b = int(estado.get("pontos_b") or estado.get("placar_b") or 0)

        rotacao_a = list(estado.get("rotacao_a") or [])
        rotacao_b = list(estado.get("rotacao_b") or [])

        if not any(str(x).strip() for x in rotacao_a):
            rotacao_a = _rotacao_fallback_por_papeleta(papeleta_a)

        if not any(str(x).strip() for x in rotacao_b):
            rotacao_b = _rotacao_fallback_por_papeleta(papeleta_b)

        tempos_a = estado.get("tempos_a")
        tempos_b = estado.get("tempos_b")
        if tempos_a is None or tempos_b is None:
            try:
                tempos = buscar_tempos_restantes_partida(partida_id, competicao)
                tempos_a = tempos.get("tempos_a")
                tempos_b = tempos.get("tempos_b")
            except Exception:
                tempos_a = estado.get("tempos_a")
                tempos_b = estado.get("tempos_b")

        estado = _aplicar_regras_e_contadores_estado(partida_id, competicao, estado, partida)
        tempos_a = estado.get("tempos_a", tempos_a)
        tempos_b = estado.get("tempos_b", tempos_b)

        return _json_no_cache({
            "ok": True,
            "pontos_a": pontos_a,
            "pontos_b": pontos_b,
            "placar_a": pontos_a,
            "placar_b": pontos_b,
            "sets_a": int(estado.get("sets_a") or 0),
            "sets_b": int(estado.get("sets_b") or 0),
            "set_atual": int(estado.get("set_atual") or 1),
            "saque_atual": estado.get("saque_atual") or "",
            "tempos_a": tempos_a,
            "tempos_b": tempos_b,
            "subs_a": int(estado.get("subs_a") or 0),
            "subs_b": int(estado.get("subs_b") or 0),
            "limite_tempos": int(estado.get("limite_tempos") or 2),
            "limite_substituicoes": int(estado.get("limite_substituicoes") or 6),
            "pontos_set": int(estado.get("pontos_set") or 25),
            "pontos_tiebreak": int(estado.get("pontos_tiebreak") or 15),
            "diferenca_minima": int(estado.get("diferenca_minima") or 2),
            "sets_para_vencer": int(estado.get("sets_para_vencer") or 2),
            "sets_tipo": estado.get("sets_tipo") or "melhor_de_3",
            "limite_substituicoes": int(estado.get("limite_substituicoes") or 6),
            "rotacao_a": rotacao_a,
            "rotacao_b": rotacao_b,
            "rotacao": {
                "equipe_a": rotacao_a,
                "equipe_b": rotacao_b
            },
            "status_jogadores_a": estado.get("status_jogadores_a") or {},
            "status_jogadores_b": estado.get("status_jogadores_b") or {},
            "sancoes_a": estado.get("sancoes_a") or [],
            "sancoes_b": estado.get("sancoes_b") or [],
            "cartoes_verdes_a": estado.get("cartoes_verdes_a") or [],
            "cartoes_verdes_b": estado.get("cartoes_verdes_b") or [],
            "historico": historico,
            "ultima_acao": ultima_acao,
            "partida_finalizada": str(estado.get("fase_partida") or "").lower() == "encerrado"
        })

    except Exception as e:
        print("ERRO estado_jogo_view:", e)
        return _json_no_cache({
            "ok": False,
            "mensagem": "Erro interno ao carregar estado do jogo."
        }, 500)


@apontadores_bp.route("/apontador/jogo/<competicao>/<int:partida_id>/encerrar", methods=["POST"])
@exigir_perfil("apontador")
def encerrar_partida_view(competicao, partida_id):
    try:
        observacoes = ""
        corpo = request.get_json(silent=True) or {}
        if request.is_json:
            observacoes = (corpo.get("observacoes") or "").strip()
        else:
            observacoes = (request.form.get("observacoes") or "").strip()

        estado = buscar_estado_jogo_partida(partida_id, competicao)
        if not estado:
            return _json_no_cache({"ok": False, "mensagem": "Estado não encontrado."}, 404)

        encerrar_partida(partida_id, competicao, observacoes)
        estado = buscar_estado_jogo_partida(partida_id, competicao) or {}
        estado["encerrado"] = True
        estado["partida_finalizada"] = True
        estado["status_jogo"] = "finalizada"

        estado = _emitir_estado_e_placar(partida_id, competicao, estado, origem="ENCERRAR_PARTIDA")

        return _json_no_cache({
            "ok": True,
            "mensagem": "Partida encerrada com sucesso.",
            "encerrado": True,
            "estado": estado,
            "partida_finalizada": True,
            **estado
        })
    except Exception as e:
        return _json_no_cache({"ok": False, "mensagem": f"Erro ao encerrar partida: {e}"}, 500)


@apontadores_bp.route("/apontador/observacoes/<competicao>/<int:partida_id>")
@exigir_perfil("apontador")
def observacoes_view(competicao, partida_id):
    partida = buscar_partida_operacional(partida_id, competicao)

    return render_template(
        "observacoes.html",
        partida=partida,
        competicao_nome=competicao
    )


@apontadores_bp.route("/apontador/observacoes/<competicao>/<int:partida_id>/salvar", methods=["POST"])
@exigir_perfil("apontador")
def salvar_observacoes_view(competicao, partida_id):
    observacoes = request.form.get("observacoes")
    encerrar_partida(partida_id, competicao, observacoes)

    estado = buscar_estado_jogo_partida(partida_id, competicao) or {}
    estado["encerrado"] = True
    estado["partida_finalizada"] = True
    estado["status_jogo"] = "finalizada"
    _emitir_estado_e_placar(partida_id, competicao, estado, origem="SALVAR_OBSERVACOES")

    return redirect("/")

# FIX: garantir fundamento/resultado corretos para falta e erro_saque

@apontadores_bp.route("/apontador/inverter-lados/<int:partida_id>", methods=["POST"])
@exigir_perfil("apontador")
def inverter_lados(partida_id):
    competicao = session.get("competicao_apontador") or ""
    estado = obter_estado_cache(partida_id) or {}

    if competicao and not estado:
        estado = buscar_estado_jogo_partida(partida_id, competicao) or {}

    estado["invertido"] = not bool(estado.get("invertido", False))

    if competicao:
        estado = _emitir_estado_e_placar(partida_id, competicao, estado, origem="INVERTER_LADOS")
    else:
        apontador_login = session.get("usuario") or estado.get("apontador") or ""
        if apontador_login:
            estado["apontador"] = apontador_login
        emitir_estado_partida(partida_id, estado)
        emitir_placar_apontador(apontador_login, partida_id, estado)

    return _json_no_cache({
        "ok": True,
        "invertido": estado["invertido"]
    })

@apontadores_bp.route("/placar-ao-vivo")
def placar_ao_vivo_redirect():
    apontador = session.get("usuario") or ""

    if apontador:
        return redirect(url_for("apontadores.placar_ao_vivo_apontador", apontador=apontador))

    return render_template(
        "placar_profissional.html",
        estado={},
        partida={},
        apontador=""
    )


@apontadores_bp.route("/placar-ao-vivo/<apontador>")
def placar_ao_vivo_apontador(apontador):
    from socket_events import obter_ultimo_placar_apontador

    estado = obter_ultimo_placar_apontador(apontador) or {}

    return render_template(
        "placar_profissional.html",
        estado=estado,
        partida=estado,
        apontador=apontador
    )
