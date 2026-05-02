from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify, make_response
import time
import threading

from banco import (
    buscar_equipe_por_login,
    buscar_partida_treinador_por_equipe,
    montar_contexto_treinador,
    salvar_papeleta,
    registrar_solicitacao_treinador,
    listar_atletas_aprovados_da_equipe,
)
from routes.utils import exigir_perfil

try:
    from socket_events import emitir_estado_partida, emitir_solicitacao_treinador
except Exception:
    def emitir_estado_partida(*args, **kwargs):
        return None

    def emitir_solicitacao_treinador(*args, **kwargs):
        return None


treinador_bp = Blueprint("treinador", __name__)

# =========================================================
# CACHE LEVE DO MODO TREINADOR
# =========================================================
# O modo treinador troca de abas várias vezes. Sem cache, cada aba acaba
# chamando consultas pesadas de atletas/contexto e dá sensação de travamento.
_CACHE_TTL_SEGUNDOS = 20
_CACHE_EQUIPE_LOGIN = {}
_CACHE_ATLETAS_EQUIPE = {}


def _cache_get(cache, chave):
    item = cache.get(chave)
    if not item:
        return None

    criado_em, valor = item
    if (time.time() - criado_em) > _CACHE_TTL_SEGUNDOS:
        cache.pop(chave, None)
        return None

    return valor


def _cache_set(cache, chave, valor):
    cache[chave] = (time.time(), valor)
    return valor


def _buscar_equipe_sessao():
    login = session.get("usuario")
    if not login:
        return None

    chave = str(login).strip()
    equipe = _cache_get(_CACHE_EQUIPE_LOGIN, chave)
    if equipe is not None:
        return equipe

    return _cache_set(_CACHE_EQUIPE_LOGIN, chave, buscar_equipe_por_login(login))


def _listar_atletas_cache(equipe_nome, competicao):
    chave = ((competicao or "").strip(), (equipe_nome or "").strip())
    atletas = _cache_get(_CACHE_ATLETAS_EQUIPE, chave)
    if atletas is not None:
        return atletas

    atletas = listar_atletas_aprovados_da_equipe(equipe_nome, competicao) or []
    atletas = [a for a in atletas if a.get("numero") not in (None, "")]
    return _cache_set(_CACHE_ATLETAS_EQUIPE, chave, atletas)


def _limpar_cache_atletas(equipe_nome=None, competicao=None):
    if not equipe_nome or not competicao:
        _CACHE_ATLETAS_EQUIPE.clear()
        return
    _CACHE_ATLETAS_EQUIPE.pop(((competicao or "").strip(), (equipe_nome or "").strip()), None)


def _resposta_json_rapida(payload, status=200):
    resp = jsonify(payload)
    resp.status_code = status
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp



def _normalizar_rotacao_visual(rotacao):
    rotacao = rotacao or []

    if not isinstance(rotacao, list):
        return ["", "", "", "", "", ""]

    rotacao = [str(x or "").strip() for x in rotacao]

    while len(rotacao) < 6:
        rotacao.append("")

    return rotacao[:6]


def _rotacao_tem_valor(rotacao):
    if not isinstance(rotacao, list):
        return False

    return any(str(x or "").strip() for x in rotacao)


def _int(valor, padrao=0):
    try:
        if valor is None or valor == "":
            return padrao
        return int(valor)
    except Exception:
        return padrao


def _json_erro(mensagem, status=400):
    return jsonify({"ok": False, "mensagem": mensagem}), status


def _mesmo_nome(a, b):
    return str(a or "").strip().lower() == str(b or "").strip().lower()


def _definir_rotacao_propria(contexto, rotacao_a, rotacao_b):
    """
    Define a rotação do treinador SEM confiar em contexto["rotacao"].
    Sempre decide pelo nome da equipe.
    """

    equipe_nome = str(contexto.get("equipe_nome") or "").strip().lower()
    equipe_a = str(contexto.get("equipe_a") or "").strip().lower()
    equipe_b = str(contexto.get("equipe_b") or "").strip().lower()

    # REGRA PRINCIPAL: pelo nome da equipe
    if equipe_nome and equipe_nome == equipe_a:
        return rotacao_a

    if equipe_nome and equipe_nome == equipe_b:
        return rotacao_b

    # FALLBACK (se algo vier estranho)
    lado = contexto.get("lado")

    if lado == "A":
        return rotacao_a

    if lado == "B":
        return rotacao_b

    # segurança total
    return ["", "", "", "", "", ""]




def _valor_contexto(contexto, *nomes):
    for nome in nomes:
        if nome in contexto and contexto.get(nome) not in (None, ""):
            return contexto.get(nome)
    return None


def _calcular_tempos_restantes(contexto):
    """
    No apontador, tempos_a/tempos_b representam TEMPOS RESTANTES.
    Em algumas montagens do contexto, tempos_restantes pode vir None/""/0 indevidamente.
    Por isso calculamos com fallback usando lado + limite da regra.
    """
    lado = str(contexto.get("lado") or "").strip().upper()

    direto = _valor_contexto(contexto, "tempos_restantes")
    if direto is not None:
        valor = _int(direto, -1)
        if valor >= 0:
            return valor

    limite = _int(_valor_contexto(contexto, "tempos_limite", "limite_tempos"), 2)
    if limite <= 0:
        limite = 2

    campo_lado = "tempos_a" if lado == "A" else "tempos_b" if lado == "B" else ""
    valor_lado = _valor_contexto(contexto, campo_lado) if campo_lado else None

    # Aqui o valor do lado já é restante, não usado.
    if valor_lado is not None:
        valor = _int(valor_lado, limite)
        return max(0, valor)

    return limite


def _calcular_subs_restantes(contexto):
    """
    No apontador, subs_a/subs_b representam substituições USADAS.
    Restante = limite da regra - usadas.
    """
    lado = str(contexto.get("lado") or "").strip().upper()

    direto = _valor_contexto(contexto, "subs_restantes")
    if direto is not None:
        valor = _int(direto, -1)
        if valor >= 0:
            return valor

    limite = _int(_valor_contexto(contexto, "subs_limite", "limite_substituicoes"), 6)
    if limite <= 0:
        limite = 6

    campo_lado = "subs_a" if lado == "A" else "subs_b" if lado == "B" else ""
    usadas = _int(_valor_contexto(contexto, campo_lado), 0) if campo_lado else 0

    return max(0, limite - usadas)


def _aplicar_restantes_contexto(contexto):
    if not isinstance(contexto, dict):
        return contexto

    tempos_limite = _int(_valor_contexto(contexto, "tempos_limite", "limite_tempos"), 2)
    subs_limite = _int(_valor_contexto(contexto, "subs_limite", "limite_substituicoes"), 6)

    contexto["tempos_limite"] = tempos_limite if tempos_limite > 0 else 2
    contexto["subs_limite"] = subs_limite if subs_limite > 0 else 6
    contexto["tempos_restantes"] = _calcular_tempos_restantes(contexto)
    contexto["subs_restantes"] = _calcular_subs_restantes(contexto)

    return contexto

def _montar_payload_estado(contexto):
    contexto = _aplicar_restantes_contexto(contexto or {})
    estado = contexto.get("estado") or {}

    rotacao_a = _normalizar_rotacao_visual(
        contexto.get("rotacao_a") or estado.get("rotacao_a")
    )

    rotacao_b = _normalizar_rotacao_visual(
        contexto.get("rotacao_b") or estado.get("rotacao_b")
    )

    rotacao_propria = _definir_rotacao_propria(contexto, rotacao_a, rotacao_b)

    lado = contexto.get("lado") or ""

    return {
        "ok": True,

        "lado": lado,
        "lado_quadra": contexto.get("lado_quadra") or "",

        "equipe_nome": contexto.get("equipe_nome") or "",
        "equipe_adversaria": contexto.get("equipe_adversaria") or "",
        "equipe_a": contexto.get("equipe_a") or "",
        "equipe_b": contexto.get("equipe_b") or "",

        "set_atual": _int(contexto.get("set_atual"), 1),
        "descricao_set": contexto.get("descricao_set") or "",

        "pontos_a": _int(contexto.get("pontos_a")),
        "pontos_b": _int(contexto.get("pontos_b")),
        "placar_a": _int(contexto.get("placar_a") or contexto.get("pontos_a")),
        "placar_b": _int(contexto.get("placar_b") or contexto.get("pontos_b")),
        "sets_a": _int(contexto.get("sets_a")),
        "sets_b": _int(contexto.get("sets_b")),

        "placar_proprio": _int(contexto.get("placar_proprio")),
        "placar_adversario": _int(contexto.get("placar_adversario")),
        "sets_proprios": _int(contexto.get("sets_proprios")),
        "sets_adversario": _int(contexto.get("sets_adversario")),

        "saque_atual": contexto.get("saque_atual") or "",
        "saque_atual_nome": contexto.get("saque_atual_nome") or "",
        "saque_inicial": contexto.get("saque_inicial") or "",
        "saque_inicial_nome": contexto.get("saque_inicial_nome") or "",

        "tempos_limite": _int(contexto.get("tempos_limite")),
        "subs_limite": _int(contexto.get("subs_limite")),
        "tempos_restantes": _int(contexto.get("tempos_restantes")),
        "subs_restantes": _int(contexto.get("subs_restantes")),
        "tempos_a": _int(contexto.get("tempos_a")),
        "tempos_b": _int(contexto.get("tempos_b")),
        "subs_a": _int(contexto.get("subs_a")),
        "subs_b": _int(contexto.get("subs_b")),

        "rotacao_a": rotacao_a,
        "rotacao_b": rotacao_b,
        "rotacao_propria": rotacao_propria,
        "rotacao": {
            "equipe_a": rotacao_a,
            "equipe_b": rotacao_b,
            "propria": rotacao_propria,
        },

        "banco": contexto.get("banco") or [],
        "scout": contexto.get("scout") or {},
        "eventos": contexto.get("eventos") or [],
        "atletas_lista": contexto.get("atletas_lista") or [],
        "solicitacoes": contexto.get("solicitacoes") or [],

        "papeleta": contexto.get("papeleta") or {},
        "papeleta_liberada": bool(contexto.get("papeleta_liberada")),
        "papeleta_editavel": bool(contexto.get("papeleta_editavel")),
        "papeleta_completa": bool(contexto.get("papeleta_completa")),
    }


@treinador_bp.route("/treinador")
@exigir_perfil("equipe")
def abrir_modo_treinador():
    equipe = _buscar_equipe_sessao()

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    partida = buscar_partida_treinador_por_equipe(
        equipe.get("competicao"),
        equipe.get("nome")
    )

    if not partida:
        flash("Nenhuma partida disponível para o modo treinador no momento.", "erro")
        return redirect(url_for("equipes.minha_equipe"))

    return redirect(
        url_for(
            "treinador.tela_treinador",
            competicao=equipe.get("competicao"),
            partida_id=partida.get("id"),
        )
    )


@treinador_bp.route("/treinador/jogo/<competicao>/<int:partida_id>")
@exigir_perfil("equipe")
def tela_treinador(competicao, partida_id):
    equipe = _buscar_equipe_sessao()

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    # Evita abrir jogo antigo da mesma equipe quando o treinador acessa um link velho.
    # Se existir uma partida ativa/correta para essa equipe, redireciona para ela.
    partida_correta = buscar_partida_treinador_por_equipe(competicao, equipe.get("nome"))
    if partida_correta and int(partida_correta.get("id") or 0) != int(partida_id):
        return redirect(url_for(
            "treinador.tela_treinador",
            competicao=competicao,
            partida_id=partida_correta.get("id"),
            aba=request.args.get("aba", "papeleta"),
        ))

    aba_ativa = (request.args.get("aba") or "papeleta").strip().lower()
    incluir_scout = aba_ativa in {"scout", "estatisticas", "estatísticas"}
    incluir_solicitacoes = aba_ativa in {"operacao", "operação", "solicitacoes", "solicitações", "pedidos"}
    incluir_banco = aba_ativa in {"papeleta", "operacao", "operação", "substituicao", "substituição", "banco"}

    contexto = montar_contexto_treinador(
        partida_id,
        competicao,
        equipe.get("nome"),
        modo_rapido=not incluir_scout,
        incluir_scout=incluir_scout,
        incluir_solicitacoes=incluir_solicitacoes,
        incluir_banco=incluir_banco,
    )

    if not contexto:
        flash("Partida não encontrada para esta equipe.", "erro")
        return redirect(url_for("equipes.minha_equipe"))

    contexto = _aplicar_restantes_contexto(contexto)

    rotacao_a = _normalizar_rotacao_visual(contexto.get("rotacao_a"))
    rotacao_b = _normalizar_rotacao_visual(contexto.get("rotacao_b"))

    contexto["rotacao_a"] = rotacao_a
    contexto["rotacao_b"] = rotacao_b
    contexto["rotacao"] = _definir_rotacao_propria(contexto, rotacao_a, rotacao_b)

    # Carrega atletas uma vez e reutiliza nas trocas de aba.
    atletas_lista = _listar_atletas_cache(equipe.get("nome"), competicao)

    contexto["atletas"] = atletas_lista
    contexto["jogadores"] = [
        {
            "numero": a.get("numero"),
            "nome": a.get("nome"),
        }
        for a in atletas_lista
    ]

    resposta = make_response(render_template(
        "treinador_jogo.html",
        competicao_nome=competicao,
        **contexto
    ))
    resposta.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resposta.headers["Pragma"] = "no-cache"
    resposta.headers["Expires"] = "0"
    return resposta


@treinador_bp.route("/treinador/jogo/<competicao>/<int:partida_id>/estado")
@exigir_perfil("equipe")
def estado_treinador_view(competicao, partida_id):
    equipe = _buscar_equipe_sessao()

    if not equipe:
        return _json_erro("Equipe não encontrada.", 404)

    aba = (request.args.get("aba") or "ao_vivo").strip().lower()

    incluir_scout = aba in {"scout", "estatisticas", "estatísticas"}
    incluir_solicitacoes = aba in {"solicitacoes", "solicitações", "pedidos", "operacao", "operação"}
    incluir_banco = aba in {"substituicao", "substituição", "banco", "papeleta", "operacao", "operação"}

    contexto = montar_contexto_treinador(
        partida_id,
        competicao,
        equipe.get("nome"),
        modo_rapido=not incluir_scout,
        incluir_scout=incluir_scout,
        incluir_solicitacoes=incluir_solicitacoes,
        incluir_banco=incluir_banco,
    )

    if not contexto:
        return _json_erro("Partida não encontrada para esta equipe.", 404)

    contexto = _aplicar_restantes_contexto(contexto)
    payload = _montar_payload_estado(contexto)

    if not incluir_scout:
        payload["scout"] = {}
        payload["eventos"] = []

    if not incluir_solicitacoes:
        payload["solicitacoes"] = []

    if not incluir_banco:
        payload["banco"] = []

    atletas_lista = _listar_atletas_cache(equipe.get("nome"), competicao)

    if aba in {"papeleta", "substituicao", "substituição", "banco", "scout", "operacao", "operação"}:
        payload["atletas_lista"] = atletas_lista
        payload["jogadores"] = [
            {"numero": a.get("numero"), "nome": a.get("nome")}
            for a in atletas_lista
        ]
    else:
        payload["atletas_lista"] = []

    # Scout completo e automático na aba Scout
    if aba == "scout":
        from banco import listar_eventos_partida

        eventos = listar_eventos_partida(partida_id, competicao, limite=2000) or []

        lado = str(contexto.get("lado") or "").strip().upper()

        mapa = {}

        for a in atletas_lista:
            numero = str(a.get("numero") or "").strip()
            if not numero:
                continue

            mapa[numero] = {
                "numero": numero,
                "nome": a.get("nome") or "",
                "pontos": 0,
                "ataques": 0,
                "aces": 0,
                "bloqueios": 0,
            }

        resumo = {
            "pontos": 0,
            "aces": 0,
            "bloqueios": 0,
            "erros_saque": 0,
            "faltas": 0,
        }

        for ev in eventos:
            equipe_evento = str(ev.get("equipe") or "").strip().upper()
            fundamento = str(ev.get("fundamento") or "").strip().lower()
            resultado = str(ev.get("resultado") or "").strip().lower()
            tipo = str(ev.get("tipo") or ev.get("tipo_evento") or "").strip().lower()
            detalhe = str(ev.get("detalhe") or ev.get("detalhes") or "").strip().lower()

            numero = str(
                ev.get("numero")
                or ev.get("atleta_numero")
                or ""
            ).strip()

            # Só conta scout da equipe do treinador
            if equipe_evento != lado:
                continue

            # Ponto normal de atleta
            if resultado == "ponto" and numero in mapa:
                mapa[numero]["pontos"] += 1
                resumo["pontos"] += 1

                if fundamento == "ataque" or detalhe == "ataque":
                    mapa[numero]["ataques"] += 1

                if fundamento == "ace" or detalhe == "ace":
                    mapa[numero]["aces"] += 1
                    resumo["aces"] += 1

                if fundamento == "bloqueio" or detalhe == "bloqueio":
                    mapa[numero]["bloqueios"] += 1
                    resumo["bloqueios"] += 1

            # Erro/falta conta para a equipe que cometeu, mas não soma ponto de atleta
            if resultado == "erro" or tipo == "erro" or detalhe in {"erro_saque", "erro_geral"}:
                if detalhe == "erro_saque" or fundamento == "erro_saque":
                    resumo["erros_saque"] += 1

            if resultado == "falta" or tipo == "falta" or detalhe in {"rede", "invasao", "rotacao", "conducao", "dois_toques"}:
                resumo["faltas"] += 1

        payload["scout"] = {
            "equipe": resumo,
            "atletas_lista": list(mapa.values()),
        }

    return _resposta_json_rapida(payload)


@treinador_bp.route("/treinador/jogo/<competicao>/<int:partida_id>/papeleta", methods=["POST"])
@exigir_perfil("equipe")
def salvar_papeleta_treinador(competicao, partida_id):
    equipe = _buscar_equipe_sessao()

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    contexto = montar_contexto_treinador(
        partida_id,
        competicao,
        equipe.get("nome"),
        modo_rapido=True,
        incluir_scout=False,
        incluir_solicitacoes=False,
        incluir_banco=False,
    )

    if not contexto:
        flash("Partida não encontrada para esta equipe.", "erro")
        return redirect(url_for("equipes.minha_equipe"))

    if not contexto.get("papeleta_editavel"):
        flash("A papeleta já está travada porque o apontador iniciou o jogo.", "erro")
        return redirect(
            url_for(
                "treinador.tela_treinador",
                competicao=competicao,
                partida_id=partida_id
            )
        )

    atletas = _listar_atletas_cache(equipe.get("nome"), competicao)

    atletas_por_numero = {
        str(a.get("numero")): a
        for a in atletas
        if a.get("numero") not in (None, "")
    }

    dados = {}
    numeros_usados = set()

    for pos in [1, 2, 3, 4, 5, 6]:
        numero = (request.form.get(f"posicao_{pos}") or "").strip()

        if not numero:
            flash("Preencha as 6 posições da papeleta.", "erro")
            return redirect(
                url_for(
                    "treinador.tela_treinador",
                    competicao=competicao,
                    partida_id=partida_id
                )
            )

        if numero in numeros_usados:
            flash("Não é permitido repetir número na papeleta.", "erro")
            return redirect(
                url_for(
                    "treinador.tela_treinador",
                    competicao=competicao,
                    partida_id=partida_id
                )
            )

        atleta = atletas_por_numero.get(numero)

        if not atleta:
            flash(f"Número {numero} não encontrado entre os atletas aprovados da equipe.", "erro")
            return redirect(
                url_for(
                    "treinador.tela_treinador",
                    competicao=competicao,
                    partida_id=partida_id
                )
            )

        numeros_usados.add(numero)
        dados[pos] = atleta

    salvar_papeleta(
        partida_id,
        competicao,
        equipe.get("nome"),
        _int(contexto.get("set_atual"), 1),
        dados
    )

    try:
        contexto_atualizado = montar_contexto_treinador(
            partida_id,
            competicao,
            equipe.get("nome"),
            modo_rapido=True,
            incluir_scout=False,
            incluir_solicitacoes=False,
            incluir_banco=False,
        ) or contexto

        emitir_estado_partida(
            partida_id,
            _montar_payload_estado(contexto_atualizado)
        )

    except Exception as e:
        print("ERRO emitir_estado_partida PAPELETA TREINADOR:", repr(e), flush=True)

    flash("Papeleta enviada com sucesso.", "sucesso")

    return redirect(
        url_for(
            "treinador.tela_treinador",
            competicao=competicao,
            partida_id=partida_id,
            aba="papeleta",
        )
    )


@treinador_bp.route("/treinador/jogo/<competicao>/<int:partida_id>/solicitar-tempo", methods=["POST"])
@exigir_perfil("equipe")
def solicitar_tempo_treinador(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}
        equipe = _buscar_equipe_sessao()

        if not equipe:
            return _json_erro("Equipe não encontrada.", 404)

        contexto = montar_contexto_treinador(
            partida_id,
            competicao,
            equipe.get("nome"),
            modo_rapido=True,
            incluir_scout=False,
            incluir_solicitacoes=False,
            incluir_banco=False,
        )

        if not contexto:
            return _json_erro("Partida não encontrada.", 404)

        lado = contexto.get("lado")

        if not lado:
            return _json_erro("Lado da equipe não definido.", 400)

        contexto = _aplicar_restantes_contexto(contexto)
        tempos_restantes = _calcular_tempos_restantes(contexto)

        if tempos_restantes <= 0:
            return _json_erro("Sua equipe não tem mais tempos disponíveis.", 400)

        payload_solicitacao = {
            "id_solicitacao": corpo.get("id_solicitacao"),
            "partida_id": partida_id,
            "tipo": "tempo",
            "equipe": lado,
            "equipe_nome": equipe.get("nome"),
            "mensagem": f"{equipe.get('nome')} solicitou tempo",
            "status": "pendente",
            "set_atual": contexto.get("set_atual"),
            "origem": corpo.get("origem") or "treinador_http",
        }

        # Se o navegador já avisou por Socket.IO, esta rota só persiste.
        # Se o Socket não estava conectado, a rota vira fallback e emite agora.
        if not corpo.get("tempo_real_emitido"):
            emitir_solicitacao_treinador(partida_id, payload_solicitacao)

        def _salvar_solicitacao_tempo():
            try:
                registrar_solicitacao_treinador(
                    partida_id,
                    competicao,
                    lado,
                    "tempo",
                    {
                        "equipe_nome": equipe.get("nome"),
                        "set_atual": contexto.get("set_atual"),
                    }
                )
            except Exception as e:
                print("ERRO async registrar solicitação tempo:", repr(e), flush=True)

        threading.Thread(target=_salvar_solicitacao_tempo, daemon=True).start()

        return _resposta_json_rapida({
            "ok": True,
            "mensagem": "Solicitação de tempo enviada ao apontador.",
            "solicitacao": payload_solicitacao,
        })

    except Exception as e:
        print("ERRO GERAL solicitar_tempo_treinador:", repr(e), flush=True)
        return _json_erro("Erro interno ao solicitar tempo.", 500)


@treinador_bp.route("/treinador/jogo/<competicao>/<int:partida_id>/solicitar-substituicao", methods=["POST"])
@exigir_perfil("equipe")
def solicitar_substituicao_treinador(competicao, partida_id):
    try:
        corpo = request.get_json(silent=True) or {}
        equipe = _buscar_equipe_sessao()

        if not equipe:
            return _json_erro("Equipe não encontrada.", 404)

        contexto = montar_contexto_treinador(
            partida_id,
            competicao,
            equipe.get("nome"),
            modo_rapido=True,
            incluir_scout=False,
            incluir_solicitacoes=False,
            incluir_banco=False,
        )

        if not contexto:
            return _json_erro("Partida não encontrada.", 404)

        lado = contexto.get("lado")

        if not lado:
            return _json_erro("Lado da equipe não definido.", 400)

        contexto = _aplicar_restantes_contexto(contexto)
        subs_restantes = _calcular_subs_restantes(contexto)

        if subs_restantes <= 0:
            return _json_erro("Sua equipe não tem mais substituições disponíveis.", 400)

        payload_solicitacao = {
            "id_solicitacao": corpo.get("id_solicitacao"),
            "partida_id": partida_id,
            "tipo": "substituicao",
            "equipe": lado,
            "equipe_nome": equipe.get("nome"),
            "mensagem": f"{equipe.get('nome')} solicitou substituição",
            "status": "pendente",
            "set_atual": contexto.get("set_atual"),
            "origem": corpo.get("origem") or "treinador_http",
        }

        # Se o navegador já avisou por Socket.IO, esta rota só persiste.
        # Se o Socket não estava conectado, a rota vira fallback e emite agora.
        if not corpo.get("tempo_real_emitido"):
            emitir_solicitacao_treinador(partida_id, payload_solicitacao)

        def _salvar_solicitacao_substituicao():
            try:
                registrar_solicitacao_treinador(
                    partida_id,
                    competicao,
                    lado,
                    "substituicao",
                    {
                        "equipe_nome": equipe.get("nome"),
                        "set_atual": contexto.get("set_atual"),
                    }
                )
            except Exception as e:
                print("ERRO async registrar solicitação substituição:", repr(e), flush=True)

        threading.Thread(target=_salvar_solicitacao_substituicao, daemon=True).start()

        return _resposta_json_rapida({
            "ok": True,
            "mensagem": "Solicitação de substituição enviada ao apontador.",
            "solicitacao": payload_solicitacao,
        })

    except Exception as e:
        print("ERRO GERAL solicitar_substituicao_treinador:", repr(e), flush=True)
        return _json_erro("Erro interno ao solicitar substituição.", 500)