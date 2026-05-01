from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify

from banco import (
    buscar_equipe_por_login,
    buscar_partida_treinador_por_equipe,
    montar_contexto_treinador,
    salvar_papeleta,
    registrar_solicitacao_treinador,
    listar_atletas_aprovados_da_equipe,
)
from routes.utils import exigir_perfil
from socket_events import emitir_solicitacao_treinador, emitir_estado_partida


treinador_bp = Blueprint("treinador", __name__)


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
    Define a rotação do treinador pelo NOME DA EQUIPE, não pelo lado A/B visual.
    Isso evita puxar a rotação da equipe adversária quando lado_quadra/inversão vier diferente.
    """
    equipe_nome = contexto.get("equipe_nome") or ""
    equipe_a = contexto.get("equipe_a") or ""
    equipe_b = contexto.get("equipe_b") or ""

    rotacao_contexto = _normalizar_rotacao_visual(contexto.get("rotacao"))

    if _rotacao_tem_valor(rotacao_contexto):
        return rotacao_contexto

    if _mesmo_nome(equipe_nome, equipe_a):
        return rotacao_a

    if _mesmo_nome(equipe_nome, equipe_b):
        return rotacao_b

    lado = contexto.get("lado") or ""

    if lado == "A":
        return rotacao_a

    if lado == "B":
        return rotacao_b

    return ["", "", "", "", "", ""]


def _montar_payload_estado(contexto):
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
    equipe = buscar_equipe_por_login(session.get("usuario"))

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
    equipe = buscar_equipe_por_login(session.get("usuario"))

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    contexto = montar_contexto_treinador(partida_id, competicao, equipe.get("nome"))

    if not contexto:
        flash("Partida não encontrada para esta equipe.", "erro")
        return redirect(url_for("equipes.minha_equipe"))

    rotacao_a = _normalizar_rotacao_visual(contexto.get("rotacao_a"))
    rotacao_b = _normalizar_rotacao_visual(contexto.get("rotacao_b"))

    contexto["rotacao_a"] = rotacao_a
    contexto["rotacao_b"] = rotacao_b
    contexto["rotacao"] = _definir_rotacao_propria(contexto, rotacao_a, rotacao_b)

    atletas_lista = listar_atletas_aprovados_da_equipe(equipe.get("nome"), competicao) or []
    atletas_lista = [
        a for a in atletas_lista
        if a.get("numero") not in (None, "")
    ]

    contexto["atletas"] = atletas_lista
    contexto["jogadores"] = [
        {
            "numero": a.get("numero"),
            "nome": a.get("nome"),
        }
        for a in atletas_lista
    ]

    return render_template(
        "treinador_jogo.html",
        competicao_nome=competicao,
        **contexto
    )


@treinador_bp.route("/treinador/jogo/<competicao>/<int:partida_id>/estado")
@exigir_perfil("equipe")
def estado_treinador_view(competicao, partida_id):
    equipe = buscar_equipe_por_login(session.get("usuario"))

    if not equipe:
        return _json_erro("Equipe não encontrada.", 404)

    contexto = montar_contexto_treinador(partida_id, competicao, equipe.get("nome"))

    if not contexto:
        return _json_erro("Partida não encontrada para esta equipe.", 404)

    return jsonify(_montar_payload_estado(contexto))


@treinador_bp.route("/treinador/jogo/<competicao>/<int:partida_id>/papeleta", methods=["POST"])
@exigir_perfil("equipe")
def salvar_papeleta_treinador(competicao, partida_id):
    equipe = buscar_equipe_por_login(session.get("usuario"))

    if not equipe:
        flash("Equipe não encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    contexto = montar_contexto_treinador(partida_id, competicao, equipe.get("nome"))

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

    atletas = listar_atletas_aprovados_da_equipe(equipe.get("nome"), competicao) or []

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
            equipe.get("nome")
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
        equipe = buscar_equipe_por_login(session.get("usuario"))

        if not equipe:
            return _json_erro("Equipe não encontrada.", 404)

        contexto = montar_contexto_treinador(partida_id, competicao, equipe.get("nome"))

        if not contexto:
            return _json_erro("Partida não encontrada.", 404)

        lado = contexto.get("lado")

        if not lado:
            return _json_erro("Lado da equipe não definido.", 400)

        if _int(contexto.get("tempos_restantes")) <= 0:
            return _json_erro("Sua equipe não tem mais tempos disponíveis.", 400)

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

        emitir_solicitacao_treinador(
            partida_id,
            {
                "tipo": "tempo",
                "equipe": lado,
                "equipe_nome": equipe.get("nome"),
                "mensagem": f"{equipe.get('nome')} solicitou tempo",
            }
        )

        return jsonify({
            "ok": True,
            "mensagem": "Solicitação de tempo enviada ao apontador."
        })

    except Exception as e:
        print("ERRO GERAL solicitar_tempo_treinador:", repr(e), flush=True)
        return _json_erro("Erro interno ao solicitar tempo.", 500)


@treinador_bp.route("/treinador/jogo/<competicao>/<int:partida_id>/solicitar-substituicao", methods=["POST"])
@exigir_perfil("equipe")
def solicitar_substituicao_treinador(competicao, partida_id):
    try:
        equipe = buscar_equipe_por_login(session.get("usuario"))

        if not equipe:
            return _json_erro("Equipe não encontrada.", 404)

        contexto = montar_contexto_treinador(partida_id, competicao, equipe.get("nome"))

        if not contexto:
            return _json_erro("Partida não encontrada.", 404)

        lado = contexto.get("lado")

        if not lado:
            return _json_erro("Lado da equipe não definido.", 400)

        if _int(contexto.get("subs_restantes")) <= 0:
            return _json_erro("Sua equipe não tem mais substituições disponíveis.", 400)

        banco = contexto.get("banco") or []

        if not banco:
            return _json_erro("Sua equipe não possui atletas disponíveis no banco.", 400)

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

        emitir_solicitacao_treinador(
            partida_id,
            {
                "tipo": "substituicao",
                "equipe": lado,
                "equipe_nome": equipe.get("nome"),
                "mensagem": f"{equipe.get('nome')} solicitou substituição",
            }
        )

        return jsonify({
            "ok": True,
            "mensagem": "Solicitação de substituição enviada ao apontador."
        })

    except Exception as e:
        print("ERRO GERAL solicitar_substituicao_treinador:", repr(e), flush=True)
        return _json_erro("Erro interno ao solicitar substituição.", 500)