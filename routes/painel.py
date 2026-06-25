from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify
import os
import time
from banco import (
    conectar,
    contar_competicoes,
    contar_equipes,
    contar_partidas,
    listar_competicoes_do_organizador,
    listar_competicoes_apontador,
    buscar_usuario_por_login,
    atualizar_senha_usuario,
    buscar_apontador,
    definir_senha_apontador,
    buscar_perfil_equipe_por_login,
    salvar_perfil_equipe_por_login,
    atualizar_dados_conta_usuario,
    atualizar_dados_conta_apontador,
    buscar_vinculo_arbitragem_por_pin,
    buscar_vinculo_operacional_por_pin,
    superadmin_eh_master,
    listar_superadmins_clientes,
    criar_superadmin_cliente,
    excluir_superadmin_cliente,
    contar_solicitacoes_pendentes,
    listar_solicitacoes_equipes,
    listar_notificacoes_sistema,
    buscar_competicao_por_organizador,
    garantir_schema_fluxo_configuracao_competicoes,
    status_configuracao_inicial_competicao,
    apontador_pode_criar_jogo_avulso,
    jogo_rapido_global_habilitado,
    definir_jogo_rapido_global_habilitado,
)
from routes.utils import login_obrigatorio

painel_bp = Blueprint("painel", __name__)


# =========================================================
# CACHE LEVE DO PAINEL
# =========================================================
# Evita consultas repetidas no Neon em telas que são abertas/polled várias vezes.
# TTL curto para não deixar informações operacionais antigas.
_PAINEL_CACHE_TTL = int(os.environ.get("PAINEL_CACHE_TTL", "20") or 20)
_ARBITRO_POLL_CACHE_TTL = int(os.environ.get("PAINEL_ARBITRO_POLL_CACHE_TTL", "5") or 5)
_CACHE_PAINEL = {}


def _cache_key(*partes):
    return tuple(str(p or "").strip() for p in partes)


def _cache_get(chave, ttl=None):
    ttl = _PAINEL_CACHE_TTL if ttl is None else ttl
    item = _CACHE_PAINEL.get(chave)
    if not item:
        return None
    criado_em, valor = item
    if (time.time() - criado_em) > ttl:
        _CACHE_PAINEL.pop(chave, None)
        return None
    return valor


def _cache_set(chave, valor):
    if len(_CACHE_PAINEL) > 300:
        _CACHE_PAINEL.clear()
    _CACHE_PAINEL[chave] = (time.time(), valor)
    return valor


def _cache_delete_prefix(prefixo):
    prefixo = tuple(prefixo)
    for chave in list(_CACHE_PAINEL.keys()):
        if isinstance(chave, tuple) and chave[:len(prefixo)] == prefixo:
            _CACHE_PAINEL.pop(chave, None)


def _buscar_usuario_cache(login):
    login = (login or "").strip()
    if not login:
        return None
    chave = _cache_key("usuario", login)
    cached = _cache_get(chave)
    if cached is not None:
        return cached
    return _cache_set(chave, buscar_usuario_por_login(login))


def _listar_competicoes_organizador_cache(login):
    login = (login or "").strip()
    chave = _cache_key("competicoes_organizador", login)
    cached = _cache_get(chave)
    if cached is not None:
        return cached
    return _cache_set(chave, listar_competicoes_do_organizador(login) or [])


def _listar_competicoes_apontador_cache(cpf):
    cpf = (cpf or "").strip()
    chave = _cache_key("competicoes_apontador", cpf)
    cached = _cache_get(chave)
    if cached is not None:
        return cached
    return _cache_set(chave, listar_competicoes_apontador(cpf) or [])


def _buscar_apontador_cache(login):
    login = (login or "").strip()
    chave = _cache_key("apontador", login)
    cached = _cache_get(chave)
    if cached is not None:
        return cached
    return _cache_set(chave, buscar_apontador(login))


STATUS_ATIVOS_ARBITRO = (
    "pre_jogo",
    "papeleta",
    "papeleta_pronta",
    "em_andamento",
    "andamento",
    "ao_vivo",
    "jogo",
    "iniciada",
    "iniciado",
    "entre_sets",
    "tiebreak_sorteio",
)

STATUS_FINALIZADOS_ARBITRO = (
    "finalizada",
    "finalizado",
    "encerrada",
    "encerrado",
)


def _perfil_normalizado():
    return (session.get("perfil") or "").strip().lower()


def _usuario_logado():
    return (session.get("usuario") or "").strip()


def _usuario_tem_perfil_arbitro():
    """
    Mantém compatibilidade com árbitros antigos por login, mas também permite
    as novas telas públicas liberadas por PIN operacional.
    """
    return _perfil_normalizado() in {"mesario", "arbitro"} or bool(session.get("arbitro_pin_validado"))


def _competicao_arbitro_logado():
    competicao = (session.get("arbitro_competicao") or "").strip()
    if competicao:
        return competicao

    competicao = (session.get("competicao_vinculada") or "").strip()
    if competicao:
        return competicao

    usuario = _buscar_usuario_cache(_usuario_logado())
    if usuario:
        competicao = (usuario.get("competicao_vinculada") or "").strip()
        if competicao:
            session["competicao_vinculada"] = competicao
            return competicao

    return ""


def _limpar_vinculo_arbitro_sessao():
    for chave in [
        "arbitro_pin_validado",
        "arbitro_pin_tipo",
        "arbitro_pin",
        "arbitro_competicao",
        "arbitro_quadra_id",
        "arbitro_quadra_nome",
        "arbitro_quadra_local",
        "arbitro_quadra_ordem",
        "arbitro_apontador_cpf",
        "arbitro_apontador_nome",
        "arbitro_jogo_avulso_codigo",
        "arbitro_jogo_avulso_pin",
        "arbitro_jogo_avulso_equipe_a",
        "arbitro_jogo_avulso_equipe_b",
    ]:
        session.pop(chave, None)


def _vinculo_arbitro_sessao():
    if not session.get("arbitro_pin_validado"):
        return None
    tipo = (session.get("arbitro_pin_tipo") or "").strip().lower()
    if tipo == "avulso":
        return {
            "tipo": "avulso",
            "codigo": session.get("arbitro_jogo_avulso_codigo") or "",
            "pin": session.get("arbitro_jogo_avulso_pin") or "",
            "equipe_a": session.get("arbitro_jogo_avulso_equipe_a") or "Equipe A",
            "equipe_b": session.get("arbitro_jogo_avulso_equipe_b") or "Equipe B",
        }
    if tipo == "competicao":
        return {
            "tipo": "competicao",
            "competicao": session.get("arbitro_competicao") or "",
            "quadra_id": session.get("arbitro_quadra_id"),
            "quadra_nome": session.get("arbitro_quadra_nome") or "",
            "quadra_local": session.get("arbitro_quadra_local") or "",
            "quadra_ordem": session.get("arbitro_quadra_ordem"),
            "pin": session.get("arbitro_pin") or "",
        }
    if tipo == "operacional":
        return {
            "tipo": "operacional",
            "competicao": session.get("arbitro_competicao") or "",
            "apontador_cpf": session.get("arbitro_apontador_cpf") or "",
            "apontador_nome": session.get("arbitro_apontador_nome") or "",
            "pin": session.get("arbitro_pin") or "",
        }
    return None

def _buscar_partida_ativa_para_painel_arbitro(competicao, quadra_id=None, quadra_nome=None, quadra_ordem=None, operador_login=None):
    """
    Busca a partida que deve aparecer nos tablets dos árbitros.
    Não usa ANY/ALL com array para evitar erro de malformed array literal no psycopg.
    Prioriza a partida ao vivo/mais recente e ignora finalizadas.
    """
    if not competicao:
        return None

    filtros = ["competicao = %s"]
    params = [competicao]

    if quadra_id:
        filtros.append("(quadra_id = %s OR quadra_nome = %s OR quadra = %s OR quadra = %s)")
        params.extend([quadra_id, quadra_nome or "", quadra_nome or "", str(quadra_ordem or "")])
    elif quadra_nome:
        filtros.append("(quadra_nome = %s OR quadra = %s)")
        params.extend([quadra_nome, quadra_nome])
    elif quadra_ordem:
        filtros.append("(quadra = %s OR quadra_nome = %s)")
        params.extend([str(quadra_ordem), f"Quadra {quadra_ordem}"])

    if operador_login:
        filtros.append("REGEXP_REPLACE(COALESCE(operador_login, ''), '\\D', '', 'g') = REGEXP_REPLACE(COALESCE(%s, ''), '\\D', '', 'g')")
        params.append(operador_login)

    where = " AND ".join(filtros)

    chave_cache = _cache_key(
        "partida_ativa_arbitro",
        competicao,
        quadra_id or "",
        quadra_nome or "",
        quadra_ordem or "",
        operador_login or "",
        where,
    )
    cached = _cache_get(chave_cache, ttl=_ARBITRO_POLL_CACHE_TTL)
    if cached is not None:
        return cached

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    id, competicao, ordem, quadra, grupo,
                    equipe_a, equipe_b, equipe_a_operacional, equipe_b_operacional,
                    status, status_operacao, status_jogo, fase_partida,
                    set_atual, pontos_a, pontos_b, sets_a, sets_b,
                    pre_jogo_finalizado, arbitro_1_nome, arbitro_2_nome,
                    operador_nome, operador_login
                FROM partidas
                WHERE {where}
                  AND LOWER(COALESCE(status, '')) NOT IN ('finalizada','finalizado','encerrada','encerrado')
                  AND LOWER(COALESCE(status_operacao, '')) NOT IN ('finalizada','finalizado','encerrada','encerrado')
                  AND LOWER(COALESCE(status_jogo, '')) NOT IN ('finalizada','finalizado','encerrada','encerrado')
                  AND LOWER(COALESCE(fase_partida, '')) NOT IN ('finalizada','finalizado','encerrada','encerrado')
                  AND (
                        COALESCE(pre_jogo_finalizado, FALSE) = TRUE
                     OR COALESCE(pontos_a, 0) > 0
                     OR COALESCE(pontos_b, 0) > 0
                     OR LOWER(COALESCE(status, '')) IN ('pre_jogo','papeleta','papeleta_pronta','em_andamento','andamento','ao_vivo','jogo','iniciada','iniciado','entre_sets','tiebreak_sorteio')
                     OR LOWER(COALESCE(status_operacao, '')) IN ('pre_jogo','papeleta','papeleta_pronta','em_andamento','andamento','ao_vivo','jogo','iniciada','iniciado','entre_sets','tiebreak_sorteio')
                     OR LOWER(COALESCE(status_jogo, '')) IN ('pre_jogo','papeleta','papeleta_pronta','em_andamento','andamento','ao_vivo','jogo','iniciada','iniciado','entre_sets','tiebreak_sorteio')
                     OR LOWER(COALESCE(fase_partida, '')) IN ('pre_jogo','papeleta','papeleta_pronta','em_andamento','andamento','ao_vivo','jogo','iniciada','iniciado','entre_sets','tiebreak_sorteio')
                  )
                ORDER BY
                    CASE
                        WHEN LOWER(COALESCE(status_jogo, '')) IN ('em_andamento','andamento','ao_vivo','jogo') THEN 1
                        WHEN LOWER(COALESCE(status_operacao, '')) IN ('em_andamento','andamento','ao_vivo','jogo') THEN 2
                        WHEN COALESCE(pontos_a, 0) > 0 OR COALESCE(pontos_b, 0) > 0 THEN 3
                        WHEN LOWER(COALESCE(status, '')) IN ('em_andamento','andamento','ao_vivo','jogo','iniciada','iniciado') THEN 4
                        WHEN COALESCE(pre_jogo_finalizado, FALSE) = TRUE THEN 5
                        ELSE 9
                    END,
                    id DESC
                LIMIT 1
                """,
                tuple(params),
            )
            return _cache_set(chave_cache, cur.fetchone())


@painel_bp.route("/inicio")
@login_obrigatorio
def inicio():
    perfil = _perfil_normalizado()

    # =========================
    # SUPER ADMIN
    # =========================
    if perfil == "superadmin":
        login_superadmin = session.get("usuario")
        eh_master = superadmin_eh_master(login_superadmin)
        totais = _cache_get(_cache_key("superadmin_totais", login_superadmin), ttl=30)
        if totais is None:
            totais = {
                "total_competicoes": contar_competicoes(login_superadmin),
                "total_equipes": contar_equipes(login_superadmin),
                "total_partidas": contar_partidas(login_superadmin),
            }
            _cache_set(_cache_key("superadmin_totais", login_superadmin), totais)

        return render_template(
            "painel_superadmin.html",
            eh_master=eh_master,
            superadmins_clientes=listar_superadmins_clientes(login_superadmin) if eh_master else [],
            **totais
        )

    # =========================
    # ORGANIZADOR
    # =========================
    elif perfil == "organizador":
        login_organizador = session.get("usuario")
        competicoes = _listar_competicoes_organizador_cache(login_organizador)

        # O organizador não pode depender de competicao_vinculada.
        # A competição dele vem da relação criada no cadastro da competição
        # (organizador_login / responsável). Se existir pelo menos uma competição,
        # o painel deve liberar a operação automaticamente.
        competicao_atual = (
            (session.get("competicao_atual") or "").strip()
            or (session.get("competicao_vinculada") or "").strip()
        )

        nomes_competicoes = []
        for comp in competicoes:
            if isinstance(comp, dict):
                nome_comp = (
                    comp.get("competicao")
                    or comp.get("nome")
                    or comp.get("nome_competicao")
                    or comp.get("titulo")
                    or ""
                )
            else:
                nome_comp = str(comp or "")
            nome_comp = nome_comp.strip()
            if nome_comp:
                nomes_competicoes.append(nome_comp)

        if not competicao_atual and nomes_competicoes:
            competicao_atual = nomes_competicoes[0]

        if competicao_atual:
            session["competicao_atual"] = competicao_atual
            session["competicao_vinculada"] = competicao_atual

        tem_competicao = bool(nomes_competicoes or competicao_atual)

        if tem_competicao and competicao_atual:
            garantir_schema_fluxo_configuracao_competicoes()
            status_config = status_configuracao_inicial_competicao(competicao_atual)
            if not status_config.get("concluida"):
                flash("Complete a configuração inicial da competição antes de liberar os demais módulos.", "erro")
                return redirect(url_for("competicoes.listar_competicoes_view"))

        solicitacoes_pendentes = contar_solicitacoes_pendentes(competicao_atual) if competicao_atual else 0
        ultimas_solicitacoes = listar_solicitacoes_equipes(competicao_atual, status="pendente", limite=5) if competicao_atual else []
        notificacoes_organizador = listar_notificacoes_sistema(competicao_atual, "organizador", limite=5) if competicao_atual else []

        return render_template(
            "painel_organizador.html",
            competicoes=competicoes,
            competicao_atual=competicao_atual,
            competicao_vinculada=competicao_atual,
            competicao=competicao_atual,
            tem_competicao=tem_competicao,
            total_competicoes=len(nomes_competicoes),
            operacao_liberada=tem_competicao,
            mensagem=None if tem_competicao else "Você ainda não possui competição cadastrada.",
            solicitacoes_pendentes=solicitacoes_pendentes,
            ultimas_solicitacoes=ultimas_solicitacoes,
            notificacoes_organizador=notificacoes_organizador,
        )

    # =========================
    # APONTADOR
    # =========================
    elif perfil == "apontador":

        cpf = session.get("usuario")
        competicoes = _listar_competicoes_apontador_cache(cpf)

        try:
            pode_jogo_avulso = bool(apontador_pode_criar_jogo_avulso(cpf))
        except Exception:
            pode_jogo_avulso = False

        if not competicoes:
            return render_template(
                "painel_apontador.html",
                competicoes=[],
                pode_jogo_avulso=pode_jogo_avulso,
                mensagem="Você não está vinculado a nenhuma competição ativa."
            )

        if len(competicoes) == 1:
            return render_template(
                "painel_apontador.html",
                competicao_unica=competicoes[0],
                pode_jogo_avulso=pode_jogo_avulso,
            )

        return render_template(
            "painel_apontador.html",
            competicoes=competicoes,
            pode_jogo_avulso=pode_jogo_avulso,
        )

    # =========================
    # ÁRBITROS
    # =========================
    elif perfil in {"mesario", "arbitro"}:

        return redirect(
            url_for("painel.painel_arbitros")
        )

    # =========================
    # EQUIPE
    # =========================
    elif perfil == "equipe":

        return redirect(
            url_for("equipes.painel_equipe_inicio_view")
        )

    # =========================
    # FALLBACK
    # =========================
    return redirect(
        url_for("auth.login")
    )


@painel_bp.route("/arbitro", methods=["GET", "POST"])
@painel_bp.route("/painel-arbitros", methods=["GET", "POST"])
def painel_arbitros():

    if request.method == "POST":
        acao = (request.form.get("acao") or "").strip()
        if acao == "trocar_pin":
            _limpar_vinculo_arbitro_sessao()
            flash("Vínculo removido. Digite o PIN da nova quadra ou jogo.", "sucesso")
            return redirect(url_for("painel.painel_arbitros"))

        pin = (request.form.get("pin") or "").strip()
        pin_limpo = "".join(ch for ch in pin if ch.isdigit())
        if len(pin_limpo) != 4:
            flash("Digite um PIN de 4 números.", "erro")
            return redirect(url_for("painel.painel_arbitros"))

        vinculo_operacional = buscar_vinculo_operacional_por_pin(pin_limpo)
        if vinculo_operacional:
            session["arbitro_pin_validado"] = True
            session["arbitro_pin_tipo"] = "operacional"
            session["arbitro_pin"] = pin_limpo
            session["arbitro_competicao"] = vinculo_operacional.get("competicao") or ""
            session["arbitro_apontador_cpf"] = vinculo_operacional.get("apontador_cpf") or ""
            session["arbitro_apontador_nome"] = vinculo_operacional.get("apontador_nome") or ""
            _cache_delete_prefix(("partida_ativa_arbitro",))
            flash("PIN validado. Escolha 1º ou 2º árbitro.", "sucesso")
            return redirect(url_for("painel.painel_arbitros"))

        vinculo = buscar_vinculo_arbitragem_por_pin(pin_limpo)
        if vinculo:
            session["arbitro_pin_validado"] = True
            session["arbitro_pin_tipo"] = "competicao"
            session["arbitro_pin"] = pin_limpo
            session["arbitro_competicao"] = vinculo.get("competicao") or ""
            session["arbitro_quadra_id"] = vinculo.get("id")
            session["arbitro_quadra_nome"] = vinculo.get("nome") or ""
            session["arbitro_quadra_local"] = vinculo.get("local") or ""
            session["arbitro_quadra_ordem"] = vinculo.get("ordem")
            _cache_delete_prefix(("partida_ativa_arbitro",))
            flash("PIN validado. Escolha 1º ou 2º árbitro.", "sucesso")
            return redirect(url_for("painel.painel_arbitros"))

        try:
            from routes.jogo_avulso import buscar_jogo_avulso_por_pin
            vinculo_avulso = buscar_jogo_avulso_por_pin(pin_limpo)
        except Exception as e:
            print("ERRO buscar_jogo_avulso_por_pin:", e, flush=True)
            vinculo_avulso = None

        if vinculo_avulso:
            session["arbitro_pin_validado"] = True
            session["arbitro_pin_tipo"] = "avulso"
            session["arbitro_jogo_avulso_pin"] = pin_limpo
            session["arbitro_jogo_avulso_codigo"] = vinculo_avulso.get("codigo") or ""
            session["arbitro_jogo_avulso_equipe_a"] = vinculo_avulso.get("equipe_a") or "Equipe A"
            session["arbitro_jogo_avulso_equipe_b"] = vinculo_avulso.get("equipe_b") or "Equipe B"
            _cache_delete_prefix(("partida_ativa_arbitro",))
            flash("PIN do jogo rápido validado. Escolha 1º ou 2º árbitro.", "sucesso")
            return redirect(url_for("painel.painel_arbitros"))

        flash("PIN não encontrado ou não está mais ativo.", "erro")
        return redirect(url_for("painel.painel_arbitros"))

    competicao = _competicao_arbitro_logado()
    vinculo_arbitro = _vinculo_arbitro_sessao()

    return render_template(
        "painel_arbitro.html",
        competicao=competicao,
        vinculo_arbitro=vinculo_arbitro,
        nome=session.get("nome") or session.get("usuario")
    )


@painel_bp.route("/painel-arbitro-1")
def painel_arbitro_1():
    if not _usuario_tem_perfil_arbitro():
        flash("Você não tem permissão para acessar o painel do 1º árbitro.", "erro")
        return redirect(url_for("painel.inicio"))

    vinculo = _vinculo_arbitro_sessao()
    if not vinculo:
        flash("Digite o PIN da quadra ou do jogo rápido antes de abrir o painel.", "erro")
        return redirect(url_for("painel.painel_arbitros"))
    if vinculo.get("tipo") == "avulso":
        return redirect(url_for("jogo_avulso.arbitro1_jogo_avulso", codigo=vinculo.get("codigo")))

    return render_template(
        "painel_arbitro_automatico.html",
        tipo="primeiro",
        titulo="Painel do 1º Árbitro",
        subtitulo="Tablet fixo do árbitro principal. A partida aparecerá automaticamente após o pré-jogo.",
        endpoint_status=url_for("painel.proxima_partida_arbitro_1"),
        voltar_url=url_for("painel.painel_arbitros"),
    )


@painel_bp.route("/painel-arbitro-2")
def painel_arbitro_2():
    if not _usuario_tem_perfil_arbitro():
        flash("Você não tem permissão para acessar o painel do 2º árbitro.", "erro")
        return redirect(url_for("painel.inicio"))

    vinculo = _vinculo_arbitro_sessao()
    if not vinculo:
        flash("Digite o PIN da quadra ou do jogo rápido antes de abrir o painel.", "erro")
        return redirect(url_for("painel.painel_arbitros"))
    if vinculo.get("tipo") == "avulso":
        return redirect(url_for("jogo_avulso.arbitro2_jogo_avulso", codigo=vinculo.get("codigo")))

    return render_template(
        "painel_arbitro_automatico.html",
        tipo="segundo",
        titulo="Painel do 2º Árbitro",
        subtitulo="Tablet fixo do segundo árbitro. A partida aparecerá automaticamente após o pré-jogo.",
        endpoint_status=url_for("painel.proxima_partida_arbitro_2"),
        voltar_url=url_for("painel.painel_arbitros"),
    )


def _resposta_proxima_partida_arbitro(tipo):
    if not _usuario_tem_perfil_arbitro():
        return jsonify({"ok": False, "erro": "sem_permissao"}), 403

    vinculo = _vinculo_arbitro_sessao()
    if not vinculo or vinculo.get("tipo") not in {"competicao", "operacional"}:
        return jsonify({"ok": False, "erro": "PIN de árbitro não validado."}), 403

    competicao = _competicao_arbitro_logado()
    partida = _buscar_partida_ativa_para_painel_arbitro(
        competicao,
        quadra_id=vinculo.get("quadra_id") if vinculo.get("tipo") == "competicao" else None,
        quadra_nome=vinculo.get("quadra_nome") if vinculo.get("tipo") == "competicao" else None,
        quadra_ordem=vinculo.get("quadra_ordem") if vinculo.get("tipo") == "competicao" else None,
        operador_login=vinculo.get("apontador_cpf") if vinculo.get("tipo") == "operacional" else None,
    )

    if not partida:
        return jsonify({
            "ok": True,
            "tem_partida": False,
            "competicao": competicao,
            "mensagem": "Aguardando o apontador salvar o pré-jogo."
        })

    rota = "oficiais.primeiro_arbitro_view" if tipo == "primeiro" else "oficiais.segundo_arbitro_view"
    url = url_for(rota, competicao=partida["competicao"], partida_id=partida["id"])

    return jsonify({
        "ok": True,
        "tem_partida": True,
        "url": url,
        "partida": {
            "id": partida.get("id"),
            "competicao": partida.get("competicao"),
            "ordem": partida.get("ordem"),
            "quadra": partida.get("quadra"),
            "grupo": partida.get("grupo"),
            "equipe_a": partida.get("equipe_a_operacional") or partida.get("equipe_a"),
            "equipe_b": partida.get("equipe_b_operacional") or partida.get("equipe_b"),
            "status": partida.get("status"),
            "status_operacao": partida.get("status_operacao"),
            "operador": partida.get("operador_nome") or partida.get("operador_login") or "",
        }
    })


@painel_bp.route("/painel-arbitro-1/proxima")
def proxima_partida_arbitro_1():
    return _resposta_proxima_partida_arbitro("primeiro")


@painel_bp.route("/painel-arbitro-2/proxima")
def proxima_partida_arbitro_2():
    return _resposta_proxima_partida_arbitro("segundo")


@painel_bp.route("/minha-conta")
@login_obrigatorio
def minha_conta():
    login = session.get("usuario")
    perfil = _perfil_normalizado()

    dados_equipe = None
    perfil_exibicao = perfil

    if perfil == "apontador":
        apontador = _buscar_apontador_cache(login)

        if not apontador:
            flash("Apontador não encontrado.", "erro")
            return redirect(url_for("painel.inicio"))

        return render_template(
            "minha_conta.html",
            usuario=apontador.get("cpf"),
            nome=apontador.get("nome") or session.get("nome") or apontador.get("cpf"),
            perfil="apontador",
            perfil_exibicao="apontador",
            dados_equipe=None
        )

    usuario_db = _buscar_usuario_cache(login)

    if not usuario_db:
        flash("Usuário não encontrado.", "erro")
        return redirect(url_for("painel.inicio"))

    if perfil == "equipe":
        try:
            dados_equipe = buscar_perfil_equipe_por_login(login)
        except Exception as e:
            print("AVISO buscar dados equipe minha conta:", repr(e))
            dados_equipe = None

    if perfil in ["mesario", "arbitro"]:
        perfil_exibicao = "árbitro"
    else:
        perfil_exibicao = usuario_db.get("perfil") or perfil

    return render_template(
        "minha_conta.html",
        usuario=usuario_db.get("login"),
        nome=usuario_db.get("nome") or usuario_db.get("login"),
        perfil=usuario_db.get("perfil") or perfil,
        perfil_exibicao=perfil_exibicao,
        dados_equipe=dados_equipe
    )


@painel_bp.route("/minha-conta/salvar-dados", methods=["POST"])
@login_obrigatorio
def salvar_dados_minha_conta():
    login_atual = session.get("usuario")
    perfil = _perfil_normalizado()

    nome = (request.form.get("nome") or "").strip()
    novo_login = (request.form.get("login") or "").strip()

    if not nome or not novo_login:
        flash("Preencha nome e login.", "erro")
        return redirect(url_for("painel.minha_conta"))

    if perfil == "apontador":
        resultado = atualizar_dados_conta_apontador(login_atual, novo_login, nome)
    else:
        resultado = atualizar_dados_conta_usuario(login_atual, novo_login, nome)

    if not resultado.get("ok"):
        flash(resultado.get("erro") or "Não foi possível atualizar sua conta.", "erro")
        return redirect(url_for("painel.minha_conta"))

    session["usuario"] = resultado.get("login") or novo_login
    session["nome"] = resultado.get("nome") or nome

    if perfil == "equipe":
        salvar_perfil_equipe_por_login(
            session.get("usuario"),
            request.form.get("cidade", "").strip(),
            request.form.get("responsavel", "").strip(),
            request.form.get("telefone", "").strip(),
            request.form.get("email", "").strip(),
            request.form.get("instagram", "").strip(),
        )

    _cache_delete_prefix(("usuario",))
    _cache_delete_prefix(("apontador",))
    _cache_delete_prefix(("competicoes_organizador",))
    _cache_delete_prefix(("competicoes_apontador",))

    flash("Dados da conta atualizados com sucesso.", "sucesso")
    return redirect(url_for("painel.minha_conta"))


@painel_bp.route("/minha-conta/alterar-senha", methods=["POST"])
@login_obrigatorio
def alterar_senha_minha_conta():
    login = session.get("usuario")
    perfil = _perfil_normalizado()

    senha_atual = (request.form.get("senha_atual") or "").strip()
    nova_senha = (request.form.get("nova_senha") or "").strip()
    confirmar_senha = (request.form.get("confirmar_senha") or "").strip()

    if not senha_atual or not nova_senha or not confirmar_senha:
        flash("Preencha todos os campos.", "erro")
        return redirect(url_for("painel.minha_conta"))

    if nova_senha != confirmar_senha:
        flash("A confirmação da senha não confere.", "erro")
        return redirect(url_for("painel.minha_conta"))

    if len(nova_senha) < 4:
        flash("A nova senha deve ter pelo menos 4 caracteres.", "erro")
        return redirect(url_for("painel.minha_conta"))

    if perfil == "apontador":
        apontador = _buscar_apontador_cache(login)

        if not apontador:
            flash("Apontador não encontrado.", "erro")
            return redirect(url_for("painel.inicio"))

        senha_salva = apontador.get("senha")

        if senha_salva and senha_atual != senha_salva:
            flash("Senha atual incorreta.", "erro")
            return redirect(url_for("painel.minha_conta"))

        definir_senha_apontador(login, nova_senha)
        _cache_delete_prefix(("apontador",))

        flash("Senha alterada com sucesso!", "sucesso")
        return redirect(url_for("painel.minha_conta"))

    usuario_db = _buscar_usuario_cache(login)

    if not usuario_db:
        flash("Usuário não encontrado.", "erro")
        return redirect(url_for("painel.inicio"))

    if senha_atual != usuario_db.get("senha"):
        flash("Senha atual incorreta.", "erro")
        return redirect(url_for("painel.minha_conta"))

    atualizar_senha_usuario(login, nova_senha)
    _cache_delete_prefix(("usuario",))

    flash("Senha alterada com sucesso!", "sucesso")
    return redirect(url_for("painel.minha_conta"))




# =========================================================
# SUPERADM MASTER - CONFIGURAÇÃO GLOBAL DO JOGO RÁPIDO
# =========================================================
def _exigir_superadmin_master_json():
    if _perfil_normalizado() != "superadmin":
        return False, (jsonify({"ok": False, "erro": "Sem permissão."}), 403)

    login = session.get("usuario")
    try:
        if not superadmin_eh_master(login):
            return False, (jsonify({"ok": False, "erro": "Apenas o SuperADM master pode alterar esta configuração."}), 403)
    except Exception:
        return False, (jsonify({"ok": False, "erro": "Não foi possível validar o SuperADM master."}), 500)

    return True, None


@painel_bp.route("/superadmin/config/jogo-rapido", methods=["GET"])
@login_obrigatorio
def config_jogo_rapido_global_get():
    ok, erro = _exigir_superadmin_master_json()
    if not ok:
        return erro

    try:
        liberado = bool(jogo_rapido_global_habilitado())
        return jsonify({
            "ok": True,
            "jogo_rapido_liberado": liberado,
            "jogo_rapido_global_habilitado": liberado,
        })
    except Exception as e:
        print("ERRO config_jogo_rapido_global_get:", repr(e), flush=True)
        return jsonify({"ok": False, "erro": "Erro ao carregar configuração do Jogo Rápido."}), 500


@painel_bp.route("/superadmin/config/jogo-rapido", methods=["POST"])
@login_obrigatorio
def config_jogo_rapido_global_post():
    ok, erro = _exigir_superadmin_master_json()
    if not ok:
        return erro

    dados = request.get_json(silent=True) or {}
    liberado = bool(
        dados.get("jogo_rapido_liberado")
        if "jogo_rapido_liberado" in dados
        else dados.get("jogo_rapido_global_habilitado")
    )

    try:
        definir_jogo_rapido_global_habilitado(liberado, atualizado_por=session.get("usuario"))
        _cache_delete_prefix(("competicoes_apontador",))
        return jsonify({
            "ok": True,
            "jogo_rapido_liberado": liberado,
            "jogo_rapido_global_habilitado": liberado,
        })
    except Exception as e:
        print("ERRO config_jogo_rapido_global_post:", repr(e), flush=True)
        return jsonify({"ok": False, "erro": "Erro ao salvar configuração do Jogo Rápido."}), 500

# =========================================================
# SUPERADM MASTER - CLIENTES / SUPERADMS ABAIXO DO THALISADM
# =========================================================
@painel_bp.route("/superadmins", methods=["GET"])
@login_obrigatorio
def superadmins_clientes():
    if _perfil_normalizado() != "superadmin":
        return redirect(url_for("painel.inicio"))

    login = session.get("usuario")
    if not superadmin_eh_master(login):
        flash("Apenas o ThalisADM master pode gerenciar SuperADMs de clientes.", "erro")
        return redirect(url_for("painel.inicio"))

    return render_template(
        "superadmins_clientes.html",
        superadmins=listar_superadmins_clientes(login),
        credenciais=session.pop("credenciais_superadmin_cliente", None),
    )


@painel_bp.route("/superadmins/novo", methods=["POST"])
@login_obrigatorio
def novo_superadmin_cliente():
    if _perfil_normalizado() != "superadmin":
        return redirect(url_for("painel.inicio"))

    login = session.get("usuario")
    if not superadmin_eh_master(login):
        flash("Apenas o ThalisADM master pode criar SuperADMs de clientes.", "erro")
        return redirect(url_for("painel.inicio"))

    resultado = criar_superadmin_cliente(
        login,
        request.form.get("nome_cliente", "").strip(),
        request.form.get("nome_admin", "").strip(),
        request.form.get("login_admin", "").strip(),
    )

    if not resultado.get("ok"):
        flash(resultado.get("erro") or "Não foi possível criar o SuperADM.", "erro")
        return redirect(url_for("painel.superadmins_clientes"))

    session["credenciais_superadmin_cliente"] = resultado
    _cache_delete_prefix(("superadmin_totais",))
    flash("SuperADM de cliente criado com sucesso.", "sucesso")
    return redirect(url_for("painel.superadmins_clientes"))


@painel_bp.route("/superadmins/excluir/<login_alvo>", methods=["POST"])
@login_obrigatorio
def excluir_superadmin_cliente_view(login_alvo):
    if _perfil_normalizado() != "superadmin":
        return redirect(url_for("painel.inicio"))

    login = session.get("usuario")
    resultado = excluir_superadmin_cliente(login, login_alvo)

    if not resultado.get("ok"):
        flash(resultado.get("erro") or "Não foi possível excluir o SuperADM.", "erro")
    else:
        flash("SuperADM removido com sucesso. O cliente foi desativado.", "sucesso")
        _cache_delete_prefix(("superadmin_totais",))

    return redirect(url_for("painel.superadmins_clientes"))
