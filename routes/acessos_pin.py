from flask import Blueprint, render_template, request, redirect, session, url_for, flash, jsonify

from banco import (
    conectar,
    buscar_vinculo_operacional_por_pin,
    buscar_vinculo_arbitragem_por_pin,
)

try:
    from socket_events import obter_ultimo_placar_apontador
except Exception:
    obter_ultimo_placar_apontador = None

acessos_pin_bp = Blueprint("acessos_pin", __name__)


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


# SQL literal seguro para evitar erro do psycopg/Postgres:
# "malformed array literal: (finalizada,finalizado,...)"
# O erro acontecia porque tuplas Python estavam sendo enviadas para ANY/ALL.
STATUS_ATIVOS_ARBITRO_SQL = "('pre_jogo','papeleta','papeleta_pronta','em_andamento','andamento','ao_vivo','jogo','iniciada','iniciado','entre_sets','tiebreak_sorteio','aberta','aberto','aguardando','aguardando_jogo','operacao','operação','ao vivo')"
STATUS_FINALIZADOS_ARBITRO_SQL = "('finalizada','finalizado','encerrada','encerrado')"


def _pin_limpo(valor):
    return "".join(ch for ch in str(valor or "") if ch.isdigit())[:4]


def _limpar_sessao_arbitro():
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


def _limpar_sessao_telao():
    for chave in [
        "telao_pin_validado",
        "telao_pin",
        "telao_competicao",
        "telao_apontador",
        "telao_apontador_nome",
    ]:
        session.pop(chave, None)


def _vinculo_arbitro_sessao():
    if not session.get("arbitro_pin_validado"):
        return None

    tipo = (session.get("arbitro_pin_tipo") or "").strip().lower()

    if tipo == "operacional":
        return {
            "tipo": "operacional",
            "competicao": session.get("arbitro_competicao") or "",
            "apontador_cpf": session.get("arbitro_apontador_cpf") or "",
            "apontador_nome": session.get("arbitro_apontador_nome") or "",
            "pin": session.get("arbitro_pin") or "",
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

    return None



def _partida_ativa_por_cache_apontador(vinculo):
    """
    Tenta descobrir a partida aberta pelo mesmo canal do telão/apontador.

    Esse é o ajuste principal para o acesso por PIN: o telão já sabe qual é a
    partida porque o apontador publica o último placar por apontador. O árbitro
    deve usar o mesmo sinal antes de depender do status salvo na tabela partidas,
    que pode demorar ou vir diferente do painel antigo.
    """
    if not vinculo or vinculo.get("tipo") != "operacional" or obter_ultimo_placar_apontador is None:
        return None

    apontador = (vinculo.get("apontador_cpf") or "").strip()
    competicao_pin = (vinculo.get("competicao") or "").strip()
    if not apontador:
        return None

    try:
        estado = obter_ultimo_placar_apontador(apontador) or {}
    except Exception as e:
        print("ERRO cache placar apontador árbitro:", e, flush=True)
        return None

    if not isinstance(estado, dict):
        return None

    partida_id = estado.get("partida_id") or estado.get("id") or estado.get("partida")
    competicao_estado = (estado.get("competicao") or competicao_pin or "").strip()

    if not partida_id:
        return None
    if competicao_pin and competicao_estado and competicao_pin != competicao_estado:
        return None

    try:
        partida_id_int = int(partida_id)
    except Exception:
        return None

    return {
        "id": partida_id_int,
        "competicao": competicao_estado or competicao_pin,
        "ordem": estado.get("ordem") or estado.get("partida_ordem"),
        "quadra": estado.get("quadra") or estado.get("quadra_nome"),
        "quadra_id": estado.get("quadra_id"),
        "quadra_nome": estado.get("quadra_nome") or estado.get("quadra"),
        "grupo": estado.get("grupo"),
        "equipe_a": estado.get("equipe_a") or estado.get("nome_a") or "Equipe A",
        "equipe_b": estado.get("equipe_b") or estado.get("nome_b") or "Equipe B",
        "equipe_a_operacional": estado.get("equipe_a") or estado.get("nome_a") or "Equipe A",
        "equipe_b_operacional": estado.get("equipe_b") or estado.get("nome_b") or "Equipe B",
        "status": estado.get("status") or "ao_vivo",
        "status_operacao": estado.get("status_operacao") or "ao_vivo",
        "status_jogo": estado.get("status_jogo") or "em_andamento",
        "fase_partida": estado.get("fase_partida") or "jogo",
        "set_atual": estado.get("set_atual") or 1,
        "pontos_a": estado.get("pontos_a") or estado.get("placar_a") or 0,
        "pontos_b": estado.get("pontos_b") or estado.get("placar_b") or 0,
        "sets_a": estado.get("sets_a") or 0,
        "sets_b": estado.get("sets_b") or 0,
        "pre_jogo_finalizado": True,
        "arbitro_1_nome": "",
        "arbitro_2_nome": "",
        "operador_nome": vinculo.get("apontador_nome") or "",
        "operador_login": apontador,
        "_origem": "cache_placar_apontador",
    }

def _buscar_partida_ativa_por_pin(vinculo):
    """
    Busca a partida que deve abrir no tablet do árbitro.

    Correções importantes:
    - tenta primeiro respeitando o apontador/quadra do PIN;
    - se não achar, faz fallback por competição, porque em algumas partidas
      antigas o operador_login/quadra_id pode estar vazio;
    - aceita status de pré-jogo, papeleta e jogo em andamento;
    - nunca pega partida finalizada.
    """
    if not vinculo:
        return None

    competicao = (vinculo.get("competicao") or "").strip()
    if not competicao:
        return None

    ativos = tuple(STATUS_ATIVOS_ARBITRO) + (
        "aberta",
        "aberto",
        "aguardando",
        "aguardando_jogo",
        "operacao",
        "operação",
        "ao vivo",
    )
    finalizados = tuple(STATUS_FINALIZADOS_ARBITRO)

    colunas = """
        id,
        competicao,
        ordem,
        quadra,
        quadra_id,
        quadra_nome,
        grupo,
        equipe_a,
        equipe_b,
        equipe_a_operacional,
        equipe_b_operacional,
        status,
        status_operacao,
        status_jogo,
        fase_partida,
        set_atual,
        pontos_a,
        pontos_b,
        sets_a,
        sets_b,
        pre_jogo_finalizado,
        arbitro_1_nome,
        arbitro_2_nome,
        operador_nome,
        operador_login
    """

    def consultar(com_filtro_operacional=True):
        filtros = ["competicao = %s"]
        params = [competicao]

        if com_filtro_operacional and vinculo.get("tipo") == "operacional":
            apontador = (vinculo.get("apontador_cpf") or "").strip()
            if apontador:
                filtros.append("(COALESCE(operador_login, '') = %s OR COALESCE(operador_nome, '') = %s OR COALESCE(operador_login, '') = '')")
                params.extend([apontador, apontador])

        if com_filtro_operacional and vinculo.get("tipo") == "competicao":
            quadra_id = vinculo.get("quadra_id")
            quadra_nome = (vinculo.get("quadra_nome") or "").strip()
            quadra_ordem = vinculo.get("quadra_ordem")
            if quadra_id:
                filtros.append("(quadra_id = %s OR quadra_nome = %s OR quadra = %s OR quadra = %s OR quadra_id IS NULL)")
                params.extend([quadra_id, quadra_nome, quadra_nome, str(quadra_ordem or "")])
            elif quadra_nome:
                filtros.append("(quadra_nome = %s OR quadra = %s OR COALESCE(quadra_nome, '') = '')")
                params.extend([quadra_nome, quadra_nome])

        where = " AND ".join(filtros)

        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {colunas}
                    FROM partidas
                    WHERE {where}
                      AND LOWER(COALESCE(status, '')) NOT IN {STATUS_FINALIZADOS_ARBITRO_SQL}
                      AND LOWER(COALESCE(status_operacao, '')) NOT IN {STATUS_FINALIZADOS_ARBITRO_SQL}
                      AND LOWER(COALESCE(status_jogo, '')) NOT IN {STATUS_FINALIZADOS_ARBITRO_SQL}
                      AND (
                            COALESCE(pre_jogo_finalizado, FALSE) = TRUE
                         OR LOWER(COALESCE(status, '')) IN {STATUS_ATIVOS_ARBITRO_SQL}
                         OR LOWER(COALESCE(status_operacao, '')) IN {STATUS_ATIVOS_ARBITRO_SQL}
                         OR LOWER(COALESCE(status_jogo, '')) IN {STATUS_ATIVOS_ARBITRO_SQL}
                         OR LOWER(COALESCE(fase_partida, '')) IN {STATUS_ATIVOS_ARBITRO_SQL}
                         OR COALESCE(pontos_a, 0) > 0
                         OR COALESCE(pontos_b, 0) > 0
                         OR COALESCE(set_atual, 1) > 1
                      )
                    ORDER BY
                        CASE
                            WHEN LOWER(COALESCE(status_jogo, '')) IN ('em_andamento', 'andamento', 'ao_vivo', 'ao vivo', 'jogo') THEN 1
                            WHEN LOWER(COALESCE(status_operacao, '')) IN ('em_andamento', 'andamento', 'ao_vivo', 'ao vivo', 'jogo', 'operacao', 'operação') THEN 2
                            WHEN LOWER(COALESCE(status, '')) IN ('em_andamento', 'andamento', 'ao_vivo', 'ao vivo', 'jogo', 'iniciada', 'iniciado') THEN 3
                            WHEN COALESCE(pre_jogo_finalizado, FALSE) = TRUE THEN 4
                            ELSE 9
                        END,
                        COALESCE(ordem, 999999),
                        id DESC
                    LIMIT 1
                    """,
                    tuple(params),
                )
                return cur.fetchone()

    try:
        partida = consultar(com_filtro_operacional=True)
        if partida:
            return partida
    except Exception as e:
        print("ERRO buscar partida árbitro com filtro:", e, flush=True)

    try:
        return consultar(com_filtro_operacional=False)
    except Exception as e:
        print("ERRO buscar partida árbitro fallback:", e, flush=True)
        return None


def _buscar_partida_aberta_por_pin(vinculo):
    """
    Fallback mais aberto para o acesso por PIN.

    Esse é o ponto que corrige o problema relatado: o login antigo do árbitro
    abria /painel-arbitro-1 e encontrava a partida mesmo quando ela ainda não
    estava marcada como pre_jogo_finalizado/status ativo. O acesso por PIN
    ficava preso em /arbitro/1 ou /arbitro/2 porque a busca era restritiva.

    Aqui pegamos a melhor partida NÃO finalizada da competição/quadra/apontador,
    mesmo que o status ainda esteja como aguardando/aberta/pendente.
    """
    if not vinculo:
        return None

    competicao = (vinculo.get("competicao") or "").strip()
    if not competicao:
        return None

    finalizados = tuple(STATUS_FINALIZADOS_ARBITRO)

    colunas = """
        id,
        competicao,
        ordem,
        quadra,
        quadra_id,
        quadra_nome,
        grupo,
        equipe_a,
        equipe_b,
        equipe_a_operacional,
        equipe_b_operacional,
        status,
        status_operacao,
        status_jogo,
        fase_partida,
        set_atual,
        pontos_a,
        pontos_b,
        sets_a,
        sets_b,
        pre_jogo_finalizado,
        arbitro_1_nome,
        arbitro_2_nome,
        operador_nome,
        operador_login
    """

    def consultar(com_filtro=True):
        filtros = ["competicao = %s"]
        params = [competicao]

        if com_filtro and vinculo.get("tipo") == "operacional":
            apontador = (vinculo.get("apontador_cpf") or "").strip()
            if apontador:
                filtros.append("(COALESCE(operador_login, '') = %s OR COALESCE(operador_login, '') = '' OR operador_login IS NULL)")
                params.append(apontador)

        if com_filtro and vinculo.get("tipo") == "competicao":
            quadra_id = vinculo.get("quadra_id")
            quadra_nome = (vinculo.get("quadra_nome") or "").strip()
            quadra_ordem = str(vinculo.get("quadra_ordem") or "").strip()
            conds = []
            if quadra_id:
                conds.append("quadra_id = %s")
                params.append(quadra_id)
            if quadra_nome:
                conds.append("quadra_nome = %s")
                params.append(quadra_nome)
                conds.append("quadra = %s")
                params.append(quadra_nome)
            if quadra_ordem:
                conds.append("quadra = %s")
                params.append(quadra_ordem)
            # Se a partida antiga não tiver quadra vinculada, ainda permite achar.
            conds.append("quadra_id IS NULL")
            conds.append("COALESCE(quadra_nome, '') = ''")
            if conds:
                filtros.append("(" + " OR ".join(conds) + ")")

        where = " AND ".join(filtros)

        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {colunas}
                    FROM partidas
                    WHERE {where}
                      AND LOWER(COALESCE(status, '')) NOT IN {STATUS_FINALIZADOS_ARBITRO_SQL}
                      AND LOWER(COALESCE(status_operacao, '')) NOT IN {STATUS_FINALIZADOS_ARBITRO_SQL}
                      AND LOWER(COALESCE(status_jogo, '')) NOT IN {STATUS_FINALIZADOS_ARBITRO_SQL}
                    ORDER BY
                        CASE
                            WHEN LOWER(COALESCE(status_jogo, '')) IN ('em_andamento','andamento','ao_vivo','ao vivo','jogo') THEN 1
                            WHEN LOWER(COALESCE(status_operacao, '')) IN ('em_andamento','andamento','ao_vivo','ao vivo','jogo','operacao','operação') THEN 2
                            WHEN LOWER(COALESCE(status, '')) IN ('em_andamento','andamento','ao_vivo','ao vivo','jogo','iniciada','iniciado') THEN 3
                            WHEN COALESCE(pre_jogo_finalizado, FALSE) = TRUE THEN 4
                            WHEN LOWER(COALESCE(status, '')) IN ('pre_jogo','papeleta','papeleta_pronta','aberta','aberto','aguardando','pendente') THEN 5
                            ELSE 9
                        END,
                        COALESCE(ordem, 999999),
                        id DESC
                    LIMIT 1
                    """,
                    tuple(params),
                )
                return cur.fetchone()

    try:
        partida = consultar(com_filtro=True)
        if partida:
            return partida
    except Exception as e:
        print("ERRO buscar partida aberta árbitro com filtro:", e, flush=True)

    try:
        return consultar(com_filtro=False)
    except Exception as e:
        print("ERRO buscar partida aberta árbitro fallback:", e, flush=True)
        return None


def _resolver_partida_para_arbitro(vinculo):
    """Busca a partida do árbitro com prioridade no estado vivo do apontador."""
    partida = _partida_ativa_por_cache_apontador(vinculo)
    if partida:
        return partida

    partida = _buscar_partida_ativa_por_pin(vinculo)
    if partida:
        return partida

    return _buscar_partida_aberta_por_pin(vinculo)


@acessos_pin_bp.route("/arbitro", methods=["GET", "POST"])
def arbitro_publico_pin():
    """
    Entrada pública do árbitro por PIN.

    O PIN só libera o canal operacional. Depois de validar, a tela entra direto
    no painel automático que aguarda o apontador abrir/iniciar uma partida.
    """
    if request.args.get("trocar") == "1":
        _limpar_sessao_arbitro()
        return redirect(url_for("acessos_pin.arbitro_publico_pin"))

    if request.method == "POST":
        pin = _pin_limpo(request.form.get("pin"))

        if len(pin) != 4:
            flash("Digite um PIN de 4 números.", "erro")
            return redirect(url_for("acessos_pin.arbitro_publico_pin"))

        vinculo_operacional = buscar_vinculo_operacional_por_pin(pin)
        if vinculo_operacional:
            session["arbitro_pin_validado"] = True
            session["arbitro_pin_tipo"] = "operacional"
            session["arbitro_pin"] = pin
            session["arbitro_competicao"] = vinculo_operacional.get("competicao") or ""
            session["arbitro_apontador_cpf"] = vinculo_operacional.get("apontador_cpf") or ""
            session["arbitro_apontador_nome"] = vinculo_operacional.get("apontador_nome") or ""
            flash("PIN validado. Escolha se este tablet será o 1º ou o 2º árbitro.", "sucesso")
            return redirect(url_for("acessos_pin.arbitro_publico_pin"))

        try:
            vinculo_quadra = buscar_vinculo_arbitragem_por_pin(pin)
        except Exception:
            vinculo_quadra = None

        if vinculo_quadra:
            session["arbitro_pin_validado"] = True
            session["arbitro_pin_tipo"] = "competicao"
            session["arbitro_pin"] = pin
            session["arbitro_competicao"] = vinculo_quadra.get("competicao") or ""
            session["arbitro_quadra_id"] = vinculo_quadra.get("id")
            session["arbitro_quadra_nome"] = vinculo_quadra.get("nome") or ""
            session["arbitro_quadra_local"] = vinculo_quadra.get("local") or ""
            session["arbitro_quadra_ordem"] = vinculo_quadra.get("ordem")
            flash("PIN validado. Escolha se este tablet será o 1º ou o 2º árbitro.", "sucesso")
            return redirect(url_for("acessos_pin.arbitro_publico_pin"))

        flash("PIN não encontrado ou inativo.", "erro")
        return redirect(url_for("acessos_pin.arbitro_publico_pin"))

    if session.get("arbitro_pin_validado"):
        return render_template(
            "painel_arbitro_pin_escolha.html",
            vinculo_arbitro=_vinculo_arbitro_sessao(),
        )

    return render_template("pin_arbitro.html")



@acessos_pin_bp.route("/arbitro/1")
def arbitro_automatico_primeiro():
    vinculo = _vinculo_arbitro_sessao()
    if not vinculo:
        flash("Digite o PIN antes de abrir o painel do árbitro.", "erro")
        return redirect(url_for("acessos_pin.arbitro_publico_pin"))

    # CORREÇÃO: se já existe partida ativa, não fica preso na tela intermediária.
    # Vai direto para a tela final do 1º árbitro.
    partida = _resolver_partida_para_arbitro(vinculo)
    if partida:
        return redirect(url_for(
            "oficiais.primeiro_arbitro_view",
            competicao=partida["competicao"],
            partida_id=partida["id"],
        ))

    return render_template(
        "painel_arbitro_automatico.html",
        tipo="primeiro",
        titulo="Painel do 1º Árbitro",
        subtitulo="Tablet do árbitro principal. Aguarde o apontador abrir ou iniciar uma partida.",
        endpoint_status=url_for("acessos_pin.proxima_partida_arbitro_primeiro"),
        voltar_url=url_for("acessos_pin.arbitro_publico_pin", trocar=1),
    )


@acessos_pin_bp.route("/arbitro/2")
def arbitro_automatico_segundo():
    vinculo = _vinculo_arbitro_sessao()
    if not vinculo:
        flash("Digite o PIN antes de abrir o painel do árbitro.", "erro")
        return redirect(url_for("acessos_pin.arbitro_publico_pin"))

    # CORREÇÃO: se já existe partida ativa, não fica preso na tela intermediária.
    # Vai direto para a tela final do 2º árbitro.
    partida = _resolver_partida_para_arbitro(vinculo)
    if partida:
        return redirect(url_for(
            "oficiais.segundo_arbitro_view",
            competicao=partida["competicao"],
            partida_id=partida["id"],
        ))

    return render_template(
        "painel_arbitro_automatico.html",
        tipo="segundo",
        titulo="Painel do 2º Árbitro",
        subtitulo="Tablet do segundo árbitro. Aguarde o apontador abrir ou iniciar uma partida.",
        endpoint_status=url_for("acessos_pin.proxima_partida_arbitro_segundo"),
        voltar_url=url_for("acessos_pin.arbitro_publico_pin", trocar=1),
    )


def _resposta_proxima_partida(tipo):
    vinculo = _vinculo_arbitro_sessao()
    if not vinculo:
        return jsonify({"ok": False, "erro": "PIN não validado."}), 403

    partida = _resolver_partida_para_arbitro(vinculo)

    if not partida:
        return jsonify({
            "ok": True,
            "tem_partida": False,
            "competicao": vinculo.get("competicao") or "",
            "mensagem": "Aguardando o apontador abrir/iniciar uma partida neste PIN."
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
            "quadra": partida.get("quadra_nome") or partida.get("quadra"),
            "grupo": partida.get("grupo"),
            "equipe_a": partida.get("equipe_a_operacional") or partida.get("equipe_a"),
            "equipe_b": partida.get("equipe_b_operacional") or partida.get("equipe_b"),
            "status": partida.get("status"),
            "status_operacao": partida.get("status_operacao"),
            "operador": partida.get("operador_nome") or partida.get("operador_login") or "",
        }
    })


@acessos_pin_bp.route("/arbitro/1/proxima")
def proxima_partida_arbitro_primeiro():
    return _resposta_proxima_partida("primeiro")


@acessos_pin_bp.route("/arbitro/2/proxima")
def proxima_partida_arbitro_segundo():
    return _resposta_proxima_partida("segundo")


@acessos_pin_bp.route("/telao", methods=["GET", "POST"])
def telao_publico_pin():
    """
    Entrada pública do telão por PIN operacional.
    Depois do PIN, mantém o comportamento antigo do telão do apontador.
    """
    if request.args.get("trocar") == "1":
        _limpar_sessao_telao()
        return redirect(url_for("acessos_pin.telao_publico_pin"))

    if request.method == "POST":
        pin = _pin_limpo(request.form.get("pin"))

        if len(pin) != 4:
            flash("Digite um PIN de 4 números.", "erro")
            return redirect(url_for("acessos_pin.telao_publico_pin"))

        vinculo = buscar_vinculo_operacional_por_pin(pin)
        if not vinculo:
            flash("PIN não encontrado ou inativo.", "erro")
            return redirect(url_for("acessos_pin.telao_publico_pin"))

        apontador = vinculo.get("apontador_cpf") or ""
        if not apontador:
            flash("Este PIN não possui apontador vinculado.", "erro")
            return redirect(url_for("acessos_pin.telao_publico_pin"))

        session["telao_pin_validado"] = True
        session["telao_pin"] = pin
        session["telao_competicao"] = vinculo.get("competicao") or ""
        session["telao_apontador"] = apontador
        session["telao_apontador_nome"] = vinculo.get("apontador_nome") or ""
        return redirect(url_for("apontadores.placar_ao_vivo_apontador", apontador=apontador))

    return render_template("pin_telao.html")
