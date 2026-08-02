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

try:
    from routes.jogo_avulso import buscar_jogo_avulso_por_pin
except Exception:
    try:
        from jogo_avulso import buscar_jogo_avulso_por_pin
    except Exception:
        buscar_jogo_avulso_por_pin = None

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
        "telao_pin_tipo",
        "telao_jogo_avulso_codigo",
        "telao_jogo_avulso_pin",
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

    if tipo == "avulso":
        return {
            "tipo": "avulso",
            "competicao": "JOGO AVULSO",
            "codigo": session.get("arbitro_jogo_avulso_codigo") or "",
            "pin": session.get("arbitro_jogo_avulso_pin") or session.get("arbitro_pin") or "",
            "equipe_a": session.get("arbitro_jogo_avulso_equipe_a") or "Equipe A",
            "equipe_b": session.get("arbitro_jogo_avulso_equipe_b") or "Equipe B",
        }

    return None



def _vinculo_telao_sessao():
    """Reconstrói o vínculo fixo do telão a partir do PIN validado."""
    if not session.get("telao_pin_validado"):
        return None

    tipo = (session.get("telao_pin_tipo") or "operacional").strip().lower()
    if tipo == "avulso":
        return {
            "tipo": "avulso",
            "competicao": "JOGO AVULSO",
            "codigo": session.get("telao_jogo_avulso_codigo") or "",
            "pin": session.get("telao_jogo_avulso_pin") or session.get("telao_pin") or "",
        }

    return {
        "tipo": "operacional",
        "competicao": session.get("telao_competicao") or "",
        "apontador_cpf": session.get("telao_apontador") or "",
        "apontador_nome": session.get("telao_apontador_nome") or "",
        "pin": session.get("telao_pin") or "",
    }



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

    # O último placar pode continuar em cache por alguns segundos depois do fim.
    # Não o devolvemos como partida ativa, pois isso impediria a busca no banco
    # de encontrar o próximo jogo que o apontador acabou de abrir.
    status_cache = " ".join([
        str(estado.get("status") or "").strip().lower(),
        str(estado.get("status_operacao") or "").strip().lower(),
        str(estado.get("status_jogo") or "").strip().lower(),
        str(estado.get("fase_partida") or "").strip().lower(),
    ])
    cache_finalizado = bool(
        estado.get("partida_finalizada")
        or estado.get("finalizada")
        or estado.get("encerrada")
        or estado.get("jogo_encerrado")
        or any(t in status_cache for t in ("finalizada", "finalizado", "encerrada", "encerrado"))
    )
    if cache_finalizado:
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
                      AND NOT (LOWER(COALESCE(status, '')) = ANY(%s))
                      AND NOT (LOWER(COALESCE(status_operacao, '')) = ANY(%s))
                      AND NOT (LOWER(COALESCE(status_jogo, '')) = ANY(%s))
                      AND (
                            COALESCE(pre_jogo_finalizado, FALSE) = TRUE
                         OR LOWER(COALESCE(status, '')) IN %s
                         OR LOWER(COALESCE(status_operacao, '')) IN %s
                         OR LOWER(COALESCE(status_jogo, '')) IN %s
                         OR LOWER(COALESCE(fase_partida, '')) IN %s
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
                    tuple(params + [list(finalizados), list(finalizados), list(finalizados), list(ativos), list(ativos), list(ativos), list(ativos)]),
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
                      AND NOT (LOWER(COALESCE(status, '')) = ANY(%s))
                      AND NOT (LOWER(COALESCE(status_operacao, '')) = ANY(%s))
                      AND NOT (LOWER(COALESCE(status_jogo, '')) = ANY(%s))
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
                    tuple(params + [list(finalizados), list(finalizados), list(finalizados)]),
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

        vinculo_avulso = buscar_jogo_avulso_por_pin(pin) if buscar_jogo_avulso_por_pin else None
        if vinculo_avulso:
            session["arbitro_pin_validado"] = True
            session["arbitro_pin_tipo"] = "avulso"
            session["arbitro_pin"] = pin
            session["arbitro_competicao"] = "JOGO AVULSO"
            session["arbitro_jogo_avulso_codigo"] = vinculo_avulso.get("codigo") or ""
            session["arbitro_jogo_avulso_pin"] = pin
            session["arbitro_jogo_avulso_equipe_a"] = vinculo_avulso.get("equipe_a") or "Equipe A"
            session["arbitro_jogo_avulso_equipe_b"] = vinculo_avulso.get("equipe_b") or "Equipe B"
            flash("PIN do jogo rápido validado. Escolha se este tablet será o 1º árbitro, 2º árbitro ou árbitro único.", "sucesso")
            return redirect(url_for("acessos_pin.arbitro_publico_pin"))

        vinculo_operacional = buscar_vinculo_operacional_por_pin(pin)
        if vinculo_operacional:
            session["arbitro_pin_validado"] = True
            session["arbitro_pin_tipo"] = "operacional"
            session["arbitro_pin"] = pin
            session["arbitro_competicao"] = vinculo_operacional.get("competicao") or ""
            session["arbitro_apontador_cpf"] = vinculo_operacional.get("apontador_cpf") or ""
            session["arbitro_apontador_nome"] = vinculo_operacional.get("apontador_nome") or ""
            flash("PIN validado. Escolha se este tablet será o 1º árbitro, 2º árbitro ou árbitro único.", "sucesso")
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
            flash("PIN validado. Escolha se este tablet será o 1º árbitro, 2º árbitro ou árbitro único.", "sucesso")
            return redirect(url_for("acessos_pin.arbitro_publico_pin"))

        flash("PIN não encontrado ou inativo.", "erro")
        return redirect(url_for("acessos_pin.arbitro_publico_pin"))

    if session.get("arbitro_pin_validado"):
        return render_template(
            "painel_arbitro_pin_escolha.html",
            vinculo_arbitro=_vinculo_arbitro_sessao(),
        )

    return render_template("pin_arbitro.html")



def _status_texto_partida(partida):
    if not partida:
        return ""
    partes = [
        partida.get("status_jogo"),
        partida.get("status_operacao"),
        partida.get("fase_partida"),
        partida.get("status"),
    ]
    return " ".join(str(p or "").strip().lower() for p in partes if p is not None)


def _partida_em_modo_operacao(partida):
    """Libera árbitro/telão quando qualquer campo confiável indicar jogo ativo.

    Algumas partidas antigas mantêm ``status='aguardando'`` mesmo depois de
    ``status_jogo='em_andamento'`` e ``fase_partida='jogo'``. A regra anterior
    procurava palavras no texto combinado e o valor legado bloqueava a abertura
    automática. Aqui os estados finais continuam tendo prioridade absoluta, mas
    um sinal forte de operação vence valores antigos de aguardando/aberta.
    """
    if not partida:
        return False

    campos = {
        "status": str(partida.get("status") or "").strip().lower(),
        "status_operacao": str(partida.get("status_operacao") or "").strip().lower(),
        "status_jogo": str(partida.get("status_jogo") or "").strip().lower(),
        "fase_partida": str(partida.get("fase_partida") or "").strip().lower(),
    }
    valores = tuple(v for v in campos.values() if v)
    if not valores:
        return False

    finais = {"finalizada", "finalizado", "encerrada", "encerrado", "concluida", "concluído", "concluido"}
    if any(any(t in valor for t in finais) for valor in valores):
        return False

    ativos_fortes = {
        "em_andamento", "andamento", "ao_vivo", "ao vivo",
        "jogo", "em_jogo", "operacao", "operação", "iniciada", "iniciado",
    }
    # Campos operacionais são a fonte de verdade para a abertura automática.
    for chave in ("status_jogo", "status_operacao", "fase_partida"):
        valor = campos[chave]
        if any(t == valor or t in valor for t in ativos_fortes):
            return True

    # Compatibilidade com bases antigas que atualizam apenas a coluna status.
    if any(t == campos["status"] or t in campos["status"] for t in ativos_fortes):
        return True

    return False


def _vinculo_avulso_em_modo_operacao(vinculo):
    pin = (vinculo or {}).get("pin") or session.get("arbitro_jogo_avulso_pin") or session.get("arbitro_pin") or ""
    atual = buscar_jogo_avulso_por_pin(pin) if buscar_jogo_avulso_por_pin else None
    if not atual:
        return None, False
    status = " ".join([
        str(atual.get("status_jogo") or "").strip().lower(),
        str(atual.get("fase_partida") or "").strip().lower(),
        str(atual.get("status") or "").strip().lower(),
    ])
    finais = {"finalizada", "finalizado", "encerrada", "encerrado", "concluida", "concluído", "concluido"}
    if any(t in status for t in finais):
        return atual, False
    liberados = {"em_andamento", "andamento", "ao_vivo", "ao vivo", "jogo", "em_jogo", "operacao", "operação", "iniciada", "iniciado"}
    if any(t in status for t in liberados):
        return atual, True
    return atual, False


def _render_standby_arbitro(tipo, titulo, subtitulo, endpoint_status, voltar_url):
    return render_template(
        "standby_arbitragem.html",
        tipo=tipo,
        titulo=titulo,
        subtitulo=subtitulo,
        endpoint_status=endpoint_status,
        voltar_url=voltar_url,
    )


@acessos_pin_bp.route("/arbitro/1")
def arbitro_automatico_primeiro():
    vinculo = _vinculo_arbitro_sessao()
    if not vinculo:
        flash("Digite o PIN antes de abrir o painel do árbitro.", "erro")
        return redirect(url_for("acessos_pin.arbitro_publico_pin"))

    if vinculo.get("tipo") == "avulso":
        atual, liberado = _vinculo_avulso_em_modo_operacao(vinculo)
        codigo = (atual or {}).get("codigo") or vinculo.get("codigo")
        if codigo:
            session["arbitro_jogo_avulso_codigo"] = codigo
        if codigo and liberado:
            return redirect(url_for("jogo_avulso.arbitro1_jogo_avulso", codigo=codigo))
        return _render_standby_arbitro(
            "primeiro",
            "Painel do 1º Árbitro",
            "Tela em standby. A partida abrirá automaticamente quando o apontador entrar no modo operação.",
            url_for("acessos_pin.proxima_partida_arbitro_primeiro"),
            url_for("acessos_pin.arbitro_publico_pin", trocar=1),
        )

    partida = _resolver_partida_para_arbitro(vinculo)
    if partida and _partida_em_modo_operacao(partida):
        return redirect(url_for(
            "oficiais.primeiro_arbitro_view",
            competicao=partida["competicao"],
            partida_id=partida["id"],
        ))

    return _render_standby_arbitro(
        "primeiro",
        "Painel do 1º Árbitro",
        "Tela em standby. A partida abrirá automaticamente quando o apontador entrar no modo operação.",
        url_for("acessos_pin.proxima_partida_arbitro_primeiro"),
        url_for("acessos_pin.arbitro_publico_pin", trocar=1),
    )


@acessos_pin_bp.route("/arbitro/2")
def arbitro_automatico_segundo():
    vinculo = _vinculo_arbitro_sessao()
    if not vinculo:
        flash("Digite o PIN antes de abrir o painel do árbitro.", "erro")
        return redirect(url_for("acessos_pin.arbitro_publico_pin"))

    if vinculo.get("tipo") == "avulso":
        atual, liberado = _vinculo_avulso_em_modo_operacao(vinculo)
        codigo = (atual or {}).get("codigo") or vinculo.get("codigo")
        if codigo:
            session["arbitro_jogo_avulso_codigo"] = codigo
        if codigo and liberado:
            return redirect(url_for("jogo_avulso.arbitro2_jogo_avulso", codigo=codigo))
        return _render_standby_arbitro(
            "segundo",
            "Painel do 2º Árbitro",
            "Tela em standby. A partida abrirá automaticamente quando o apontador entrar no modo operação.",
            url_for("acessos_pin.proxima_partida_arbitro_segundo"),
            url_for("acessos_pin.arbitro_publico_pin", trocar=1),
        )

    partida = _resolver_partida_para_arbitro(vinculo)
    if partida and _partida_em_modo_operacao(partida):
        return redirect(url_for(
            "oficiais.segundo_arbitro_view",
            competicao=partida["competicao"],
            partida_id=partida["id"],
        ))

    return _render_standby_arbitro(
        "segundo",
        "Painel do 2º Árbitro",
        "Tela em standby. A partida abrirá automaticamente quando o apontador entrar no modo operação.",
        url_for("acessos_pin.proxima_partida_arbitro_segundo"),
        url_for("acessos_pin.arbitro_publico_pin", trocar=1),
    )


@acessos_pin_bp.route("/arbitro/unico")
def arbitro_automatico_unico():
    vinculo = _vinculo_arbitro_sessao()
    if not vinculo:
        flash("Digite o PIN antes de abrir o painel do árbitro.", "erro")
        return redirect(url_for("acessos_pin.arbitro_publico_pin"))

    if vinculo.get("tipo") == "avulso":
        atual, liberado = _vinculo_avulso_em_modo_operacao(vinculo)
        codigo = (atual or {}).get("codigo") or vinculo.get("codigo")
        if codigo:
            session["arbitro_jogo_avulso_codigo"] = codigo
        if codigo and liberado:
            return redirect(url_for("jogo_avulso.arbitro_unico_jogo_avulso", codigo=codigo))
        return _render_standby_arbitro(
            "unico",
            "Painel do Árbitro Único",
            "Tela em standby. A partida abrirá automaticamente quando o apontador entrar no modo operação.",
            url_for("acessos_pin.proxima_partida_arbitro_unico"),
            url_for("acessos_pin.arbitro_publico_pin", trocar=1),
        )

    partida = _resolver_partida_para_arbitro(vinculo)
    if partida and _partida_em_modo_operacao(partida):
        return redirect(url_for(
            "oficiais.arbitro_unico_view",
            competicao=partida["competicao"],
            partida_id=partida["id"],
        ))

    return _render_standby_arbitro(
        "unico",
        "Painel do Árbitro Único",
        "Tela em standby. A partida abrirá automaticamente quando o apontador entrar no modo operação.",
        url_for("acessos_pin.proxima_partida_arbitro_unico"),
        url_for("acessos_pin.arbitro_publico_pin", trocar=1),
    )


def _resposta_proxima_partida(tipo):
    vinculo = _vinculo_arbitro_sessao()
    if not vinculo:
        return jsonify({"ok": False, "erro": "PIN não validado."}), 403

    if vinculo.get("tipo") == "avulso":
        pin = vinculo.get("pin") or session.get("arbitro_jogo_avulso_pin") or ""
        atual = buscar_jogo_avulso_por_pin(pin) if buscar_jogo_avulso_por_pin else None
        if not atual:
            return jsonify({"ok": True, "tem_partida": False, "competicao": "JOGO AVULSO", "mensagem": "Aguardando o apontador criar um jogo rápido neste PIN."})
        session["arbitro_jogo_avulso_codigo"] = atual.get("codigo") or ""
        status_avulso = " ".join([
            str(atual.get("status_jogo") or "").strip().lower(),
            str(atual.get("fase_partida") or "").strip().lower(),
            str(atual.get("status") or "").strip().lower(),
        ])
        bloqueados = {"papeleta", "pre_jogo", "pré_jogo", "sorteio", "aguardando", "finalizada", "finalizado", "encerrada", "encerrado"}
        liberados = {"em_andamento", "andamento", "ao_vivo", "ao vivo", "jogo", "operacao", "operação", "iniciada", "iniciado"}
        if any(t in status_avulso for t in bloqueados) or not any(t in status_avulso for t in liberados):
            return jsonify({
                "ok": True,
                "tem_partida": False,
                "competicao": "JOGO AVULSO",
                "status": atual.get("status_jogo") or atual.get("fase_partida") or "",
                "mensagem": "Jogo encontrado, aguardando o apontador entrar no modo operação."
            })
        if tipo == "primeiro":
            rota = "jogo_avulso.arbitro1_jogo_avulso"
        elif tipo == "unico":
            rota = "jogo_avulso.arbitro_unico_jogo_avulso"
        else:
            rota = "jogo_avulso.arbitro2_jogo_avulso"
        return jsonify({
            "ok": True,
            "tem_partida": True,
            "url": url_for(rota, codigo=atual.get("codigo")),
            "partida": {
                "id": atual.get("codigo"),
                "competicao": "JOGO AVULSO",
                "equipe_a": atual.get("equipe_a"),
                "equipe_b": atual.get("equipe_b"),
                "status": atual.get("status_jogo") or atual.get("fase_partida") or "jogo rápido",
                "operador": atual.get("apontador") or "",
            }
        })

    partida = _resolver_partida_para_arbitro(vinculo)

    if not partida or not _partida_em_modo_operacao(partida):
        return jsonify({
            "ok": True,
            "tem_partida": False,
            "competicao": vinculo.get("competicao") or "",
            "status": _status_texto_partida(partida),
            "mensagem": "Aguardando o apontador entrar no modo operação."
        })

    if tipo == "primeiro":
        rota = "oficiais.primeiro_arbitro_view"
    elif tipo == "unico":
        rota = "oficiais.arbitro_unico_view"
    else:
        rota = "oficiais.segundo_arbitro_view"
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


@acessos_pin_bp.route("/arbitro/unico/proxima")
def proxima_partida_arbitro_unico():
    return _resposta_proxima_partida("unico")


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

        vinculo_avulso = buscar_jogo_avulso_por_pin(pin) if buscar_jogo_avulso_por_pin else None
        if vinculo_avulso:
            session["telao_pin_validado"] = True
            session["telao_pin_tipo"] = "avulso"
            session["telao_pin"] = pin
            session["telao_competicao"] = "JOGO AVULSO"
            session["telao_jogo_avulso_codigo"] = vinculo_avulso.get("codigo") or ""
            session["telao_jogo_avulso_pin"] = pin
            return redirect(url_for("jogo_avulso.telao_jogo_avulso_por_pin", pin=pin))

        vinculo = buscar_vinculo_operacional_por_pin(pin)
        if not vinculo:
            flash("PIN não encontrado ou inativo.", "erro")
            return redirect(url_for("acessos_pin.telao_publico_pin"))

        apontador = vinculo.get("apontador_cpf") or ""
        if not apontador:
            flash("Este PIN não possui apontador vinculado.", "erro")
            return redirect(url_for("acessos_pin.telao_publico_pin"))

        session["telao_pin_validado"] = True
        session["telao_pin_tipo"] = "operacional"
        session["telao_pin"] = pin
        session["telao_competicao"] = vinculo.get("competicao") or ""
        session["telao_apontador"] = apontador
        session["telao_apontador_nome"] = vinculo.get("apontador_nome") or ""
        return redirect(url_for("acessos_pin.telao_automatico"))

    return render_template("pin_telao.html")


@acessos_pin_bp.route("/telao/automatico")
def telao_automatico():
    """Tela de descanso do placar vinculada permanentemente ao PIN/apontador."""
    vinculo = _vinculo_telao_sessao()
    if not vinculo:
        flash("Digite o PIN antes de abrir o placar.", "erro")
        return redirect(url_for("acessos_pin.telao_publico_pin"))

    if vinculo.get("tipo") == "avulso":
        pin = vinculo.get("pin") or ""
        atual = buscar_jogo_avulso_por_pin(pin) if buscar_jogo_avulso_por_pin else None
        if atual:
            status = " ".join([
                str(atual.get("status_jogo") or "").lower(),
                str(atual.get("fase_partida") or "").lower(),
                str(atual.get("status") or "").lower(),
            ])
            if not any(t in status for t in ("finalizada", "finalizado", "encerrada", "encerrado", "aguardando", "papeleta", "pre_jogo", "sorteio")):
                return redirect(url_for("jogo_avulso.telao_jogo_avulso_por_pin", pin=pin))
    else:
        partida = _resolver_partida_para_arbitro(vinculo)
        if partida and _partida_em_modo_operacao(partida):
            return redirect(url_for(
                "apontadores.placar_ao_vivo_apontador",
                apontador=vinculo.get("apontador_cpf") or "",
                auto_pin=1,
            ))

    return render_template(
        "standby_arbitragem.html",
        titulo="Placar da quadra",
        modulo="Módulo de Placar",
        mensagem_principal="Aguardando próxima partida",
        descricao="O placar abrirá automaticamente quando o apontador clicar em Iniciar jogo. Não precisa atualizar nem apertar F5.",
        endpoint_status=url_for("acessos_pin.proxima_partida_telao"),
        voltar_url=url_for("acessos_pin.telao_publico_pin", trocar=1),
        texto_abrindo="Jogo iniciado. Abrindo o placar...",
    )


@acessos_pin_bp.route("/telao/proxima")
def proxima_partida_telao():
    vinculo = _vinculo_telao_sessao()
    if not vinculo:
        return jsonify({"ok": False, "erro": "PIN não validado."}), 403

    if vinculo.get("tipo") == "avulso":
        pin = vinculo.get("pin") or ""
        atual = buscar_jogo_avulso_por_pin(pin) if buscar_jogo_avulso_por_pin else None
        if not atual:
            return jsonify({"ok": True, "tem_partida": False, "mensagem": "Aguardando o apontador iniciar o jogo."})
        status = " ".join([
            str(atual.get("status_jogo") or "").lower(),
            str(atual.get("fase_partida") or "").lower(),
            str(atual.get("status") or "").lower(),
        ])
        bloqueados = ("finalizada", "finalizado", "encerrada", "encerrado", "aguardando", "papeleta", "pre_jogo", "sorteio")
        if any(t in status for t in bloqueados):
            return jsonify({"ok": True, "tem_partida": False, "mensagem": "Jogo encontrado; aguardando o apontador clicar em Iniciar jogo."})
        return jsonify({
            "ok": True,
            "tem_partida": True,
            "url": url_for("jogo_avulso.telao_jogo_avulso_por_pin", pin=pin),
        })

    partida = _resolver_partida_para_arbitro(vinculo)
    if not partida or not _partida_em_modo_operacao(partida):
        return jsonify({
            "ok": True,
            "tem_partida": False,
            "competicao": vinculo.get("competicao") or "",
            "mensagem": "Aguardando o apontador clicar em Iniciar jogo.",
        })

    return jsonify({
        "ok": True,
        "tem_partida": True,
        "url": url_for(
            "apontadores.placar_ao_vivo_apontador",
            apontador=vinculo.get("apontador_cpf") or "",
            auto_pin=1,
        ),
        "partida": {
            "id": partida.get("id"),
            "competicao": partida.get("competicao"),
            "quadra": partida.get("quadra_nome") or partida.get("quadra"),
            "equipe_a": partida.get("equipe_a_operacional") or partida.get("equipe_a"),
            "equipe_b": partida.get("equipe_b_operacional") or partida.get("equipe_b"),
        },
    })
