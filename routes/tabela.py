from flask import Blueprint, render_template, request, redirect, session, url_for, flash
from functools import wraps
import random
import json

from banco import (
    buscar_competicao_por_organizador,
    buscar_competicao_por_nome,
    listar_equipes_da_competicao,
    criar_grupo,
    listar_grupos,
    adicionar_equipe_no_grupo,
    listar_equipes_por_grupo,
    criar_partida,
    listar_partidas,
    limpar_partidas,
    limpar_partidas_por_fase,
    remover_equipe_do_grupo,
    excluir_grupo as excluir_grupo_banco,
    excluir_partida as excluir_partida_banco,
    atualizar_partida,
    competicao_esta_travada,
    fase_grupos_esta_travada_por_jogo,
    fase_partidas_pode_ser_alterada,
    fase_tem_partida_iniciada,
    listar_quadras_competicao,
    garantir_quadras_competicao,
    buscar_quadra_competicao_por_id,
    buscar_quadra_competicao_por_texto,
    formatar_quadra_exibicao,
    normalizar_vinculos_quadras_competicao,
    vincular_grupo_a_quadra,
    aplicar_quadra_em_partida,
    conectar,
    buscar_configuracao_agenda_competicao,
    atualizar_configuracao_agenda_competicao,
    inicializar_configuracao_agenda_competicao,
    _buscar_colunas_tabela,
)

from routes.utils import exigir_perfil, aplicar_placar_exibicao_partida

tabela_bp = Blueprint("tabela", __name__)


# =========================================================
# PERMISSÃO ROBUSTA DA TABELA
# =========================================================
def exigir_organizador_da_competicao(func):
    """
    Evita falso bloqueio de perfil.
    Algumas sessões antigas podem ter perfil escrito de forma diferente,
    mas se o usuário logado possui competição vinculada como organizador,
    ele pode acessar e alterar a tabela.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        usuario = session.get("usuario")

        if not usuario:
            flash("Sessão expirada. Faça login novamente.", "erro")
            return redirect(url_for("auth.login"))

        perfil = (session.get("perfil") or "").strip().lower()
        if perfil in {"organizador", "superadmin"}:
            return func(*args, **kwargs)

        competicao = buscar_competicao_por_organizador(usuario)
        if competicao:
            return func(*args, **kwargs)

        flash("Você não tem permissão para acessar esta área.", "erro")
        return redirect(url_for("painel.inicio"))

    return wrapper




def _remover_flash_permissao_falso():
    """Remove aviso antigo de permissão quando a tela da tabela foi carregada com acesso válido.

    Esse flash pode ficar pendurado na sessão quando alguma rota anterior gerou
    o aviso, mas a tela atual foi liberada corretamente pelo organizador.
    """
    flashes = session.get("_flashes") or []
    if not flashes:
        return

    session["_flashes"] = [
        item for item in flashes
        if not (
            isinstance(item, (list, tuple))
            and len(item) >= 2
            and str(item[1]).strip() == "Você não tem permissão para acessar esta área."
        )
    ]


# =========================================================
# HELPERS
# =========================================================
def _fases_disponiveis(competicao):
    formato_finais = (competicao.get("formato_finais") or "").strip().lower()

    tem_quartas = formato_finais == "quartas"
    tem_semifinais = formato_finais in {"quartas", "semifinal"}
    tem_finais = True

    return {
        "tem_quartas": tem_quartas,
        "tem_semifinais": tem_semifinais,
        "tem_finais": tem_finais,
    }


def _fase_subaba_para_banco(fase_subaba):
    mapa = {
        "classificatorias": "grupos",
        "quartas": "quartas",
        "semifinais": "semifinal",
        "finais": "final",
    }
    return mapa.get(fase_subaba, "grupos")


def _nome_fase_mata_mata(fase_subaba):
    mapa = {
        "quartas": "Quartas",
        "semifinais": "Semifinal",
        "finais": "Final",
    }
    return mapa.get(fase_subaba, "")


def _to_int_or_none(valor):
    try:
        if valor in (None, ""):
            return None
        return int(valor)
    except (TypeError, ValueError):
        return None


def _normalizar_url_escudo_tabela(valor):
    valor = str(valor or "").strip()

    if not valor:
        return "/static/img/escudo_padrao.svg"

    if valor.startswith(("http://", "https://", "/static/", "data:")):
        return valor

    valor = valor.replace("\\", "/")

    if valor.startswith("static/"):
        return "/" + valor

    if valor.startswith("uploads/"):
        return "/static/" + valor

    if "/uploads/" in valor:
        return "/static/uploads/" + valor.split("/uploads/", 1)[1]

    return "/static/uploads/escudos/" + valor.lstrip("/")


def _mapa_escudos_equipes(equipes):
    mapa = {}

    for equipe in equipes or []:
        nome = (
            equipe.get("nome")
            or equipe.get("equipe")
            or equipe.get("nome_equipe")
            or ""
        ).strip()

        if not nome:
            continue

        escudo = (
            equipe.get("escudo")
            or equipe.get("escudo_url")
            or equipe.get("logo")
            or equipe.get("imagem")
            or ""
        )

        mapa[nome] = _normalizar_url_escudo_tabela(escudo)
        mapa[nome.lower()] = mapa[nome]
        mapa[nome.upper()] = mapa[nome]

    return mapa


def _buscar_escudo_mapa(mapa_escudos, nome_equipe):
    nome = str(nome_equipe or "").strip()
    if not nome:
        return _normalizar_url_escudo_tabela("")
    return (
        (mapa_escudos or {}).get(nome)
        or (mapa_escudos or {}).get(nome.lower())
        or (mapa_escudos or {}).get(nome.upper())
        or _normalizar_url_escudo_tabela("")
    )


def _quadra_label(item):
    """Texto visual de quadra, sem usar nome de grupo como fallback.

    Antes o fallback em `nome` fazia Grupo A aparecer como Quadra padrão A.
    Agora grupo sem quadra_id/quadra_nome aparece corretamente como Sem quadra.
    """
    if not item:
        return "Sem quadra"

    for campo in ("quadra_label", "quadra_exibicao", "quadra_nome"):
        valor = str((item or {}).get(campo) or "").strip()
        if valor:
            return valor

    # Para partidas antigas, o campo legacy `quadra` pode conter texto. Se for só número/id,
    # deixamos sem exibir até ser normalizado pelo banco.
    valor_legacy = str((item or {}).get("quadra") or "").strip()
    if valor_legacy and not valor_legacy.isdigit():
        return valor_legacy

    return "Sem quadra"


def _quadra_id_do_grupo(grupo):
    return _to_int_or_none((grupo or {}).get("quadra_id"))


def _quadra_padrao_do_grupo(grupos_raw, grupo_nome):
    grupo_nome = (grupo_nome or "").strip().upper()
    for g in grupos_raw or []:
        if (g.get("nome") or "").strip().upper() == grupo_nome:
            return _quadra_id_do_grupo(g)
    return None


def _dados_quadra(nome_competicao, quadra_id):
    quadra_id = _to_int_or_none(quadra_id)
    if not quadra_id:
        return None, ""
    quadra = buscar_quadra_competicao_por_id(nome_competicao, quadra_id)
    if not quadra:
        return None, ""
    return int(quadra["id"]), formatar_quadra_exibicao(quadra)


def _quadra_label_por_id(nome_competicao, quadra_id):
    quadra_id, quadra_label = _dados_quadra(nome_competicao, quadra_id)
    return quadra_label or "Sem quadra"


def _status_texto(valor):
    return str(valor or "").strip().lower().replace("-", "_")


STATUS_FINALIZADO = {
    "finalizada",
    "finalizado",
    "encerrado",
    "encerrada",
    "partida_encerrada",
}

STATUS_AO_VIVO = {
    "ao_vivo",
    "ao vivo",
    "em_andamento",
    "em andamento",
    "andamento",
    "iniciada",
    "iniciado",
}

STATUS_PRE_JOGO = {
    "pre_jogo",
    "pré_jogo",
    "pre jogo",
    "pré jogo",
}

STATUS_AGUARDANDO = {
    "aguardando",
    "agendada",
    "agendado",
    "pendente",
}


def _partida_tem_flag_finalizada(partida):
    """Finalizado sempre tem prioridade máxima sobre qualquer status ao vivo.

    Em algumas telas o jogo pode continuar com status/status_jogo antigo como
    em_andamento, mesmo depois de ter sido encerrado pelo apontador. Por isso
    verificamos todos os campos possíveis antes de classificar como AO VIVO.
    """
    if not partida:
        return False

    for campo in (
        "status",
        "status_jogo",
        "fase_partida",
        "situacao",
        "estado",
        "estado_jogo",
    ):
        if _status_texto(partida.get(campo)) in STATUS_FINALIZADO:
            return True

    for campo in ("finalizada", "partida_encerrada", "encerrada"):
        valor = partida.get(campo)
        if isinstance(valor, bool) and valor:
            return True
        if isinstance(valor, (int, float)) and int(valor) == 1:
            return True
        if isinstance(valor, str) and valor.strip().lower() in {"1", "true", "sim", "yes", "on"}:
            return True

    return bool(partida.get("finalizado_em") or partida.get("encerrado_em"))


def _status_tabela_para_trava(partida):
    """Status consolidado da tabela.

    A prioridade correta é:
    1. finalizada/encerrado;
    2. ao vivo/em andamento;
    3. pré-jogo/agendada.

    Isso evita o erro em que uma partida encerrada pelo apontador continuava
    aparecendo como AO VIVO na tabela ou no visualizador público porque algum
    campo antigo ainda estava salvo como em_andamento.
    """
    if _partida_tem_flag_finalizada(partida):
        return "finalizada"

    status = _status_texto(partida.get("status"))
    fase_partida = _status_texto(partida.get("fase_partida"))
    status_jogo = _status_texto(partida.get("status_jogo"))

    for valor in (status, fase_partida, status_jogo):
        if valor in STATUS_AO_VIVO:
            return valor

    for valor in (status, fase_partida, status_jogo):
        if valor in STATUS_PRE_JOGO:
            return "pre_jogo"

    for valor in (status, fase_partida, status_jogo):
        if valor in STATUS_AGUARDANDO:
            return "aguardando"

    for valor in (status, fase_partida, status_jogo):
        if valor:
            return valor

    return "aguardando"


def _partida_conta_como_iniciada_para_trava(partida):
    """
    Só trava edição/exclusão quando o jogo realmente saiu do estado inicial.

    IMPORTANTE:
    No banco antigo, algumas partidas novas aparecem com status/status_jogo = pre_jogo
    mesmo sem ninguém ter aberto o pré-jogo. Por isso pre_jogo sozinho NÃO bloqueia.
    A partida só conta como iniciada quando houver sinal real de jogo: placar, sets,
    status ao vivo/finalizado, fase ao vivo/finalizada ou campo de início preenchido.
    """
    status = _status_tabela_para_trava(partida)

    if status in STATUS_FINALIZADO or status in STATUS_AO_VIVO:
        return True

    if partida.get("pre_jogo_iniciado_em") or partida.get("jogo_iniciado_em") or partida.get("finalizado_em") or partida.get("encerrado_em"):
        return True

    for campo in ("pontos_a", "pontos_b", "placar_a", "placar_b", "sets_a", "sets_b"):
        try:
            if int(partida.get(campo) or 0) > 0:
                return True
        except (TypeError, ValueError):
            pass

    return False


def _fase_tem_jogo_realmente_iniciado(competicao_nome, fase_banco):
    fase_banco = (fase_banco or "grupos").strip().lower()

    for partida in listar_partidas(competicao_nome):
        fase_partida = _fase_partida_normalizada(partida)

        if fase_banco == "semifinal":
            mesma_fase = fase_partida in {"semifinal", "semifinais"}
        else:
            mesma_fase = fase_partida == fase_banco

        if mesma_fase and _partida_conta_como_iniciada_para_trava(partida):
            return True

    return False


def _fase_pode_ser_alterada_sem_travar_mata_mata(competicao_nome, fase_banco):
    """
    Regra correta:
    - Grupos/classificatórias travam quando algum jogo classificatório REALMENTE inicia.
    - Quartas, semifinal e final NÃO dependem do fim das classificatórias.
    - Criar uma partida agendada/pendente no mata-mata NÃO pode bloquear a fase.
    - Mata-mata só trava quando um jogo da própria fase vai para pré-jogo, ao vivo ou finalizado.
    """
    fase_banco = (fase_banco or "grupos").strip().lower()
    return not _fase_tem_jogo_realmente_iniciado(competicao_nome, fase_banco)



def _criar_partida_para_tabela(competicao_nome, grupo, equipe_a, equipe_b, ordem, fase_banco, origem="manual", quadra_id=None):
    """
    Cria partida pela tela da tabela.

    - Grupos usam a função padrão do banco, porque a classificatória deve respeitar o travamento estrutural.
    - Mata-mata faz INSERT direto para NÃO ser bloqueado pela classificatória travada.

    Também grava status_jogo='aguardando', porque no banco antigo status_jogo tem DEFAULT antigo 'pre_jogo'
    e isso fazia a tela achar que a partida já tinha iniciado logo depois de criar.
    """
    quadra_id, quadra_nome = _dados_quadra(competicao_nome, quadra_id)

    if fase_banco == "grupos":
        retorno = criar_partida(
            competicao_nome,
            grupo,
            equipe_a,
            equipe_b,
            ordem,
            quadra=str(quadra_id) if quadra_id else None,
            fase=fase_banco,
            origem=origem,
            quadra_id=quadra_id,
            quadra_nome=quadra_nome,
        )
        return retorno is not False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO partidas (
                    competicao, grupo, equipe_a, equipe_b, fase, ordem,
                    quadra, quadra_id, quadra_nome, origem, status, status_jogo, fase_partida
                )
                VALUES (%s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, 'aguardando', 'aguardando', 'aguardando')
            """, (competicao_nome, equipe_a, equipe_b, fase_banco, ordem, str(quadra_id) if quadra_id else None, quadra_id, quadra_nome or '', origem))
        conn.commit()

    return True


def _mapa_quadras_formatadas(nome_competicao):
    """Busca as quadras uma única vez e monta cache id -> nome formatado.

    A geração automática pode criar dezenas/centenas de jogos. Chamar
    buscar_quadra_competicao_por_id() para cada jogo faz uma consulta ao Neon
    por partida e deixa a geração muito lenta. Este mapa evita esse gargalo.
    """
    mapa = {}
    try:
        for q in listar_quadras_competicao(nome_competicao) or []:
            qid = _to_int_or_none(q.get("id"))
            if qid:
                mapa[qid] = formatar_quadra_exibicao(q)
    except Exception as e:
        print("AVISO _mapa_quadras_formatadas:", repr(e))
    return mapa


def _quadra_nome_cache(mapa_quadras, quadra_id):
    quadra_id = _to_int_or_none(quadra_id)
    if not quadra_id:
        return None, ""
    return quadra_id, (mapa_quadras or {}).get(quadra_id, "")


def _inserir_partidas_em_lote(partidas):
    """Insere partidas em lote com um único roundtrip/commit.

    Substitui o fluxo antigo que chamava criar_partida() para cada jogo.
    Esse fluxo antigo abria validações/consultas/commits repetidos e era o
    principal motivo da geração automática demorar minutos.

    Status inicial correto: AGUARDANDO. A partida só vira PRE_JOGO quando o
    apontador realmente assumir/abrir a conferência.
    """
    partidas = [p for p in (partidas or []) if p]
    if not partidas:
        return 0

    colunas_partidas = _buscar_colunas_tabela("partidas") or set()

    campos_base = [
        "competicao", "grupo", "equipe_a", "equipe_b", "fase", "ordem",
        "quadra", "quadra_id", "quadra_nome", "origem", "rodada", "status",
    ]
    extras_possiveis = [
        "status_jogo", "fase_partida", "status_operacao",
        "sets_a", "sets_b", "pontos_a", "pontos_b",
    ]

    # Compatibilidade com bancos antigos: só insere colunas que realmente existem.
    campos = [c for c in campos_base if c in colunas_partidas]
    campos.extend([c for c in extras_possiveis if c in colunas_partidas and c not in campos])

    valores = []
    for p in partidas:
        quadra_id = _to_int_or_none(p.get("quadra_id"))
        mapa_valores = {
            "competicao": p.get("competicao"),
            "grupo": p.get("grupo"),
            "equipe_a": p.get("equipe_a"),
            "equipe_b": p.get("equipe_b"),
            "fase": p.get("fase") or "grupos",
            "ordem": int(p.get("ordem") or 0),
            "quadra": str(quadra_id) if quadra_id else None,
            "quadra_id": quadra_id,
            "quadra_nome": p.get("quadra_nome") or "",
            "origem": p.get("origem") or "automatica",
            "rodada": p.get("rodada"),
            "status": "aguardando",
            "status_jogo": "aguardando",
            "fase_partida": "aguardando",
            "status_operacao": "livre",
            "sets_a": 0,
            "sets_b": 0,
            "pontos_a": 0,
            "pontos_b": 0,
        }
        valores.append(tuple(mapa_valores.get(c) for c in campos))

    placeholders = ", ".join(["%s"] * len(campos))

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO partidas ({", ".join(campos)})
                VALUES ({placeholders})
                """,
                valores,
            )
        conn.commit()

    return len(valores)

def _fase_partida_normalizada(partida):
    fase = (
        partida.get("fase")
        or partida.get("fase_partida")
        or "grupos"
    )
    fase = str(fase).strip().lower()

    if fase in {"classificatoria", "classificatorias", "grupo", "grupos"}:
        return "grupos"
    if "quarta" in fase:
        return "quartas"
    if "semi" in fase:
        return "semifinal"
    if "final" in fase:
        return "final"

    return fase or "grupos"

def _filtrar_partidas_por_fase(partidas, fase_subaba):
    fase_subaba = (fase_subaba or "classificatorias").strip().lower()

    def mesma_fase(partida):
        fase = _fase_partida_normalizada(partida)

        if fase_subaba == "classificatorias":
            return fase == "grupos"
        if fase_subaba == "quartas":
            return fase == "quartas"
        if fase_subaba == "semifinais":
            return fase in {"semifinal", "semifinais"}
        if fase_subaba == "finais":
            return fase == "final"

        return False

    return [p for p in partidas if mesma_fase(p)]


def _status_normalizado(partida):
    return _status_tabela_para_trava(partida)


def _status_exibicao(partida):
    status = _status_normalizado(partida)

    mapa = {
        "pre_jogo": "PRÉ-JOGO",
        "aguardando": "AGUARDANDO",
        "agendada": "AGUARDANDO",
        "em andamento": "AO VIVO",
        "ao vivo": "AO VIVO",
        "ao_vivo": "AO VIVO",
        "andamento": "AO VIVO",
        "em_andamento": "AO VIVO",
        "finalizada": "FINALIZADO",
        "finalizado": "FINALIZADO",
        "encerrado": "FINALIZADO",
        "encerrada": "FINALIZADO",
    }

    return mapa.get(status, (status or "AGUARDANDO").replace("_", " ").upper())


def _partida_esta_finalizada(partida):
    return _partida_tem_flag_finalizada(partida) or _status_normalizado(partida) in STATUS_FINALIZADO


def _partida_esta_ao_vivo(partida):
    if _partida_esta_finalizada(partida):
        return False
    return _status_normalizado(partida) in STATUS_AO_VIVO


def _montar_parciais(partida):
    parciais = []

    for i in range(1, 6):
        a = partida.get(f"set{i}_a")
        b = partida.get(f"set{i}_b")

        if a is not None and b is not None:
            try:
                parciais.append(f"{int(a)}x{int(b)}")
            except (TypeError, ValueError):
                parciais.append(f"{a}x{b}")

    return " / ".join(parciais) if parciais else "-"


def _preparar_partidas(partidas, mapa_escudos=None, competicao=None):
    partidas_preparadas = []

    for p in partidas:
        partida = dict(p)

        partida["fase_normalizada"] = _fase_partida_normalizada(partida)
        partida["status_normalizado"] = _status_normalizado(partida)
        partida["status_exibicao"] = _status_exibicao(partida)
        partida["ao_vivo"] = _partida_esta_ao_vivo(partida)
        partida["finalizada"] = _partida_esta_finalizada(partida)
        partida["parciais_formatadas"] = _montar_parciais(partida)
        partida["pode_excluir"] = not _partida_conta_como_iniciada_para_trava(partida)

        partida["placar_ao_vivo_a"] = int(
            partida.get("pontos_a")
            or partida.get("placar_a")
            or 0
        )

        partida["placar_ao_vivo_b"] = int(
            partida.get("pontos_b")
            or partida.get("placar_b")
            or 0
        )

        aplicar_placar_exibicao_partida(partida, competicao or {})

        partida["quadra_label"] = _quadra_label(partida)
        partida["quadra_id_normalizado"] = _to_int_or_none(partida.get("quadra_id"))

        partida["escudo_a"] = _buscar_escudo_mapa(mapa_escudos, partida.get("equipe_a"))
        partida["escudo_b"] = _buscar_escudo_mapa(mapa_escudos, partida.get("equipe_b"))
        partida["equipe_a_escudo"] = partida["escudo_a"]
        partida["equipe_b_escudo"] = partida["escudo_b"]

        partidas_preparadas.append(partida)

    return sorted(
        partidas_preparadas,
        key=lambda p: (
            p.get("ordem") or 0,
            p.get("grupo") or "",
            p.get("equipe_a") or "",
            p.get("equipe_b") or "",
        )
    )
    

def _to_bool(valor):
    if isinstance(valor, bool):
        return valor
    if valor is None:
        return False
    return str(valor).strip().lower() in {"1", "true", "sim", "yes", "on"}


def _valor_inteiro_regra(competicao, chaves, padrao):
    for chave in chaves:
        valor = competicao.get(chave)
        if valor not in (None, ""):
            try:
                return int(valor)
            except (TypeError, ValueError):
                pass
    return padrao


def _bool_por_chaves(competicao, chaves):
    for chave in chaves:
        if chave in competicao:
            return _to_bool(competicao.get(chave))
    return False


CRITERIOS_CLASSIFICACAO_PADRAO = [
    "pontos",
    "vitorias",
    "saldo_sets",
    "sets_average",
    "saldo_pontos",
    "pontos_average",
    "confronto_direto",
    "sorteio",
]

CRITERIOS_CLASSIFICACAO_SUPORTADOS = {
    "pontos",
    "vitorias",
    "sets_average",
    "pontos_average",
    "saldo_sets",
    "saldo_pontos",
    "sets_pro",
    "sets_contra",
    "pontos_pro",
    "pontos_contra",
    "confronto_direto",
    "coef_sets",
    "coef_pontos",
    "fair_play",
    "menor_wo",
    "sorteio",
}

CRITERIOS_MENOR_MELHOR = {"sets_contra", "pontos_contra", "fair_play", "menor_wo"}


CRITERIOS_CLASSIFICACAO_COLUNAS = {
    "pontos": {"campo": "pontos", "titulo": "P"},
    "vitorias": {"campo": "vitorias", "titulo": "V"},
    "derrotas": {"campo": "derrotas", "titulo": "D"},
    "jogos": {"campo": "jogos", "titulo": "J"},
    "saldo_sets": {"campo": "saldo_sets", "titulo": "DS"},
    "sets_average": {"campo": "sets_average_exibicao", "titulo": "SA"},
    "coef_sets": {"campo": "sets_average_exibicao", "titulo": "SA"},
    "saldo_pontos": {"campo": "saldo_pontos", "titulo": "DP"},
    "pontos_average": {"campo": "pontos_average_exibicao", "titulo": "PA"},
    "coef_pontos": {"campo": "pontos_average_exibicao", "titulo": "PA"},
    "sets_pro": {"campo": "sets_pro", "titulo": "SP"},
    "sets_contra": {"campo": "sets_contra", "titulo": "SC"},
    "pontos_pro": {"campo": "pontos_pro", "titulo": "PF"},
    "pontos_contra": {"campo": "pontos_contra", "titulo": "PC"},
    "fair_play": {"campo": "fair_play", "titulo": "FP"},
    "menor_wo": {"campo": "wo", "titulo": "WO"},
}

COLUNAS_PUBLICAS_SET_UNICO = [
    {"campo": "pontos", "titulo": "P", "descricao": "Pontos na classificação"},
    {"campo": "jogos", "titulo": "J", "descricao": "Jogos disputados"},
    {"campo": "vitorias", "titulo": "V", "descricao": "Vitórias"},
    {"campo": "derrotas", "titulo": "D", "descricao": "Derrotas"},
    {"campo": "pontos_average_exibicao", "titulo": "PA", "descricao": "Pontos average: PF dividido por PC"},
    {"campo": "saldo_pontos", "titulo": "DP", "descricao": "Diferença de pontos: PF menos PC"},
    {"campo": "pontos_pro", "titulo": "PF", "descricao": "Pontos feitos"},
    {"campo": "pontos_contra", "titulo": "PC", "descricao": "Pontos cedidos"},
]

COLUNAS_PUBLICAS_SETS = [
    {"campo": "pontos", "titulo": "P", "descricao": "Pontos na classificação"},
    {"campo": "jogos", "titulo": "J", "descricao": "Jogos disputados"},
    {"campo": "vitorias", "titulo": "V", "descricao": "Vitórias"},
    {"campo": "derrotas", "titulo": "D", "descricao": "Derrotas"},
    {"campo": "sets_pro", "titulo": "SP", "descricao": "Sets pró"},
    {"campo": "sets_contra", "titulo": "SC", "descricao": "Sets contra"},
    {"campo": "saldo_sets", "titulo": "DS", "descricao": "Diferença de sets: SP menos SC"},
    {"campo": "sets_average_exibicao", "titulo": "SA", "descricao": "Sets average: SP dividido por SC"},
    {"campo": "pontos_average_exibicao", "titulo": "PA", "descricao": "Pontos average: PF dividido por PC"},
    {"campo": "saldo_pontos", "titulo": "DP", "descricao": "Diferença de pontos: PF menos PC"},
    {"campo": "pontos_pro", "titulo": "PF", "descricao": "Pontos feitos"},
    {"campo": "pontos_contra", "titulo": "PC", "descricao": "Pontos cedidos"},
]


def _formatar_numero_decimal(valor):
    try:
        valor = float(valor or 0)
    except (TypeError, ValueError):
        valor = 0.0

    if valor == float("inf"):
        return "∞"

    texto = f"{valor:.3f}".rstrip("0").rstrip(".")
    return texto or "0"


def _calcular_sets_average_valor(sets_pro, sets_contra):
    """Calcula sets average pelo acumulado da equipe.

    Regra técnica adotada no sistema:
    - enquanto a equipe não sofreu sets, usa divisor 0.5;
    - depois que sofreu pelo menos 1 set, usa o valor real acumulado.
    """
    try:
        sets_pro = int(sets_pro or 0)
    except (TypeError, ValueError):
        sets_pro = 0

    try:
        sets_contra = int(sets_contra or 0)
    except (TypeError, ValueError):
        sets_contra = 0

    if sets_pro <= 0:
        return 0.0

    if sets_contra <= 0:
        return float("inf")

    return sets_pro / sets_contra


def _calcular_pontos_average_valor(pontos_pro, pontos_contra):
    """Calcula pontos average pelo acumulado da equipe.

    Regra técnica adotada no sistema:
    - enquanto a equipe não sofreu pontos, usa divisor 1;
    - depois que sofreu pelo menos 1 ponto, usa o valor real acumulado.
    """
    try:
        pontos_pro = int(pontos_pro or 0)
    except (TypeError, ValueError):
        pontos_pro = 0

    try:
        pontos_contra = int(pontos_contra or 0)
    except (TypeError, ValueError):
        pontos_contra = 0

    if pontos_pro <= 0:
        return 0.0

    if pontos_contra <= 0:
        return float("inf")

    return pontos_pro / pontos_contra


def _formatar_sets_average_exibicao(sets_pro, sets_contra):
    return _formatar_numero_decimal(_calcular_sets_average_valor(sets_pro, sets_contra))


def _formatar_pontos_average_exibicao(pontos_pro, pontos_contra):
    return _formatar_numero_decimal(_calcular_pontos_average_valor(pontos_pro, pontos_contra))


def _criterios_efetivos_ate_sorteio(criterios):
    criterios = list(criterios or [])
    if "sorteio" in criterios:
        return criterios[:criterios.index("sorteio") + 1]
    return criterios


def _competicao_eh_set_unico_tabela(competicao):
    competicao = competicao or {}
    texto = " ".join(
        str(competicao.get(chave) or "")
        for chave in ("sets_tipo", "tipo_sets", "formato_sets", "melhor_de")
    ).strip().lower().replace("-", "_").replace(" ", "_")

    return texto in {"set_unico", "único", "unico", "1_set", "melhor_de_1", "md1", "1"} or "set_unico" in texto


def _colunas_classificacao_publica(competicao):
    """Colunas exibidas no link público.

    A exibição é independente da ordem de desempate. A classificação continua
    sendo ordenada por _aplicar_criterios_classificacao usando os critérios
    configurados pelo organizador.
    """
    colunas = COLUNAS_PUBLICAS_SET_UNICO if _competicao_eh_set_unico_tabela(competicao) else COLUNAS_PUBLICAS_SETS
    return [dict(c) for c in colunas]


def _colunas_classificacao_por_criterios(criterios):
    """Compatibilidade com telas antigas que exibem apenas critérios ativos."""
    colunas = []
    vistos = set()

    for criterio in _criterios_efetivos_ate_sorteio(criterios):
        cfg = CRITERIOS_CLASSIFICACAO_COLUNAS.get(criterio)
        if not cfg:
            continue

        campo = cfg["campo"]
        if campo in vistos:
            continue

        colunas.append({
            "criterio": criterio,
            "campo": campo,
            "titulo": cfg["titulo"],
            "descricao": cfg.get("descricao", cfg["titulo"]),
        })
        vistos.add(campo)

    if not colunas:
        colunas.append({"criterio": "pontos", "campo": "pontos", "titulo": "P", "descricao": "Pontos"})

    return colunas


def _normalizar_criterios_classificacao(valor):
    """
    Lê a ordem salva em competicoes.criterios_desempate.

    A coluna antiga foi mantida por compatibilidade, mas agora ela representa
    a ORDEM DOS CRITÉRIOS DE CLASSIFICAÇÃO. Ex.:
    pontos,vitorias,saldo_sets,confronto_direto,saldo_pontos,sorteio
    """
    if isinstance(valor, (list, tuple)):
        brutos = valor
    else:
        texto = str(valor or "").strip()
        if texto.startswith("["):
            try:
                import json
                carregado = json.loads(texto)
                brutos = carregado if isinstance(carregado, list) else []
            except Exception:
                brutos = []
        else:
            brutos = texto.split(",")

    criterios = []
    vistos = set()

    aliases = {
        "vitórias": "vitorias",
        "vitorias": "vitorias",
        "pontos average": "pontos_average",
        "sets average": "sets_average",
        "saldo de sets": "saldo_sets",
        "saldo de pontos": "saldo_pontos",
        "confronto": "confronto_direto",
        "confronto direto": "confronto_direto",
        "wo": "menor_wo",
        "menor numero de wo": "menor_wo",
        "menor número de w.o.": "menor_wo",
    }

    for item in brutos:
        criterio = str(item or "").strip().lower()
        criterio = criterio.replace("-", "_").replace(" ", "_")
        criterio = aliases.get(criterio, criterio)

        if criterio in CRITERIOS_CLASSIFICACAO_SUPORTADOS and criterio not in vistos:
            criterios.append(criterio)
            vistos.add(criterio)

    if not criterios:
        criterios = list(CRITERIOS_CLASSIFICACAO_PADRAO)

    # Não corta os critérios abaixo do sorteio.
    # O sorteio encerra o desempate apenas no momento do cálculo, dentro de
    # _aplicar_criterios_classificacao. Assim a tela continua podendo salvar
    # e reordenar todos os critérios escolhidos pelo organizador.
    return criterios


def _sets_para_vitoria_classificacao(competicao):
    """Define quantos sets o vencedor precisa fazer conforme a regra da competição."""
    texto = " ".join(
        str(competicao.get(chave) or "")
        for chave in ("sets_tipo", "tipo_sets", "formato_sets", "melhor_de")
    ).strip().lower()

    if "5" in texto or "cinco" in texto:
        return 3

    if "unico" in texto or "único" in texto or "1" in texto:
        return 1

    return 2


def _resultado_foi_tiebreak(sets_vencedor, sets_perdedor, competicao):
    sets_para_vitoria = _sets_para_vitoria_classificacao(competicao)

    if sets_para_vitoria <= 1:
        return False

    return int(sets_vencedor or 0) == sets_para_vitoria and int(sets_perdedor or 0) == (sets_para_vitoria - 1)


def _obter_regras_classificacao(competicao):
    criterios = _normalizar_criterios_classificacao(
        competicao.get("criterios_desempate")
        or competicao.get("criterios_classificacao")
        or ""
    )

    return {
        "pontos_vitoria": _valor_inteiro_regra(
            competicao,
            ["pontos_vitoria", "vitoria_set_unico", "vitoria_2x0", "vitoria_3x0"],
            2
        ),
        "pontos_derrota": _valor_inteiro_regra(
            competicao,
            ["pontos_derrota", "derrota_set_unico", "derrota_0x2", "derrota_0x3"],
            0
        ),
        "pontos_tiebreak_vitoria": _valor_inteiro_regra(
            competicao,
            ["pontos_tiebreak_vitoria", "vitoria_tiebreak", "vitoria_2x1", "vitoria_3x2"],
            2
        ),
        "pontos_tiebreak_derrota": _valor_inteiro_regra(
            competicao,
            ["pontos_tiebreak_derrota", "derrota_tiebreak", "derrota_1x2", "derrota_2x3"],
            1
        ),
        "criterios": criterios,
    }


def _valor_criterio(linha, nome):
    if nome == "pontos":
        return linha.get("pontos", 0)

    if nome == "vitorias":
        return linha.get("vitorias", 0)

    if nome in {"sets_average", "coef_sets"}:
        return linha.get(
            "sets_average_valor",
            _calcular_sets_average_valor(linha.get("sets_pro", 0), linha.get("sets_contra", 0))
        )

    if nome in {"pontos_average", "coef_pontos"}:
        return linha.get(
            "pontos_average_valor",
            _calcular_pontos_average_valor(linha.get("pontos_pro", 0), linha.get("pontos_contra", 0))
        )

    if nome == "saldo_sets":
        return linha.get("saldo_sets", 0)

    if nome == "saldo_pontos":
        return linha.get("saldo_pontos", 0)

    if nome == "sets_pro":
        return linha.get("sets_pro", 0)

    if nome == "sets_contra":
        return linha.get("sets_contra", 0)

    if nome == "pontos_pro":
        return linha.get("pontos_pro", 0)

    if nome == "pontos_contra":
        return linha.get("pontos_contra", 0)

    if nome == "fair_play":
        return linha.get("fair_play", 0)

    if nome == "menor_wo":
        return linha.get("wo", linha.get("wos", 0))

    return 0


def _valor_ordenacao_criterio(linha, criterio):
    valor = _valor_criterio(linha, criterio)
    if criterio in CRITERIOS_MENOR_MELHOR:
        try:
            return -float(valor)
        except (TypeError, ValueError):
            return 0
    return valor


def _resolver_confronto_direto(bloco, partidas, grupo):
    if len(bloco) <= 1:
        return bloco

    nomes = [l["equipe"] for l in bloco]
    mini = {
        nome: {
            "pontos": 0,
            "saldo_sets": 0,
            "pontos_pro": 0,
            "pontos_contra": 0,
            "saldo_pontos": 0,
            "vitorias": 0,
        }
        for nome in nomes
    }

    for p in partidas:
        if not _partida_esta_finalizada(p):
            continue

        if p.get("grupo") != grupo:
            continue

        a = p.get("equipe_a")
        b = p.get("equipe_b")

        if a not in mini or b not in mini:
            continue

        try:
            sets_a = int(p.get("sets_a") or 0)
        except (TypeError, ValueError):
            sets_a = 0

        try:
            sets_b = int(p.get("sets_b") or 0)
        except (TypeError, ValueError):
            sets_b = 0

        if sets_a == sets_b:
            continue

        mini[a]["saldo_sets"] += sets_a - sets_b
        mini[b]["saldo_sets"] += sets_b - sets_a

        pontos_a = 0
        pontos_b = 0
        for i in range(1, 6):
            sa = p.get(f"set{i}_a")
            sb = p.get(f"set{i}_b")
            if sa is not None and sb is not None:
                try:
                    pontos_a += int(sa)
                    pontos_b += int(sb)
                except (TypeError, ValueError):
                    pass

        mini[a]["pontos_pro"] += pontos_a
        mini[a]["pontos_contra"] += pontos_b
        mini[b]["pontos_pro"] += pontos_b
        mini[b]["pontos_contra"] += pontos_a
        mini[a]["saldo_pontos"] = mini[a]["pontos_pro"] - mini[a]["pontos_contra"]
        mini[b]["saldo_pontos"] = mini[b]["pontos_pro"] - mini[b]["pontos_contra"]

        if sets_a > sets_b:
            mini[a]["pontos"] += 1
            mini[a]["vitorias"] += 1
        else:
            mini[b]["pontos"] += 1
            mini[b]["vitorias"] += 1

    return sorted(
        bloco,
        key=lambda linha: (
            mini[linha["equipe"]]["pontos"],
            mini[linha["equipe"]]["vitorias"],
            mini[linha["equipe"]]["saldo_sets"],
            mini[linha["equipe"]]["saldo_pontos"],
            mini[linha["equipe"]]["pontos_pro"],
        ),
        reverse=True
    )


def _aplicar_criterios_classificacao(linhas, partidas, grupo, criterios):
    """
    Aplica a classificação exatamente na ordem cadastrada pelo organizador.
    Cada critério só mexe dentro de blocos que ainda estão empatados no critério anterior.
    """
    if not linhas:
        return linhas

    def aplicar_bloco(bloco, indice_criterio):
        if len(bloco) <= 1 or indice_criterio >= len(criterios):
            return bloco

        criterio = criterios[indice_criterio]

        if criterio == "sorteio":
            bloco = list(bloco)
            random.shuffle(bloco)
            return bloco

        if criterio == "confronto_direto":
            ordenado = _resolver_confronto_direto(bloco, partidas, grupo)
            # Depois do confronto direto, segue para os próximos critérios apenas nos empates técnicos restantes.
            return aplicar_bloco(ordenado, indice_criterio + 1)

        ordenado = sorted(
            bloco,
            key=lambda linha: _valor_ordenacao_criterio(linha, criterio),
            reverse=True,
        )

        resultado = []
        pos = 0
        while pos < len(ordenado):
            atual = ordenado[pos]
            valor_atual = _valor_ordenacao_criterio(atual, criterio)
            sub_bloco = [atual]
            prox = pos + 1

            while prox < len(ordenado) and _valor_ordenacao_criterio(ordenado[prox], criterio) == valor_atual:
                sub_bloco.append(ordenado[prox])
                prox += 1

            resultado.extend(aplicar_bloco(sub_bloco, indice_criterio + 1))
            pos = prox

        return resultado

    return aplicar_bloco(list(linhas), 0)


# Compatibilidade com chamadas antigas.
def _aplicar_desempates_profissional(linhas, partidas, grupo, criterios):
    return _aplicar_criterios_classificacao(linhas, partidas, grupo, criterios)


def _calcular_classificacao(partidas, grupos, competicao, mapa_escudos=None):
    regras = _obter_regras_classificacao(competicao)
    classificacao = {}

    for g in grupos:
        nome_grupo = g["grupo"]["nome"]
        classificacao[nome_grupo] = []

        equipes_ordenadas = sorted(
            g["equipes"],
            key=lambda e: (e.get("equipe") or "").lower()
        )

        for e in equipes_ordenadas:
            classificacao[nome_grupo].append({
                "equipe": e["equipe"],
                "escudo": _buscar_escudo_mapa(mapa_escudos, e.get("equipe")),
                "jogos": 0,
                "vitorias": 0,
                "derrotas": 0,
                "sets_pro": 0,
                "sets_contra": 0,
                "saldo_sets": 0,
                "pontos_pro": 0,
                "pontos_contra": 0,
                "saldo_pontos": 0,
                "pontos": 0,
            })

    mapa = {
        grupo: {linha["equipe"]: linha for linha in linhas}
        for grupo, linhas in classificacao.items()
    }

    for p in partidas:
        if not _partida_esta_finalizada(p):
            continue

        grupo = p.get("grupo")
        equipe_a = p.get("equipe_a")
        equipe_b = p.get("equipe_b")

        if not grupo or grupo not in mapa:
            continue
        if equipe_a not in mapa[grupo] or equipe_b not in mapa[grupo]:
            continue

        try:
            sets_a = int(p.get("sets_a") or 0)
        except (TypeError, ValueError):
            sets_a = 0

        try:
            sets_b = int(p.get("sets_b") or 0)
        except (TypeError, ValueError):
            sets_b = 0

        if sets_a == sets_b:
            continue

        linha_a = mapa[grupo][equipe_a]
        linha_b = mapa[grupo][equipe_b]

        linha_a["jogos"] += 1
        linha_b["jogos"] += 1

        linha_a["sets_pro"] += sets_a
        linha_a["sets_contra"] += sets_b
        linha_b["sets_pro"] += sets_b
        linha_b["sets_contra"] += sets_a

        pontos_a = 0
        pontos_b = 0

        for i in range(1, 6):
            sa = p.get(f"set{i}_a")
            sb = p.get(f"set{i}_b")
            if sa is not None and sb is not None:
                try:
                    pontos_a += int(sa)
                    pontos_b += int(sb)
                except (TypeError, ValueError):
                    pass

        linha_a["pontos_pro"] += pontos_a
        linha_a["pontos_contra"] += pontos_b
        linha_b["pontos_pro"] += pontos_b
        linha_b["pontos_contra"] += pontos_a

        if sets_a > sets_b:
            linha_a["vitorias"] += 1
            linha_b["derrotas"] += 1

            if _resultado_foi_tiebreak(sets_a, sets_b, competicao):
                linha_a["pontos"] += regras["pontos_tiebreak_vitoria"]
                linha_b["pontos"] += regras["pontos_tiebreak_derrota"]
            else:
                linha_a["pontos"] += regras["pontos_vitoria"]
                linha_b["pontos"] += regras["pontos_derrota"]
        else:
            linha_b["vitorias"] += 1
            linha_a["derrotas"] += 1

            if _resultado_foi_tiebreak(sets_b, sets_a, competicao):
                linha_b["pontos"] += regras["pontos_tiebreak_vitoria"]
                linha_a["pontos"] += regras["pontos_tiebreak_derrota"]
            else:
                linha_b["pontos"] += regras["pontos_vitoria"]
                linha_a["pontos"] += regras["pontos_derrota"]

    for grupo, linhas in classificacao.items():
        for linha in linhas:
            linha["saldo_sets"] = linha["sets_pro"] - linha["sets_contra"]
            linha["saldo_pontos"] = linha["pontos_pro"] - linha["pontos_contra"]
            linha["sets_average_valor"] = _calcular_sets_average_valor(linha["sets_pro"], linha["sets_contra"])
            linha["pontos_average_valor"] = _calcular_pontos_average_valor(linha["pontos_pro"], linha["pontos_contra"])
            linha["sets_average_exibicao"] = _formatar_numero_decimal(linha["sets_average_valor"])
            linha["pontos_average_exibicao"] = _formatar_numero_decimal(linha["pontos_average_valor"])
            linha.setdefault("fair_play", 0)
            linha.setdefault("wo", 0)

    criterios_ativos = regras.get("criterios") or list(CRITERIOS_CLASSIFICACAO_PADRAO)

    for grupo, linhas in classificacao.items():
        classificacao[grupo] = _aplicar_criterios_classificacao(
            linhas,
            partidas,
            grupo,
            criterios_ativos,
        )

    return classificacao


# =========================================================
# VISUALIZADOR PÚBLICO
# =========================================================
@tabela_bp.route("/visualizador/<competicao_nome>")
def visualizador_publico(competicao_nome):
    try:
        normalizar_vinculos_quadras_competicao(competicao_nome)
    except Exception as e:
        print("AVISO visualizador/normalizar_quadras:", repr(e))

    grupos_raw = listar_grupos(competicao_nome)
    partidas = listar_partidas(competicao_nome)
    equipes_competicao = listar_equipes_da_competicao(competicao_nome)
    mapa_escudos = _mapa_escudos_equipes(equipes_competicao)

    grupos = []
    for g in grupos_raw:
        equipes_grupo = listar_equipes_por_grupo(g["id"])
        grupos.append({
            "grupo": g,
            "equipes": equipes_grupo,
            "quadra_label": _quadra_label_por_id(competicao_nome, _quadra_id_do_grupo(g)),
            "quadra_id": _quadra_id_do_grupo(g),
        })

    competicao = buscar_competicao_por_nome(competicao_nome) or {
        "nome": competicao_nome
    }

    partidas_preparadas = _preparar_partidas(partidas, mapa_escudos, competicao)
    classificacao = _calcular_classificacao(partidas_preparadas, grupos, competicao, mapa_escudos)
    regras_classificacao = _obter_regras_classificacao(competicao)
    criterios_classificacao = _criterios_efetivos_ate_sorteio(regras_classificacao.get("criterios"))
    colunas_classificacao = _colunas_classificacao_publica(competicao)
    set_unico = _competicao_eh_set_unico_tabela(competicao)

    return render_template(
        "visualizador_publico.html",
        competicao_nome=competicao_nome,
        grupos=grupos,
        classificacao=classificacao,
        partidas=partidas_preparadas,
        criterios_classificacao=criterios_classificacao,
        colunas_classificacao=colunas_classificacao,
        set_unico=set_unico,
    )


# =========================================================
# TELA PRINCIPAL
# =========================================================
@tabela_bp.route("/tabela")
@exigir_organizador_da_competicao
def tabela_view():
    usuario = session.get("usuario")

    if not usuario:
        flash("Sessão expirada. Faça login novamente.", "erro")
        return redirect(url_for("painel.inicio"))

    competicao = buscar_competicao_por_organizador(usuario)

    if not competicao:
        flash("Nenhuma competição vinculada a este organizador.", "erro")
        return redirect(url_for("painel.inicio"))

    _remover_flash_permissao_falso()

    aba = (request.args.get("aba") or "geracao").strip().lower()
    if aba not in {"geracao", "partidas", "classificacao", "visualizador"}:
        aba = "geracao"

    fase_subaba = (request.args.get("fase") or "classificatorias").strip().lower()
    if fase_subaba not in {"classificatorias", "quartas", "semifinais", "finais"}:
        fase_subaba = "classificatorias"

    quadras = garantir_quadras_competicao(competicao["nome"], competicao.get("qtd_quadras") or 1)
    try:
        normalizar_vinculos_quadras_competicao(competicao["nome"])
    except Exception as e:
        print("AVISO tabela/normalizar_quadras:", repr(e))
    grupos_raw = listar_grupos(competicao["nome"])
    equipes = listar_equipes_da_competicao(competicao["nome"])
    mapa_escudos = _mapa_escudos_equipes(equipes)
    partidas = listar_partidas(competicao["nome"])

    grupos = []
    for g in grupos_raw:
        equipes_grupo = listar_equipes_por_grupo(g["id"])
        grupos.append({
            "grupo": g,
            "equipes": equipes_grupo,
            "quadra_label": _quadra_label_por_id(competicao["nome"], _quadra_id_do_grupo(g)),
            "quadra_id": _quadra_id_do_grupo(g),
        })

    partidas_preparadas = _preparar_partidas(partidas, mapa_escudos, competicao)
    partidas_fase = _filtrar_partidas_por_fase(partidas_preparadas, fase_subaba)
    classificacao = _calcular_classificacao(partidas_preparadas, grupos, competicao, mapa_escudos)
    regras_classificacao = _obter_regras_classificacao(competicao)
    criterios_classificacao = _criterios_efetivos_ate_sorteio(regras_classificacao.get("criterios"))
    colunas_classificacao = _colunas_classificacao_por_criterios(criterios_classificacao)

    fases = _fases_disponiveis(competicao)
    inicializar_configuracao_agenda_competicao(competicao["nome"])
    config_agenda = buscar_configuracao_agenda_competicao(competicao["nome"])

    return render_template(
        "tabela.html",
        competicao=competicao,
        grupos=grupos,
        equipes=equipes,
        quadras=quadras,
        partidas=partidas_preparadas,
        partidas_fase=partidas_fase,
        classificacao=classificacao,
        criterios_classificacao=criterios_classificacao,
        colunas_classificacao=colunas_classificacao,
        aba_ativa=aba,
        fase_ativa=fase_subaba,
        competicao_travada=competicao_esta_travada(competicao["nome"]),
        config_agenda=config_agenda,
        config_geracao=config_agenda,
        grupos_travados=fase_grupos_esta_travada_por_jogo(competicao["nome"]),
        fase_atual_travada=not _fase_pode_ser_alterada_sem_travar_mata_mata(competicao["nome"], _fase_subaba_para_banco(fase_subaba)),
        fase_banco_ativa=_fase_subaba_para_banco(fase_subaba),
        **fases,
    )


# =========================================================
# CRIAR GRUPO
# =========================================================
@tabela_bp.route("/tabela/criar-grupo", methods=["POST"])
@exigir_organizador_da_competicao
def criar_grupo_view():
    nome = request.form.get("nome", "").strip().upper()
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if not nome:
        flash("Informe o nome do grupo.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível criar grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    criar_grupo(nome, competicao["nome"])

    flash("Grupo criado com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# VINCULAR GRUPO À QUADRA
# =========================================================
@tabela_bp.route("/tabela/grupo-quadra", methods=["POST"])
@exigir_organizador_da_competicao
def vincular_grupo_quadra_view():
    grupo_nome = (request.form.get("grupo_nome") or "").strip().upper()
    quadra_id = _to_int_or_none(request.form.get("quadra_id"))
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if not grupo_nome:
        flash("Grupo inválido.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível trocar a quadra padrão do grupo.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if not quadra_id:
        flash("Selecione uma quadra válida.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if vincular_grupo_a_quadra(competicao["nome"], grupo_nome, quadra_id):
        flash(f"Grupo {grupo_nome} vinculado à quadra.", "sucesso")
    else:
        flash("Não foi possível vincular a quadra ao grupo.", "erro")

    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# ADICIONAR EQUIPE AO GRUPO
# =========================================================
@tabela_bp.route("/tabela/adicionar-equipe", methods=["POST"])
@exigir_organizador_da_competicao
def adicionar_equipe_grupo():
    grupo_id = request.form.get("grupo_id")
    equipe = request.form.get("equipe")
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if not grupo_id or not equipe:
        flash("Preencha todos os campos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível alterar grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    adicionar_equipe_no_grupo(grupo_id, equipe, competicao["nome"])

    flash("Equipe adicionada ao grupo.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# REMOVER EQUIPE DO GRUPO
# =========================================================
@tabela_bp.route("/tabela/remover-equipe-grupo", methods=["POST"])
@exigir_organizador_da_competicao
def remover_equipe_grupo_view():
    grupo_id = request.form.get("grupo_id")
    equipe = request.form.get("equipe")
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if not grupo_id or not equipe:
        flash("Dados inválidos para remover equipe do grupo.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível alterar grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    remover_equipe_do_grupo(grupo_id, equipe, competicao["nome"])

    flash("Equipe removida do grupo.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# EXCLUIR GRUPO
# =========================================================
@tabela_bp.route("/tabela/excluir-grupo/<int:grupo_id>", methods=["POST"])
@exigir_organizador_da_competicao
def excluir_grupo_view(grupo_id):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível excluir grupos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    excluir_grupo_banco(grupo_id, competicao["nome"])

    flash("Grupo excluído com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


# =========================================================
# LIMPEZA DE PARTIDAS
# =========================================================
@tabela_bp.route("/tabela/limpar", methods=["POST"])
@exigir_organizador_da_competicao
def limpar_tabela():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível limpar toda a tabela.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="geracao"))

    ok = limpar_partidas(competicao["nome"])

    if ok is False:
        flash("Não foi possível limpar a tabela porque já existe partida iniciada.", "erro")
    else:
        flash("Tabela limpa com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="geracao"))


@tabela_bp.route("/tabela/limpar-fase", methods=["POST"])
@exigir_organizador_da_competicao
def limpar_fase_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    fase_banco = _fase_subaba_para_banco(fase_subaba)

    if not _fase_pode_ser_alterada_sem_travar_mata_mata(competicao["nome"], fase_banco):
        flash("Esta fase já iniciou. Não é possível limpar as partidas dela.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    ok = limpar_partidas_por_fase(competicao["nome"], fase_banco)

    if ok is False:
        flash("Não foi possível limpar esta fase porque já existe partida iniciada.", "erro")
    else:
        flash("Partidas da fase removidas com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))


# =========================================================
# CRIAR PARTIDA MANUAL
# =========================================================
@tabela_bp.route("/tabela/nova-partida", methods=["POST"])
@exigir_organizador_da_competicao
def nova_partida():
    grupo = request.form.get("grupo")
    # Aceita os nomes principais e também alternativas, para não falhar se o template antigo ficar em cache.
    equipe_a = (request.form.get("equipe_a") or request.form.get("time_a") or request.form.get("mandante") or "").strip()
    equipe_b = (request.form.get("equipe_b") or request.form.get("time_b") or request.form.get("visitante") or "").strip()
    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    quadra_id = _to_int_or_none(request.form.get("quadra_id"))

    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_banco = _fase_subaba_para_banco(fase_subaba)

    # O mata-mata NÃO usa grupo. Grupo só é obrigatório nas classificatórias.
    grupo = (grupo or "").strip().upper() if fase_banco == "grupos" else None
    if fase_banco == "grupos" and not quadra_id:
        quadra_id = _quadra_padrao_do_grupo(listar_grupos(competicao["nome"]), grupo)

    if fase_banco == "grupos" and not grupo:
        flash("Informe o grupo para jogo classificatório.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    # Regra principal:
    # - grupos travam quando a classificatória inicia;
    # - mata-mata só trava quando a própria fase iniciar.
    if not _fase_pode_ser_alterada_sem_travar_mata_mata(competicao["nome"], fase_banco):
        flash("Esta fase já iniciou. Não é possível criar novas partidas nela.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    if fase_banco == "grupos":
        if not equipe_a or not equipe_b:
            flash("Selecione as duas equipes.", "erro")
            return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

        if equipe_a == equipe_b:
            flash("A partida precisa ter duas equipes diferentes.", "erro")
            return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))
    else:
        # Mata-mata manual pode ser criado antes do fim da classificatória.
        # Se o organizador ainda não quiser escolher as equipes, salva como A definir.
        if equipe_a and equipe_b and equipe_a == equipe_b:
            flash("A partida precisa ter duas equipes diferentes.", "erro")
            return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

        equipe_a = equipe_a or "A definir"
        equipe_b = equipe_b or "A definir"

    partidas = listar_partidas(competicao["nome"])
    ordens = []
    for partida in partidas:
        try:
            ordens.append(int(partida.get("ordem") or 0))
        except (TypeError, ValueError):
            pass
    ordem = (max(ordens) + 1) if ordens else 1

    ok_criacao = _criar_partida_para_tabela(
        competicao["nome"],
        grupo,
        equipe_a,
        equipe_b,
        ordem,
        fase_banco,
        origem="manual",
        quadra_id=quadra_id,
    )

    if not ok_criacao:
        flash("Não foi possível criar a partida. Verifique se esta fase já iniciou.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    flash("Partida criada com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))



# =========================================================
# ATUALIZAR PARTIDA MANUAL DO MATA-MATA
# =========================================================
@tabela_bp.route("/tabela/atualizar-partida/<int:partida_id>", methods=["POST"])
@exigir_organizador_da_competicao
def atualizar_partida_view(partida_id):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    fase_banco = _fase_subaba_para_banco(fase_subaba)
    quadra_id = _to_int_or_none(request.form.get("quadra_id"))

    if fase_banco == "grupos":
        flash("Jogos classificatórios não podem ser editados por aqui depois da geração. Use excluir e recriar antes do início.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    equipe_a = (request.form.get("equipe_a") or request.form.get("time_a") or request.form.get("mandante") or "").strip()
    equipe_b = (request.form.get("equipe_b") or request.form.get("time_b") or request.form.get("visitante") or "").strip()

    if equipe_a and equipe_b and equipe_a == equipe_b:
        flash("A partida precisa ter duas equipes diferentes.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    equipe_a = equipe_a or "A definir"
    equipe_b = equipe_b or "A definir"

    if not _fase_pode_ser_alterada_sem_travar_mata_mata(competicao["nome"], fase_banco):
        flash("Esta fase já iniciou. Não é possível alterar partidas dela.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    quadra_id, quadra_nome = _dados_quadra(competicao["nome"], quadra_id)

    ok = atualizar_partida(
        partida_id,
        competicao["nome"],
        None,
        fase_banco,
        equipe_a,
        equipe_b,
        quadra=str(quadra_id) if quadra_id else None,
        quadra_id=quadra_id,
        quadra_nome=quadra_nome,
        status="aguardando",
    )

    if ok is False:
        flash("Não foi possível salvar. A partida já iniciou ou está bloqueada.", "erro")
    else:
        flash("Partida salva com sucesso.", "sucesso")

    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))


# =========================================================
# EXCLUIR PARTIDA
# =========================================================
@tabela_bp.route("/tabela/excluir-partida/<int:partida_id>", methods=["POST"])
@exigir_organizador_da_competicao
def excluir_partida_view(partida_id):
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()

    ok, mensagem = excluir_partida_banco(partida_id, competicao["nome"])
    flash(mensagem, "sucesso" if ok else "erro")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))



# =========================================================
# MOTOR INTELIGENTE DE AGENDA DA FASE CLASSIFICATÓRIA
# =========================================================
def _gerar_rodadas_round_robin(equipes):
    """Gera rodadas todos-contra-todos pelo método do círculo."""
    times = list(equipes or [])
    if len(times) < 2:
        return []

    if len(times) % 2 == 1:
        times.append(None)

    n = len(times)
    rodadas = []

    for rodada_idx in range(n - 1):
        rodada = []
        for i in range(n // 2):
            t1 = times[i]
            t2 = times[n - 1 - i]
            if t1 is not None and t2 is not None:
                # Alterna mando/ordem visual para não deixar sempre o mesmo time primeiro.
                if rodada_idx % 2 == 0:
                    rodada.append((t1, t2))
                else:
                    rodada.append((t2, t1))
        rodadas.append(rodada)
        times = [times[0]] + [times[-1]] + times[1:-1]

    return rodadas


def _ids_quadras_ativas(quadras):
    ids = []
    for q in quadras or []:
        if q.get("ativa") is False:
            continue
        try:
            ids.append(int(q.get("id")))
        except (TypeError, ValueError):
            pass
    return ids


def _normalizar_lista_ids(valores):
    if valores in (None, ""):
        return []
    if isinstance(valores, str):
        try:
            valores = json.loads(valores)
        except Exception:
            valores = [v.strip() for v in valores.split(",")]
    ids = []
    for v in valores or []:
        try:
            n = int(v)
            if n > 0 and n not in ids:
                ids.append(n)
        except (TypeError, ValueError):
            pass
    return ids


def _parse_grupos_compartilhados_form():
    """Lê configurações opcionais do formulário sem depender do HTML novo.

    Aceita formatos simples:
    - grupos_compartilhados_json = {"A":[1,2], "B":[1,2]}
    - quadras_compartilhadas_json = [1,2]
    - grupo_quadras_A = 1,2
    """
    bruto = request.form.get("grupos_compartilhados_json") or request.form.get("grupos_compartilhados")
    if bruto:
        try:
            dados = json.loads(bruto)
            if isinstance(dados, dict):
                return {str(k).strip().upper(): _normalizar_lista_ids(v) for k, v in dados.items()}
        except Exception:
            pass

    dados = {}
    for chave, valor in request.form.items():
        if not chave.startswith("grupo_quadras_"):
            continue
        grupo = chave.replace("grupo_quadras_", "", 1).strip().upper()
        ids = _normalizar_lista_ids(valor)
        if grupo and ids:
            dados[grupo] = ids
    return dados


def _config_agenda_da_requisicao(nome_competicao):
    inicializar_configuracao_agenda_competicao(nome_competicao)
    config = buscar_configuracao_agenda_competicao(nome_competicao) or {}

    if request.method == "POST":
        modo = (request.form.get("modo_distribuicao") or request.form.get("modo_distribuicao_agenda") or config.get("modo_distribuicao") or "automatico_inteligente").strip().lower()
        rodizio = (request.form.get("rodizio_grupos") or config.get("rodizio_grupos") or "por_rodada").strip().lower()
        descanso = request.form.get("descanso_minimo_jogos", config.get("descanso_minimo_jogos", 1))
        permitir_relaxar = request.form.get("permitir_relaxar_descanso")
        if permitir_relaxar is None:
            permitir_relaxar = config.get("permitir_relaxar_descanso", True)
        else:
            permitir_relaxar = str(permitir_relaxar).strip().lower() in {"1", "true", "on", "sim", "yes"}

        grupos_comp = _parse_grupos_compartilhados_form() or config.get("grupos_compartilhados") or {}
        quadras_comp = _normalizar_lista_ids(
            request.form.get("quadras_compartilhadas_json")
            or request.form.get("quadras_compartilhadas")
            or config.get("quadras_compartilhadas")
        )

        atualizar_configuracao_agenda_competicao(
            nome_competicao,
            modo_distribuicao=modo,
            descanso_minimo_jogos=descanso,
            rodizio_grupos=rodizio,
            permitir_relaxar_descanso=permitir_relaxar,
            grupos_compartilhados=grupos_comp,
            quadras_compartilhadas=quadras_comp,
        )
        config = buscar_configuracao_agenda_competicao(nome_competicao) or config

    return config


def _quadras_permitidas_para_grupo(nome_competicao, grupos_raw, grupo_nome, quadras_ativas, config):
    """Define quais quadras o grupo pode usar.

    REGRA IMPORTANTE:
    - Se o grupo tem quadra padrão definida na aba Configurações, ele fica FIXO nessa quadra.
    - Uma quadra fixa de um grupo fica reservada para esse grupo.
    - Grupos sem quadra definida usam somente as quadras livres, ou seja, não invadem
      quadras reservadas por grupos fixos.
    - Só usamos uma configuração específica do modal quando ela existir para o grupo.
    """
    grupo_nome = str(grupo_nome or "").strip().upper()
    quadras_ativas = [qid for qid in (quadras_ativas or []) if qid is not None]
    if not quadras_ativas:
        return []

    # Mapa de quadras fixas cadastradas na aba Configurações.
    fixas_por_grupo = {}
    quadras_reservadas = set()
    for g in grupos_raw or []:
        nome_g = str(g.get("nome") or "").strip().upper()
        qid = _quadra_id_do_grupo(g)
        try:
            qid = int(qid or 0)
        except (TypeError, ValueError):
            qid = None
        if nome_g and qid and qid in quadras_ativas:
            fixas_por_grupo[nome_g] = qid
            quadras_reservadas.add(qid)

    # 1) Grupo com quadra definida é sempre fixo.
    if grupo_nome in fixas_por_grupo:
        return [fixas_por_grupo[grupo_nome]]

    # 2) Para grupos sem quadra definida, remove as quadras reservadas para grupos fixos.
    quadras_livres = [qid for qid in quadras_ativas if qid not in quadras_reservadas]
    if not quadras_livres:
        # Se todas as quadras estão reservadas, libera fallback para não travar a geração.
        quadras_livres = list(quadras_ativas)

    compartilhados = (config or {}).get("grupos_compartilhados") or {}
    quadras_compartilhadas = _normalizar_lista_ids((config or {}).get("quadras_compartilhadas"))

    # 3) Configuração específica por grupo no modal, filtrada pelas quadras livres.
    ids = _normalizar_lista_ids(compartilhados.get(grupo_nome) or compartilhados.get(grupo_nome.lower()))
    ids = [qid for qid in ids if qid in quadras_livres]
    if ids:
        return ids

    # 4) Pool geral compartilhado, também sem invadir quadras reservadas.
    if quadras_compartilhadas:
        ids = [qid for qid in quadras_compartilhadas if qid in quadras_livres]
        if ids:
            return ids

    # 5) Fallback: qualquer quadra livre.
    return list(quadras_livres)


def _montar_fila_jogos_classificatorios(rodadas_por_grupo, rodizio):
    """Monta uma fila respeitando a ideia de rodadas entre grupos."""
    fila = []
    grupos = sorted(rodadas_por_grupo.keys())
    max_rodadas = max((len(r) for r in rodadas_por_grupo.values()), default=0)

    if rodizio == "por_grupo_inteiro":
        for grupo in grupos:
            for rodada_idx, rodada in enumerate(rodadas_por_grupo.get(grupo) or [], start=1):
                for equipe_a, equipe_b in rodada:
                    fila.append({"grupo": grupo, "rodada_grupo": rodada_idx, "equipe_a": equipe_a, "equipe_b": equipe_b})
        return fila

    # Padrão: por rodada. O alternado inteligente usa a mesma base e o encaixe abaixo decide o melhor jogo.
    for rodada_idx in range(max_rodadas):
        for grupo in grupos:
            rodadas = rodadas_por_grupo.get(grupo) or []
            if rodada_idx >= len(rodadas):
                continue
            for equipe_a, equipe_b in rodadas[rodada_idx]:
                fila.append({"grupo": grupo, "rodada_grupo": rodada_idx + 1, "equipe_a": equipe_a, "equipe_b": equipe_b})
    return fila


def _jogo_respeita_descanso(jogo, historico_slots, descanso_minimo):
    if descanso_minimo <= 0:
        return True
    equipes = {jogo["equipe_a"], jogo["equipe_b"]}
    for slot in historico_slots[-descanso_minimo:]:
        if equipes.intersection(slot):
            return False
    return True


def _proximo_jogo_sem_conflito(lista_jogos, equipes_slot, equipes_slot_anterior=None):
    """Remove e retorna o primeiro jogo possível sem conflito no slot.

    Primeiro tenta evitar equipes que jogaram no slot anterior. Se não existir
    opção, relaxa essa regra para não travar grupos com poucos times/quadra única
    como o caso da Apolo.
    """
    equipes_slot = set(equipes_slot or set())
    equipes_slot_anterior = set(equipes_slot_anterior or set())

    for idx, jogo in enumerate(lista_jogos or []):
        equipes = {jogo.get("equipe_a"), jogo.get("equipe_b")}
        if equipes.intersection(equipes_slot):
            continue
        if equipes_slot_anterior and equipes.intersection(equipes_slot_anterior):
            continue
        return lista_jogos.pop(idx)

    for idx, jogo in enumerate(lista_jogos or []):
        equipes = {jogo.get("equipe_a"), jogo.get("equipe_b")}
        if equipes.intersection(equipes_slot):
            continue
        return lista_jogos.pop(idx)

    return None


def _montar_blocos_por_pool_classificatoria(nome_competicao, grupos_raw, quadras_ativas, config):
    """Agrupa os grupos pelo conjunto de quadras que eles podem usar.

    Exemplo prático:
    - Grupo C permite apenas Apolo => pool (Apolo)
    - Grupos A/B/D permitem Floresta 1 e 2 => pool (Floresta 1, Floresta 2)

    Isso é o que permite gerar rodadas simultâneas por local/quadras sem misturar
    um grupo fixo com grupos rotativos.
    """
    pools = {}
    for g in grupos_raw or []:
        grupo = str(g.get("nome") or "").strip().upper()
        if not grupo:
            continue
        permitidas = _quadras_permitidas_para_grupo(nome_competicao, grupos_raw, grupo, quadras_ativas, config)
        permitidas = tuple(qid for qid in permitidas if qid in quadras_ativas)
        if not permitidas:
            continue
        pools.setdefault(permitidas, []).append(grupo)

    # Pools com mais quadras primeiro. Na prática, Floresta vem antes da Apolo,
    # mas o slot final continua sincronizado por número de linha.
    return dict(sorted(pools.items(), key=lambda item: (-len(item[0]), item[0])))


def _grupo_com_mais_rodadas_restantes(rodadas_por_grupo, grupos_pool, ultimo_grupo=None):
    candidatos = []
    for grupo in grupos_pool or []:
        restante = len(rodadas_por_grupo.get(grupo) or [])
        if restante <= 0:
            continue
        if ultimo_grupo and grupo == ultimo_grupo and len(grupos_pool) > 1:
            continue
        candidatos.append((restante, grupo))

    if not candidatos and ultimo_grupo:
        for grupo in grupos_pool or []:
            restante = len(rodadas_por_grupo.get(grupo) or [])
            if restante > 0:
                candidatos.append((restante, grupo))

    if not candidatos:
        return None

    # Maior quantidade restante ganha. Em empate, ordem alfabética/visual.
    candidatos.sort(key=lambda x: (-x[0], x[1]))
    return candidatos[0][1]


def _gerar_slots_pool_multiquadra(rodadas_por_grupo, grupos_pool, quadras_pool):
    """Gera slots em bloco: um grupo por slot, várias quadras simultâneas.

    Esta é a regra que tu aprovou:
    - com duas quadras na Floresta, o slot recebe 2 jogos do mesmo grupo;
    - o grupo com mais rodadas pendentes aparece mais vezes;
    - não repete o mesmo grupo em slot seguido quando há alternativa;
    - como os confrontos vêm por rodada round-robin, nenhuma equipe dobra no slot.
    """
    capacidade = max(1, len(quadras_pool or []))
    slots = []
    ultimo_grupo = None

    while any(rodadas_por_grupo.get(g) for g in grupos_pool):
        grupo = _grupo_com_mais_rodadas_restantes(rodadas_por_grupo, grupos_pool, ultimo_grupo)
        if not grupo:
            break

        rodada = list((rodadas_por_grupo.get(grupo) or []).pop(0) or [])
        jogos_slot = []
        equipes_slot = set()

        for qid in quadras_pool[:capacidade]:
            if not rodada:
                break
            jogo_tuple = rodada.pop(0)
            equipe_a, equipe_b = jogo_tuple
            if equipe_a in equipes_slot or equipe_b in equipes_slot:
                continue
            jogos_slot.append({
                "grupo": grupo,
                "equipe_a": equipe_a,
                "equipe_b": equipe_b,
                "quadra_id": qid,
            })
            equipes_slot.update({equipe_a, equipe_b})

        # Se sobrou jogo por alguma rodada maior que a capacidade, devolve como
        # próxima rodada do mesmo grupo. Isso mantém compatibilidade com qualquer
        # grupo/tamanho de quadras, embora no teu caso sejam blocos de 2.
        if rodada:
            rodadas_por_grupo.setdefault(grupo, []).insert(0, rodada)

        if jogos_slot:
            slots.append(jogos_slot)
            ultimo_grupo = grupo
        else:
            # Proteção para não criar loop infinito em dados estranhos.
            ultimo_grupo = None

    return slots


def _gerar_slots_pool_quadra_unica(rodadas_por_grupo, grupos_pool, quadra_id):
    """Gera slots para uma quadra só, tentando evitar equipe em slot seguido."""
    jogos_por_grupo = {}
    for grupo in grupos_pool or []:
        jogos = []
        for rodada in rodadas_por_grupo.get(grupo) or []:
            for equipe_a, equipe_b in rodada:
                jogos.append({
                    "grupo": grupo,
                    "equipe_a": equipe_a,
                    "equipe_b": equipe_b,
                    "quadra_id": quadra_id,
                })
        jogos_por_grupo[grupo] = jogos

    slots = []
    ultimo_grupo = None
    equipes_slot_anterior = set()

    while any(jogos_por_grupo.get(g) for g in grupos_pool):
        grupo = None
        candidatos = []
        for g in grupos_pool or []:
            restante = len(jogos_por_grupo.get(g) or [])
            if restante <= 0:
                continue
            if ultimo_grupo and g == ultimo_grupo and len(grupos_pool) > 1:
                continue
            candidatos.append((restante, g))
        if not candidatos:
            candidatos = [(len(jogos_por_grupo.get(g) or []), g) for g in grupos_pool if jogos_por_grupo.get(g)]
        if not candidatos:
            break
        candidatos.sort(key=lambda x: (-x[0], x[1]))
        grupo = candidatos[0][1]

        jogo = _proximo_jogo_sem_conflito(jogos_por_grupo.get(grupo), set(), equipes_slot_anterior)
        if not jogo:
            break
        slots.append([jogo])
        ultimo_grupo = grupo
        equipes_slot_anterior = {jogo["equipe_a"], jogo["equipe_b"]}

    return slots


def _gerar_agenda_classificatoria_inteligente(nome_competicao, grupos_raw, config):
    """Gera a classificatória por SLOTS simultâneos.

    A lógica principal agora é:
    1. gerar os confrontos de cada grupo em memória;
    2. separar grupos por pool de quadras permitidas;
    3. em pools com 2+ quadras, colocar um bloco/rodada do mesmo grupo por slot;
    4. alternar grupos pelo maior número de rodadas restantes, evitando grupo repetido;
    5. salvar o slot em `rodada`, para a tela entender que Floresta 1 e 2 acontecem juntas.
    """
    quadras = garantir_quadras_competicao(nome_competicao, 1)
    quadras_ativas = _ids_quadras_ativas(quadras)
    if not quadras_ativas:
        quadras_ativas = [None]

    rodadas_por_grupo = {}
    for g in grupos_raw or []:
        equipes = listar_equipes_por_grupo(g["id"])
        nomes = [e.get("equipe") for e in equipes if e.get("equipe")]
        if len(nomes) >= 2:
            rodadas_por_grupo[str(g.get("nome") or "").strip().upper()] = _gerar_rodadas_round_robin(nomes)

    if not rodadas_por_grupo:
        return {"ok": False, "mensagem": "Não há grupos com equipes suficientes para gerar jogos."}

    pools = _montar_blocos_por_pool_classificatoria(nome_competicao, grupos_raw, quadras_ativas, config)
    if not pools:
        return {"ok": False, "mensagem": "Não foi possível definir as quadras permitidas dos grupos."}

    slots_por_pool = []
    for quadras_pool, grupos_pool in pools.items():
        # Copia só as rodadas dos grupos deste pool para não consumir o dict global.
        rodadas_pool = {
            g: [list(rodada) for rodada in (rodadas_por_grupo.get(g) or [])]
            for g in grupos_pool
        }

        if len(quadras_pool) >= 2:
            slots_pool = _gerar_slots_pool_multiquadra(rodadas_pool, grupos_pool, list(quadras_pool))
        else:
            slots_pool = _gerar_slots_pool_quadra_unica(rodadas_pool, grupos_pool, quadras_pool[0])

        slots_por_pool.append(slots_pool)

    total_slots = max((len(s) for s in slots_por_pool), default=0)
    agenda = []

    for slot_idx in range(total_slots):
        slot_numero = slot_idx + 1
        ordem_no_slot = 1
        for slots_pool in slots_por_pool:
            if slot_idx >= len(slots_pool):
                continue
            for jogo in slots_pool[slot_idx]:
                item = dict(jogo)
                item["slot"] = slot_numero
                item["ordem_no_slot"] = ordem_no_slot
                item["rodada_grupo"] = slot_numero
                agenda.append(item)
                ordem_no_slot += 1

    if not agenda:
        return {"ok": False, "mensagem": "Não foi possível montar a agenda dos jogos."}

    return {"ok": True, "agenda": agenda, "slots": total_slots, "quadras": len(quadras_ativas)}



# =========================================================
# SALVAR CONFIGURAÇÃO DA GERAÇÃO AUTOMÁTICA
# =========================================================
@tabela_bp.route("/tabela/salvar-config-geracao", methods=["POST"])
@exigir_organizador_da_competicao
def salvar_config_geracao_view():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    if fase_grupos_esta_travada_por_jogo(competicao["nome"]):
        flash("A fase classificatória já iniciou. Não é possível alterar a configuração da geração.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))

    # Compatibilidade com o modal do tabela.html atual.
    modo_form = (request.form.get("modo_distribuicao") or "rodizio").strip().lower()
    if modo_form in {"fixa", "grupo_fixo", "fixo"}:
        modo = "grupo_fixo"
    else:
        modo = "automatico_inteligente"

    ordem_form = (request.form.get("ordem_jogos") or "intercalar_grupos").strip().lower()
    mapa_ordem = {
        "intercalar_grupos": "por_rodada",
        "por_grupo": "por_grupo_inteiro",
        "balancear_quadras": "por_rodada",
    }
    rodizio = mapa_ordem.get(ordem_form, "por_rodada")

    try:
        descanso = int(request.form.get("descanso_minimo") or 1)
    except (TypeError, ValueError):
        descanso = 1
    descanso = max(0, min(descanso, 5))

    grupos_raw = listar_grupos(competicao["nome"])
    grupos_compartilhados = {}
    for g in grupos_raw:
        nome_g = str(g.get("nome") or "").strip().upper()
        if not nome_g:
            continue

        # O HTML envia quadras_grupo_A[]; o request.form.getlist aceita esse nome completo.
        ids = []
        for valor in request.form.getlist(f"quadras_grupo_{nome_g}[]"):
            try:
                qid = int(valor)
                if qid > 0 and qid not in ids:
                    ids.append(qid)
            except (TypeError, ValueError):
                pass
        if ids:
            grupos_compartilhados[nome_g] = ids

    quadras_compartilhadas = []
    for ids in grupos_compartilhados.values():
        for qid in ids:
            if qid not in quadras_compartilhadas:
                quadras_compartilhadas.append(qid)

    atualizar_configuracao_agenda_competicao(
        competicao["nome"],
        modo_distribuicao=modo,
        descanso_minimo_jogos=descanso,
        rodizio_grupos=rodizio,
        permitir_relaxar_descanso=True,
        grupos_compartilhados=grupos_compartilhados,
        quadras_compartilhadas=quadras_compartilhadas,
    )

    flash("Configuração da geração automática salva com sucesso.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))


# =========================================================
# GERAR JOGOS AUTOMÁTICOS
# =========================================================
@tabela_bp.route("/tabela/gerar-automatico", methods=["POST"])
@exigir_organizador_da_competicao
def gerar_automatico():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    nome_competicao = competicao["nome"]
    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    fase_banco = _fase_subaba_para_banco(fase_subaba)

    # Esta validação é feita uma única vez. Antes, algumas funções de criação
    # podiam repetir consultas de trava para cada partida gerada.
    if not _fase_pode_ser_alterada_sem_travar_mata_mata(nome_competicao, fase_banco):
        flash("Esta fase já iniciou. Não é possível gerar jogos automaticamente nela.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    grupos_raw = listar_grupos(nome_competicao)
    mapa_quadras = _mapa_quadras_formatadas(nome_competicao)

    if fase_banco != "grupos":
        partidas = listar_partidas(nome_competicao)
        grupos = []
        for g in grupos_raw:
            grupos.append({"grupo": g, "equipes": listar_equipes_por_grupo(g["id"])})

        mapa_escudos = _mapa_escudos_equipes(listar_equipes_da_competicao(nome_competicao))
        partidas_preparadas = _preparar_partidas(partidas, mapa_escudos, competicao)
        classificacao = _calcular_classificacao(partidas_preparadas, grupos, competicao, mapa_escudos)

        def _vencedor_ou_placeholder(partida, prefixo, indice):
            if partida and _partida_esta_finalizada(partida):
                try:
                    sets_a = int(partida.get("sets_a") or 0)
                    sets_b = int(partida.get("sets_b") or 0)
                except (TypeError, ValueError):
                    sets_a = sets_b = 0
                if sets_a > sets_b:
                    return partida.get("equipe_a") or f"Vencedor {prefixo} {indice}"
                if sets_b > sets_a:
                    return partida.get("equipe_b") or f"Vencedor {prefixo} {indice}"
            return f"Vencedor {prefixo} {indice}"

        confrontos = []
        if fase_banco == "quartas":
            classificados = []
            maior_tamanho = max((len(linhas) for linhas in classificacao.values()), default=0)
            for posicao in range(maior_tamanho):
                for nome_grupo in sorted(classificacao.keys()):
                    linhas = classificacao.get(nome_grupo) or []
                    if posicao < len(linhas):
                        classificados.append(linhas[posicao]["equipe"])

            if len(classificados) < 8:
                flash("Para gerar quartas automaticamente, precisa ter pelo menos 8 equipes classificadas.", "erro")
                return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

            top8 = classificados[:8]
            confrontos = [
                (top8[0], top8[7]),
                (top8[3], top8[4]),
                (top8[1], top8[6]),
                (top8[2], top8[5]),
            ]
        elif fase_banco == "semifinal":
            quartas = _filtrar_partidas_por_fase(partidas_preparadas, "quartas")
            quartas = sorted(quartas, key=lambda p: (p.get("ordem") or 0, p.get("id") or 0))
            if len(quartas) >= 4:
                confrontos = [
                    (_vencedor_ou_placeholder(quartas[0], "Quartas", 1), _vencedor_ou_placeholder(quartas[1], "Quartas", 2)),
                    (_vencedor_ou_placeholder(quartas[2], "Quartas", 3), _vencedor_ou_placeholder(quartas[3], "Quartas", 4)),
                ]
            else:
                classificados = []
                maior_tamanho = max((len(linhas) for linhas in classificacao.values()), default=0)
                for posicao in range(maior_tamanho):
                    for nome_grupo in sorted(classificacao.keys()):
                        linhas = classificacao.get(nome_grupo) or []
                        if posicao < len(linhas):
                            classificados.append(linhas[posicao]["equipe"])
                if len(classificados) < 4:
                    flash("Para gerar semifinais automaticamente, precisa ter quartas criadas ou pelo menos 4 equipes classificadas.", "erro")
                    return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))
                top4 = classificados[:4]
                confrontos = [(top4[0], top4[3]), (top4[1], top4[2])]
        elif fase_banco == "final":
            semis = _filtrar_partidas_por_fase(partidas_preparadas, "semifinais")
            semis = sorted(semis, key=lambda p: (p.get("ordem") or 0, p.get("id") or 0))
            if len(semis) < 2:
                flash("Para gerar a final automaticamente, crie as duas semifinais primeiro.", "erro")
                return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))
            confrontos = [(_vencedor_ou_placeholder(semis[0], "Semifinal", 1), _vencedor_ou_placeholder(semis[1], "Semifinal", 2))]

        if not confrontos:
            flash("Não foi possível montar confrontos automáticos para esta fase.", "erro")
            return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

        limpar_partidas_por_fase(nome_competicao, fase_banco)
        ordem_inicial = len(listar_partidas(nome_competicao)) + 1
        quadra_id, quadra_nome = _quadra_nome_cache(mapa_quadras, _to_int_or_none(request.form.get("quadra_id")))

        partidas_para_salvar = []
        for indice, (equipe_a, equipe_b) in enumerate(confrontos):
            partidas_para_salvar.append({
                "competicao": nome_competicao,
                "grupo": None,
                "equipe_a": equipe_a,
                "equipe_b": equipe_b,
                "fase": fase_banco,
                "ordem": ordem_inicial + indice,
                "quadra_id": quadra_id,
                "quadra_nome": quadra_nome,
                "origem": "automatica",
                "rodada": None,
            })

        _inserir_partidas_em_lote(partidas_para_salvar)

        flash("Jogos do mata-mata gerados automaticamente. Você ainda pode excluir e recriar enquanto a fase não iniciar.", "sucesso")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    config_agenda = _config_agenda_da_requisicao(nome_competicao)
    resultado_agenda = _gerar_agenda_classificatoria_inteligente(nome_competicao, grupos_raw, config_agenda)

    if not resultado_agenda.get("ok"):
        flash(resultado_agenda.get("mensagem") or "Não foi possível gerar os jogos automaticamente.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))

    limpar_partidas_por_fase(nome_competicao, "grupos")

    partidas_para_salvar = []
    for ordem, jogo in enumerate(resultado_agenda.get("agenda") or [], start=1):
        quadra_id, quadra_nome = _quadra_nome_cache(mapa_quadras, jogo.get("quadra_id"))
        partidas_para_salvar.append({
            "competicao": nome_competicao,
            "grupo": jogo["grupo"],
            "equipe_a": jogo["equipe_a"],
            "equipe_b": jogo["equipe_b"],
            "fase": "grupos",
            "ordem": ordem,
            "quadra_id": quadra_id,
            "quadra_nome": quadra_nome,
            "origem": "automatica_inteligente",
            "rodada": jogo.get("slot"),
        })

    total_inserido = _inserir_partidas_em_lote(partidas_para_salvar)

    flash(
        f"{total_inserido} jogos gerados automaticamente com agenda inteligente: "
        f"{resultado_agenda.get('slots', 0)} rodadas/slots, "
        f"descanso mínimo de {config_agenda.get('descanso_minimo_jogos', 1)} jogo(s) quando possível.",
        "sucesso",
    )
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))

