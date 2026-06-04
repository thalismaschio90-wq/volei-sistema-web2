from flask import Blueprint, render_template, request, redirect, session, url_for, flash
from functools import wraps
import random

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
    vincular_grupo_a_quadra,
    aplicar_quadra_em_partida,
    conectar,
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
    if not item:
        return "Sem quadra"
    return (
        item.get("quadra_nome")
        or item.get("quadra")
        or item.get("nome")
        or "Sem quadra"
    )


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
    return int(quadra["id"]), (quadra.get("nome") or f"Quadra {quadra.get('ordem') or ''}").strip()


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
        if valor:
            return valor

    return "agendada"


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

    Também grava status_jogo='agendada', porque no banco antigo status_jogo tem DEFAULT 'pre_jogo'
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
            quadra=quadra_nome or None,
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
                    quadra, quadra_id, quadra_nome, origem, status
                )
                VALUES (%s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, 'agendada')
            """, (competicao_nome, equipe_a, equipe_b, fase_banco, ordem, quadra_nome or None, quadra_id, quadra_nome or '', origem))
        conn.commit()

    return True

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
        "agendada": "AGENDADA",
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
            "quadra_label": _quadra_label(g),
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
            "quadra_label": _quadra_label(g),
            "quadra_id": _quadra_id_do_grupo(g),
        })

    partidas_preparadas = _preparar_partidas(partidas, mapa_escudos, competicao)
    partidas_fase = _filtrar_partidas_por_fase(partidas_preparadas, fase_subaba)
    classificacao = _calcular_classificacao(partidas_preparadas, grupos, competicao, mapa_escudos)
    regras_classificacao = _obter_regras_classificacao(competicao)
    criterios_classificacao = _criterios_efetivos_ate_sorteio(regras_classificacao.get("criterios"))
    colunas_classificacao = _colunas_classificacao_por_criterios(criterios_classificacao)

    fases = _fases_disponiveis(competicao)

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
        quadra=quadra_nome or None,
        quadra_id=quadra_id,
        quadra_nome=quadra_nome,
        status="agendada",
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
# GERAR JOGOS AUTOMÁTICOS
# =========================================================
@tabela_bp.route("/tabela/gerar-automatico", methods=["POST"])
@exigir_organizador_da_competicao
def gerar_automatico():
    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_subaba = (request.form.get("fase_subaba") or "classificatorias").strip().lower()
    fase_banco = _fase_subaba_para_banco(fase_subaba)

    if not _fase_pode_ser_alterada_sem_travar_mata_mata(competicao["nome"], fase_banco):
        flash("Esta fase já iniciou. Não é possível gerar jogos automaticamente nela.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    grupos_raw = listar_grupos(competicao["nome"])

    if fase_banco != "grupos":
        partidas = listar_partidas(competicao["nome"])
        grupos = []
        for g in grupos_raw:
            grupos.append({"grupo": g, "equipes": listar_equipes_por_grupo(g["id"])})

        mapa_escudos = _mapa_escudos_equipes(listar_equipes_da_competicao(competicao["nome"]))
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

        limpar_partidas_por_fase(competicao["nome"], fase_banco)
        ordem = len(listar_partidas(competicao["nome"])) + 1
        for equipe_a, equipe_b in confrontos:
            _criar_partida_para_tabela(competicao["nome"], None, equipe_a, equipe_b, ordem, fase_banco, origem="automatica", quadra_id=_to_int_or_none(request.form.get("quadra_id")))
            ordem += 1

        flash("Jogos do mata-mata gerados automaticamente. Você ainda pode excluir e recriar enquanto a fase não iniciar.", "sucesso")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase=fase_subaba))

    limpar_partidas_por_fase(competicao["nome"], "grupos")

    ordem = 1

    def gerar_rodadas(equipes):
        times = equipes[:]

        if len(times) % 2 == 1:
            times.append(None)

        n = len(times)
        rodadas = []

        for _ in range(n - 1):
            rodada = []
            for i in range(n // 2):
                t1 = times[i]
                t2 = times[n - 1 - i]

                if t1 is not None and t2 is not None:
                    rodada.append((t1, t2))

            rodadas.append(rodada)
            times = [times[0]] + [times[-1]] + times[1:-1]

        return rodadas

    rodadas_por_grupo = {}

    for g in grupos_raw:
        equipes = listar_equipes_por_grupo(g["id"])
        nomes = [e["equipe"] for e in equipes]

        if len(nomes) >= 2:
            rodadas = gerar_rodadas(nomes)
            rodadas_por_grupo[g["nome"]] = rodadas

    if not rodadas_por_grupo:
        flash("Não há grupos com equipes suficientes para gerar jogos.", "erro")
        return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))

    max_rodadas = max(len(r) for r in rodadas_por_grupo.values())

    ultimo_times_usados = set()

    for rodada_index in range(max_rodadas):
        jogos_da_rodada = []

        for grupo_nome, rodadas in rodadas_por_grupo.items():
            if rodada_index < len(rodadas):
                for jogo in rodadas[rodada_index]:
                    jogos_da_rodada.append((grupo_nome, jogo))

        jogos_ordenados = []

        while jogos_da_rodada:
            melhor_jogo = None

            for j in jogos_da_rodada:
                t1, t2 = j[1]
                if t1 not in ultimo_times_usados and t2 not in ultimo_times_usados:
                    melhor_jogo = j
                    break

            if not melhor_jogo:
                melhor_jogo = jogos_da_rodada[0]

            jogos_ordenados.append(melhor_jogo)

            t1, t2 = melhor_jogo[1]
            ultimo_times_usados = {t1, t2}

            jogos_da_rodada.remove(melhor_jogo)

        for grupo_nome, (t1, t2) in jogos_ordenados:
            quadra_id = _quadra_padrao_do_grupo(grupos_raw, grupo_nome)
            _, quadra_nome = _dados_quadra(competicao["nome"], quadra_id)
            criar_partida(
                competicao["nome"],
                grupo_nome,
                t1,
                t2,
                ordem,
                quadra=quadra_nome or None,
                fase="grupos",
                origem="automatica",
                quadra_id=quadra_id,
                quadra_nome=quadra_nome,
            )
            ordem += 1

    flash("Jogos gerados automaticamente com rodadas equilibradas.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))