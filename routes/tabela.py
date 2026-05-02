from flask import Blueprint, render_template, request, redirect, session, url_for, flash
from functools import wraps
import random

from banco import (
    buscar_competicao_por_organizador,
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
    conectar,
)

from routes.utils import exigir_perfil

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


def _status_tabela_para_trava(partida):
    """
    Usa primeiro o campo status da tabela de partidas.

    IMPORTANTE: no banco atual existe status_jogo com DEFAULT 'pre_jogo'.
    Então uma partida recém-criada pode nascer com status_jogo='pre_jogo' mesmo sem ter sido iniciada.
    Por isso status_jogo NÃO pode ser usado sozinho para bloquear criação/exclusão na tela da tabela.
    """
    status = str(partida.get("status") or "").strip().lower()

    if status:
        return status

    fase_partida = str(partida.get("fase_partida") or "").strip().lower()
    if fase_partida in {"ao_vivo", "em_andamento", "em andamento", "finalizada", "finalizado", "encerrado"}:
        return fase_partida

    status_jogo = str(partida.get("status_jogo") or "").strip().lower()
    if status_jogo in {"ao_vivo", "em_andamento", "em andamento", "finalizada", "finalizado", "encerrado"}:
        return status_jogo

    return status or "agendada"


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

    if status in {"finalizada", "finalizado", "encerrado", "ao vivo", "em andamento", "andamento", "em_andamento"}:
        return True

    fase_partida = str(partida.get("fase_partida") or "").strip().lower()
    if fase_partida in {"ao_vivo", "ao vivo", "em_andamento", "em andamento", "finalizada", "finalizado", "encerrado"}:
        return True

    if partida.get("pre_jogo_iniciado_em") or partida.get("jogo_iniciado_em") or partida.get("finalizado_em"):
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



def _criar_partida_para_tabela(competicao_nome, grupo, equipe_a, equipe_b, ordem, fase_banco, origem="manual"):
    """
    Cria partida pela tela da tabela.

    - Grupos usam a função padrão do banco, porque a classificatória deve respeitar o travamento estrutural.
    - Mata-mata faz INSERT direto para NÃO ser bloqueado pela classificatória travada.

    Também grava status_jogo='agendada', porque no banco antigo status_jogo tem DEFAULT 'pre_jogo'
    e isso fazia a tela achar que a partida já tinha iniciado logo depois de criar.
    """
    if fase_banco == "grupos":
        retorno = criar_partida(
            competicao_nome,
            grupo,
            equipe_a,
            equipe_b,
            ordem,
            fase=fase_banco,
            origem=origem,
        )
        return retorno is not False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO partidas (
                    competicao, grupo, equipe_a, equipe_b, fase, ordem, origem, status
                )
                VALUES (%s, NULL, %s, %s, %s, %s, %s, 'agendada')
            """, (competicao_nome, equipe_a, equipe_b, fase_banco, ordem, origem))
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
    # Para a tela da tabela, o campo status é o mais confiável.
    # status_jogo pode nascer como pre_jogo por DEFAULT do banco antigo e não significa partida iniciada.
    return _status_tabela_para_trava(partida)


def _status_exibicao(partida):
    status = _status_normalizado(partida)

    mapa = {
        "pre_jogo": "PRÉ-JOGO",
        "agendada": "AGENDADA",
        "em andamento": "AO VIVO",
        "ao vivo": "AO VIVO",
        "andamento": "AO VIVO",
        "em_andamento": "AO VIVO",
        "finalizada": "FINALIZADO",
        "finalizado": "FINALIZADO",
        "encerrado": "FINALIZADO",
    }

    return mapa.get(status, (status or "AGUARDANDO").replace("_", " ").upper())


def _partida_esta_finalizada(partida):
    return _status_normalizado(partida) in {"finalizada", "finalizado", "encerrado"}


def _partida_esta_ao_vivo(partida):
    return _status_normalizado(partida) in {"em andamento", "ao vivo", "andamento", "em_andamento"}


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


def _preparar_partidas(partidas):
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


def _obter_regras_classificacao(competicao):
    criterios = [
        ("pontos", _bool_por_chaves(competicao, ["criterio_pontos", "usar_pontos", "pontos_criterio"])),
        ("sets_average", _bool_por_chaves(competicao, ["criterio_sets_average", "usar_sets_average", "sets_average"])),
        ("pontos_average", _bool_por_chaves(competicao, ["criterio_pontos_average", "usar_pontos_average", "pontos_average"])),
        ("confronto_direto", _bool_por_chaves(competicao, ["criterio_confronto_direto", "usar_confronto_direto", "confronto_direto"])),
        ("saldo_sets", _bool_por_chaves(competicao, ["criterio_saldo_sets", "usar_saldo_sets", "saldo_sets"])),
        ("saldo_pontos", _bool_por_chaves(competicao, ["criterio_saldo_pontos", "usar_saldo_pontos", "saldo_pontos"])),
        ("sorteio", _bool_por_chaves(competicao, ["criterio_sorteio", "usar_sorteio", "sorteio"])),
    ]

    return {
        "pontos_vitoria": _valor_inteiro_regra(
            competicao,
            ["pontos_vitoria"],
            2
        ),
        "pontos_derrota": _valor_inteiro_regra(
            competicao,
            ["pontos_derrota"],
            0
        ),
        "pontos_tiebreak_vitoria": _valor_inteiro_regra(
            competicao,
            ["pontos_tiebreak_vitoria", "vitoria_tiebreak"],
            2
        ),
        "pontos_tiebreak_derrota": _valor_inteiro_regra(
            competicao,
            ["pontos_tiebreak_derrota", "derrota_tiebreak"],
            1
        ),
        "criterios": criterios,
    }


def _valor_criterio(linha, nome):
    if nome == "pontos":
        return linha["pontos"]

    if nome == "sets_average":
        sets_contra = linha["sets_contra"]
        if sets_contra > 0:
            return linha["sets_pro"] / sets_contra
        return float(linha["sets_pro"])

    if nome == "pontos_average":
        pontos_contra = linha["pontos_contra"]
        if pontos_contra > 0:
            return linha["pontos_pro"] / pontos_contra
        return float(linha["pontos_pro"])

    if nome == "saldo_sets":
        return linha["saldo_sets"]

    if nome == "saldo_pontos":
        return linha["saldo_pontos"]

    return 0


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


def _aplicar_desempates_profissional(linhas, partidas, grupo, criterios):
    if not linhas:
        return linhas

    criterios_base = [c for c in criterios if c not in {"confronto_direto", "sorteio"}]

    def assinatura_base(linha):
        return tuple(_valor_criterio(linha, c) for c in criterios_base)

    resultado_final = []
    i = 0

    while i < len(linhas):
        atual = linhas[i]
        bloco = [atual]
        j = i + 1

        while j < len(linhas) and assinatura_base(linhas[j]) == assinatura_base(atual):
            bloco.append(linhas[j])
            j += 1

        if len(bloco) > 1 and "confronto_direto" in criterios:
            bloco = _resolver_confronto_direto(bloco, partidas, grupo)

        if len(bloco) > 1 and "sorteio" in criterios:
            random.shuffle(bloco)

        resultado_final.extend(bloco)
        i = j

    return resultado_final


def _calcular_classificacao(partidas, grupos, competicao):
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

            if sets_b >= 1:
                linha_a["pontos"] += regras["pontos_tiebreak_vitoria"]
                linha_b["pontos"] += regras["pontos_tiebreak_derrota"]
            else:
                linha_a["pontos"] += regras["pontos_vitoria"]
                linha_b["pontos"] += regras["pontos_derrota"]
        else:
            linha_b["vitorias"] += 1
            linha_a["derrotas"] += 1

            if sets_a >= 1:
                linha_b["pontos"] += regras["pontos_tiebreak_vitoria"]
                linha_a["pontos"] += regras["pontos_tiebreak_derrota"]
            else:
                linha_b["pontos"] += regras["pontos_vitoria"]
                linha_a["pontos"] += regras["pontos_derrota"]

    for grupo, linhas in classificacao.items():
        for linha in linhas:
            linha["saldo_sets"] = linha["sets_pro"] - linha["sets_contra"]
            linha["saldo_pontos"] = linha["pontos_pro"] - linha["pontos_contra"]

    criterios_ativos = [c for c, ativo in regras["criterios"] if ativo]
    if not criterios_ativos:
        criterios_ativos = ["pontos", "saldo_sets", "saldo_pontos"]

    def chave(linha):
        valores = []

        for criterio in criterios_ativos:
            if criterio in {"confronto_direto", "sorteio"}:
                continue
            valores.append(_valor_criterio(linha, criterio))

        valores.append(linha["vitorias"])
        valores.append(linha["sets_pro"])
        valores.append(linha["pontos_pro"])

        return tuple(valores)

    for grupo, linhas in classificacao.items():
        linhas.sort(key=chave, reverse=True)
        classificacao[grupo] = _aplicar_desempates_profissional(
            linhas,
            partidas,
            grupo,
            criterios_ativos
        )

    return classificacao


# =========================================================
# VISUALIZADOR PÚBLICO
# =========================================================
@tabela_bp.route("/visualizador/<competicao_nome>")
def visualizador_publico(competicao_nome):
    grupos_raw = listar_grupos(competicao_nome)
    partidas = listar_partidas(competicao_nome)

    grupos = []
    for g in grupos_raw:
        equipes_grupo = listar_equipes_por_grupo(g["id"])
        grupos.append({
            "grupo": g,
            "equipes": equipes_grupo
        })

    competicao_fake = {
        "nome": competicao_nome
    }

    partidas_preparadas = _preparar_partidas(partidas)
    classificacao = _calcular_classificacao(partidas_preparadas, grupos, competicao_fake)

    return render_template(
        "visualizador_publico.html",
        competicao_nome=competicao_nome,
        grupos=grupos,
        classificacao=classificacao,
        partidas=partidas_preparadas,
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

    aba = (request.args.get("aba") or "geracao").strip().lower()
    if aba not in {"geracao", "partidas", "classificacao", "visualizador"}:
        aba = "geracao"

    fase_subaba = (request.args.get("fase") or "classificatorias").strip().lower()
    if fase_subaba not in {"classificatorias", "quartas", "semifinais", "finais"}:
        fase_subaba = "classificatorias"

    grupos_raw = listar_grupos(competicao["nome"])
    equipes = listar_equipes_da_competicao(competicao["nome"])
    partidas = listar_partidas(competicao["nome"])

    grupos = []
    for g in grupos_raw:
        equipes_grupo = listar_equipes_por_grupo(g["id"])
        grupos.append({
            "grupo": g,
            "equipes": equipes_grupo
        })

    partidas_preparadas = _preparar_partidas(partidas)
    partidas_fase = _filtrar_partidas_por_fase(partidas_preparadas, fase_subaba)
    classificacao = _calcular_classificacao(partidas_preparadas, grupos, competicao)

    fases = _fases_disponiveis(competicao)

    return render_template(
        "tabela.html",
        competicao=competicao,
        grupos=grupos,
        equipes=equipes,
        partidas=partidas_preparadas,
        partidas_fase=partidas_fase,
        classificacao=classificacao,
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

    competicao = buscar_competicao_por_organizador(session.get("usuario"))

    if not competicao:
        flash("Nenhuma competição encontrada.", "erro")
        return redirect(url_for("painel.inicio"))

    fase_banco = _fase_subaba_para_banco(fase_subaba)

    # O mata-mata NÃO usa grupo. Grupo só é obrigatório nas classificatórias.
    grupo = (grupo or "").strip().upper() if fase_banco == "grupos" else None

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

    ok = atualizar_partida(
        partida_id,
        competicao["nome"],
        None,
        fase_banco,
        equipe_a,
        equipe_b,
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

        partidas_preparadas = _preparar_partidas(partidas)
        classificacao = _calcular_classificacao(partidas_preparadas, grupos, competicao)

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
            _criar_partida_para_tabela(competicao["nome"], None, equipe_a, equipe_b, ordem, fase_banco, origem="automatica")
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
            criar_partida(
                competicao["nome"],
                grupo_nome,
                t1,
                t2,
                ordem,
                fase="grupos",
                origem="automatica",
            )
            ordem += 1

    flash("Jogos gerados automaticamente com rodadas equilibradas.", "sucesso")
    return redirect(url_for("tabela.tabela_view", aba="partidas", fase="classificatorias"))