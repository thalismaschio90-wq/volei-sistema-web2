
"""Regras e cálculo da classificação do VolleyTablePro.

Este módulo não conhece Flask nem templates. Ele recebe estruturas já carregadas,
aplica pontuação/desempates e coordena o cache legado de forma defensiva.
"""
from __future__ import annotations

import hashlib
import json
import random

from repositories.classificacao_cache import (
    obter_cache_classificacao,
    salvar_cache_classificacao,
)

STATUS_FINALIZADO = {
    "finalizada", "finalizado", "encerrada", "encerrado", "fim",
    "concluida", "concluído", "concluido", "final",
}

def _status_texto(valor):
    return str(valor or "").strip().lower().replace("_", " ")

def _partida_esta_finalizada(partida):
    partida = partida or {}
    for campo in ("finalizada", "finalizado", "encerrada", "encerrado"):
        valor = partida.get(campo)
        if valor is True or str(valor or "").strip().lower() in {"1", "true", "sim", "yes", "on"}:
            return True
    for campo in ("status_normalizado", "status", "status_jogo", "situacao"):
        if _status_texto(partida.get(campo)) in STATUS_FINALIZADO:
            return True
    return False

def _partidas_finalizadas_por_grupo(partidas):
    mapa = {}
    for partida in partidas or []:
        if not _partida_esta_finalizada(partida):
            continue
        grupo = partida.get("grupo")
        if grupo:
            mapa.setdefault(grupo, []).append(partida)
    return mapa

def _buscar_escudo_mapa(mapa_escudos, nome_equipe):
    nome = str(nome_equipe or "").strip()
    if not nome:
        return ""
    return (
        (mapa_escudos or {}).get(nome)
        or (mapa_escudos or {}).get(nome.lower())
        or (mapa_escudos or {}).get(nome.upper())
        or ""
    )

def _assinatura_classificacao_local(competicao_nome, partidas_preparadas, grupos, competicao):
    base = {
        "competicao": competicao_nome,
        "criterios": (competicao or {}).get("criterios_desempate") or (competicao or {}).get("criterios_classificacao") or "",
        "sets_tipo": (competicao or {}).get("sets_tipo") or "",
        "grupos": [
            {
                "grupo": (g.get("grupo") or {}).get("nome"),
                "equipes": sorted(str(e.get("equipe") or "") for e in (g.get("equipes") or [])),
            }
            for g in (grupos or [])
        ],
        "partidas": [
            {
                "id": p.get("id"), "grupo": p.get("grupo"),
                "fase": p.get("fase_normalizada") or p.get("fase"),
                "a": p.get("equipe_a"), "b": p.get("equipe_b"),
                "status": p.get("status_normalizado") or p.get("status"),
                "sets_a": p.get("sets_a"), "sets_b": p.get("sets_b"),
                "pontos_a": p.get("pontos_a"), "pontos_b": p.get("pontos_b"),
                **{f"set{i}_{lado}": p.get(f"set{i}_{lado}") for i in range(1, 6) for lado in ("a", "b")},
            }
            for p in (partidas_preparadas or []) if _partida_esta_finalizada(p)
        ],
    }
    texto = json.dumps(base, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(texto.encode("utf-8")).hexdigest()

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

    # Otimização: quando receber dict, já vem indexado por grupo e só varre
    # partidas daquele grupo. Antes varria TODAS as partidas dentro de cada
    # bloco de empate, o que fazia a classificação ficar muito lenta.
    if isinstance(partidas, dict):
        partidas_iter = partidas.get(grupo) or []
    else:
        partidas_iter = [p for p in (partidas or []) if p.get("grupo") == grupo and _partida_esta_finalizada(p)]

    for p in partidas_iter:
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
                "wo": 0,
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

        tipo_encerramento = str(p.get("tipo_encerramento") or "").strip().lower()
        origem_resultado = str(p.get("origem_resultado") or "").strip().lower()
        if tipo_encerramento in {"wo", "w.o.", "w.o"} or origem_resultado == "wo":
            if sets_a > sets_b:
                linha_b["wo"] = int(linha_b.get("wo") or 0) + 1
            elif sets_b > sets_a:
                linha_a["wo"] = int(linha_a.get("wo") or 0) + 1

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
    partidas_por_grupo = _partidas_finalizadas_por_grupo(partidas)

    for grupo, linhas in classificacao.items():
        classificacao[grupo] = _aplicar_criterios_classificacao(
            linhas,
            partidas_por_grupo,
            grupo,
            criterios_ativos,
        )

    return classificacao



def _normalizar_cache_classificacao(valor_cache, assinatura_atual=None):
    """Extrai a classificação salva no cache sem depender de um formato único.

    O banco já teve mais de uma versão dessa função de cache. Por isso este
    helper aceita dict, JSON em texto ou tupla/lista e só usa o cache quando
    ele realmente contém uma classificação válida.
    """
    if not valor_cache:
        return None

    if isinstance(valor_cache, str):
        try:
            valor_cache = json.loads(valor_cache)
        except Exception:
            return None

    if isinstance(valor_cache, (list, tuple)):
        # Compatibilidade com retornos antigos: (classificacao, assinatura) ou
        # (assinatura, classificacao). Preferimos o item que parece dict/list.
        candidatos = list(valor_cache)
        for item in candidatos:
            normalizado = _normalizar_cache_classificacao(item, assinatura_atual)
            if normalizado:
                return normalizado
        return None

    if not isinstance(valor_cache, dict):
        return None

    assinatura_cache = valor_cache.get("assinatura") or valor_cache.get("hash") or valor_cache.get("checksum")
    if assinatura_atual and assinatura_cache and str(assinatura_cache) != str(assinatura_atual):
        return None

    classificacao = (
        valor_cache.get("classificacao")
        or valor_cache.get("dados")
        or valor_cache.get("valor")
        or valor_cache.get("cache")
    )

    if isinstance(classificacao, str):
        try:
            classificacao = json.loads(classificacao)
        except Exception:
            return None

    return classificacao if isinstance(classificacao, dict) else None


def _assinatura_classificacao_segura(competicao_nome, partidas_preparadas, grupos, competicao):
    """Gera assinatura sem bater no banco.

    Isso remove uma consulta pesada que antes rodava na abertura da tabela e na
    geração de mata-mata. Se algo der errado, retorna None e a classificação é
    calculada normalmente, sem cache.
    """
    try:
        return _assinatura_classificacao_local(competicao_nome, partidas_preparadas, grupos, competicao)
    except Exception as e:
        print("AVISO classificacao/assinatura_local:", repr(e))
        return None


def _obter_cache_classificacao_seguro(competicao_nome, assinatura):
    try:
        return obter_cache_classificacao(competicao_nome, assinatura)
    except TypeError:
        try:
            return obter_cache_classificacao(competicao_nome)
        except Exception as e:
            print("AVISO classificacao/obter_cache:", repr(e))
    except Exception as e:
        print("AVISO classificacao/obter_cache:", repr(e))
    return None


def _salvar_cache_classificacao_seguro(competicao_nome, assinatura, classificacao):
    try:
        salvar_cache_classificacao(competicao_nome, assinatura, classificacao)
        return
    except TypeError:
        pass
    except Exception as e:
        print("AVISO classificacao/salvar_cache:", repr(e))
        return

    tentativas = [
        (competicao_nome, classificacao, assinatura),
        (competicao_nome, {"assinatura": assinatura, "classificacao": classificacao}),
        (competicao_nome, classificacao),
    ]
    for args in tentativas:
        try:
            salvar_cache_classificacao(*args)
            return
        except TypeError:
            continue
        except Exception as e:
            print("AVISO classificacao/salvar_cache:", repr(e))
            return


def _calcular_ou_obter_classificacao_cacheada(competicao_nome, partidas_preparadas, grupos, competicao, mapa_escudos=None):
    """Usa cache de classificação quando possível e calcula como fallback.

    Esta função estava sendo chamada pelo visualizador público e pela aba de
    classificação, mas não existia no arquivo. Sem ela, a rota pública quebrava
    com NameError e retornava 500. A implementação abaixo é defensiva: qualquer
    problema no cache apenas recalcula a classificação, sem derrubar a tela.
    """
    assinatura = _assinatura_classificacao_segura(competicao_nome, partidas_preparadas, grupos, competicao)

    if assinatura:
        cache_bruto = _obter_cache_classificacao_seguro(competicao_nome, assinatura)
        classificacao_cache = _normalizar_cache_classificacao(cache_bruto, assinatura)
        if classificacao_cache:
            return classificacao_cache, True

    classificacao = _calcular_classificacao(partidas_preparadas, grupos, competicao, mapa_escudos)

    if assinatura:
        _salvar_cache_classificacao_seguro(competicao_nome, assinatura, classificacao)

    return classificacao, False




