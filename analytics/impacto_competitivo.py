"""Análise comparativa e indicadores de impacto pós-partida.

As métricas são determinísticas e derivadas exclusivamente da linha do tempo
persistida. Nenhum processamento é executado durante a operação ao vivo.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable

from analytics.scout_inteligente import _chave_atleta, _equipe_pontuadora, _fundamento, _numero, _texto


_PESOS_FUNDAMENTO = {
    "ataque": 1.00,
    "ace": 1.30,
    "bloqueio": 1.20,
    "ponto": 0.70,
    "erro": 0.45,
    "erro_saque": 0.45,
    "erro_ataque": 0.45,
    "falta": 0.45,
    "invasao": 0.45,
    "erro_rotacao": 0.45,
}


def _nome_equipes(partida: dict[str, Any] | None) -> dict[str, str]:
    partida = partida or {}
    return {
        "A": _texto(partida.get("equipe_a")) or "Equipe A",
        "B": _texto(partida.get("equipe_b")) or "Equipe B",
    }


def _bonus_decisivo(placar_a: int, placar_b: int) -> float:
    total = placar_a + placar_b
    diferenca = abs(placar_a - placar_b)
    if total >= 38 and diferenca <= 2:
        return 0.60
    if total >= 30 and diferenca <= 2:
        return 0.35
    return 0.0


def calcular_impacto_competitivo(
    eventos: Iterable[dict[str, Any]],
    partida: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Gera tendências por set, comparação de equipes e ranking de impacto."""
    linha = [dict(item) for item in eventos]
    nomes = _nome_equipes(partida)
    placares_set: dict[int, dict[str, int]] = defaultdict(lambda: {"A": 0, "B": 0})
    equipes_set: dict[int, dict[str, Counter]] = defaultdict(
        lambda: {"A": Counter(), "B": Counter()}
    )
    sequencia_set: dict[int, dict[str, Any]] = defaultdict(
        lambda: {"equipe": None, "quantidade": 0, "max_A": 0, "max_B": 0}
    )
    lider_set: dict[int, str | None] = defaultdict(lambda: None)
    atletas: dict[tuple[str, str], Counter] = defaultdict(Counter)
    rotulos: dict[tuple[str, str], str] = {}
    momentos: list[dict[str, Any]] = []
    total_pontos_identificados = 0
    pontos_com_atleta = 0

    for evento in linha:
        set_numero = _numero(evento.get("set_numero")) or 0
        categoria = _texto(evento.get("categoria")).lower()
        equipe_evento = _texto(evento.get("equipe")).upper()

        if categoria in {"tempo", "substituicao", "disciplina"} and equipe_evento in {"A", "B"}:
            equipes_set[set_numero][equipe_evento][categoria] += 1

        if categoria != "ponto":
            continue
        pontuadora = _equipe_pontuadora(evento)
        if pontuadora not in {"A", "B"}:
            continue

        total_pontos_identificados += 1
        adversaria = "B" if pontuadora == "A" else "A"
        fundamento = _fundamento(evento)
        placares_set[set_numero][pontuadora] += 1
        placar = placares_set[set_numero]
        equipe_dados = equipes_set[set_numero][pontuadora]
        equipe_dados["pontos"] += 1
        equipe_dados[fundamento] += 1
        if fundamento in {"ataque", "ace", "bloqueio"}:
            equipe_dados["acoes_proprias"] += 1
        elif fundamento in {"erro", "erro_saque", "erro_ataque", "falta", "invasao", "erro_rotacao"}:
            equipe_dados["erros_adversario"] += 1

        sequencia = sequencia_set[set_numero]
        if sequencia["equipe"] == pontuadora:
            sequencia["quantidade"] += 1
        else:
            sequencia["equipe"] = pontuadora
            sequencia["quantidade"] = 1
        chave_max = f"max_{pontuadora}"
        sequencia[chave_max] = max(int(sequencia[chave_max]), int(sequencia["quantidade"]))

        lider_novo = "A" if placar["A"] > placar["B"] else "B" if placar["B"] > placar["A"] else None
        lider_antigo = lider_set[set_numero]
        if lider_novo and lider_antigo and lider_novo != lider_antigo:
            equipes_set[set_numero][pontuadora]["viradas_lideranca"] += 1
        if lider_novo:
            lider_set[set_numero] = lider_novo

        bonus = _bonus_decisivo(placar["A"], placar["B"])
        if bonus:
            equipe_dados["pontos_decisivos"] += 1
            momentos.append({
                "id": evento.get("id"),
                "set": set_numero,
                "equipe": pontuadora,
                "equipe_nome": nomes[pontuadora],
                "placar": f"{placar['A']} x {placar['B']}",
                "fundamento": fundamento,
                "descricao": evento.get("descricao") or fundamento,
                "peso": round(_PESOS_FUNDAMENTO.get(fundamento, 0.6) + bonus, 2),
            })

        atleta_info = _chave_atleta(evento)
        if atleta_info:
            pontos_com_atleta += 1
            chave_atleta, rotulo = atleta_info
            chave = (pontuadora, chave_atleta)
            rotulos.setdefault(chave, rotulo)
            registro = atletas[chave]
            registro["pontos"] += 1
            registro[fundamento] += 1
            registro["sets_atuados_mask"] |= 1 << max(0, set_numero)
            registro["impacto_centavos"] += round(
                (_PESOS_FUNDAMENTO.get(fundamento, 0.60) + bonus) * 100
            )
            if bonus:
                registro["pontos_decisivos"] += 1

    sets_saida: list[dict[str, Any]] = []
    totais_equipes = {"A": Counter(), "B": Counter()}
    for set_numero in sorted(k for k in placares_set if k):
        placar = placares_set[set_numero]
        bloco = {"set": set_numero, "placar_a": placar["A"], "placar_b": placar["B"], "equipes": {}}
        for equipe in ("A", "B"):
            dados = Counter(equipes_set[set_numero][equipe])
            dados["maior_sequencia"] = int(sequencia_set[set_numero][f"max_{equipe}"])
            dados["saldo"] = placar[equipe] - placar["B" if equipe == "A" else "A"]
            dados["percentual_acoes_proprias"] = round(
                dados.get("acoes_proprias", 0) / dados.get("pontos", 1) * 100, 1
            ) if dados.get("pontos", 0) else 0.0
            bloco["equipes"][equipe] = dict(dados)
            max_sequencia_anterior = int(totais_equipes[equipe].get("maior_sequencia", 0))
            dados_sem_max = Counter(dados)
            dados_sem_max.pop("maior_sequencia", None)
            totais_equipes[equipe].update(dados_sem_max)
            totais_equipes[equipe]["maior_sequencia"] = max(
                max_sequencia_anterior, int(dados.get("maior_sequencia", 0))
            )
        sets_saida.append(bloco)

    comparativo = {}
    for equipe in ("A", "B"):
        dados = totais_equipes[equipe]
        pontos = int(dados.get("pontos", 0))
        comparativo[equipe] = {
            **dict(dados),
            "nome": nomes[equipe],
            "indice_autonomia": round(dados.get("acoes_proprias", 0) / pontos * 100, 1) if pontos else 0.0,
            "indice_pressao": round(
                (dados.get("ace", 0) * 1.3 + dados.get("bloqueio", 0) * 1.2 + dados.get("maior_sequencia", 0))
                / max(1, pontos) * 10,
                1,
            ),
        }

    ranking = []
    for (equipe, chave_atleta), contagem in atletas.items():
        sets_mask = int(contagem.get("sets_atuados_mask", 0))
        sets_atuados = max(1, sets_mask.bit_count())
        impacto = round(int(contagem.get("impacto_centavos", 0)) / 100, 2)
        ranking.append({
            "equipe": equipe,
            "equipe_nome": nomes[equipe],
            "atleta": rotulos.get((equipe, chave_atleta), chave_atleta),
            "pontos": int(contagem.get("pontos", 0)),
            "ataque": int(contagem.get("ataque", 0)),
            "ace": int(contagem.get("ace", 0)),
            "bloqueio": int(contagem.get("bloqueio", 0)),
            "pontos_decisivos": int(contagem.get("pontos_decisivos", 0)),
            "sets_atuados": sets_atuados,
            "impacto": impacto,
            "impacto_por_set": round(impacto / sets_atuados, 2),
        })
    ranking.sort(key=lambda item: (-item["impacto"], -item["pontos"], item["atleta"]))

    tendencias: list[dict[str, str]] = []
    if sets_saida:
        for equipe in ("A", "B"):
            saldos = [int(item["equipes"][equipe].get("saldo", 0)) for item in sets_saida]
            if len(saldos) >= 2:
                variacao = saldos[-1] - saldos[0]
                if variacao >= 4:
                    tendencias.append({"equipe": equipe, "tipo": "crescimento", "texto": f"{nomes[equipe]} terminou a partida com evolução de {variacao} pontos no saldo por set."})
                elif variacao <= -4:
                    tendencias.append({"equipe": equipe, "tipo": "queda", "texto": f"{nomes[equipe]} perdeu {abs(variacao)} pontos de saldo entre o primeiro e o último set analisado."})
            melhor = max(sets_saida, key=lambda item: int(item["equipes"][equipe].get("saldo", 0)))
            tendencias.append({"equipe": equipe, "tipo": "melhor_set", "texto": f"O melhor saldo de {nomes[equipe]} ocorreu no set {melhor['set']}: {melhor['equipes'][equipe].get('saldo', 0):+d}."})

    vencedor_analitico = None
    if comparativo["A"].get("pontos", 0) != comparativo["B"].get("pontos", 0):
        vencedor_analitico = "A" if comparativo["A"].get("pontos", 0) > comparativo["B"].get("pontos", 0) else "B"

    return {
        "comparativo_equipes": comparativo,
        "tendencias_por_set": sets_saida,
        "ranking_impacto": ranking,
        "momentos_alto_impacto": momentos[-20:],
        "tendencias": tendencias,
        "vencedor_analitico": vencedor_analitico,
        "cobertura": {
            "total_eventos": len(linha),
            "pontos_identificados": total_pontos_identificados,
            "pontos_com_atleta": pontos_com_atleta,
            "percentual_pontos_com_atleta": round(pontos_com_atleta / total_pontos_identificados * 100, 1) if total_pontos_identificados else 0.0,
        },
        "metodologia": {
            "versao": 1,
            "descricao": "Índice determinístico baseado em fundamento, ponto decisivo e consistência por set.",
            "pesos": dict(_PESOS_FUNDAMENTO),
        },
    }
