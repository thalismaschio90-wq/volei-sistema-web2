"""Scout inteligente determinístico baseado na linha do tempo persistida.

Não usa serviços externos nem executa durante a operação ao vivo. As análises
são derivadas somente quando a tela administrativa é aberta.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable


def _texto(valor: object) -> str:
    return str(valor or "").strip()


def _numero(valor: object) -> int | None:
    try:
        return int(valor) if valor not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _equipe_pontuadora(evento: dict[str, Any]) -> str | None:
    detalhes = evento.get("detalhes") if isinstance(evento.get("detalhes"), dict) else {}
    candidatos = (
        detalhes.get("equipe_pontuadora"),
        detalhes.get("ponto_para"),
        evento.get("resultado"),
        evento.get("equipe"),
    )
    for valor in candidatos:
        texto = _texto(valor)
        if texto.upper() in {"A", "B"}:
            return texto.upper()
        baixo = texto.lower()
        if "ponto para a" in baixo or baixo.endswith(" equipe a"):
            return "A"
        if "ponto para b" in baixo or baixo.endswith(" equipe b"):
            return "B"
    return None


def _fundamento(evento: dict[str, Any]) -> str:
    valor = _texto(evento.get("fundamento") or evento.get("tipo") or evento.get("categoria")).lower()
    aliases = {
        "bloqueio": "bloqueio",
        "block": "bloqueio",
        "ace": "ace",
        "ataque": "ataque",
        "erro saque": "erro_saque",
        "erro de saque": "erro_saque",
        "erro ataque": "erro_ataque",
        "erro de ataque": "erro_ataque",
        "falta": "falta",
        "invasao": "invasao",
        "invasão": "invasao",
        "rotacao": "erro_rotacao",
        "rotação": "erro_rotacao",
    }
    for chave, normalizado in aliases.items():
        if chave in valor:
            return normalizado
    if "erro" in valor:
        return "erro"
    if evento.get("categoria") == "ponto":
        return "ponto"
    return valor.replace(" ", "_") or "outro"


def _chave_atleta(evento: dict[str, Any]) -> tuple[str, str] | None:
    atleta_id = _numero(evento.get("atleta_id"))
    nome = _texto(evento.get("atleta_nome"))
    numero = _numero(evento.get("numero"))
    if atleta_id is not None:
        chave = f"id:{atleta_id}"
    elif nome:
        chave = f"nome:{nome.lower()}"
    elif numero is not None:
        chave = f"numero:{numero}"
    else:
        return None
    rotulo = f"#{numero} {nome}" if numero is not None and nome else nome or f"#{numero}"
    return chave, rotulo


def calcular_scout(eventos: Iterable[dict[str, Any]], partida: dict[str, Any] | None = None) -> dict[str, Any]:
    linha = [dict(item) for item in eventos]
    por_equipe: dict[str, Counter] = {"A": Counter(), "B": Counter()}
    por_atleta: dict[tuple[str, str], Counter] = defaultdict(Counter)
    rotulos_atletas: dict[tuple[str, str], str] = {}
    por_set: dict[int, Counter] = defaultdict(Counter)
    placar = {"A": 0, "B": 0}
    sequencia_atual = {"equipe": None, "quantidade": 0}
    maiores_sequencias = {"A": 0, "B": 0}
    lider_anterior: str | None = None
    trocas_lideranca = 0
    empates = 0
    eventos_decisivos: list[dict[str, Any]] = []

    for evento in linha:
        categoria = _texto(evento.get("categoria")).lower()
        equipe_evento = _texto(evento.get("equipe")).upper()
        set_numero = _numero(evento.get("set_numero")) or 0

        if categoria == "substituicao" and equipe_evento in por_equipe:
            por_equipe[equipe_evento]["substituicoes"] += 1
            por_set[set_numero][f"substituicoes_{equipe_evento.lower()}"] += 1
        elif categoria == "tempo" and equipe_evento in por_equipe:
            por_equipe[equipe_evento]["tempos"] += 1
            por_set[set_numero][f"tempos_{equipe_evento.lower()}"] += 1
        elif categoria == "disciplina" and equipe_evento in por_equipe:
            por_equipe[equipe_evento]["disciplina"] += 1
            por_set[set_numero][f"disciplina_{equipe_evento.lower()}"] += 1

        if categoria != "ponto":
            continue

        pontuadora = _equipe_pontuadora(evento)
        if pontuadora not in {"A", "B"}:
            continue
        fundamento = _fundamento(evento)
        placar[pontuadora] += 1
        por_equipe[pontuadora]["pontos"] += 1
        por_equipe[pontuadora][fundamento] += 1
        por_set[set_numero][f"pontos_{pontuadora.lower()}"] += 1
        por_set[set_numero][fundamento] += 1

        atleta_info = _chave_atleta(evento)
        if atleta_info:
            chave_atleta, rotulo_atleta = atleta_info
            chave = (pontuadora, chave_atleta)
            rotulos_atletas.setdefault(chave, rotulo_atleta)
            por_atleta[chave]["pontos"] += 1
            por_atleta[chave][fundamento] += 1

        if sequencia_atual["equipe"] == pontuadora:
            sequencia_atual["quantidade"] += 1
        else:
            sequencia_atual = {"equipe": pontuadora, "quantidade": 1}
        maiores_sequencias[pontuadora] = max(maiores_sequencias[pontuadora], int(sequencia_atual["quantidade"]))

        lider = "A" if placar["A"] > placar["B"] else "B" if placar["B"] > placar["A"] else None
        if lider is None:
            empates += 1
        elif lider_anterior and lider != lider_anterior:
            trocas_lideranca += 1
        if lider:
            lider_anterior = lider

        total = placar["A"] + placar["B"]
        diferenca = abs(placar["A"] - placar["B"])
        if total >= 30 and diferenca <= 2:
            eventos_decisivos.append({
                "id": evento.get("id"),
                "set": set_numero,
                "placar": f"{placar['A']} x {placar['B']}",
                "equipe": pontuadora,
                "fundamento": fundamento,
                "descricao": evento.get("descricao"),
            })

    por_equipe_saida = {}
    for equipe in ("A", "B"):
        dados = dict(por_equipe[equipe])
        dados["maior_sequencia"] = maiores_sequencias[equipe]
        dados["pontos_positivos"] = sum(dados.get(k, 0) for k in ("ataque", "ace", "bloqueio"))
        dados["pontos_por_erro_adversario"] = sum(dados.get(k, 0) for k in ("erro", "erro_saque", "erro_ataque", "falta", "invasao", "erro_rotacao"))
        por_equipe_saida[equipe] = dados

    ranking_atletas = [
        {"equipe": equipe, "atleta": rotulos_atletas.get((equipe, chave_atleta), chave_atleta), **dict(contagem)}
        for (equipe, chave_atleta), contagem in por_atleta.items()
    ]
    ranking_atletas.sort(key=lambda item: (-int(item.get("pontos", 0)), item["atleta"]))

    insights: list[dict[str, str]] = []
    nomes = {
        "A": _texto((partida or {}).get("equipe_a")) or "Equipe A",
        "B": _texto((partida or {}).get("equipe_b")) or "Equipe B",
    }
    for equipe in ("A", "B"):
        dados = por_equipe_saida[equipe]
        positivos = int(dados.get("pontos_positivos", 0))
        erros = int(dados.get("pontos_por_erro_adversario", 0))
        if positivos or erros:
            origem = "ações próprias" if positivos >= erros else "erros do adversário"
            insights.append({"tipo": "fundamentos", "texto": f"{nomes[equipe]} obteve mais pontos por {origem}."})
        if dados.get("maior_sequencia", 0) >= 4:
            insights.append({"tipo": "sequencia", "texto": f"{nomes[equipe]} teve uma sequência máxima de {dados['maior_sequencia']} pontos."})
    if trocas_lideranca >= 3:
        insights.append({"tipo": "equilibrio", "texto": f"A partida teve {trocas_lideranca} trocas de liderança, indicando alto equilíbrio."})
    if ranking_atletas:
        melhor = ranking_atletas[0]
        insights.append({"tipo": "destaque", "texto": f"{melhor['atleta']} foi o maior pontuador identificado, com {melhor.get('pontos', 0)} pontos."})

    fundamentos_decisivos = []
    for equipe in ("A", "B"):
        itens = [(k, v) for k, v in por_equipe_saida[equipe].items() if k in {"ataque", "ace", "bloqueio", "erro", "erro_saque", "erro_ataque", "falta", "invasao", "erro_rotacao"} and v]
        itens.sort(key=lambda item: (-item[1], item[0]))
        fundamentos_decisivos.append({"equipe": equipe, "itens": [{"fundamento": k, "quantidade": v} for k, v in itens[:5]]})

    return {
        "total_eventos": len(linha),
        "placar_reconstruido": placar,
        "por_equipe": por_equipe_saida,
        "por_set": {str(k): dict(v) for k, v in sorted(por_set.items()) if k},
        "ranking_atletas": ranking_atletas,
        "fundamentos_decisivos": fundamentos_decisivos,
        "momentos_decisivos": eventos_decisivos[-20:],
        "trocas_lideranca": trocas_lideranca,
        "empates": empates,
        "insights": insights,
        "cobertura": {
            "eventos_com_equipe_pontuadora": sum(por_equipe_saida[e].get("pontos", 0) for e in ("A", "B")),
            "atletas_identificados": len(ranking_atletas),
        },
    }
