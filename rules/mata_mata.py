"""Regras puras para montagem automática do mata-mata.

Não acessa Flask, sessão, banco ou Socket.IO. Recebe classificação e partidas
já preparadas e devolve confrontos previsíveis.
"""


def texto(valor):
    return str(valor or "").strip()


def origem_partida_avanco(partida):
    origem = texto((partida or {}).get("origem"))
    if not origem.startswith("avanco:"):
        return "", ""
    partes = origem.split(":", 2)
    if len(partes) >= 3:
        return partes[1].strip().lower(), partes[2].strip()
    return "", ""


def filtrar_serie(partidas, serie):
    serie = texto(serie).lower()
    if not serie:
        return list(partidas or [])
    filtradas = []
    for partida in partidas or []:
        serie_partida, _ = origem_partida_avanco(partida)
        if serie_partida == serie:
            filtradas.append(partida)
    return filtradas


def ordenar_por_ordem(partidas):
    return sorted(partidas or [], key=lambda p: (p.get("ordem") or 0, p.get("id") or 0))


def classificados_intercalados(classificacao):
    """Intercala posições dos grupos: 1ºA, 1ºB, 2ºA, 2ºB..."""
    resultado = []
    maior = max((len(linhas) for linhas in (classificacao or {}).values()), default=0)
    for posicao in range(maior):
        for nome_grupo in sorted((classificacao or {}).keys()):
            linhas = classificacao.get(nome_grupo) or []
            if posicao < len(linhas):
                equipe = texto(linhas[posicao].get("equipe"))
                if equipe:
                    resultado.append(equipe)
    return resultado


def perdedor_partida(partida, vencedor, placeholder):
    vencedor = texto(vencedor)
    equipe_a = texto((partida or {}).get("equipe_a"))
    equipe_b = texto((partida or {}).get("equipe_b"))
    if vencedor and vencedor == equipe_a:
        return equipe_b or placeholder
    if vencedor and vencedor == equipe_b:
        return equipe_a or placeholder
    return placeholder


def montar_confrontos_mata_mata(
    fase,
    *,
    classificacao=None,
    quartas=None,
    semifinais=None,
    resolver_vencedor=None,
):
    """Monta confrontos de quartas, semifinal, final ou terceiro lugar.

    Retorno: {"ok": bool, "confrontos": [...], "mensagem": str}
    """
    fase = texto(fase).lower()
    resolver_vencedor = resolver_vencedor or (lambda partida, placeholder: placeholder)

    if fase == "quartas":
        classificados = classificados_intercalados(classificacao)
        if len(classificados) < 8:
            return {"ok": False, "confrontos": [], "mensagem": "Para gerar quartas automaticamente, precisa ter pelo menos 8 equipes classificadas."}
        top8 = classificados[:8]
        return {"ok": True, "confrontos": [(top8[0], top8[7]), (top8[3], top8[4]), (top8[1], top8[6]), (top8[2], top8[5])], "mensagem": ""}

    if fase == "semifinal":
        quartas = ordenar_por_ordem(quartas)
        if len(quartas) >= 4:
            return {
                "ok": True,
                "confrontos": [
                    (resolver_vencedor(quartas[0], "Vencedor Quartas 1"), resolver_vencedor(quartas[1], "Vencedor Quartas 2")),
                    (resolver_vencedor(quartas[2], "Vencedor Quartas 3"), resolver_vencedor(quartas[3], "Vencedor Quartas 4")),
                ],
                "mensagem": "",
            }
        classificados = classificados_intercalados(classificacao)
        if len(classificados) < 4:
            return {"ok": False, "confrontos": [], "mensagem": "Para gerar semifinais automaticamente, precisa ter quartas criadas ou pelo menos 4 equipes classificadas."}
        top4 = classificados[:4]
        return {"ok": True, "confrontos": [(top4[0], top4[3]), (top4[1], top4[2])], "mensagem": ""}

    semifinais = ordenar_por_ordem(semifinais)
    if fase in {"final", "terceiro_lugar"} and len(semifinais) < 2:
        nome = "a final" if fase == "final" else "3º lugar"
        return {"ok": False, "confrontos": [], "mensagem": f"Para gerar {nome} automaticamente, crie as duas semifinais primeiro."}

    if fase == "final":
        return {
            "ok": True,
            "confrontos": [(resolver_vencedor(semifinais[0], "Vencedor Semifinal 1"), resolver_vencedor(semifinais[1], "Vencedor Semifinal 2"))],
            "mensagem": "",
        }

    if fase == "terceiro_lugar":
        vencedor_1 = resolver_vencedor(semifinais[0], "")
        vencedor_2 = resolver_vencedor(semifinais[1], "")
        return {
            "ok": True,
            "confrontos": [(
                perdedor_partida(semifinais[0], vencedor_1, "Perdedor Semifinal 1"),
                perdedor_partida(semifinais[1], vencedor_2, "Perdedor Semifinal 2"),
            )],
            "mensagem": "",
        }

    return {"ok": False, "confrontos": [], "mensagem": "Não foi possível montar confrontos automáticos para esta fase."}
