"""Regras puras para estrutura, sincronização e sorteio dos grupos."""


def to_bool(valor):
    if isinstance(valor, bool):
        return valor
    if valor is None:
        return False
    return str(valor).strip().lower() in {"1", "true", "sim", "s", "yes", "on"}


def estrutura_grupo_unico(competicao):
    try:
        qtd = int((competicao or {}).get("qtd_grupos") or 0)
    except (TypeError, ValueError):
        qtd = 0
    return qtd <= 1 or not to_bool((competicao or {}).get("tem_grupos"))


def nomes_grupos_automaticos(qtd):
    try:
        qtd = int(qtd or 1)
    except (TypeError, ValueError):
        qtd = 1
    qtd = max(1, min(qtd, 26))
    return [chr(ord("A") + i) for i in range(qtd)]


def qtd_grupos_configurada(competicao):
    try:
        qtd = int((competicao or {}).get("qtd_grupos") or 0)
    except (TypeError, ValueError):
        qtd = 0
    if estrutura_grupo_unico(competicao):
        return 1
    return max(2, min(qtd or 2, 26))


def nome_grupo_normalizado(valor):
    return str(valor or "").strip().upper()


def nomes_equipes_unicos(equipes):
    resultado = []
    vistos = set()
    for equipe in equipes or []:
        nome = str((equipe or {}).get("nome") or (equipe or {}).get("equipe") or "").strip()
        chave = nome.casefold()
        if nome and chave not in vistos:
            resultado.append(nome)
            vistos.add(chave)
    return resultado


def selecionar_grupos_estrutura(grupos, qtd):
    nomes_alvo = set(nomes_grupos_automaticos(qtd))
    selecionados = [
        grupo for grupo in (grupos or [])
        if nome_grupo_normalizado((grupo or {}).get("nome")) in nomes_alvo
    ]
    return sorted(selecionados, key=lambda g: nome_grupo_normalizado((g or {}).get("nome")))


def distribuir_equipes_balanceado(nomes_equipes, grupos):
    grupos = list(grupos or [])
    if not grupos:
        return []
    return [
        {
            "grupo_id": grupos[indice % len(grupos)].get("id"),
            "grupo_nome": nome_grupo_normalizado(grupos[indice % len(grupos)].get("nome")),
            "equipe": equipe,
        }
        for indice, equipe in enumerate(nomes_equipes or [])
    ]


def resumo_distribuicao(distribuicao, grupos):
    tamanhos = {nome_grupo_normalizado(g.get("nome")): 0 for g in (grupos or [])}
    for item in distribuicao or []:
        nome = nome_grupo_normalizado(item.get("grupo_nome"))
        tamanhos[nome] = tamanhos.get(nome, 0) + 1
    return ", ".join(f"{grupo}: {quantidade}" for grupo, quantidade in tamanhos.items())
