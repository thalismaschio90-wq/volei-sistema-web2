"""Regras puras do domínio de grupos da competição."""


def normalizar_nome_grupo(valor, *, limite=30):
    nome = " ".join(str(valor or "").strip().split())
    return nome[:limite]


def normalizar_nome_equipe(valor):
    return " ".join(str(valor or "").strip().split())


def dados_grupo_validos(nome, competicao):
    return bool(normalizar_nome_grupo(nome) and str(competicao or "").strip())


def vinculo_grupo_valido(grupo_id, equipe, competicao):
    try:
        grupo_id = int(grupo_id)
    except (TypeError, ValueError):
        return False
    return grupo_id > 0 and bool(normalizar_nome_equipe(equipe)) and bool(str(competicao or "").strip())
