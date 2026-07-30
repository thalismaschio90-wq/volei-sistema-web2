"""Regras puras para rodadas programadas de competições."""


def texto(valor):
    return str(valor or "").strip()


def normalizar_numero_rodada(valor):
    try:
        numero = int(valor or 1)
    except (TypeError, ValueError):
        numero = 1
    return max(1, numero)


def normalizar_tipo_fase(valor):
    tipo = texto(valor or "classificatoria").lower()
    return tipo if tipo in {"classificatoria", "avanco"} else "classificatoria"


def combinar_data_hora(data, hora):
    data = texto(data)
    hora = texto(hora)
    if not data:
        return ""
    return f"{data}T{hora}" if hora else data


def normalizar_rodada(competicao, rodada):
    if not isinstance(rodada, dict):
        return None
    competicao = texto(competicao)
    if not competicao:
        return None
    numero = normalizar_numero_rodada(rodada.get("numero_rodada") or rodada.get("numero"))
    tipo_fase = normalizar_tipo_fase(rodada.get("tipo_fase"))
    fase_padrao = "grupos" if tipo_fase == "classificatoria" else "avanco"
    fase = texto(rodada.get("fase") or fase_padrao).lower()
    serie = texto(rodada.get("serie")).lower()
    nome_padrao = f"Rodada {numero}" if tipo_fase == "classificatoria" else fase.title()
    nome = texto(rodada.get("nome") or nome_padrao)
    data = texto(rodada.get("data"))
    hora = texto(rodada.get("hora"))
    return (
        competicao, tipo_fase, fase, serie, numero, nome, data, hora,
        combinar_data_hora(data, hora), bool(rodada.get("ativo", True)),
    )


def chave_rodada(rodada):
    return (
        texto(rodada.get("tipo_fase")).lower(),
        texto(rodada.get("fase")).lower(),
        texto(rodada.get("serie")).lower(),
        normalizar_numero_rodada(rodada.get("numero_rodada")),
    )
