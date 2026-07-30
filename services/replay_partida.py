"""Preparação segura da linha do tempo de replay e auditoria."""
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Iterable


_CAMPOS_AUTOR = (
    "autor",
    "operador",
    "operador_nome",
    "usuario",
    "usuario_nome",
    "apontador",
    "responsavel",
)


def _texto(valor: object) -> str:
    return str(valor or "").strip()


def _inteiro(valor: object) -> int | None:
    try:
        return int(valor) if valor not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _serializar_data(valor: object) -> str | None:
    if isinstance(valor, datetime):
        return valor.isoformat(timespec="seconds")
    if isinstance(valor, date):
        return valor.isoformat()
    texto = _texto(valor)
    return texto or None


def carregar_detalhes(valor: object) -> dict[str, Any]:
    """Converte ``detalhes`` em dicionário, tolerando registros legados."""
    if isinstance(valor, dict):
        return dict(valor)
    if valor in (None, ""):
        return {}
    try:
        convertido = json.loads(str(valor))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return dict(convertido) if isinstance(convertido, dict) else {}


def _auditoria(detalhes: dict[str, Any]) -> dict[str, Any]:
    valor = detalhes.get("auditoria")
    return dict(valor) if isinstance(valor, dict) else {}


def identificar_autor(evento: dict[str, Any], detalhes: dict[str, Any]) -> str | None:
    """Localiza o responsável nos metadados novos ou em registros legados."""
    auditoria = _auditoria(detalhes)
    for campo in ("nome", "usuario"):
        valor = _texto(auditoria.get(campo))
        if valor:
            return valor
    for campo in _CAMPOS_AUTOR:
        valor = _texto(detalhes.get(campo))
        if valor:
            return valor
    for campo in _CAMPOS_AUTOR:
        valor = _texto(evento.get(campo))
        if valor:
            return valor
    return None


def classificar_evento(evento: dict[str, Any], detalhes: dict[str, Any]) -> str:
    """Retorna uma categoria estável usada pelo replay e pelos filtros."""
    candidatos = (
        evento.get("tipo_evento"),
        evento.get("tipo"),
        evento.get("fundamento"),
        detalhes.get("tipo"),
        detalhes.get("acao"),
    )
    texto = " ".join(_texto(item).lower() for item in candidatos if _texto(item))
    if "fim_set" in texto or "fim set" in texto:
        return "fim_set"
    if "fim_partida" in texto or "finalizacao" in texto or "finalização" in texto:
        return "fim_partida"
    if "substit" in texto:
        return "substituicao"
    if "tempo" in texto:
        return "tempo"
    if any(chave in texto for chave in ("cartao", "cartão", "sanc", "penal", "expuls", "desqual")):
        return "disciplina"
    if any(chave in texto for chave in ("ace", "ataque", "bloqueio", "erro", "falta", "ponto")):
        return "ponto"
    return "outro"


def descrever_evento(evento: dict[str, Any], detalhes: dict[str, Any]) -> str:
    """Monta uma descrição humana sem depender do template."""
    detalhe = _texto(evento.get("detalhe"))
    if detalhe:
        return detalhe

    categoria = classificar_evento(evento, detalhes)
    equipe = _texto(evento.get("equipe"))
    fundamento = _texto(evento.get("fundamento") or evento.get("tipo_evento") or evento.get("tipo"))
    numero = _inteiro(evento.get("numero"))
    atleta = _texto(evento.get("atleta_nome"))

    partes: list[str] = []
    if categoria == "fim_set":
        partes.append(f"Fim do set {evento.get('set_numero') or '-'}")
    elif categoria == "fim_partida":
        partes.append("Partida finalizada")
    else:
        partes.append(fundamento.replace("_", " ").title() if fundamento else "Evento")
    if equipe:
        partes.append(f"Equipe {equipe}")
    if numero is not None:
        partes.append(f"#{numero}")
    if atleta:
        partes.append(atleta)
    return " — ".join(partes)


def preparar_evento_replay(evento: dict[str, Any]) -> dict[str, Any]:
    detalhes = carregar_detalhes(evento.get("detalhes"))
    return {
        "id": int(evento.get("id") or 0),
        "partida_id": int(evento.get("partida_id") or 0),
        "set_numero": _inteiro(evento.get("set_numero")),
        "equipe": _texto(evento.get("equipe")) or None,
        "categoria": classificar_evento(evento, detalhes),
        "tipo": _texto(evento.get("tipo_evento") or evento.get("tipo")) or None,
        "fundamento": _texto(evento.get("fundamento")) or None,
        "resultado": _texto(evento.get("resultado")) or None,
        "numero": _inteiro(evento.get("numero")),
        "atleta_id": _inteiro(evento.get("atleta_id")),
        "atleta_nome": _texto(evento.get("atleta_nome")) or None,
        "descricao": descrever_evento(evento, detalhes),
        "autor": identificar_autor(evento, detalhes),
        "auditoria": {
            "usuario": _texto(_auditoria(detalhes).get("usuario")) or None,
            "nome": _texto(_auditoria(detalhes).get("nome")) or None,
            "perfil": _texto(_auditoria(detalhes).get("perfil")) or None,
            "origem": _texto(_auditoria(detalhes).get("origem")) or None,
            "endpoint": _texto(_auditoria(detalhes).get("endpoint")) or None,
            "request_id": _texto(_auditoria(detalhes).get("request_id")) or None,
            "dispositivo_fingerprint": _texto(_auditoria(detalhes).get("dispositivo_fingerprint")) or None,
        },
        "criado_em": _serializar_data(evento.get("criado_em")),
        "detalhes": detalhes,
    }


def preparar_linha_tempo(eventos: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [preparar_evento_replay(dict(evento)) for evento in eventos]


def filtrar_linha_tempo(
    eventos: Iterable[dict[str, Any]],
    *,
    categoria: str = "",
    equipe: str = "",
    autor: str = "",
    busca: str = "",
) -> list[dict[str, Any]]:
    """Filtra eventos já preparados sem acessar novamente o banco."""
    categoria_norm = _texto(categoria).lower()
    equipe_norm = _texto(equipe).lower()
    autor_norm = _texto(autor).lower()
    busca_norm = _texto(busca).lower()
    resultado: list[dict[str, Any]] = []
    for evento in eventos:
        item = dict(evento)
        if categoria_norm and _texto(item.get("categoria")).lower() != categoria_norm:
            continue
        if equipe_norm and _texto(item.get("equipe")).lower() != equipe_norm:
            continue
        if autor_norm and autor_norm not in _texto(item.get("autor")).lower():
            continue
        if busca_norm:
            palheiro = " ".join(
                _texto(item.get(campo)).lower()
                for campo in ("descricao", "tipo", "fundamento", "resultado", "atleta_nome", "autor")
            )
            if busca_norm not in palheiro:
                continue
        resultado.append(item)
    return resultado


def resumir_replay(eventos: Iterable[dict[str, Any]]) -> dict[str, Any]:
    linha = list(eventos)
    categorias: dict[str, int] = {}
    sets: dict[str, int] = {}
    equipes: dict[str, int] = {}
    autores: dict[str, int] = {}
    com_autor = 0
    for evento in linha:
        categoria = _texto(evento.get("categoria")) or "outro"
        categorias[categoria] = categorias.get(categoria, 0) + 1
        set_numero = evento.get("set_numero")
        if set_numero is not None:
            chave = str(set_numero)
            sets[chave] = sets.get(chave, 0) + 1
        equipe = _texto(evento.get("equipe"))
        if equipe:
            equipes[equipe] = equipes.get(equipe, 0) + 1
        autor = _texto(evento.get("autor"))
        if autor:
            com_autor += 1
            autores[autor] = autores.get(autor, 0) + 1
    return {
        "total": len(linha),
        "por_categoria": categorias,
        "por_set": sets,
        "por_equipe": equipes,
        "por_autor": autores,
        "eventos_com_autor": com_autor,
        "percentual_com_autor": round((com_autor / len(linha) * 100), 1) if linha else 0.0,
        "primeiro_evento_id": int(linha[0].get("id") or 0) if linha else 0,
        "ultimo_evento_id": int(linha[-1].get("id") or 0) if linha else 0,
    }
