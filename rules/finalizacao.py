"""Regras puras do fluxo de finalização de set e partida."""

from __future__ import annotations

from typing import Any, Mapping


_STATUS_FINALIZADOS = {"finalizada", "finalizado", "encerrada", "encerrado"}


def texto(valor: Any) -> str:
    return str(valor or "").strip()


def inteiro(valor: Any, padrao: int = 0) -> int:
    try:
        if valor in (None, ""):
            return padrao
        return int(valor)
    except (TypeError, ValueError):
        return padrao


def normalizar_estado_final(estado: Mapping[str, Any] | None) -> dict[str, Any]:
    """Normaliza placar, sets e set atual recebidos do navegador."""
    resultado = dict(estado or {})
    resultado["pontos_a"] = inteiro(
        resultado.get("pontos_a", resultado.get("placar_a", 0)), 0
    )
    resultado["pontos_b"] = inteiro(
        resultado.get("pontos_b", resultado.get("placar_b", 0)), 0
    )
    resultado["placar_a"] = resultado["pontos_a"]
    resultado["placar_b"] = resultado["pontos_b"]
    resultado["sets_a"] = inteiro(resultado.get("sets_a"), 0)
    resultado["sets_b"] = inteiro(resultado.get("sets_b"), 0)
    resultado["set_atual"] = max(1, inteiro(resultado.get("set_atual"), 1))
    return resultado


def campos_extras_estado_final(estado: Mapping[str, Any] | None) -> dict[str, Any]:
    """Retorna parciais e metadados que não fazem parte do snapshot manual."""
    dados = dict(estado or {})
    extras: dict[str, Any] = {}
    for numero_set in range(1, 6):
        for lado in ("a", "b"):
            campo = f"set{numero_set}_{lado}"
            if dados.get(campo) not in (None, ""):
                extras[campo] = inteiro(dados.get(campo), 0)

    for campo in ("tipo_encerramento", "sets_tipo", "sets_max", "sets_para_vencer"):
        if dados.get(campo) not in (None, ""):
            extras[campo] = dados.get(campo)
    return extras


def estado_entre_sets(estado: Mapping[str, Any] | None) -> dict[str, Any]:
    resultado = dict(estado or {})
    resultado.update(
        {
            "encerrado": False,
            "partida_finalizada": False,
            "fim_jogo": False,
            "abrir_observacoes": False,
            "status_jogo": "entre_sets",
        }
    )
    return resultado


def estado_partida_finalizada(estado: Mapping[str, Any] | None) -> dict[str, Any]:
    resultado = dict(estado or {})
    resultado.update(
        {
            "encerrado": True,
            "fim_jogo": True,
            "partida_finalizada": True,
            "status_jogo": "finalizada",
            "fase_partida": "encerrado",
        }
    )
    return resultado


def partida_esta_finalizada(partida: Mapping[str, Any] | None) -> bool:
    dados = dict(partida or {})
    status = texto(dados.get("status_jogo")).lower()
    fase = texto(dados.get("fase_partida")).lower()
    tipo = texto(dados.get("tipo_encerramento")).lower()
    return status in _STATUS_FINALIZADOS or fase == "encerrado" or tipo == "wo"


def normalizar_destaque(formulario: Mapping[str, Any] | None) -> dict[str, str]:
    dados = formulario or {}
    return {
        "lado": texto(dados.get("destaque_lado")).upper(),
        "atleta_id": texto(dados.get("destaque_atleta_id")),
        "numero": texto(dados.get("destaque_numero")),
        "nome": texto(dados.get("destaque_nome")),
        "observacao": texto(dados.get("destaque_observacao")),
    }


__all__ = [
    "campos_extras_estado_final",
    "estado_entre_sets",
    "estado_partida_finalizada",
    "inteiro",
    "normalizar_destaque",
    "normalizar_estado_final",
    "partida_esta_finalizada",
    "texto",
]
