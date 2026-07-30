"""Serviço de coordenação do fluxo de finalização do apontador."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from rules.finalizacao import (
    campos_extras_estado_final,
    estado_entre_sets,
    estado_partida_finalizada,
    normalizar_destaque,
    normalizar_estado_final,
    partida_esta_finalizada,
    texto,
)


def separar_eventos_pendentes(
    eventos: Sequence[Any] | None,
    ids_sincronizados: set[str] | Sequence[str] | None,
) -> tuple[list[Any], list[str]]:
    """Separa eventos ainda não persistidos e retorna todos os IDs locais válidos."""
    sincronizados: set[str] = set()
    for item in ids_sincronizados or ():
        identificador = texto(item)
        if identificador:
            sincronizados.add(identificador)

    pendentes: list[Any] = []
    ids: list[str] = []
    for item in eventos or ():
        if not isinstance(item, Mapping):
            pendentes.append(item)
            continue

        identificador = texto(item.get("id_local"))
        if identificador:
            ids.append(identificador)
        if not identificador or identificador not in sincronizados:
            pendentes.append(item)

    return pendentes, ids


def eventos_processados_com_sucesso(
    eventos: Sequence[Any] | None,
    resultados: Sequence[Any] | None,
) -> list[dict[str, Any]]:
    confirmados: list[dict[str, Any]] = []
    for item, resultado in zip(eventos or (), resultados or ()):
        if (
            isinstance(item, Mapping)
            and isinstance(resultado, Mapping)
            and resultado.get("ok")
        ):
            confirmados.append(dict(item))
    return confirmados


def preparar_estado_cliente(estado: Mapping[str, Any] | None) -> tuple[dict[str, Any], dict[str, Any]]:
    normalizado = normalizar_estado_final(estado)
    return normalizado, campos_extras_estado_final(normalizado)


def confirmar_sets(estado_esperado: Mapping[str, Any], estado_confirmado: Mapping[str, Any]) -> None:
    esperado = normalizar_estado_final(estado_esperado)
    confirmado = normalizar_estado_final(estado_confirmado)
    if confirmado["sets_a"] != esperado["sets_a"] or confirmado["sets_b"] != esperado["sets_b"]:
        raise RuntimeError(
            "O estado final não foi confirmado no banco "
            f"(esperado {esperado['sets_a']} x {esperado['sets_b']}, "
            f"confirmado {confirmado['sets_a']} x {confirmado['sets_b']})."
        )


def resposta_entre_sets(
    estado: Mapping[str, Any] | None,
    url_papeleta: str,
    eventos_processados: Sequence[Any] | None = None,
) -> dict[str, Any]:
    atualizado = estado_entre_sets(estado)
    return {
        "ok": True,
        "mensagem": "Set finalizado. Prepare a papeleta do próximo set.",
        "encerrado": False,
        "partida_finalizada": False,
        "fim_jogo": False,
        "abrir_observacoes": False,
        "redirecionar_papeleta": True,
        "url_redirecionamento": url_papeleta,
        "estado": atualizado,
        "eventos_processados": list(eventos_processados or []),
        **atualizado,
    }


def resposta_partida_finalizada(
    estado: Mapping[str, Any] | None,
    url_observacoes: str,
    eventos_processados: Sequence[Any] | None = None,
    pendencia_destaques: Mapping[str, Any] | None = None,
    url_destaques: str = "",
) -> dict[str, Any]:
    atualizado = estado_partida_finalizada(estado)
    pendencia = dict(pendencia_destaques or {})
    return {
        "ok": True,
        "mensagem": "Partida salva no banco e encerrada com sucesso.",
        "encerrado": True,
        "estado": atualizado,
        "partida_finalizada": True,
        "abrir_observacoes": True,
        "url_observacoes": url_observacoes,
        "abrir_destaques_competicao": bool(pendencia.get("abrir")),
        "url_destaques_competicao": url_destaques if pendencia.get("abrir") else "",
        "eventos_processados": list(eventos_processados or []),
        **atualizado,
    }


def contexto_observacoes(
    dados_finalizacao: Mapping[str, Any] | None,
    partida_fallback: Mapping[str, Any] | None,
    destaques_config: Mapping[str, Any] | None,
    competicao: str,
) -> dict[str, Any]:
    dados = dict(dados_finalizacao or {})
    partida = dict(dados.get("partida") or partida_fallback or {})
    return {
        "finalizada": partida_esta_finalizada(partida),
        "template": {
            "partida": partida,
            "competicao_nome": competicao,
            "dados_finalizacao": dados,
            "equipes_finalizacao": list(dados.get("equipes") or []),
            "destaques_partida": list(dados.get("destaques_partida") or []),
            "destaques_config": dict(destaques_config or {}),
        },
    }


def preparar_formulario_finalizacao(formulario: Mapping[str, Any] | None) -> tuple[str, dict[str, str]]:
    dados = formulario or {}
    return texto(dados.get("observacoes")), normalizar_destaque(dados)


__all__ = [
    "confirmar_sets",
    "contexto_observacoes",
    "eventos_processados_com_sucesso",
    "preparar_estado_cliente",
    "preparar_formulario_finalizacao",
    "resposta_entre_sets",
    "resposta_partida_finalizada",
    "separar_eventos_pendentes",
    "estado_partida_finalizada",
]
