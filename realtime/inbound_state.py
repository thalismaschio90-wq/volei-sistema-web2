"""Validação e aceitação atômica de estados recebidos pelo Socket.IO.

Evita que uma aba atrasada, reconexão ou snapshot antigo sobrescreva o estado
vivo mais recente da partida. A decisão combina versão otimista e progresso
esportivo, mantendo exceções explícitas para desfazer e transições de set.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True, slots=True)
class ResultadoEstadoRecebido:
    aceito: bool
    motivo: str
    estado: dict[str, Any]
    versao_atual: int
    versao_recebida: int
    snapshot_atrasado: bool = False
    conflito_versao: bool = False


def _int(valor: object, padrao: int = 0) -> int:
    try:
        if valor in (None, ""):
            return padrao
        return int(valor)
    except (TypeError, ValueError):
        return padrao


def _bool(valor: object) -> bool:
    if isinstance(valor, bool):
        return valor
    if isinstance(valor, (int, float)):
        return bool(valor)
    return str(valor or "").strip().lower() in {"1", "true", "sim", "s", "yes", "on"}


def versao_recebida(dados: dict[str, Any]) -> int:
    """Obtém a versão-base informada pelo cliente, aceitando nomes legados."""
    for chave in (
        "estado_versao_base",
        "versao_base",
        "expected_version",
        "estado_versao_esperada",
        "estado_versao",
    ):
        valor = _int(dados.get(chave), 0)
        if valor > 0:
            return valor
    return 0


def progresso_estado(estado: dict[str, Any]) -> tuple[int, int, int]:
    """Retorna uma chave comparável: sets encerrados, set atual e pontos do set."""
    return (
        _int(estado.get("sets_a"), 0) + _int(estado.get("sets_b"), 0),
        max(1, _int(estado.get("set_atual"), 1)),
        _int(estado.get("pontos_a"), 0) + _int(estado.get("pontos_b"), 0),
    )


def permite_reducao_estado(dados: dict[str, Any], atual: dict[str, Any], novo: dict[str, Any]) -> bool:
    origem = str(dados.get("origem") or novo.get("origem") or "").strip().lower()
    if "desfazer" in origem or "undo" in origem:
        return True
    if any(
        _bool(dados.get(chave)) or _bool(novo.get(chave))
        for chave in (
            "permitir_regressao_estado",
            "permitir_regressao_set",
            "transicao_set",
            "fim_set",
            "set_finalizado",
        )
    ):
        return True
    return progresso_estado(novo)[:2] > progresso_estado(atual)[:2]


def avaliar_estado_recebido(
    *,
    atual: dict[str, Any] | None,
    versao_atual: int,
    novo: dict[str, Any],
    dados_originais: dict[str, Any] | None = None,
) -> ResultadoEstadoRecebido:
    atual = dict(atual or {})
    novo = dict(novo or {})
    dados = dict(dados_originais or {})
    recebida = versao_recebida(dados or novo)
    permite_reducao = permite_reducao_estado(dados, atual, novo)

    # Controle otimista: quem leu uma versão anterior não pode sobrescrever uma
    # versão mais nova, salvo operações explicitamente autorizadas.
    if atual and recebida > 0 and recebida < int(versao_atual or 0) and not permite_reducao:
        return ResultadoEstadoRecebido(
            aceito=False,
            motivo="conflito_versao",
            estado=atual,
            versao_atual=int(versao_atual or 0),
            versao_recebida=recebida,
            conflito_versao=True,
        )

    # Mesmo clientes antigos, que ainda não enviam versão, ficam protegidos por
    # progresso esportivo para não fazer placar/set regredir após reconexão.
    if atual and progresso_estado(novo) < progresso_estado(atual) and not permite_reducao:
        return ResultadoEstadoRecebido(
            aceito=False,
            motivo="snapshot_atrasado",
            estado=atual,
            versao_atual=int(versao_atual or 0),
            versao_recebida=recebida,
            snapshot_atrasado=True,
        )

    return ResultadoEstadoRecebido(
        aceito=True,
        motivo="aceito",
        estado=novo,
        versao_atual=int(versao_atual or 0),
        versao_recebida=recebida,
    )


def aceitar_e_salvar_estado(
    *,
    store: Any,
    partida_id: object,
    novo: dict[str, Any],
    dados_originais: dict[str, Any] | None = None,
    normalizar: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> ResultadoEstadoRecebido:
    """Avalia e salva sob a trava do store quando a implementação suporta isso."""
    estado_novo = normalizar(dict(novo or {})) if normalizar else dict(novo or {})

    # A implementação local expõe uma operação atômica; stores futuros podem
    # implementar o mesmo contrato com WATCH/MULTI ou script Lua no Redis.
    if hasattr(store, "salvar_se_aceito"):
        return store.salvar_se_aceito(
            partida_id,
            estado_novo,
            lambda atual, versao: avaliar_estado_recebido(
                atual=atual,
                versao_atual=versao,
                novo=estado_novo,
                dados_originais=dados_originais,
            ),
        )

    atual = store.obter(partida_id) or {}
    versao = store.versao(partida_id)
    resultado = avaliar_estado_recebido(
        atual=atual,
        versao_atual=versao,
        novo=estado_novo,
        dados_originais=dados_originais,
    )
    if not resultado.aceito:
        return resultado
    salvo = store.salvar(partida_id, estado_novo)
    if salvo is None:
        return ResultadoEstadoRecebido(False, "falha_salvar", atual, versao, resultado.versao_recebida)
    return ResultadoEstadoRecebido(
        True,
        "aceito",
        salvo.estado,
        salvo.versao,
        resultado.versao_recebida,
    )
