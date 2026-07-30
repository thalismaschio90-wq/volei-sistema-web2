"""Serviços de integração segura do Game Engine experimental."""
from __future__ import annotations

from typing import Any, Mapping

from .contracts import ComandoJogo, ResultadoSombra
from .events import evento_de_comando
from .reducer import aplicar_evento
from .validators import inteiro


_CHAVES_COMPARACAO = (
    "pontos_a",
    "pontos_b",
    "placar_a",
    "placar_b",
    "saque_atual",
)


def _versao(estado: Mapping[str, Any]) -> int | None:
    for chave in ("estado_versao", "versao", "version"):
        if estado.get(chave) is not None:
            return inteiro(estado.get(chave))
    return None


def comparar_ponto_em_modo_sombra(
    *,
    partida_id: int,
    competicao: str,
    comando_ponto: Mapping[str, Any],
    estado_anterior: Mapping[str, Any] | None,
    estado_oficial: Mapping[str, Any] | None,
) -> ResultadoSombra:
    """Calcula o ponto no motor novo e compara com o estado oficial.

    Não possui efeitos colaterais. Se não houver snapshot anterior confiável,
    simplesmente informa que a comparação não foi executada.
    """
    if not estado_anterior:
        return ResultadoSombra(False, {}, motivo="estado_anterior_indisponivel")
    if not estado_oficial:
        return ResultadoSombra(False, {}, motivo="estado_oficial_indisponivel")

    comando = ComandoJogo(
        tipo="REGISTRAR_PONTO",
        partida_id=int(partida_id),
        competicao=str(competicao or ""),
        dados=dict(comando_ponto or {}),
        versao_esperada=_versao(estado_anterior),
    )
    evento = evento_de_comando(comando)
    previsto = aplicar_evento(estado_anterior, evento)

    divergencias: dict[str, dict[str, Any]] = {}
    for chave in _CHAVES_COMPARACAO:
        if chave not in estado_oficial:
            continue
        esperado = previsto.get(chave)
        recebido = estado_oficial.get(chave)
        if str(esperado) != str(recebido):
            divergencias[chave] = {"previsto": esperado, "oficial": recebido}

    return ResultadoSombra(True, divergencias, estado_previsto=previsto)
