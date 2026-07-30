"""Conversão de comandos validados em eventos imutáveis."""
from __future__ import annotations

from dataclasses import replace

from .contracts import ComandoJogo, EventoJogo
from .validators import validar_comando_ponto


def evento_de_comando(comando: ComandoJogo) -> EventoJogo:
    tipo = str(comando.tipo or "").strip().upper()
    if tipo != "REGISTRAR_PONTO":
        raise ValueError(f"Comando ainda não suportado pelo Game Engine: {tipo}")

    dados = validar_comando_ponto(comando.dados)
    sequencia = None if comando.versao_esperada is None else comando.versao_esperada + 1
    return EventoJogo(
        tipo="PONTO_REGISTRADO",
        partida_id=comando.partida_id,
        competicao=comando.competicao,
        dados=dados,
        comando_id=comando.comando_id,
        sequencia=sequencia,
    )


def evento_ponto_registrado(comando: ComandoJogo, sequencia: int | None = None) -> EventoJogo:
    """Fachada compatível para o primeiro contrato público do Game Engine.

    Mantém clientes e testes antigos funcionando enquanto ``evento_de_comando``
    permanece como fábrica oficial. Quando uma sequência explícita é informada,
    ela substitui a sequência derivada da versão esperada.
    """
    evento = evento_de_comando(comando)
    if sequencia is None or evento.sequencia == sequencia:
        return evento
    return replace(evento, sequencia=sequencia)


__all__ = ["evento_de_comando", "evento_ponto_registrado"]
