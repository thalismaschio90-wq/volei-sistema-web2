"""Publicação padronizada para todas as salas de uma partida."""
from __future__ import annotations

from typing import Any, Callable

from realtime.event_priority import event_dispatcher
from realtime.rooms import salas_partida


def publicar_nas_salas(
    socketio: Any,
    evento: str,
    payload: dict[str, Any],
    partida_id: object,
    *,
    competicao: object = None,
    normalizar: Callable[[Any], Any] | None = None,
    prioridade: str | None = None,
    deduplicar_ms: float = 0.0,
    **kwargs: Any,
) -> None:
    dados = normalizar(payload) if normalizar else payload
    comp = competicao
    if comp is None and isinstance(dados, dict):
        comp = dados.get("competicao")
    # Algumas combinações de ID/competição podem produzir a mesma sala por
    # aliases de compatibilidade. Preserva a ordem e elimina duplicatas antes
    # de serializar/enfileirar o evento.
    salas = tuple(dict.fromkeys(salas_partida(partida_id, comp)))
    for sala in salas:
        event_dispatcher.publicar(
            socketio,
            evento,
            dados,
            sala=sala,
            prioridade=prioridade,
            deduplicar_ms=deduplicar_ms,
            **kwargs,
        )
