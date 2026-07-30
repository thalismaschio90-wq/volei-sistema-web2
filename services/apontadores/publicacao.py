"""Publicação padronizada do estado operacional do apontador."""
from __future__ import annotations

from typing import Any, Callable, Mapping


Callback = Callable[..., Any]


def publicar_estado(
    *,
    partida_id: int,
    estado: Mapping[str, Any],
    atualizar_cache: Callback,
    emitir_estado: Callback,
    apontador_login: str = "",
    emitir_placar: Callback | None = None,
    origem: str = "",
) -> dict[str, Any]:
    """Atualiza a fonte viva e publica o estado, sem duplicar lógica na rota.

    Falhas de Socket.IO não desfazem uma ação já persistida. O comportamento é
    o mesmo das rotas atuais: registra o erro e devolve o estado preparado.
    """
    payload = dict(estado or {})
    login = str(apontador_login or "").strip()
    if login:
        payload["apontador"] = login

    try:
        salvo = atualizar_cache(partida_id, payload)
        if isinstance(salvo, Mapping):
            payload = dict(salvo)
        emitir_estado(partida_id, payload)
        if login and emitir_placar is not None:
            emitir_placar(login, partida_id, payload)
    except Exception as exc:
        rotulo = f" {origem}" if origem else ""
        print(f"ERRO publicar estado do apontador{rotulo}: {exc}", flush=True)

    return payload


def publicar_estado_sem_cache(
    *,
    partida_id: int,
    estado: Mapping[str, Any],
    emitir_estado: Callback,
    apontador_login: str = "",
    emitir_placar: Callback | None = None,
    origem: str = "",
) -> dict[str, Any]:
    """Publica um estado que já foi salvo pela camada `realtime`."""
    payload = dict(estado or {})
    login = str(apontador_login or "").strip()
    if login:
        payload["apontador"] = login
    try:
        emitir_estado(partida_id, payload)
        if login and emitir_placar is not None:
            emitir_placar(login, partida_id, payload)
    except Exception as exc:
        rotulo = f" {origem}" if origem else ""
        print(f"ERRO publicar estado sem cache{rotulo}: {exc}", flush=True)
    return payload


__all__ = ["publicar_estado", "publicar_estado_sem_cache"]
