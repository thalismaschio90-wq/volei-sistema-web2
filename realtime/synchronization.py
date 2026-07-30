"""Entrada em salas e sincronização inicial do tempo real.

Mantém os handlers Socket.IO pequenos e garante que apontador, árbitros,
telão e visualizador usem a mesma sequência de inscrição e envio do estado.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterable

from realtime.rooms import normalizar_id_partida, sala_delta, sala_legacy


@dataclass(frozen=True)
class EntradaTempoReal:
    partida_id: str
    competicao: str = ""
    perfil: str = ""
    room_extra: str = ""
    suporta_delta: bool = False

    @property
    def valida(self) -> bool:
        return bool(self.partida_id or self.room_extra)


def normalizar_entrada(data: Any) -> EntradaTempoReal:
    dados = data if isinstance(data, dict) else {}
    return EntradaTempoReal(
        partida_id=normalizar_id_partida(dados.get("partida_id")),
        competicao=str(dados.get("competicao") or "").strip(),
        perfil=str(dados.get("perfil") or "").strip(),
        room_extra=str(dados.get("room") or dados.get("sala") or "").strip(),
        suporta_delta=str(dados.get("suporta_delta") or dados.get("supports_delta") or "").strip().lower() in {"1", "true", "sim", "yes", "on"},
    )


def salas_para_entrada(entrada: EntradaTempoReal) -> list[str]:
    """Retorna somente as salas necessárias para a conexão atual.

    O publicador ainda envia para aliases antigos por compatibilidade, porém uma
    conexão moderna não deve entrar em todos esses aliases ao mesmo tempo. Caso
    contrário, o mesmo evento é recebido repetidamente pela mesma tela.
    """
    salas: list[str] = []
    if entrada.room_extra:
        salas.append(entrada.room_extra)
    if entrada.partida_id:
        # Sala canônica da partida. Os aliases continuam existindo apenas para
        # clientes legados que os utilizam explicitamente.
        salas.append(entrada.partida_id)
        salas.append(sala_delta(entrada.partida_id) if entrada.suporta_delta else sala_legacy(entrada.partida_id))
    return list(dict.fromkeys(sala for sala in salas if sala))


def inscrever_em_salas(
    entrada: EntradaTempoReal,
    entrar_sala: Callable[[str], Any],
) -> list[str]:
    salas = salas_para_entrada(entrada)
    for sala in salas:
        entrar_sala(sala)
    return salas


def montar_confirmacao(
    entrada: EntradaTempoReal,
    *,
    room: str = "",
    arbitro: bool = False,
    extras: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": True,
        "partida_id": entrada.partida_id,
        "competicao": entrada.competicao,
        "room": room or entrada.room_extra or entrada.partida_id,
    }
    if entrada.perfil:
        payload["perfil"] = entrada.perfil
    if entrada.suporta_delta:
        payload["suporta_delta"] = True
    if arbitro:
        payload["arbitro"] = True
    if extras:
        payload.update(extras)
    return payload


def obter_estado_inicial(
    store: Any,
    partida_id: object,
    *,
    chaves_alternativas: Iterable[object] = (),
) -> dict[str, Any] | None:
    principal = normalizar_id_partida(partida_id)
    chaves = [principal, *[normalizar_id_partida(chave) for chave in chaves_alternativas]]
    for chave in dict.fromkeys(chave for chave in chaves if chave):
        estado = store.obter(chave)
        if isinstance(estado, dict) and estado:
            return estado
    return None


def emitir_para_cliente(
    socketio: Any,
    sid: str,
    eventos: Iterable[str],
    payload: dict[str, Any],
) -> None:
    for evento in dict.fromkeys(str(e or "").strip() for e in eventos if str(e or "").strip()):
        socketio.emit(evento, payload, room=sid)
