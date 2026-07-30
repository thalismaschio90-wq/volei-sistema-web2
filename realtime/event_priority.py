"""Priorização, deduplicação e agrupamento seguro de eventos Socket.IO.

Eventos críticos são sempre emitidos imediatamente. Eventos auxiliares podem ser
suprimidos quando são duplicatas exatas em uma janela curta. Eventos de baixa
prioridade podem ser agrupados pelo par ``evento + sala`` e apenas a versão mais
recente é transmitida.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any

from realtime.load_shedding import load_shedding_manager


def _env_bool(nome: str, padrao: bool) -> bool:
    valor = str(os.environ.get(nome, "1" if padrao else "0") or "").strip().lower()
    return valor in {"1", "true", "sim", "s", "yes", "on"}


def _env_float(nome: str, padrao: float) -> float:
    try:
        return max(0.0, float(os.environ.get(nome, str(padrao)) or padrao))
    except (TypeError, ValueError):
        return padrao


PRIORIDADE_CRITICA = "critica"
PRIORIDADE_NORMAL = "normal"
PRIORIDADE_BAIXA = "baixa"

_EVENTOS_CRITICOS = {
    "estado_partida_delta",
    "placar_rapido",
    "placar_apontador_atualizado",
    "placar_geral_atualizado",
    "saque_arbitros",
    "estado_partida_local_ok",
    "estado_avulso_local_ok",
    "resposta_solicitacao",
}

_EVENTOS_BAIXOS = {
    "telemetria_realtime",
    "metricas_realtime",
    "performance_realtime",
    "diagnostico_realtime",
}


def classificar_evento(evento: object) -> str:
    nome = str(evento or "").strip().lower()
    if nome in _EVENTOS_CRITICOS:
        return PRIORIDADE_CRITICA
    if nome in _EVENTOS_BAIXOS:
        return PRIORIDADE_BAIXA
    return PRIORIDADE_NORMAL


def _serializar_payload(payload: Any) -> bytes:
    try:
        bruto = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":"))
    except Exception:
        bruto = repr(payload)
    return bruto.encode("utf-8", errors="replace")


def _tamanho_payload(payload: Any) -> int:
    return len(_serializar_payload(payload))


def _fingerprint_payload(payload: Any) -> str:
    return hashlib.sha1(_serializar_payload(payload)).hexdigest()[:16]


@dataclass
class EventoPendente:
    socketio: Any
    evento: str
    payload: Any
    sala: str
    kwargs: dict[str, Any]
    criado_em: float
    bytes_payload: int


class RealtimeDispatchMetrics:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._dados: dict[str, float] = {}
        self._eventos: dict[str, dict[str, float]] = {}

    def limpar(self) -> None:
        with self._lock:
            self._dados.clear()
            self._eventos.clear()

    def registrar(
        self,
        *,
        prioridade: str,
        evento: str = "",
        emitido: bool = False,
        recebido: bool = True,
        enfileirado: bool = False,
        agrupado: bool = False,
        duplicado: bool = False,
        espera_ms: float = 0.0,
        tamanho_fila: int = 0,
        bytes_payload: int = 0,
        bytes_economizados: int = 0,
    ) -> None:
        with self._lock:
            prefixo = str(prioridade or PRIORIDADE_NORMAL)
            tamanho = max(0, int(bytes_payload or 0))
            economia = max(0, int(bytes_economizados or 0))
            if recebido:
                self._dados[f"recebidos_{prefixo}"] = self._dados.get(f"recebidos_{prefixo}", 0) + 1
                self._dados["bytes_recebidos_estimados"] = self._dados.get("bytes_recebidos_estimados", 0) + tamanho
            if emitido:
                self._dados[f"emitidos_{prefixo}"] = self._dados.get(f"emitidos_{prefixo}", 0) + 1
                self._dados["bytes_emitidos_estimados"] = self._dados.get("bytes_emitidos_estimados", 0) + tamanho
                self._dados["espera_total_ms"] = self._dados.get("espera_total_ms", 0.0) + max(0.0, espera_ms)
                self._dados["emissoes_com_espera"] = self._dados.get("emissoes_com_espera", 0) + 1
                self._dados["espera_max_ms"] = max(self._dados.get("espera_max_ms", 0.0), max(0.0, espera_ms))
            if enfileirado:
                self._dados["enfileirados"] = self._dados.get("enfileirados", 0) + 1
            if agrupado:
                self._dados["agrupados"] = self._dados.get("agrupados", 0) + 1
            if duplicado:
                self._dados["duplicados_descartados"] = self._dados.get("duplicados_descartados", 0) + 1
            if economia:
                self._dados["bytes_economizados_despacho"] = self._dados.get("bytes_economizados_despacho", 0) + economia
            self._dados["fila_maxima"] = max(self._dados.get("fila_maxima", 0), max(0, tamanho_fila))

            nome = str(evento or "").strip()[:120]
            if nome:
                item = self._eventos.setdefault(nome, {"recebidos": 0, "emitidos": 0, "bytes": 0, "economizados": 0})
                if recebido:
                    item["recebidos"] += 1
                if emitido:
                    item["emitidos"] += 1
                    item["bytes"] += tamanho
                item["economizados"] += economia

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            dados = dict(self._dados)
            eventos = {nome: dict(item) for nome, item in self._eventos.items()}
        emissoes = int(dados.get("emissoes_com_espera", 0) or 0)
        dados["espera_media_ms"] = round(float(dados.get("espera_total_ms", 0.0) or 0.0) / emissoes, 3) if emissoes else 0.0
        dados["espera_max_ms"] = round(float(dados.get("espera_max_ms", 0.0) or 0.0), 3)
        recebidos = int(dados.get("bytes_recebidos_estimados", 0) or 0)
        economizados = int(dados.get("bytes_economizados_despacho", 0) or 0)
        dados["economia_despacho_percentual"] = round(100.0 * economizados / recebidos, 2) if recebidos else 0.0
        dados["eventos_por_trafego"] = sorted(
            ({"evento": nome, **item} for nome, item in eventos.items()),
            key=lambda item: (-int(item.get("bytes", 0)), str(item.get("evento", ""))),
        )[:20]
        dados.pop("espera_total_ms", None)
        dados.pop("emissoes_com_espera", None)
        return dados


dispatch_metrics_store = RealtimeDispatchMetrics()


class RealtimeEventDispatcher:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._pendentes: dict[tuple[str, str], EventoPendente] = {}
        self._ultimo_payload: dict[tuple[str, str], tuple[str, float]] = {}
        self._flush_agendado = False

    @property
    def habilitado(self) -> bool:
        return _env_bool("SOCKET_PRIORITY_ENABLED", True)

    def publicar(
        self,
        socketio: Any,
        evento: str,
        payload: Any,
        *,
        sala: object,
        prioridade: str | None = None,
        deduplicar_ms: float = 0.0,
        **kwargs: Any,
    ) -> bool:
        sala_norm = str(sala or "").strip()
        if not sala_norm:
            return False
        prioridade_norm = prioridade or classificar_evento(evento)
        bytes_payload = _tamanho_payload(payload)
        load_shedding_manager.observar_evento(tamanho_fila=len(self._pendentes))
        if not self.habilitado:
            socketio.emit(evento, payload, room=sala_norm, **kwargs)
            dispatch_metrics_store.registrar(prioridade=prioridade_norm, evento=evento, emitido=True, bytes_payload=bytes_payload)
            return True

        if deduplicar_ms > 0 and self._eh_duplicado(evento, sala_norm, payload, deduplicar_ms):
            dispatch_metrics_store.registrar(
                prioridade=prioridade_norm, evento=evento, duplicado=True,
                bytes_payload=bytes_payload, bytes_economizados=bytes_payload,
            )
            return False

        if prioridade_norm != PRIORIDADE_BAIXA:
            socketio.emit(evento, payload, room=sala_norm, **kwargs)
            dispatch_metrics_store.registrar(
                prioridade=prioridade_norm, evento=evento, emitido=True, bytes_payload=bytes_payload
            )
            return True

        if load_shedding_manager.deve_descartar_baixa():
            load_shedding_manager.registrar_descarte_baixa()
            dispatch_metrics_store.registrar(
                prioridade=PRIORIDADE_BAIXA, evento=evento, duplicado=True,
                bytes_payload=bytes_payload, bytes_economizados=bytes_payload,
            )
            return False

        self._enfileirar_baixa(socketio, evento, payload, sala_norm, kwargs)
        return True

    def _eh_duplicado(self, evento: str, sala: str, payload: Any, janela_ms: float) -> bool:
        chave = (str(evento), sala)
        agora = time.monotonic()
        fingerprint = _fingerprint_payload(payload)
        with self._lock:
            anterior = self._ultimo_payload.get(chave)
            self._ultimo_payload[chave] = (fingerprint, agora)
            if not anterior:
                return False
            fp_anterior, instante = anterior
            return fp_anterior == fingerprint and (agora - instante) * 1000.0 <= janela_ms

    def _enfileirar_baixa(self, socketio: Any, evento: str, payload: Any, sala: str, kwargs: dict[str, Any]) -> None:
        chave = (str(evento), sala)
        bytes_payload = _tamanho_payload(payload)
        pendente = EventoPendente(socketio, str(evento), payload, sala, dict(kwargs), time.monotonic(), bytes_payload)
        with self._lock:
            anterior = self._pendentes.get(chave)
            agrupado = anterior is not None
            self._pendentes[chave] = pendente
            tamanho = len(self._pendentes)
            precisa_agendar = not self._flush_agendado
            if precisa_agendar:
                self._flush_agendado = True
        load_shedding_manager.atualizar_fila(tamanho)
        dispatch_metrics_store.registrar(
            prioridade=PRIORIDADE_BAIXA,
            enfileirado=True,
            agrupado=agrupado,
            tamanho_fila=tamanho,
            evento=evento,
            bytes_payload=bytes_payload,
            bytes_economizados=(anterior.bytes_payload if anterior is not None else 0),
        )
        if precisa_agendar:
            self._agendar_flush(socketio)

    def _agendar_flush(self, socketio: Any) -> None:
        if hasattr(socketio, "start_background_task") and hasattr(socketio, "sleep"):
            socketio.start_background_task(self._flush_background, socketio)
            return
        atraso_ms = load_shedding_manager.atraso_baixa_ms(
            _env_float("SOCKET_LOW_PRIORITY_BATCH_MS", 100.0)
        )
        atraso = atraso_ms / 1000.0
        timer = threading.Timer(atraso, self.flush)
        timer.daemon = True
        timer.start()

    def _flush_background(self, socketio: Any) -> None:
        atraso_ms = load_shedding_manager.atraso_baixa_ms(
            _env_float("SOCKET_LOW_PRIORITY_BATCH_MS", 100.0)
        )
        socketio.sleep(atraso_ms / 1000.0)
        self.flush()

    def flush(self) -> int:
        with self._lock:
            eventos = list(self._pendentes.values())
            self._pendentes.clear()
            self._flush_agendado = False
        load_shedding_manager.atualizar_fila(0)
        agora = time.monotonic()
        for item in eventos:
            item.socketio.emit(item.evento, item.payload, room=item.sala, **item.kwargs)
            dispatch_metrics_store.registrar(
                prioridade=PRIORIDADE_BAIXA,
                evento=item.evento,
                emitido=True,
                recebido=False,
                espera_ms=(agora - item.criado_em) * 1000.0,
                bytes_payload=item.bytes_payload,
            )
        return len(eventos)


event_dispatcher = RealtimeEventDispatcher()
