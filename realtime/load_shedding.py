"""Degradação controlada do fluxo Socket.IO sob pressão.

O objetivo é proteger eventos críticos da partida. O módulo nunca bloqueia ponto,
saque, substituição ou placar. Apenas eventos explicitamente classificados como
baixa prioridade podem ser agrupados por mais tempo ou descartados quando a
pressão atingir o nível crítico.
"""
from __future__ import annotations

import os
import threading
import time
from collections import deque
from typing import Any

MODO_NORMAL = "normal"
MODO_CONTROLADO = "controlado"
MODO_CRITICO = "critico"
_MODOS_VALIDOS = {MODO_NORMAL, MODO_CONTROLADO, MODO_CRITICO}


def _env_bool(nome: str, padrao: bool) -> bool:
    valor = str(os.environ.get(nome, "1" if padrao else "0") or "").strip().lower()
    return valor in {"1", "true", "sim", "s", "yes", "on"}


def _env_float(nome: str, padrao: float) -> float:
    try:
        return max(0.0, float(os.environ.get(nome, str(padrao)) or padrao))
    except (TypeError, ValueError):
        return padrao


def _env_int(nome: str, padrao: int) -> int:
    try:
        return max(0, int(os.environ.get(nome, str(padrao)) or padrao))
    except (TypeError, ValueError):
        return padrao


class RealtimeLoadSheddingManager:
    """Avalia pressão recente e define um modo conservador de degradação."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._eventos: deque[float] = deque()
        self._modo = MODO_NORMAL
        self._modo_desde = time.time()
        self._ultima_pressao = 0.0
        self._fila_atual = 0
        self._fila_maxima = 0
        self._transicoes = 0
        self._descartados_baixa = 0
        self._agrupamentos_estendidos = 0

    @property
    def habilitado(self) -> bool:
        return _env_bool("SOCKET_DEGRADATION_ENABLED", True)

    def limpar(self) -> None:
        with self._lock:
            self._eventos.clear()
            self._modo = MODO_NORMAL
            self._modo_desde = time.time()
            self._ultima_pressao = 0.0
            self._fila_atual = 0
            self._fila_maxima = 0
            self._transicoes = 0
            self._descartados_baixa = 0
            self._agrupamentos_estendidos = 0

    def observar_evento(self, *, tamanho_fila: int = 0) -> str:
        agora_mono = time.monotonic()
        janela = max(1.0, _env_float("SOCKET_DEGRADATION_WINDOW_SECONDS", 5.0))
        with self._lock:
            self._eventos.append(agora_mono)
            limite = agora_mono - janela
            while self._eventos and self._eventos[0] < limite:
                self._eventos.popleft()
            self._fila_atual = max(0, int(tamanho_fila or 0))
            self._fila_maxima = max(self._fila_maxima, self._fila_atual)
            taxa = len(self._eventos) / janela
            self._ultima_pressao = taxa
            self._recalcular_locked(taxa, self._fila_atual)
            return self._modo

    def atualizar_fila(self, tamanho_fila: int) -> str:
        with self._lock:
            self._fila_atual = max(0, int(tamanho_fila or 0))
            self._fila_maxima = max(self._fila_maxima, self._fila_atual)
            self._recalcular_locked(self._ultima_pressao, self._fila_atual)
            return self._modo

    def _recalcular_locked(self, taxa: float, fila: int) -> None:
        forcar = str(os.environ.get("SOCKET_DEGRADATION_FORCE_MODE", "") or "").strip().lower()
        if forcar in _MODOS_VALIDOS:
            novo = forcar
        elif not self.habilitado:
            novo = MODO_NORMAL
        else:
            taxa_controlado = _env_float("SOCKET_DEGRADATION_CONTROLLED_EVENTS_PER_SEC", 120.0)
            taxa_critico = _env_float("SOCKET_DEGRADATION_CRITICAL_EVENTS_PER_SEC", 300.0)
            fila_controlado = _env_int("SOCKET_DEGRADATION_CONTROLLED_QUEUE", 25)
            fila_critico = _env_int("SOCKET_DEGRADATION_CRITICAL_QUEUE", 100)

            if taxa >= taxa_critico or fila >= fila_critico:
                novo = MODO_CRITICO
            elif taxa >= taxa_controlado or fila >= fila_controlado:
                novo = MODO_CONTROLADO
            else:
                novo = MODO_NORMAL

            # Evita oscilar imediatamente para um modo inferior.
            if self._modo != MODO_NORMAL and novo == MODO_NORMAL:
                cooldown = _env_float("SOCKET_DEGRADATION_COOLDOWN_SECONDS", 10.0)
                if time.time() - self._modo_desde < cooldown:
                    novo = self._modo

        if novo != self._modo:
            self._modo = novo
            self._modo_desde = time.time()
            self._transicoes += 1

    def modo_atual(self) -> str:
        with self._lock:
            return self._modo

    def deve_descartar_baixa(self) -> bool:
        if not _env_bool("SOCKET_DEGRADATION_DROP_LOW_ON_CRITICAL", True):
            return False
        with self._lock:
            return self.habilitado and self._modo == MODO_CRITICO

    def atraso_baixa_ms(self, base_ms: float) -> float:
        base = max(0.0, float(base_ms or 0.0))
        with self._lock:
            modo = self._modo
        if modo == MODO_CONTROLADO:
            fator = max(1.0, _env_float("SOCKET_DEGRADATION_CONTROLLED_BATCH_FACTOR", 2.0))
            self._registrar_agrupamento_estendido()
            return base * fator
        if modo == MODO_CRITICO:
            fator = max(1.0, _env_float("SOCKET_DEGRADATION_CRITICAL_BATCH_FACTOR", 5.0))
            self._registrar_agrupamento_estendido()
            return base * fator
        return base

    def registrar_descarte_baixa(self) -> None:
        with self._lock:
            self._descartados_baixa += 1

    def _registrar_agrupamento_estendido(self) -> None:
        with self._lock:
            self._agrupamentos_estendidos += 1

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "habilitado": self.habilitado,
                "modo": self._modo,
                "modo_desde_epoch": self._modo_desde,
                "eventos_por_segundo_estimados": round(self._ultima_pressao, 2),
                "fila_atual": self._fila_atual,
                "fila_maxima": self._fila_maxima,
                "transicoes": self._transicoes,
                "eventos_baixa_descartados": self._descartados_baixa,
                "agrupamentos_estendidos": self._agrupamentos_estendidos,
                "eventos_criticos_protegidos": True,
            }


load_shedding_manager = RealtimeLoadSheddingManager()
