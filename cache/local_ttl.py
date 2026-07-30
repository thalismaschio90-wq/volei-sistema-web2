"""Cache TTL pequeno e thread-safe para dados não críticos."""
from __future__ import annotations

import time
from threading import RLock
from typing import Any


class CacheTTL:
    def __init__(self) -> None:
        self._dados: dict[str, tuple[float, Any]] = {}
        self._lock = RLock()

    def obter(self, chave: str, padrao: Any = None) -> Any:
        agora = time.monotonic()
        with self._lock:
            item = self._dados.get(chave)
            if item is None:
                return padrao
            expira_em, valor = item
            if expira_em <= agora:
                self._dados.pop(chave, None)
                return padrao
            return valor

    def definir(self, chave: str, valor: Any, ttl_segundos: float) -> None:
        with self._lock:
            self._dados[chave] = (time.monotonic() + max(0.0, ttl_segundos), valor)

    def invalidar(self, chave: str) -> None:
        with self._lock:
            self._dados.pop(chave, None)

    def limpar(self) -> None:
        with self._lock:
            self._dados.clear()
