from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event, Lock
from time import perf_counter
from typing import Any

from .metrics import MetricCollector

try:
    import socketio  # type: ignore
except Exception:  # pragma: no cover - dependência opcional do laboratório
    socketio = None


@dataclass
class SocketObservation:
    versions: list[int] = field(default_factory=list)
    received: int = 0
    stale: int = 0
    last_payload: dict[str, Any] = field(default_factory=dict)


class RealtimeProbe:
    EVENTS = (
        "estado_partida",
        "estado_jogo_atualizado",
        "estado_arbitros",
        "estado_partida_tempo_real",
        "placar_atualizado",
        "ponto_registrado",
        "estado_partida_delta",
    )

    def __init__(
        self,
        base_url: str,
        partida_id: int,
        competicao: str,
        metrics: MetricCollector,
        *,
        profile: str,
        session_cookie: str = "",
        metric_profile: str = "",
    ):
        if socketio is None:
            raise RuntimeError(
                "Cliente Socket.IO indisponível. Instale requirements-loadtest.txt."
            )
        self.base_url = base_url
        self.partida_id = str(partida_id)
        self.competicao = competicao
        self.metrics = metrics
        self.profile = profile
        self.metric_profile = metric_profile or profile
        self.session_cookie = session_cookie
        self.client = socketio.Client(
            reconnection=True,
            reconnection_attempts=10,
            logger=False,
            engineio_logger=False,
        )
        self.observation = SocketObservation()
        self.connected = Event()
        self._lock = Lock()
        self._sent_markers: dict[str, float] = {}
        self._register_handlers()

    def _register_handlers(self) -> None:
        @self.client.event
        def connect():
            self.connected.set()
            event = "entrar_arbitro" if self.profile.startswith("arbitro") else "entrar_partida_tempo_real"
            self.client.emit(
                event,
                {
                    "partida_id": self.partida_id,
                    "competicao": self.competicao,
                    "perfil": self.profile,
                    "suporta_delta": True,
                },
            )

        @self.client.event
        def disconnect():
            self.connected.clear()

        for event_name in self.EVENTS:
            self.client.on(event_name, handler=self._make_event_handler(event_name))

    def _make_event_handler(self, event_name: str):
        def handler(payload):
            data = dict(payload or {})
            version = int(data.get("estado_versao") or 0)
            marker = str(data.get("load_test_marker") or "")
            now = perf_counter()
            with self._lock:
                previous = self.observation.versions[-1] if self.observation.versions else 0
                if version and version < previous:
                    self.observation.stale += 1
                if version:
                    self.observation.versions.append(version)
                self.observation.received += 1
                self.observation.last_payload = data
                started_at = self._sent_markers.pop(marker, None) if marker else None
            if started_at is not None:
                self.metrics.add(
                    f"socket_delivery_{self.metric_profile}",
                    (now - started_at) * 1000.0,
                    ok=True,
                )
        return handler

    def connect(self, timeout_seconds: float = 10.0) -> None:
        headers = {"Cookie": self.session_cookie} if self.session_cookie else None
        with self.metrics.timer(f"socket_connect_{self.profile}"):
            self.client.connect(
                self.base_url,
                headers=headers,
                transports=["websocket", "polling"],
                wait_timeout=timeout_seconds,
            )
        if not self.connected.wait(timeout_seconds):
            raise TimeoutError(f"Socket {self.profile} não confirmou conexão.")

    def mark_expected(self, marker: str) -> None:
        chave = str(marker or "").strip()
        if not chave:
            return
        with self._lock:
            self._sent_markers[chave] = perf_counter()

    def clear_expected(self, marker: str) -> None:
        chave = str(marker or "").strip()
        if not chave:
            return
        with self._lock:
            self._sent_markers.pop(chave, None)

    def disconnect(self) -> None:
        if self.client.connected:
            self.client.disconnect()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            versions = list(self.observation.versions)
            return {
                "profile": self.profile,
                "metric_profile": self.metric_profile,
                "received": self.observation.received,
                "stale": self.observation.stale,
                "first_version": versions[0] if versions else 0,
                "last_version": versions[-1] if versions else 0,
                "monotonic": all(a <= b for a, b in zip(versions, versions[1:])),
                "connected": bool(self.client.connected),
                "pending_markers": len(self._sent_markers),
            }
