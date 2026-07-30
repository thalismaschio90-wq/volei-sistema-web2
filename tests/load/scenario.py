from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
from datetime import datetime, timezone
import random
from threading import Event
from time import monotonic, sleep
from typing import Any
from urllib.parse import quote
from uuid import uuid4

from .config import LoadTestConfig
from .http_client import LoadHttpClient
from .metrics import MetricCollector
from .report import write_report
from .snapshots import build_snapshot_delta, extract_runtime_summary
from .socket_client import RealtimeProbe


class ChampionshipLoadScenario:
    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.metrics = MetricCollector()
        self.stop_event = Event()
        self.probes: list[RealtimeProbe] = []

    def _client(self) -> LoadHttpClient:
        return LoadHttpClient(
            self.config.base_url,
            self.metrics,
            timeout_seconds=self.config.request_timeout_seconds,
            session_cookie=self.config.session_cookie,
            operator_token=self.config.operator_token,
        )

    def preflight(self) -> dict[str, Any]:
        client = self._client()
        health = client.get("http_healthz", "/healthz")
        ready = client.get("http_readyz", "/readyz")
        return {
            "health_status": health.status_code,
            "ready_status": ready.status_code,
            "ready_payload": ready.json(),
        }

    def _admin_snapshots(self) -> dict[str, Any]:
        if not self.config.collect_admin_metrics:
            return {}
        client = self._client()
        result: dict[str, Any] = {}
        for name, path in (
            ("runtime", "/admin/runtime-status"),
            ("performance", "/admin/performance-status"),
            ("realtime", "/admin/realtime-delta-status"),
        ):
            response = client.get(f"admin_snapshot_{name}", path)
            if response.status_code == 200:
                result[name] = response.json()
            else:
                result[name] = {"ok": False, "status_code": response.status_code}
        return result

    def _readiness_monitor(self) -> None:
        client = self._client()
        deadline = monotonic() + self.config.duration_seconds
        while monotonic() < deadline and not self.stop_event.is_set():
            try:
                client.get("http_readyz_during_load", "/readyz")
            except Exception:
                pass
            self.stop_event.wait(self.config.readiness_interval_seconds)

    def _public_viewer(self, viewer_id: int) -> None:
        client = self._client()
        competition = quote(self.config.competicao, safe="")
        if self.config.public_code:
            code = quote(self.config.public_code, safe="")
            paths = [
                f"/v/{code}",
                f"/v/{code}/ao-vivo/dados",
                f"/v/{code}/partida/{self.config.partida_id}",
                f"/v/{code}/partida/{self.config.partida_id}/dados",
            ]
        else:
            paths = [
                f"/visualizador/{competition}",
                f"/visualizador/{competition}/ao-vivo/dados",
            ]
        deadline = monotonic() + self.config.duration_seconds
        index = viewer_id % len(paths)
        if self.config.warmup_seconds:
            sleep(random.uniform(0, self.config.warmup_seconds))
        while monotonic() < deadline and not self.stop_event.is_set():
            path = paths[index % len(paths)]
            try:
                client.get("http_public_view", path)
            except Exception:
                pass
            index += 1
            sleep(random.uniform(1.2, 3.0))

    def _start_probes(self) -> None:
        if not self.config.socket_enabled:
            return
        profiles = [
            ("arbitro_1", "arbitro"),
            ("arbitro_2", "arbitro"),
            ("placar", "placar"),
            ("visualizador", "visualizador"),
        ]
        profiles.extend(
            (f"visualizador_publico_{index + 1}", "publico_socket")
            for index in range(self.config.socket_viewers)
        )
        for profile, metric_profile in profiles:
            probe = RealtimeProbe(
                self.config.base_url,
                self.config.partida_id,
                self.config.competicao,
                self.metrics,
                profile=profile,
                metric_profile=metric_profile,
                session_cookie=self.config.session_cookie,
            )
            try:
                probe.connect(self.config.request_timeout_seconds)
            except Exception:
                probe.disconnect()
                raise
            self.probes.append(probe)

    def _point_writer(self) -> None:
        if not self.config.allow_writes:
            return
        client = self._client()
        competition = quote(self.config.competicao, safe="")
        endpoint = f"/apontador/jogo/{competition}/{self.config.partida_id}/ponto"
        deadline = monotonic() + self.config.duration_seconds
        team = "A"
        while monotonic() < deadline and not self.stop_event.is_set():
            marker = uuid4().hex
            for probe in self.probes:
                probe.mark_expected(marker)
            payload = {
                "equipe": team,
                "tipo_ponto": "ponto_simples",
                "load_test_marker": marker,
                "origem": "laboratorio_carga",
            }
            try:
                result = client.post_json("http_register_point", endpoint, payload)
                if result.status_code >= 400:
                    for probe in self.probes:
                        probe.clear_expected(marker)
                    self.stop_event.set()
                    break
            except Exception:
                for probe in self.probes:
                    probe.clear_expected(marker)
                self.stop_event.set()
                break
            team = "B" if team == "A" else "A"
            sleep(self.config.point_interval_seconds)

    def _conclusions(self, summary: dict[str, Any], observations: list[dict[str, Any]]) -> dict[str, Any]:
        reasons: list[str] = []
        approved = True
        for metric_name, limit in (
            ("http_public_view", self.config.max_public_p95_ms),
            ("http_register_point", self.config.max_point_p95_ms),
        ):
            item = summary.get(metric_name, {})
            if item and item.get("p95_ms", 0) > limit:
                approved = False
                reasons.append(f"P95 de {metric_name} ficou acima de {limit:.0f} ms.")
            if item and item.get("error_rate_percent", 0) > self.config.max_error_rate_percent:
                approved = False
                reasons.append(f"Taxa de erro de {metric_name} ultrapassou {self.config.max_error_rate_percent:.2f}%.")
        for name, item in summary.items():
            if name.startswith("socket_delivery_") and item.get("p95_ms", 0) > self.config.max_socket_p95_ms:
                approved = False
                reasons.append(f"P95 de {name} ficou acima de {self.config.max_socket_p95_ms:.0f} ms.")
        ready = summary.get("http_readyz_during_load", {})
        if ready and ready.get("failed", 0):
            approved = False
            reasons.append("O endpoint /readyz apresentou falha durante a carga.")
        socket_connect_failures = sum(
            int(item.get("failed", 0) or 0)
            for name, item in summary.items()
            if name.startswith("socket_connect_")
        )
        if socket_connect_failures:
            approved = False
            reasons.append(f"Houve {socket_connect_failures} falha(s) de conexão Socket.IO.")
        for observation in observations:
            profile = observation.get("profile")
            if observation.get("stale", 0) > 0 or not observation.get("monotonic", True):
                approved = False
                reasons.append(f"O receptor {profile} recebeu versões fora de ordem.")
            if self.config.allow_writes and observation.get("received", 0) < self.config.min_socket_events_per_probe:
                approved = False
                reasons.append(
                    f"O receptor {profile} recebeu menos de "
                    f"{self.config.min_socket_events_per_probe} evento(s) durante o teste com escrita."
                )
            if observation.get("pending_markers", 0) > self.config.max_pending_socket_markers:
                approved = False
                reasons.append(
                    f"O receptor {profile} terminou com "
                    f"{observation.get('pending_markers')} marcador(es) de entrega pendente(s)."
                )
        if not reasons:
            reasons.append("Nenhum critério crítico de reprovação foi detectado.")
        return {"approved": approved, "reasons": reasons}

    def run(self) -> tuple[str, str]:
        errors = self.config.validate()
        if errors:
            raise ValueError(" ".join(errors))

        preflight = self.preflight()
        if preflight["health_status"] != 200:
            raise RuntimeError("O endpoint /healthz não respondeu com HTTP 200.")
        if preflight["ready_status"] != 200:
            raise RuntimeError(f"Aplicação não pronta: {preflight['ready_payload']}")

        admin_before = self._admin_snapshots()
        try:
            self._start_probes()
            workers = self.config.viewers + 3
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(self._readiness_monitor)]
                futures.extend(
                    executor.submit(self._public_viewer, viewer_id)
                    for viewer_id in range(self.config.viewers)
                )
                if self.config.allow_writes:
                    futures.append(executor.submit(self._point_writer))
                for future in as_completed(futures):
                    future.result()
        finally:
            self.stop_event.set()
            for probe in self.probes:
                probe.disconnect()

        admin_after = self._admin_snapshots()
        summary = self.metrics.summary()
        observations = [probe.snapshot() for probe in self.probes]
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "config": asdict(self.config),
            "preflight": preflight,
            "metrics": summary,
            "socket_observations": observations,
            "admin_metrics": {
                "before": admin_before,
                "after": admin_after,
                "delta": build_snapshot_delta(admin_before, admin_after),
                "runtime_before": extract_runtime_summary(admin_before.get("runtime", {})),
                "runtime_after": extract_runtime_summary(admin_after.get("runtime", {})),
            },
            "conclusions": self._conclusions(summary, observations),
        }
        json_path, md_path = write_report(self.config.report_dir, payload)
        return str(json_path), str(md_path)
