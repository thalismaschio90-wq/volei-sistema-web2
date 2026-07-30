from __future__ import annotations

from dataclasses import dataclass
import os


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    try:
        value = int(str(os.getenv(name, default)).strip())
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    try:
        value = float(str(os.getenv(name, default)).strip())
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _env_bool(name: str, default: bool = False) -> bool:
    value = str(os.getenv(name, "1" if default else "0")).strip().lower()
    return value in {"1", "true", "yes", "sim", "on"}


@dataclass(frozen=True)
class LoadTestConfig:
    base_url: str
    competicao: str
    partida_id: int
    public_code: str = ""
    viewers: int = 30
    duration_seconds: int = 60
    point_interval_seconds: float = 2.0
    request_timeout_seconds: float = 10.0
    session_cookie: str = ""
    operator_token: str = ""
    allow_writes: bool = False
    socket_enabled: bool = True
    report_dir: str = "load_reports"
    socket_viewers: int = 10
    warmup_seconds: int = 5
    readiness_interval_seconds: float = 10.0
    collect_admin_metrics: bool = True
    max_public_p95_ms: float = 1000.0
    max_point_p95_ms: float = 500.0
    max_socket_p95_ms: float = 300.0
    max_error_rate_percent: float = 1.0
    min_socket_events_per_probe: int = 1
    max_pending_socket_markers: int = 0

    @classmethod
    def from_env(cls) -> "LoadTestConfig":
        base_url = str(os.getenv("VTP_LOAD_BASE_URL", "http://127.0.0.1:5000")).rstrip("/")
        competicao = str(os.getenv("VTP_LOAD_COMPETICAO", "")).strip()
        partida_id = _env_int("VTP_LOAD_PARTIDA_ID", 0)
        return cls(
            base_url=base_url,
            competicao=competicao,
            partida_id=partida_id,
            public_code=str(os.getenv("VTP_LOAD_PUBLIC_CODE", "")).strip(),
            viewers=_env_int("VTP_LOAD_VIEWERS", 30, 1),
            duration_seconds=_env_int("VTP_LOAD_DURATION_SECONDS", 60, 5),
            point_interval_seconds=_env_float("VTP_LOAD_POINT_INTERVAL_SECONDS", 2.0, 0.1),
            request_timeout_seconds=_env_float("VTP_LOAD_TIMEOUT_SECONDS", 10.0, 1.0),
            session_cookie=str(os.getenv("VTP_LOAD_SESSION_COOKIE", "")).strip(),
            operator_token=str(os.getenv("VTP_LOAD_OPERATOR_TOKEN", "")).strip(),
            allow_writes=_env_bool("VTP_LOAD_ALLOW_WRITES", False),
            socket_enabled=_env_bool("VTP_LOAD_SOCKET_ENABLED", True),
            report_dir=str(os.getenv("VTP_LOAD_REPORT_DIR", "load_reports")).strip() or "load_reports",
            socket_viewers=_env_int("VTP_LOAD_SOCKET_VIEWERS", 10, 0),
            warmup_seconds=_env_int("VTP_LOAD_WARMUP_SECONDS", 5, 0),
            readiness_interval_seconds=_env_float("VTP_LOAD_READINESS_INTERVAL_SECONDS", 10.0, 1.0),
            collect_admin_metrics=_env_bool("VTP_LOAD_COLLECT_ADMIN_METRICS", True),
            max_public_p95_ms=_env_float("VTP_LOAD_MAX_PUBLIC_P95_MS", 1000.0, 1.0),
            max_point_p95_ms=_env_float("VTP_LOAD_MAX_POINT_P95_MS", 500.0, 1.0),
            max_socket_p95_ms=_env_float("VTP_LOAD_MAX_SOCKET_P95_MS", 300.0, 1.0),
            max_error_rate_percent=_env_float("VTP_LOAD_MAX_ERROR_RATE_PERCENT", 1.0, 0.0),
            min_socket_events_per_probe=_env_int("VTP_LOAD_MIN_SOCKET_EVENTS_PER_PROBE", 1, 0),
            max_pending_socket_markers=_env_int("VTP_LOAD_MAX_PENDING_SOCKET_MARKERS", 0, 0),
        )

    def validate(self) -> list[str]:
        errors: list[str] = []
        if not self.base_url.startswith(("http://", "https://")):
            errors.append("VTP_LOAD_BASE_URL deve começar com http:// ou https://.")
        if self.partida_id <= 0:
            errors.append("VTP_LOAD_PARTIDA_ID deve indicar uma partida de homologação.")
        if not self.competicao:
            errors.append("VTP_LOAD_COMPETICAO deve indicar a competição de homologação.")
        if self.socket_viewers > self.viewers:
            errors.append("VTP_LOAD_SOCKET_VIEWERS não deve superar VTP_LOAD_VIEWERS.")
        if self.collect_admin_metrics and not self.session_cookie:
            errors.append("Coleta de métricas administrativas exige VTP_LOAD_SESSION_COOKIE de Super ADM ou VTP_LOAD_COLLECT_ADMIN_METRICS=0.")
        if self.socket_enabled and self.socket_viewers < 0:
            errors.append("VTP_LOAD_SOCKET_VIEWERS não pode ser negativo.")
        if self.allow_writes and not self.session_cookie and not self.operator_token:
            errors.append(
                "Teste com escrita exige VTP_LOAD_SESSION_COOKIE ou VTP_LOAD_OPERATOR_TOKEN."
            )
        return errors
