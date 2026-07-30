"""Auditoria final de homologação e prontidão para produção.

A auditoria combina configuração estática, saúde das dependências e evidências
opcionais de testes. Nenhum segredo é incluído no relatório público.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from core.runtime_config import load_runtime_config

_TRUE = {"1", "true", "sim", "yes", "on"}


def _bool_env(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    raw = str(env.get(name, "")).strip().lower()
    return default if not raw else raw in _TRUE


def _int_env(env: Mapping[str, str], name: str, default: int) -> int:
    try:
        return int(str(env.get(name, default)).strip())
    except (TypeError, ValueError):
        return default


@dataclass(slots=True)
class CheckResult:
    code: str
    title: str
    status: str
    detail: str
    blocking: bool = False
    category: str = "geral"

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "title": self.title,
            "status": self.status,
            "detail": self.detail,
            "blocking": self.blocking,
            "category": self.category,
        }


@dataclass(slots=True)
class ReleaseReadinessReport:
    environment: str
    checks: list[CheckResult] = field(default_factory=list)
    generated_at: float = field(default_factory=time.time)

    @property
    def blocking_failures(self) -> list[CheckResult]:
        return [c for c in self.checks if c.blocking and c.status == "fail"]

    @property
    def warnings(self) -> list[CheckResult]:
        return [c for c in self.checks if c.status == "warn"]

    @property
    def approved(self) -> bool:
        return not self.blocking_failures

    def as_dict(self) -> dict[str, Any]:
        counts = {status: sum(1 for c in self.checks if c.status == status) for status in ("pass", "warn", "fail")}
        return {
            "approved": self.approved,
            "environment": self.environment,
            "generated_at": self.generated_at,
            "summary": {
                "total": len(self.checks),
                "passed": counts["pass"],
                "warnings": counts["warn"],
                "failed": counts["fail"],
                "blocking_failures": len(self.blocking_failures),
            },
            "checks": [c.as_dict() for c in self.checks],
        }

    def to_markdown(self) -> str:
        data = self.as_dict()
        result = "APROVADO" if self.approved else "REPROVADO"
        lines = [
            "# Relatório de prontidão do VolleyTablePro",
            "",
            f"**Resultado:** {result}",
            f"**Ambiente:** {self.environment}",
            "",
            "## Resumo",
            "",
            f"- Verificações: {data['summary']['total']}",
            f"- Aprovadas: {data['summary']['passed']}",
            f"- Avisos: {data['summary']['warnings']}",
            f"- Falhas: {data['summary']['failed']}",
            f"- Falhas bloqueantes: {data['summary']['blocking_failures']}",
            "",
            "## Verificações",
            "",
            "| Status | Categoria | Verificação | Detalhe |",
            "|---|---|---|---|",
        ]
        icons = {"pass": "✅", "warn": "⚠️", "fail": "❌"}
        for check in self.checks:
            detail = check.detail.replace("|", "\\|").replace("\n", " ")
            lines.append(f"| {icons.get(check.status, check.status)} | {check.category} | {check.title} | {detail} |")
        lines += [
            "",
            "## Interpretação",
            "",
            "- Uma falha bloqueante impede a aprovação para produção.",
            "- Avisos exigem revisão, mas podem ser aceitos conscientemente.",
            "- O relatório não contém URLs de banco, cookies, tokens ou credenciais.",
        ]
        return "\n".join(lines) + "\n"


def _http_json(url: str, timeout: float = 10.0, cookie: str = "") -> tuple[bool, int, dict[str, Any] | None, str]:
    request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "VolleyTablePro-readiness/1.0"})
    if cookie:
        request.add_header("Cookie", cookie)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = int(getattr(response, "status", 200))
            raw = response.read(2_000_000)
            try:
                payload = json.loads(raw.decode("utf-8")) if raw else None
            except Exception:
                payload = None
            return 200 <= status < 300, status, payload, "ok"
    except urllib.error.HTTPError as exc:
        return False, int(exc.code), None, "HTTPError"
    except Exception as exc:
        return False, 0, None, type(exc).__name__


def _load_json_file(path: str | Path | None) -> dict[str, Any] | None:
    if not path:
        return None
    file_path = Path(path)
    if not file_path.exists() or not file_path.is_file():
        return None
    try:
        data = json.loads(file_path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def build_release_readiness_report(
    env: Mapping[str, str] | None = None,
    *,
    base_url: str = "",
    load_report_path: str | Path | None = None,
    admin_cookie: str = "",
    http_timeout: float = 10.0,
) -> ReleaseReadinessReport:
    source = os.environ if env is None else env
    environment = str(source.get("APP_ENV") or source.get("RENDER_SERVICE_NAME") or "local").strip()
    report = ReleaseReadinessReport(environment=environment)
    runtime = load_runtime_config(source)

    runtime_errors = runtime.errors()
    report.checks.append(CheckResult(
        "runtime_config",
        "Configuração de workers e tempo real",
        "fail" if runtime_errors else "pass",
        " | ".join(runtime_errors) if runtime_errors else f"{runtime.workers} worker(s), {runtime.threads} thread(s), estado={runtime.state_backend}.",
        blocking=True,
        category="runtime",
    ))

    db_url = bool(str(source.get("DATABASE_URL") or "").strip())
    report.checks.append(CheckResult(
        "database_url",
        "DATABASE_URL configurada",
        "pass" if db_url else "fail",
        "Configurada." if db_url else "Ausente.",
        blocking=True,
        category="postgresql",
    ))

    secret = str(source.get("SECRET_KEY") or source.get("FLASK_SECRET_KEY") or "")
    secret_ok = len(secret) >= 32 and secret.lower() not in {"secret", "dev", "change-me", "changeme"}
    report.checks.append(CheckResult(
        "secret_key",
        "Chave de sessão forte",
        "pass" if secret_ok else "fail",
        "Comprimento adequado." if secret_ok else "Defina SECRET_KEY/FLASK_SECRET_KEY com pelo menos 32 caracteres aleatórios.",
        blocking=True,
        category="segurança",
    ))

    debug_enabled = _bool_env(source, "FLASK_DEBUG", False) or _bool_env(source, "DEBUG", False)
    report.checks.append(CheckResult(
        "debug_disabled",
        "Modo debug desativado",
        "fail" if debug_enabled else "pass",
        "Debug ativo." if debug_enabled else "Debug desativado.",
        blocking=True,
        category="segurança",
    ))

    pool_enabled = _bool_env(source, "DB_POOL_ENABLED", True)
    pool_max = _int_env(source, "DB_POOL_MAX", 8)
    estimated_capacity = runtime.workers * runtime.threads
    if not pool_enabled:
        pool_status, pool_detail = "warn", "Pool desativado; conexões diretas podem aumentar latência e carga no Neon."
    elif pool_max < max(4, runtime.workers * 2):
        pool_status, pool_detail = "warn", f"DB_POOL_MAX={pool_max} pode ser baixo para {runtime.workers} worker(s)."
    else:
        pool_status, pool_detail = "pass", f"Pool habilitado, máximo={pool_max}; capacidade HTTP teórica={estimated_capacity} threads."
    report.checks.append(CheckResult("db_pool", "Pool PostgreSQL", pool_status, pool_detail, category="postgresql"))

    redis_enabled = runtime.state_backend == "redis"
    if runtime.multiple_workers:
        redis_status = "pass" if redis_enabled and runtime.socket_queue_enabled else "fail"
        redis_detail = "Redis compartilhando estado e Socket.IO." if redis_status == "pass" else "Múltiplos workers exigem Redis para estado e fila Socket.IO."
        redis_blocking = True
    else:
        redis_status = "pass" if redis_enabled else "warn"
        redis_detail = "Redis ativo." if redis_enabled else "Modo local seguro com um worker; Redis ainda precisa de homologação antes de escalar."
        redis_blocking = False
    report.checks.append(CheckResult("redis_realtime", "Estado e Socket.IO compartilhados", redis_status, redis_detail, blocking=redis_blocking, category="realtime"))

    delta_enabled = _bool_env(source, "SOCKET_DELTA_ENABLED", True)
    report.checks.append(CheckResult(
        "socket_delta",
        "Delta do Socket.IO",
        "pass" if delta_enabled else "warn",
        "Ativo." if delta_enabled else "Desativado; haverá mais tráfego de snapshots completos.",
        category="realtime",
    ))

    legacy_events = _bool_env(source, "SOCKET_LEGACY_STATE_EVENTS", True)
    legacy_guard = _bool_env(source, "SOCKET_LEGACY_REQUIRE_DELTA_HEALTHY", False)
    if not legacy_events and not legacy_guard:
        status, detail, blocking = "fail", "Eventos legados desligados sem a proteção de saúde dos deltas.", True
    elif legacy_events:
        status, detail, blocking = "warn", "Eventos legados ainda ativos; seguro para homologação, mas aumenta o tráfego.", False
    else:
        status, detail, blocking = "pass", "Eventos legados condicionados à saúde dos deltas.", False
    report.checks.append(CheckResult("socket_legacy", "Compatibilidade dos eventos Socket.IO", status, detail, blocking=blocking, category="realtime"))

    performance_enabled = _bool_env(source, "PERFORMANCE_LOG_ENABLED", False)
    sql_performance_enabled = _bool_env(source, "SQL_PERFORMANCE_LOG_ENABLED", False)
    report.checks.append(CheckResult(
        "observability",
        "Observabilidade de rotas e SQL",
        "pass" if performance_enabled and sql_performance_enabled else "warn",
        "Métricas de rota e SQL ativas." if performance_enabled and sql_performance_enabled else "Ative PERFORMANCE_LOG_ENABLED e SQL_PERFORMANCE_LOG_ENABLED durante homologação.",
        category="observabilidade",
    ))

    explain_analyze = _bool_env(source, "SQL_EXPLAIN_ANALYZE_ENABLED", False)
    report.checks.append(CheckResult(
        "explain_analyze",
        "EXPLAIN ANALYZE automático em produção",
        "warn" if explain_analyze else "pass",
        "Ativo; pode repetir SELECTs lentos. Use apenas em homologação." if explain_analyze else "Desativado.",
        category="postgresql",
    ))

    async_reports = _bool_env(source, "RELATORIOS_ASYNC_ENABLED", False)
    if async_reports:
        rq_ok = runtime.redis_url_configured and bool(str(source.get("RELATORIOS_RQ_QUEUE") or "relatorios").strip())
        status, detail, blocking = ("pass", "Fila de relatórios configurada.", False) if rq_ok else ("fail", "Relatórios assíncronos exigem Redis e fila RQ.", True)
    else:
        status, detail, blocking = "warn", "Relatórios continuam síncronos; relatórios grandes ainda podem ocupar uma thread web.", False
    report.checks.append(CheckResult("async_reports", "Worker de relatórios", status, detail, blocking=blocking, category="tarefas"))

    normalized_base = base_url.rstrip("/")
    if normalized_base:
        for endpoint, code, title in (("/healthz", "healthz", "Liveness HTTP"), ("/readyz", "readyz", "Readiness HTTP")):
            ok, http_status, payload, error = _http_json(normalized_base + endpoint, timeout=http_timeout)
            if endpoint == "/readyz" and isinstance(payload, dict):
                ok = ok and bool(payload.get("ok", False))
            report.checks.append(CheckResult(
                code,
                title,
                "pass" if ok else "fail",
                f"HTTP {http_status}." if http_status else f"Falha: {error}.",
                blocking=True,
                category="saúde",
            ))

        if admin_cookie:
            ok, http_status, payload, error = _http_json(normalized_base + "/admin/runtime-status", timeout=http_timeout, cookie=admin_cookie)
            report.checks.append(CheckResult(
                "admin_runtime_status",
                "Diagnóstico administrativo",
                "pass" if ok and isinstance(payload, dict) else "warn",
                f"HTTP {http_status}." if http_status else f"Não coletado: {error}.",
                category="observabilidade",
            ))
    else:
        report.checks.append(CheckResult("remote_health", "Teste do serviço publicado", "warn", "BASE_URL não informada; /healthz e /readyz não foram testados.", category="saúde"))

    load_report = _load_json_file(load_report_path)
    if load_report is None:
        report.checks.append(CheckResult("load_test", "Ensaio de campeonato", "warn", "Relatório de carga não informado ou inválido.", category="carga"))
    else:
        approved = bool(load_report.get("aprovado", load_report.get("approved", load_report.get("ok", False))))
        report.checks.append(CheckResult(
            "load_test",
            "Ensaio de campeonato",
            "pass" if approved else "fail",
            "Relatório de carga aprovado." if approved else "O relatório de carga indica reprovação ou não contém aprovação explícita.",
            blocking=True,
            category="carga",
        ))

    return report
