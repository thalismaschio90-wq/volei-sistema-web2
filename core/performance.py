"""Instrumentação leve de tempo das requisições."""
from __future__ import annotations

import logging
import os
import time
from flask import Flask, before_render_template, g, request, template_rendered

from core.performance_store import performance_store
from core.profiler import finalizar_profile, iniciar_profile, registrar_tempo
from core.sql_performance import finalizar_medicao_sql, iniciar_medicao_sql

_logger = logging.getLogger("volleytablepro.performance")


def _env_bool(nome: str, padrao: bool = False) -> bool:
    valor = os.environ.get(nome)
    if valor is None:
        return padrao
    return valor.strip().lower() in {"1", "true", "yes", "on", "sim"}


def _env_float(nome: str, padrao: float) -> float:
    try:
        return float(os.environ.get(nome, padrao))
    except (TypeError, ValueError):
        return padrao


def registrar_instrumentacao_performance(app: Flask) -> None:
    """Registra medição das rotas quando habilitada por variável de ambiente."""
    if not _env_bool("PERFORMANCE_LOG_ENABLED", False):
        return

    limite_ms = max(0.0, _env_float("PERFORMANCE_LOG_THRESHOLD_MS", 500.0))

    def _antes_template(sender, template, context, **extra):
        try:
            g._vtp_template_inicio = time.perf_counter()
        except Exception:
            pass

    def _depois_template(sender, template, context, **extra):
        inicio = getattr(g, "_vtp_template_inicio", None)
        if inicio is not None:
            registrar_tempo("template", (time.perf_counter() - inicio) * 1000.0)
            g._vtp_template_inicio = None

    before_render_template.connect(_antes_template, app, weak=False)
    template_rendered.connect(_depois_template, app, weak=False)

    @app.before_request
    def _iniciar_medicao_requisicao() -> None:
        g._vtp_inicio_request = time.perf_counter()
        g._vtp_template_inicio = None
        iniciar_medicao_sql()
        iniciar_profile()

    @app.after_request
    def _finalizar_medicao_requisicao(response):
        inicio = getattr(g, "_vtp_inicio_request", None)
        if inicio is None:
            return response

        duracao_ms = (time.perf_counter() - inicio) * 1000.0
        sql = finalizar_medicao_sql()
        secoes = finalizar_profile()
        template_ms = float(secoes.get("template") or 0.0)
        db_ms = float(sql.get("duracao_total_ms") or 0.0)
        secoes["python_estimado"] = round(max(0.0, duracao_ms - db_ms - template_ms), 2)
        try:
            tamanho_resposta = int(response.calculate_content_length() or 0)
        except Exception:
            tamanho_resposta = 0
        secoes["resposta_kb"] = round(tamanho_resposta / 1024.0, 2)

        timing = [f"app;dur={duracao_ms:.1f}"]
        if sql["quantidade"]:
            timing.append(f"db;dur={db_ms:.1f}")
            timing.append(f"db_count;desc=\"{sql['quantidade']} consultas\"")
        if template_ms:
            timing.append(f"template;dur={template_ms:.1f}")
        response.headers["Server-Timing"] = ", ".join(timing)

        performance_store.registrar_requisicao(
            metodo=request.method,
            endpoint=request.endpoint or "",
            rota=request.path,
            status=response.status_code,
            duracao_ms=duracao_ms,
            sql=sql,
            secoes=secoes,
        )

        if duracao_ms >= limite_ms:
            _logger.warning(
                "rota_lenta metodo=%s rota=%s status=%s duracao_ms=%.1f sql_count=%s sql_total_ms=%.1f sql_max_ms=%.1f template_ms=%.1f resposta_kb=%.1f",
                request.method,
                request.path,
                response.status_code,
                duracao_ms,
                sql["quantidade"],
                db_ms,
                sql["duracao_max_ms"],
                template_ms,
                secoes["resposta_kb"],
            )
        return response
