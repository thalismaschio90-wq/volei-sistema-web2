"""Dashboard operacional somente leitura para o Super ADM."""
from __future__ import annotations

from flask import Blueprint, jsonify, render_template, session

from core.performance_store import performance_store
from core.readiness import readiness_report
from core.runtime_config import load_runtime_config
from realtime.delta_metrics import delta_metrics_store
from realtime.event_priority import dispatch_metrics_store
from realtime.load_shedding import load_shedding_manager
from realtime.presence import presence_store
from repositories.conexao import obter_estatisticas_pool
from services.dashboard_operacional import montar_dashboard_operacional


dashboard_operacional_bp = Blueprint("dashboard_operacional", __name__)


def _permitido() -> bool:
    return str(session.get("perfil") or "").strip().lower() == "superadmin"


def _snapshot() -> dict:
    realtime = delta_metrics_store.snapshot()
    realtime["despacho"] = dispatch_metrics_store.snapshot()
    realtime["degradacao"] = load_shedding_manager.snapshot()
    return montar_dashboard_operacional(
        readiness=readiness_report(ttl_seconds=1),
        runtime_config=load_runtime_config().public_dict(),
        pool=obter_estatisticas_pool(),
        performance=performance_store.snapshot(),
        realtime=realtime,
        presence=presence_store.snapshot(),
    )


@dashboard_operacional_bp.get("/admin/dashboard-operacional")
def pagina():
    if not _permitido():
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    return render_template("admin_dashboard_operacional.html", dados=_snapshot())


@dashboard_operacional_bp.get("/admin/dashboard-operacional/status")
def status():
    if not _permitido():
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    return jsonify({"ok": True, "dados": _snapshot()})
