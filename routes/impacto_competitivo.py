"""Dashboard pós-partida de impacto competitivo e tendências."""
from __future__ import annotations

from flask import Blueprint, jsonify, render_template, request, session

from analytics.impacto_competitivo import calcular_impacto_competitivo
from repositories.replay_partida import buscar_partida_replay, listar_eventos_replay
from services.replay_partida import preparar_linha_tempo

impacto_competitivo_bp = Blueprint("impacto_competitivo", __name__)


def _permitido() -> bool:
    return str(session.get("perfil") or "").strip().lower() == "superadmin"


def _parametros() -> tuple[int, str]:
    try:
        partida_id = int(request.args.get("partida_id") or 0)
    except (TypeError, ValueError):
        partida_id = 0
    return max(0, partida_id), str(request.args.get("competicao") or "").strip()


def _carregar(partida_id: int, competicao: str):
    partida = buscar_partida_replay(partida_id, competicao)
    if not partida:
        return None, calcular_impacto_competitivo([])
    eventos = preparar_linha_tempo(listar_eventos_replay(partida_id, competicao, limite=5000))
    return partida, calcular_impacto_competitivo(eventos, partida)


@impacto_competitivo_bp.get("/admin/impacto-competitivo")
def pagina():
    if not _permitido():
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    partida_id, competicao = _parametros()
    partida = None
    analise = calcular_impacto_competitivo([])
    erro = None
    if partida_id and competicao:
        partida, analise = _carregar(partida_id, competicao)
        if not partida:
            erro = "Partida não encontrada para a competição informada."
    return render_template(
        "admin_impacto_competitivo.html",
        partida=partida,
        analise=analise,
        erro=erro,
        filtros={"partida_id": partida_id, "competicao": competicao},
    )


@impacto_competitivo_bp.get("/admin/impacto-competitivo/dados")
def dados():
    if not _permitido():
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    partida_id, competicao = _parametros()
    if not partida_id or not competicao:
        return {"ok": False, "erro": "Informe partida_id e competicao."}, 400
    partida, analise = _carregar(partida_id, competicao)
    if not partida:
        return {"ok": False, "erro": "Partida não encontrada."}, 404
    return jsonify({"ok": True, "partida": partida, "analise": analise})
