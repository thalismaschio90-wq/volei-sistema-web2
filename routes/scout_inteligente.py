"""Dashboard administrativo de scout inteligente pós-partida."""
from __future__ import annotations

from flask import Blueprint, jsonify, render_template, request, session

from analytics.scout_inteligente import calcular_scout
from repositories.replay_partida import buscar_partida_replay, listar_eventos_replay
from services.replay_partida import preparar_linha_tempo

scout_inteligente_bp = Blueprint("scout_inteligente", __name__)


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
        return None, calcular_scout([])
    eventos = preparar_linha_tempo(listar_eventos_replay(partida_id, competicao, limite=5000))
    return partida, calcular_scout(eventos, partida)


@scout_inteligente_bp.get("/admin/scout-inteligente")
def pagina():
    if not _permitido():
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    partida_id, competicao = _parametros()
    partida = None
    scout = calcular_scout([])
    erro = None
    if partida_id and competicao:
        partida, scout = _carregar(partida_id, competicao)
        if not partida:
            erro = "Partida não encontrada para a competição informada."
    return render_template(
        "admin_scout_inteligente.html",
        partida=partida,
        scout=scout,
        erro=erro,
        filtros={"partida_id": partida_id, "competicao": competicao},
    )


@scout_inteligente_bp.get("/admin/scout-inteligente/dados")
def dados():
    if not _permitido():
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    partida_id, competicao = _parametros()
    if not partida_id or not competicao:
        return {"ok": False, "erro": "Informe partida_id e competicao."}, 400
    partida, scout = _carregar(partida_id, competicao)
    if not partida:
        return {"ok": False, "erro": "Partida não encontrada."}, 404
    return jsonify({"ok": True, "partida": partida, "scout": scout})
