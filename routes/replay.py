"""Replay e auditoria somente leitura para o Super ADM."""
from __future__ import annotations

import csv
import io

from flask import Blueprint, Response, jsonify, render_template, request, session

from repositories.replay_partida import buscar_partida_replay, listar_eventos_replay
from services.replay_partida import filtrar_linha_tempo, preparar_linha_tempo, resumir_replay

replay_bp = Blueprint("replay", __name__)


def _eh_superadmin() -> bool:
    return str(session.get("perfil") or "").strip().lower() == "superadmin"


def _inteiro_param(nome: str, padrao: int, minimo: int = 0, maximo: int | None = None) -> int:
    try:
        valor = int(request.args.get(nome) or padrao)
    except (TypeError, ValueError):
        valor = padrao
    valor = max(minimo, valor)
    return min(valor, maximo) if maximo is not None else valor


def _parametros():
    return {
        "partida_id": _inteiro_param("partida_id", 0),
        "competicao": str(request.args.get("competicao") or "").strip(),
        "depois_do_id": _inteiro_param("depois_do_id", 0),
        "limite": _inteiro_param("limite", 1000, 1, 5000),
        "set_numero": _inteiro_param("set_numero", 0) or None,
        "categoria": str(request.args.get("categoria") or "").strip().lower(),
        "equipe": str(request.args.get("equipe") or "").strip(),
        "autor": str(request.args.get("autor") or "").strip(),
        "busca": str(request.args.get("busca") or "").strip(),
    }


def _carregar(filtros: dict):
    partida = buscar_partida_replay(filtros["partida_id"], filtros["competicao"])
    if not partida:
        return None, [], resumir_replay([])
    brutos = listar_eventos_replay(
        filtros["partida_id"], filtros["competicao"],
        depois_do_id=filtros["depois_do_id"], limite=filtros["limite"],
        set_numero=filtros["set_numero"],
    )
    linha = filtrar_linha_tempo(
        preparar_linha_tempo(brutos), categoria=filtros["categoria"],
        equipe=filtros["equipe"], autor=filtros["autor"], busca=filtros["busca"],
    )
    return partida, linha, resumir_replay(linha)


@replay_bp.get("/admin/replay-partida")
def replay_partida_pagina():
    if not _eh_superadmin():
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    filtros = _parametros()
    partida = None
    eventos = []
    resumo = resumir_replay([])
    erro = None
    if filtros["partida_id"] and filtros["competicao"]:
        partida, eventos, resumo = _carregar(filtros)
        if not partida:
            erro = "Partida não encontrada para a competição informada."
    return render_template("admin_replay_partida.html", partida=partida, eventos=eventos, resumo=resumo, erro=erro, filtros=filtros)


@replay_bp.get("/admin/replay-partida/dados")
def replay_partida_dados():
    if not _eh_superadmin():
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    filtros = _parametros()
    if not filtros["partida_id"] or not filtros["competicao"]:
        return {"ok": False, "erro": "Informe partida_id e competicao."}, 400
    partida, eventos, resumo = _carregar(filtros)
    if not partida:
        return {"ok": False, "erro": "Partida não encontrada."}, 404
    return jsonify({"ok": True, "partida": partida, "eventos": eventos, "resumo": resumo, "filtros": filtros})


@replay_bp.get("/admin/replay-partida/exportar.csv")
def replay_partida_exportar_csv():
    if not _eh_superadmin():
        return {"ok": False, "erro": "Acesso restrito ao superadmin."}, 403
    filtros = _parametros()
    if not filtros["partida_id"] or not filtros["competicao"]:
        return {"ok": False, "erro": "Informe partida_id e competicao."}, 400
    partida, eventos, _ = _carregar(filtros)
    if not partida:
        return {"ok": False, "erro": "Partida não encontrada."}, 404
    saida = io.StringIO(newline="")
    escritor = csv.writer(saida, delimiter=";")
    escritor.writerow(["ID", "Data/Hora", "Set", "Equipe", "Categoria", "Tipo", "Descrição", "Atleta", "Número", "Autor", "Perfil", "Origem", "Request ID"])
    for evento in eventos:
        auditoria = evento.get("auditoria") or {}
        escritor.writerow([
            evento.get("id"), evento.get("criado_em"), evento.get("set_numero"), evento.get("equipe"),
            evento.get("categoria"), evento.get("tipo"), evento.get("descricao"), evento.get("atleta_nome"),
            evento.get("numero"), evento.get("autor"), auditoria.get("perfil"), auditoria.get("origem"), auditoria.get("request_id"),
        ])
    nome = f"replay_partida_{filtros['partida_id']}.csv"
    return Response("\ufeff" + saida.getvalue(), mimetype="text/csv; charset=utf-8", headers={"Content-Disposition": f'attachment; filename="{nome}"'})
