"""Regras puras de apresentação e estado visual de partidas.

Este módulo não acessa Flask nem PostgreSQL. Ele concentra normalização de
status, fases, datas, quadras, escudos e parciais para que painel, tabela e
visualizador possam usar exatamente a mesma interpretação.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

ESCUDO_PADRAO = "/static/img/escudo_padrao.svg"

STATUS_FINALIZADO = {
    "finalizada", "finalizado", "encerrado", "encerrada", "partida_encerrada",
}
STATUS_AO_VIVO = {
    "ao_vivo", "ao vivo", "em_andamento", "em andamento", "andamento", "iniciada", "iniciado",
}
STATUS_PRE_JOGO = {"pre_jogo", "pré_jogo", "pre jogo", "pré jogo"}
STATUS_AGUARDANDO = {"aguardando", "agendada", "agendado", "pendente"}


def to_int_or_none(valor: Any) -> int | None:
    try:
        if valor in (None, ""):
            return None
        return int(valor)
    except (TypeError, ValueError):
        return None


def int_seguro(valor: Any, padrao: int = 0) -> int:
    try:
        return int(valor or 0)
    except (TypeError, ValueError):
        return padrao


def normalizar_url_escudo(valor: Any) -> str:
    valor = str(valor or "").strip()
    if not valor:
        return ESCUDO_PADRAO
    if valor.startswith(("http://", "https://", "/static/", "data:")):
        return valor
    valor = valor.replace("\\", "/")
    if valor.startswith("static/"):
        return "/" + valor
    if valor.startswith("uploads/"):
        return "/static/" + valor
    if "/uploads/" in valor:
        return "/static/uploads/" + valor.split("/uploads/", 1)[1]
    return "/static/uploads/escudos/" + valor.lstrip("/")


def mapa_escudos_equipes(equipes: Iterable[Mapping[str, Any]] | None) -> dict[str, str]:
    mapa: dict[str, str] = {}
    for equipe in equipes or []:
        nome = str(equipe.get("nome") or equipe.get("equipe") or equipe.get("nome_equipe") or "").strip()
        login = str(equipe.get("login") or equipe.get("equipe_login") or "").strip()
        escudo = equipe.get("escudo") or equipe.get("escudo_url") or equipe.get("logo") or equipe.get("imagem") or ""
        escudo_url = normalizar_url_escudo(escudo)
        for chave in (nome, login):
            chave = str(chave or "").strip()
            if chave:
                mapa[chave] = escudo_url
                mapa[chave.lower()] = escudo_url
                mapa[chave.upper()] = escudo_url
    return mapa


def buscar_escudo_mapa(mapa_escudos: Mapping[str, str] | None, nome_equipe: Any) -> str:
    nome = str(nome_equipe or "").strip()
    if not nome:
        return ESCUDO_PADRAO
    mapa = mapa_escudos or {}
    return mapa.get(nome) or mapa.get(nome.lower()) or mapa.get(nome.upper()) or ESCUDO_PADRAO


def quadra_label(item: Mapping[str, Any] | None) -> str:
    if not item:
        return "Sem quadra"
    for campo in ("quadra_label", "quadra_exibicao", "quadra_nome"):
        valor = str(item.get(campo) or "").strip()
        if valor:
            return valor
    legado = str(item.get("quadra") or "").strip()
    if legado and not legado.isdigit():
        return legado
    return "Sem quadra"


def status_texto(valor: Any) -> str:
    return str(valor or "").strip().lower().replace("-", "_")


def partida_tem_flag_finalizada(partida: Mapping[str, Any] | None) -> bool:
    if not partida:
        return False
    for campo in ("status", "status_jogo", "fase_partida", "situacao", "estado", "estado_jogo"):
        if status_texto(partida.get(campo)) in STATUS_FINALIZADO:
            return True
    for campo in ("finalizada", "partida_encerrada", "encerrada"):
        valor = partida.get(campo)
        if isinstance(valor, bool) and valor:
            return True
        if isinstance(valor, (int, float)) and int(valor) == 1:
            return True
        if isinstance(valor, str) and valor.strip().lower() in {"1", "true", "sim", "yes", "on"}:
            return True
    return bool(partida.get("finalizado_em") or partida.get("encerrado_em"))


def status_normalizado(partida: Mapping[str, Any]) -> str:
    if partida_tem_flag_finalizada(partida):
        return "finalizada"
    valores = [status_texto(partida.get(c)) for c in ("status", "fase_partida", "status_jogo")]
    for valor in valores:
        if valor in STATUS_AO_VIVO:
            return valor
    for valor in valores:
        if valor in STATUS_PRE_JOGO:
            return "pre_jogo"
    for valor in valores:
        if valor in STATUS_AGUARDANDO:
            return "aguardando"
    return next((v for v in valores if v), "aguardando")


def status_exibicao(partida: Mapping[str, Any]) -> str:
    status = status_normalizado(partida)
    mapa = {
        "pre_jogo": "PRÉ-JOGO", "aguardando": "AGUARDANDO", "agendada": "AGUARDANDO",
        "em andamento": "AO VIVO", "ao vivo": "AO VIVO", "ao_vivo": "AO VIVO",
        "andamento": "AO VIVO", "em_andamento": "AO VIVO",
        "finalizada": "FINALIZADO", "finalizado": "FINALIZADO",
        "encerrado": "FINALIZADO", "encerrada": "FINALIZADO",
    }
    return mapa.get(status, (status or "AGUARDANDO").replace("_", " ").upper())


def partida_esta_finalizada(partida: Mapping[str, Any]) -> bool:
    return partida_tem_flag_finalizada(partida) or status_normalizado(partida) in STATUS_FINALIZADO


def partida_esta_ao_vivo(partida: Mapping[str, Any]) -> bool:
    if partida_esta_finalizada(partida):
        return False
    if status_normalizado(partida) in STATUS_AO_VIVO:
        return True
    if partida.get("jogo_iniciado_em") or partida.get("pre_jogo_iniciado_em"):
        return True
    return any(int_seguro(partida.get(c)) > 0 for c in ("pontos_a", "pontos_b", "placar_a", "placar_b", "sets_a", "sets_b"))


def partida_conta_como_iniciada(partida: Mapping[str, Any]) -> bool:
    status = status_normalizado(partida)
    if status in STATUS_FINALIZADO or status in STATUS_AO_VIVO:
        return True
    if partida.get("pre_jogo_iniciado_em") or partida.get("jogo_iniciado_em") or partida.get("finalizado_em") or partida.get("encerrado_em"):
        return True
    return any(int_seguro(partida.get(c)) > 0 for c in ("pontos_a", "pontos_b", "placar_a", "placar_b", "sets_a", "sets_b"))


def fase_partida_normalizada(partida: Mapping[str, Any]) -> str:
    fase = str(partida.get("fase") or partida.get("fase_partida") or "grupos").strip().lower()
    if fase in {"classificatoria", "classificatorias", "grupo", "grupos"}:
        return "grupos"
    if "oitava" in fase:
        return "oitavas"
    if "quarta" in fase:
        return "quartas"
    if "semi" in fase:
        return "semifinal"
    if "terceiro" in fase or ("3" in fase and "lugar" in fase):
        return "terceiro_lugar"
    if "final" in fase:
        return "final"
    return fase or "grupos"


def filtrar_partidas_por_fase(partidas: Iterable[Mapping[str, Any]], fase_subaba: str | None) -> list[Mapping[str, Any]]:
    alvo = (fase_subaba or "classificatorias").strip().lower()
    equivalencias = {
        "classificatorias": {"grupos"}, "quartas": {"quartas"}, "oitavas": {"oitavas"},
        "semifinais": {"semifinal", "semifinais"}, "semifinal": {"semifinal", "semifinais"},
        "finais": {"final"}, "final": {"final"}, "terceiro_lugar": {"terceiro_lugar"},
    }
    permitidas = equivalencias.get(alvo, set())
    return [p for p in partidas if fase_partida_normalizada(p) in permitidas]


def montar_parciais(partida: Mapping[str, Any]) -> str:
    parciais: list[str] = []
    for i in range(1, 6):
        a, b = partida.get(f"set{i}_a"), partida.get(f"set{i}_b")
        if a is not None and b is not None:
            try:
                parciais.append(f"{int(a)}x{int(b)}")
            except (TypeError, ValueError):
                parciais.append(f"{a}x{b}")
    return " / ".join(parciais) if parciais else "-"


def formatar_data_hora(valor: Any) -> tuple[str, str, str]:
    bruto = str(valor or "").strip()
    normalizado = bruto.replace(" ", "T")
    if len(normalizado) >= 16:
        entrada = normalizado[:16]
        data_p, hora_p = normalizado[:10], normalizado[11:16]
        label = f"{data_p[8:10]}/{data_p[5:7]}/{data_p[0:4]} {hora_p}"
        return bruto, entrada, label
    return bruto, normalizado, normalizado
