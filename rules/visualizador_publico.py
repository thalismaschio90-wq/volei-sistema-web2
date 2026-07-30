"""Regras puras de apresentação dos eventos no visualizador público.

Este módulo não acessa Flask, Socket.IO nem PostgreSQL. Ele recebe estruturas já
carregadas e transforma os eventos em descrições, linha do tempo, evolução dos
sets e estatísticas por fundamento.
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Sequence


def bool_publico(valor: Any) -> bool:
    if valor is True or valor == 1:
        return True
    if valor is False or valor is None:
        return False
    return str(valor).strip().lower() in {
        "1", "true", "sim", "yes", "on", "avancado", "avançado"
    }


def modo_scout_ativo_publico(partida: Mapping[str, Any] | None, competicao: Mapping[str, Any] | None) -> bool:
    partida = partida or {}
    competicao = competicao or {}
    modo = str(partida.get("modo_operacao") or competicao.get("modo_operacao") or "simples").strip().lower()
    return modo in {"avancado", "avançado", "scout", "com_scout"} or bool_publico(partida.get("scout_ativo"))


def evento_detalhes_publico(ev: Mapping[str, Any] | None) -> dict[str, Any]:
    raw = ev.get("detalhes") if isinstance(ev, Mapping) else None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            dado = json.loads(raw)
            return dado if isinstance(dado, dict) else {}
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
    return {}


def lado_para_nome_publico(partida: Mapping[str, Any] | None, lado: Any) -> str:
    partida = partida or {}
    lado = str(lado or "").strip().upper()
    if lado == "A":
        return str(partida.get("equipe_a_operacional") or partida.get("equipe_a") or "Equipe A")
    if lado == "B":
        return str(partida.get("equipe_b_operacional") or partida.get("equipe_b") or "Equipe B")
    return "Equipe"


def lado_pontuador_evento_publico(ev: Mapping[str, Any]) -> str:
    detalhes = evento_detalhes_publico(ev)
    lado = str(
        detalhes.get("equipe_pontuadora")
        or detalhes.get("equipe_ponto")
        or detalhes.get("lado_ponto")
        or ""
    ).strip().upper()
    if lado in {"A", "B"}:
        return lado

    tipo = str(ev.get("tipo") or ev.get("tipo_evento") or "").strip().lower()
    resultado = str(ev.get("resultado") or detalhes.get("resultado") or detalhes.get("tipo_lance") or "").strip().lower()
    equipe = str(ev.get("equipe") or "").strip().upper()
    if equipe in {"A", "B"}:
        if resultado in {"erro", "falta"}:
            return "B" if equipe == "A" else "A"
        if tipo in {"ponto", "retardamento_penalidade"}:
            return equipe
    return ""


def normalizar_acao_publica(valor: Any) -> str:
    texto = str(valor or "").strip().lower().replace("_", " ").replace("-", " ")
    trocas = {
        "á": "a", "à": "a", "ã": "a", "â": "a",
        "é": "e", "ê": "e", "í": "i",
        "ó": "o", "ô": "o", "õ": "o",
        "ú": "u", "ç": "c",
    }
    for origem, destino in trocas.items():
        texto = texto.replace(origem, destino)
    return " ".join(texto.split())


def evento_eh_acao_negativa_adversario_publico(ev: Mapping[str, Any]) -> bool:
    detalhes = evento_detalhes_publico(ev)
    valores = [
        ev.get("fundamento"), ev.get("resultado"), ev.get("detalhe"), ev.get("tipo"),
        detalhes.get("fundamento"), detalhes.get("resultado"), detalhes.get("tipo_lance"),
        detalhes.get("tipo_erro"), detalhes.get("detalhe_lance"), detalhes.get("detalhe"),
    ]
    texto = " | ".join(normalizar_acao_publica(v) for v in valores if v not in (None, ""))
    marcadores = (
        "erro de saque", "erro saque", "saque errado", "erro geral", "erro", "falta",
        "erro de rotacao", "erro rotacao", "rotacao", "invasao", "conducao", "dois toques",
    )
    return any(marcador in texto for marcador in marcadores)


def lado_responsavel_evento_publico(ev: Mapping[str, Any], lado_ponto: str) -> str:
    if evento_eh_acao_negativa_adversario_publico(ev) and lado_ponto in {"A", "B"}:
        return "B" if lado_ponto == "A" else "A"

    detalhes = evento_detalhes_publico(ev)
    lado_explicito = str(
        detalhes.get("equipe_responsavel")
        or detalhes.get("lado_responsavel")
        or detalhes.get("equipe_autora")
        or detalhes.get("lado_acao")
        or ""
    ).strip().upper()
    if lado_explicito in {"A", "B"}:
        return lado_explicito
    return lado_ponto


def rotulo_fundamento_publico(valor: Any) -> str:
    txt_normalizado = normalizar_acao_publica(valor)
    mapa = {
        "ataque": "Ataque", "bloqueio": "Bloqueio", "saque": "Saque", "ace": "Ace",
        "erro": "Erro geral", "erro geral": "Erro geral", "erro saque": "Erro de saque",
        "erro de saque": "Erro de saque", "erro rotacao": "Erro de rotação",
        "erro de rotacao": "Erro de rotação", "rotacao": "Erro de rotação", "falta": "Falta",
        "invasao": "Invasão", "conducao": "Condução", "dois toques": "Dois toques",
        "levantamento": "Levantamento", "defesa": "Defesa", "recepcao": "Recepção",
    }
    if txt_normalizado in mapa:
        return mapa[txt_normalizado]
    return str(valor or "Ponto").strip().replace("_", " ").title() or "Ponto"


def descricao_evento_publico(ev: Mapping[str, Any], partida: Mapping[str, Any], scout_ativo: bool) -> str:
    detalhes = evento_detalhes_publico(ev)
    tipo = str(ev.get("tipo") or ev.get("tipo_evento") or "").strip().lower()
    fundamento = ev.get("fundamento") or detalhes.get("fundamento") or detalhes.get("detalhe_lance") or detalhes.get("tipo_erro")
    resultado = ev.get("resultado") or detalhes.get("resultado") or detalhes.get("tipo_lance")
    detalhe = ev.get("detalhe") or detalhes.get("detalhe") or detalhes.get("detalhe_lance")
    numero = ev.get("numero") or detalhes.get("atleta_numero") or detalhes.get("numero")
    atleta = ev.get("atleta_nome") or detalhes.get("atleta_nome") or ""
    lado_ponto = lado_pontuador_evento_publico(ev)
    equipe_ponto = lado_para_nome_publico(partida, lado_ponto)
    lado_responsavel = lado_responsavel_evento_publico(ev, lado_ponto)
    equipe_responsavel = lado_para_nome_publico(partida, lado_responsavel)
    acao_negativa = evento_eh_acao_negativa_adversario_publico(ev)

    if tipo not in {"ponto", "retardamento_penalidade"}:
        base = tipo.replace("_", " ").title() if tipo else "Evento"
        if detalhe:
            base += f" • {detalhe}"
        return base
    if not scout_ativo:
        return f"Ponto para {equipe_ponto}"

    acao = rotulo_fundamento_publico(fundamento or resultado or tipo)
    pessoa = f"#{numero} {atleta}" if numero and atleta else (f"#{numero}" if numero else str(atleta or ""))
    if acao_negativa:
        if pessoa:
            return f"{acao} de {pessoa} ({equipe_responsavel}) — ponto para {equipe_ponto}"
        if detalhe and normalizar_acao_publica(detalhe) != normalizar_acao_publica(acao):
            return f"{acao} • {detalhe} ({equipe_responsavel}) — ponto para {equipe_ponto}"
        return f"{acao} da {equipe_responsavel} — ponto para {equipe_ponto}"
    if pessoa:
        return f"{acao} de {pessoa} ({equipe_responsavel})"
    if detalhe and str(detalhe).strip().lower() != str(acao).lower():
        return f"{acao} • {detalhe} ({equipe_responsavel})"
    return f"{acao} ({equipe_responsavel})"


def montar_linha_ponto_publico(
    partida: Mapping[str, Any],
    eventos: Sequence[Mapping[str, Any]] | None,
    scout_ativo: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, int]]]:
    eventos_ordenados = list(reversed(eventos or []))
    placares_por_set: dict[int, dict[str, int]] = {}
    linhas: list[dict[str, Any]] = []
    stats: dict[str, dict[str, int]] = {}

    for ev in eventos_ordenados:
        tipo = str(ev.get("tipo") or ev.get("tipo_evento") or "").strip().lower()
        if tipo not in {"ponto", "retardamento_penalidade"}:
            continue
        try:
            set_num = int(ev.get("set_numero") or 1)
        except (TypeError, ValueError):
            set_num = 1
        atual = placares_por_set.setdefault(set_num, {"a": 0, "b": 0})
        lado = lado_pontuador_evento_publico(ev)
        if lado == "A":
            atual["a"] += 1
        elif lado == "B":
            atual["b"] += 1
        else:
            continue

        detalhes = evento_detalhes_publico(ev)
        fundamento = rotulo_fundamento_publico(
            ev.get("fundamento") or detalhes.get("fundamento") or detalhes.get("detalhe_lance")
            or detalhes.get("tipo_erro") or ev.get("resultado")
        )
        lado_responsavel = lado_responsavel_evento_publico(ev, lado)
        equipe_nome = lado_para_nome_publico(partida, lado_responsavel)
        stats.setdefault(equipe_nome, {})
        stats[equipe_nome][fundamento] = stats[equipe_nome].get(fundamento, 0) + 1
        linhas.append({
            "id": ev.get("id"), "set": set_num, "placar_a": atual["a"], "placar_b": atual["b"],
            "placar": f'{atual["a"]} x {atual["b"]}', "lado_ponto": lado,
            "equipe_ponto": lado_para_nome_publico(partida, lado), "equipe_responsavel": equipe_nome,
            "descricao": descricao_evento_publico(ev, partida, scout_ativo), "fundamento": fundamento,
            "numero": ev.get("numero") or detalhes.get("atleta_numero") or detalhes.get("numero") or "",
            "atleta_nome": ev.get("atleta_nome") or detalhes.get("atleta_nome") or "",
        })

    linhas.sort(key=lambda item: (item["set"], item["id"] or 0), reverse=True)
    evolucao_sets: list[dict[str, Any]] = []
    for set_num in sorted(placares_por_set):
        pontos = [{"placar": "0 x 0", "a": 0, "b": 0}]
        for linha in reversed([item for item in linhas if item["set"] == set_num]):
            pontos.append({"placar": linha["placar"], "a": linha["placar_a"], "b": linha["placar_b"]})
        evolucao_sets.append({"set": set_num, "pontos": pontos})
    return linhas, evolucao_sets, stats
