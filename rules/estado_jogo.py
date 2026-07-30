"""Regras puras para montar o estado operacional exibido no jogo.

Este módulo não acessa Flask, banco, cache ou Socket.IO. Ele apenas normaliza
estruturas já carregadas e evita que estado antigo sobrescreva dados oficiais.
"""
from __future__ import annotations

from typing import Any, Iterable


def numero_atleta(valor: Any) -> str:
    if isinstance(valor, dict):
        valor = (
            valor.get("numero")
            or valor.get("camisa")
            or valor.get("numero_camisa")
            or valor.get("atleta_numero")
            or valor.get("n")
            or ""
        )
    return str(valor or "").strip()


def normalizar_atleta(atleta: Any, numero_fallback: Any = "") -> dict[str, Any] | None:
    atleta = dict(atleta or {})
    numero = numero_atleta(
        atleta.get("numero")
        or atleta.get("camisa")
        or atleta.get("numero_camisa")
        or atleta.get("atleta_numero")
        or numero_fallback
    )
    if not numero:
        return None

    atleta["numero"] = numero
    atleta.setdefault("camisa", numero)
    atleta.setdefault("numero_camisa", numero)
    atleta["nome"] = str(atleta.get("nome") or atleta.get("atleta_nome") or "Atleta").strip() or "Atleta"
    return atleta


def mesclar_atletas(
    atletas: Iterable[dict[str, Any]] | None,
    papeleta: dict[int, Any] | None = None,
    rotacao: Any = None,
) -> list[dict[str, Any]]:
    saida: list[dict[str, Any]] = []
    vistos: set[str] = set()

    def adicionar(item: Any, fallback: Any = "") -> None:
        atleta = normalizar_atleta(item, fallback)
        if not atleta:
            return
        numero = atleta["numero"]
        if numero in vistos:
            return
        vistos.add(numero)
        saida.append(atleta)

    for atleta in atletas or []:
        adicionar(atleta)

    if isinstance(papeleta, dict):
        for posicao in range(1, 7):
            valor = papeleta.get(posicao)
            adicionar({"numero": valor, "nome": "Atleta"}, valor)

    if isinstance(rotacao, dict):
        rotacao = rotacao.get("equipe_a") or rotacao.get("equipe_b") or []

    if isinstance(rotacao, (list, tuple)):
        for valor in rotacao:
            adicionar({"numero": valor, "nome": "Atleta"}, valor)

    return saida


def rotacao_por_papeleta(papeleta: dict[int, Any] | None) -> list[str]:
    papeleta = papeleta or {}
    return [
        numero_atleta(papeleta.get(4, "")),
        numero_atleta(papeleta.get(3, "")),
        numero_atleta(papeleta.get(2, "")),
        numero_atleta(papeleta.get(5, "")),
        numero_atleta(papeleta.get(6, "")),
        numero_atleta(papeleta.get(1, "")),
    ]


def rotacao_tem_atletas(rotacao: Any) -> bool:
    if isinstance(rotacao, dict):
        rotacao = rotacao.get("equipe_a") or rotacao.get("equipe_b") or []
    if not isinstance(rotacao, list):
        return False
    return any(numero_atleta(item) for item in rotacao)


def equipes_operacionais(partida: dict[str, Any], estado: dict[str, Any]) -> tuple[str, str]:
    equipe_a = str(
        partida.get("equipe_a_operacional")
        or partida.get("equipe_a")
        or estado.get("equipe_a_operacional")
        or estado.get("equipe_a")
        or ""
    ).strip()
    equipe_b = str(
        partida.get("equipe_b_operacional")
        or partida.get("equipe_b")
        or estado.get("equipe_b_operacional")
        or estado.get("equipe_b")
        or ""
    ).strip()
    return equipe_a, equipe_b


def aplicar_campos_autoritativos(
    estado: dict[str, Any],
    partida: dict[str, Any],
    competicao: str,
    partida_id: int,
) -> dict[str, Any]:
    """Aplica campos oficiais que nunca podem voltar por causa de cache antigo."""
    estado = dict(estado or {})
    estado.setdefault("ok", True)
    estado["competicao"] = competicao
    estado["partida_id"] = partida_id

    equipe_a, equipe_b = equipes_operacionais(partida, estado)
    estado["equipe_a_operacional"] = equipe_a
    estado["equipe_b_operacional"] = equipe_b
    estado["equipe_a"] = equipe_a
    estado["equipe_b"] = equipe_b

    for campo, padrao in (
        ("pontos_a", partida.get("pontos_a") or 0),
        ("pontos_b", partida.get("pontos_b") or 0),
        ("sets_a", partida.get("sets_a") or 0),
        ("sets_b", partida.get("sets_b") or 0),
        ("set_atual", partida.get("set_atual") or 1),
    ):
        estado[campo] = padrao

    estado["placar_a"] = estado.get("placar_a", estado.get("pontos_a", 0))
    estado["placar_b"] = estado.get("placar_b", estado.get("pontos_b", 0))
    estado["saque_atual"] = (
        estado.get("saque_atual")
        or partida.get("saque_atual")
        or partida.get("saque_inicial")
        or ""
    )
    estado.setdefault("status_jogo", str(partida.get("status_jogo") or "em_andamento").strip().lower())
    estado.setdefault("fase_partida", partida.get("fase_partida") or "jogo")
    return estado


def finalizar_estado_operacional(
    estado: dict[str, Any],
    modo_operacao: str,
    papeleta_a: dict[int, Any],
    papeleta_b: dict[int, Any],
) -> dict[str, Any]:
    estado = dict(estado or {})

    if not rotacao_tem_atletas(estado.get("rotacao_a")):
        estado["rotacao_a"] = rotacao_por_papeleta(papeleta_a)
    if not rotacao_tem_atletas(estado.get("rotacao_b")):
        estado["rotacao_b"] = rotacao_por_papeleta(papeleta_b)

    estado["rotacao"] = {
        "equipe_a": estado.get("rotacao_a") or ["", "", "", "", "", ""],
        "equipe_b": estado.get("rotacao_b") or ["", "", "", "", "", ""],
    }

    estado.setdefault("historico", [])
    estado.setdefault("ultima_acao", estado.get("ultima_acao") or "Partida retomada")
    estado.setdefault("evolucao_pontos", estado.get("evolucao_pontos") or [])
    estado.setdefault("scout", estado.get("scout") or {})

    estado["modo_operacao"] = modo_operacao
    estado["modo_operacao_resolvido"] = modo_operacao
    estado["permite_scout"] = modo_operacao == "avancado"
    return estado
