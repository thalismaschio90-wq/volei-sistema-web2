"""Regras puras do registro de pontos no jogo do apontador."""
from __future__ import annotations

from typing import Any, Mapping


DETALHES_VALIDOS = {
    "ponto": {"ataque", "bloqueio", "ace", "ponto_simples"},
    "erro": {"erro_saque", "erro_geral"},
    "falta": {"rede", "invasao", "rotacao", "conducao", "dois_toques"},
}

DETALHES_COM_ATLETA = {"ataque", "bloqueio", "ace"}


class ErroPonto(ValueError):
    """Erro de validação de um comando de ponto."""


def _texto(valor: Any) -> str:
    return str(valor or "").strip()


def lado_oposto(lado: str) -> str:
    lado = _texto(lado).upper()
    if lado not in {"A", "B"}:
        raise ErroPonto("Equipe inválida.")
    return "B" if lado == "A" else "A"


def normalizar_comando_ponto(dados: Mapping[str, Any]) -> dict[str, str]:
    """Valida e normaliza o comando vindo de formulário ou JSON."""
    equipe = _texto(dados.get("equipe")).upper()
    if equipe not in {"A", "B"}:
        raise ErroPonto("Equipe inválida.")

    fundamento = _texto(dados.get("fundamento")).lower()
    resultado = _texto(dados.get("resultado")).lower()
    tipo_lance = _texto(dados.get("tipo_lance")).lower()
    detalhe_lance = _texto(dados.get("detalhe_lance")).lower()
    tipo_erro = _texto(dados.get("tipo_erro")).lower()
    atleta_numero = _texto(dados.get("atleta_numero"))
    atleta_nome = _texto(dados.get("atleta_nome"))
    atleta_label = _texto(dados.get("atleta_label"))
    responsavel_lado = _texto(dados.get("responsavel_lado")).upper()

    if not tipo_lance:
        raise ErroPonto("Selecione se foi ponto, erro ou falta.")

    if tipo_lance == "ponto_simples":
        tipo_lance = "ponto"
        resultado = "ponto"
        detalhe_lance = detalhe_lance or "ponto_simples"

    if tipo_lance not in DETALHES_VALIDOS:
        raise ErroPonto("Tipo de lance inválido.")

    detalhe_final = (detalhe_lance or tipo_erro or resultado or fundamento).strip().lower()
    if detalhe_final not in DETALHES_VALIDOS[tipo_lance]:
        raise ErroPonto("Detalhe da jogada inválido.")

    exige_atleta = detalhe_final in DETALHES_COM_ATLETA
    if exige_atleta and not atleta_numero:
        raise ErroPonto("Selecione o atleta da jogada.")
    if not exige_atleta:
        atleta_numero = ""
        atleta_nome = ""
        atleta_label = ""

    if tipo_lance in {"erro", "falta"}:
        equipe_scout = responsavel_lado if responsavel_lado in {"A", "B"} else equipe
        equipe_pontuadora = lado_oposto(equipe_scout)
    else:
        equipe_pontuadora = equipe
        equipe_scout = equipe

    return {
        "equipe": equipe,
        "fundamento": fundamento,
        "resultado": resultado,
        "tipo_lance": tipo_lance,
        "detalhe_lance": detalhe_final,
        "tipo_erro": tipo_erro,
        "atleta_numero": atleta_numero,
        "atleta_nome": atleta_nome,
        "atleta_label": atleta_label,
        "responsavel_lado": equipe_scout,
        "equipe_scout": equipe_scout,
        "equipe_pontuadora": equipe_pontuadora,
    }


def detalhes_evento_ponto(comando: Mapping[str, Any]) -> dict[str, str]:
    return {
        "fundamento": _texto(comando.get("detalhe_lance")).lower(),
        "resultado": _texto(comando.get("tipo_lance")).lower(),
        "tipo_lance": _texto(comando.get("tipo_lance")).lower(),
        "detalhe_lance": _texto(comando.get("detalhe_lance")).lower(),
        "tipo_erro": _texto(comando.get("tipo_erro")).lower(),
        "atleta_numero": _texto(comando.get("atleta_numero")),
        "atleta_nome": _texto(comando.get("atleta_nome")),
        "atleta_label": _texto(comando.get("atleta_label")),
        "equipe_pontuadora": _texto(comando.get("equipe_pontuadora")).upper(),
        "equipe_scout": _texto(comando.get("equipe_scout")).upper(),
        "responsavel_lado": _texto(comando.get("equipe_scout")).upper(),
    }
