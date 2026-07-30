"""Regras puras de entrada e obrigatoriedade para atletas.

Este módulo não abre conexão, não conhece Flask e não emite Socket.IO. Ele pode
ser reutilizado pelas rotas, serviços, importações e testes sem efeitos colaterais.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DadosAtletaNormalizados:
    nome: str
    cpf_informado: str
    data_nascimento: str
    numero: int | None
    equipe: str
    competicao: str
    foto_atleta: str
    instagram: str


def normalizar_instagram(valor: Any) -> str:
    instagram = str(valor or "").strip()
    if not instagram:
        return ""
    return "@" + instagram.lstrip("@")


def normalizar_numero(valor: Any) -> tuple[bool, int | None, str]:
    if valor in (None, ""):
        return True, None, ""
    try:
        numero = int(valor)
    except (TypeError, ValueError):
        return False, None, "Número inválido."
    if numero < 0:
        return False, None, "Número inválido."
    return True, numero, ""


def normalizar_dados_atleta(
    *,
    nome: Any,
    cpf: Any,
    data_nascimento: Any,
    numero: Any = None,
    equipe: Any = "",
    competicao: Any = "",
    foto_atleta: Any = "",
    instagram: Any = "",
) -> tuple[bool, DadosAtletaNormalizados | None, str]:
    ok_numero, numero_normalizado, erro_numero = normalizar_numero(numero)
    if not ok_numero:
        return False, None, erro_numero

    dados = DadosAtletaNormalizados(
        nome=str(nome or "").strip(),
        cpf_informado=str(cpf or "").strip(),
        data_nascimento=str(data_nascimento or "").strip(),
        numero=numero_normalizado,
        equipe=str(equipe or "").strip(),
        competicao=str(competicao or "").strip(),
        foto_atleta=str(foto_atleta or "").strip(),
        instagram=normalizar_instagram(instagram),
    )
    return True, dados, ""


def validar_campos_basicos_cadastro(nome: str, cpf_limpo: str, data_nascimento: str) -> tuple[bool, str]:
    if not nome or not cpf_limpo:
        return False, "Informe nome e CPF do atleta."
    if not data_nascimento:
        return False, "Informe a data de nascimento do atleta."
    return True, ""


def validar_campos_basicos_edicao(nome: str, cpf_informado: str, data_nascimento: str) -> tuple[bool, str]:
    if not nome or not cpf_informado or not data_nascimento:
        return False, "Preencha nome, CPF e data de nascimento."
    return True, ""


def pendencias_obrigatorias(
    *,
    exigir_foto: bool,
    exigir_instagram: bool,
    foto_atleta: str,
    instagram: str,
) -> list[str]:
    pendencias: list[str] = []
    if exigir_foto and not str(foto_atleta or "").strip():
        pendencias.append("foto")
    if exigir_instagram and not str(instagram or "").strip():
        pendencias.append("Instagram")
    return pendencias


def mensagem_pendencias_obrigatorias(pendencias: list[str], *, acao: str) -> str:
    if not pendencias:
        return ""
    complemento = "concluir a inscrição" if acao == "cadastro" else "salvar"
    return (
        "Esta competição exige "
        + " e ".join(pendencias)
        + f" dos atletas. Preencha antes de {complemento}."
    )
