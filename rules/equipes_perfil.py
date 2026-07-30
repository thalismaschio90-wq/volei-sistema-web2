"""Regras puras para perfil, escudo e nome de equipes."""
from __future__ import annotations

from dataclasses import dataclass


def normalizar_texto(valor: object) -> str:
    return " ".join(str(valor or "").strip().split())


def normalizar_campo_livre(valor: object) -> str:
    return str(valor or "").strip()


@dataclass(frozen=True)
class DadosPerfilEquipe:
    cidade: str
    responsavel: str
    telefone: str
    email: str
    instagram: str

    @property
    def completo(self) -> bool:
        return bool(self.cidade and self.responsavel and self.telefone)


def preparar_perfil_equipe(
    cidade: object = "",
    responsavel: object = "",
    telefone: object = "",
    email: object = "",
    instagram: object = "",
) -> DadosPerfilEquipe:
    return DadosPerfilEquipe(
        cidade=normalizar_texto(cidade),
        responsavel=normalizar_texto(responsavel),
        telefone=normalizar_campo_livre(telefone),
        email=normalizar_campo_livre(email),
        instagram=normalizar_campo_livre(instagram).lstrip("@"),
    )


def validar_renomeacao_equipe(nome_atual: object, competicao: object, novo_nome: object) -> tuple[bool, str, tuple[str, str, str]]:
    atual = normalizar_texto(nome_atual)
    comp = normalizar_texto(competicao)
    novo = normalizar_texto(novo_nome)
    if not atual or not comp or not novo:
        return False, "Informe a equipe, a competição e o novo nome.", (atual, comp, novo)
    if atual.casefold() == novo.casefold():
        return False, "O novo nome é igual ao nome atual.", (atual, comp, novo)
    return True, "", (atual, comp, novo)
