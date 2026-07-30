"""Fachada de consultas de equipes usada pelas rotas."""
from repositories.equipes import (
    buscar_equipe_por_login,
    buscar_equipe_por_nome_e_competicao,
    listar_equipes_da_competicao,
)

__all__ = [
    "buscar_equipe_por_login",
    "buscar_equipe_por_nome_e_competicao",
    "listar_equipes_da_competicao",
]
