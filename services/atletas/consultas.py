"""Fachada de consultas de atletas usada pelas rotas."""
from repositories.atletas import (
    contar_atletas_da_equipe,
    listar_atletas_aprovados_da_equipe,
    listar_atletas_da_equipe,
    numero_atleta_disponivel,
    resumir_atletas_da_equipe,
)

__all__ = [
    "contar_atletas_da_equipe",
    "listar_atletas_aprovados_da_equipe",
    "listar_atletas_da_equipe",
    "numero_atleta_disponivel",
    "resumir_atletas_da_equipe",
]
