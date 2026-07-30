"""Núcleo experimental do motor de jogo do VolleyTablePro.

Nesta primeira etapa o motor opera apenas em modo sombra: calcula o efeito
esperado de um comando sem alterar banco, cache ou Socket.IO.
"""

from .service import comparar_ponto_em_modo_sombra

__all__ = ["comparar_ponto_em_modo_sombra"]
