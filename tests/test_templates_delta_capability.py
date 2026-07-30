from pathlib import Path

from tests._template_assets import ler_template_com_assets


TEMPLATES = [
    "jogo_apontador.html",
    "primeiro_arbitro.html",
    "segundo_arbitro.html",
    "arbitro_unico.html",
    "placar_profissional.html",
    "visualizador_partida_publica.html",
]


def test_telas_modernas_declaram_suporte_a_delta():
    for nome in TEMPLATES:
        texto = ler_template_com_assets(f"templates/{nome}")
        assert "suporta_delta" in texto, nome
        assert "true" in texto, nome
