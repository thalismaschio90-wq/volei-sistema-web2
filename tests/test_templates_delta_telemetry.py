from pathlib import Path

from tests._template_assets import ler_template_com_assets

ROOT = Path(__file__).resolve().parents[1]


def test_telas_declaram_tipo_cliente_delta():
    esperados = {
        "templates/jogo_apontador.html": "apontador",
        "templates/primeiro_arbitro.html": "arbitro_primeiro",
        "templates/segundo_arbitro.html": "arbitro_segundo",
        "templates/arbitro_unico.html": "arbitro_unico",
        "templates/placar_profissional.html": "placar_profissional",
        "templates/visualizador_partida_publica.html": "visualizador_publico",
    }
    for arquivo, tipo in esperados.items():
        texto = ler_template_com_assets(arquivo)
        assert f'clientType: "{tipo}"' in texto


def test_cliente_delta_envia_telemetria_em_lote():
    texto = (ROOT / "static/js/realtime/delta_client.js").read_text(encoding="utf-8")
    assert "eventos: eventos" in texto
    assert "total >= 10" in texto
    assert "setTimeout(descarregarTelemetria, 5000)" in texto
