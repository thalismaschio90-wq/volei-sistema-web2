from pathlib import Path

from tests._template_assets import ler_template_com_assets

ROOT = Path(__file__).resolve().parents[1]


def test_render_scheduler_existe_e_agrega_atualizacoes():
    texto = (ROOT / "static/js/realtime/render_scheduler.js").read_text(encoding="utf-8")
    assert "VTPRealtimeRenderScheduler" in texto
    assert "requestAnimationFrame" in texto
    assert "quantidade_agregada" in texto
    assert "schedule: agendar" in texto


def test_telas_criticas_carregam_e_usam_agendador():
    arquivos = [
        "templates/jogo_apontador.html",
        "templates/primeiro_arbitro.html",
        "templates/segundo_arbitro.html",
        "templates/arbitro_unico.html",
        "templates/placar_profissional.html",
        "templates/visualizador_partida_publica.html",
    ]
    for relativo in arquivos:
        texto = ler_template_com_assets(relativo)
        assert "js/realtime/render_scheduler.js" in texto, relativo
        assert "agendadorRenderDelta" in texto, relativo
        assert ".schedule(novoEstado" in texto, relativo
