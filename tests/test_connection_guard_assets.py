from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_connection_guard_asset_exists():
    asset = ROOT / "static/js/realtime/connection_guard.js"
    assert asset.exists()
    text = asset.read_text(encoding="utf-8")
    assert "RealtimeConnectionGuard" in text
    assert "proximoAtraso" in text
    assert "Math.pow(2, this.failures)" in text


def test_critical_templates_load_connection_guard():
    templates = [
        "jogo_apontador.html",
        "primeiro_arbitro.html",
        "segundo_arbitro.html",
        "arbitro_unico.html",
        "placar_profissional.html",
        "visualizador_partida_publica.html",
    ]
    for name in templates:
        text = (ROOT / "templates" / name).read_text(encoding="utf-8")
        assert "js/realtime/connection_guard.js" in text, name


def test_referee_polling_uses_timeout_and_jitter():
    for name in ["primeiro_arbitro.html", "segundo_arbitro.html", "arbitro_unico.html"]:
        text = (ROOT / "templates" / name).read_text(encoding="utf-8")
        assert "guardaConexaoRealtime.proximoAtraso" in text, name
        assert "pollingRapido = setTimeout" in text, name
        assert "randomizationFactor: 0.5" in text, name


def test_public_viewer_uses_adaptive_guard():
    text = (ROOT / "templates/visualizador_partida_publica.html").read_text(encoding="utf-8")
    assert "guardaConexao.proximoAtraso" in text
    assert "guardaConexao.registrarFalha" in text
    assert "guardaConexao.registrarConexao" in text
