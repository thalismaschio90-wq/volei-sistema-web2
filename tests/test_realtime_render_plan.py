from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def test_render_plan_javascript():
    subprocess.run(["node", str(ROOT / "tests" / "render_plan_node_test.js")], check=True)


def test_telas_carregam_planejador():
    telas = [
        "jogo_apontador.html", "primeiro_arbitro.html", "segundo_arbitro.html",
        "arbitro_unico.html", "placar_profissional.html", "visualizador_partida_publica.html",
    ]
    for nome in telas:
        texto = (ROOT / "templates" / nome).read_text(encoding="utf-8")
        assert "js/realtime/render_plan.js" in texto


def test_placar_e_visualizador_usam_render_seletivo():
    placar = (ROOT / "templates" / "placar_profissional.html").read_text(encoding="utf-8")
    visualizador = (ROOT / "templates" / "visualizador_partida_publica.html").read_text(encoding="utf-8")
    assert "planoRender.placar" in placar
    assert "planoRender.timeline" in placar
    assert "if (plano.topo)" in visualizador
    assert "if (plano.detalhes)" in visualizador
