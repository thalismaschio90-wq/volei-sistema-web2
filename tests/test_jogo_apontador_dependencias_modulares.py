from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "templates" / "apontador" / "components" / "runtime_assets.html"
VALIDADOR = ROOT / "static" / "js" / "apontador" / "modulos-dependencias.js"
MAIN = ROOT / "static" / "js" / "apontador" / "jogo-apontador-main.js"


def test_validador_carrega_imediatamente_antes_do_main():
    html = RUNTIME.read_text(encoding="utf-8")
    scripts = re.findall(r"filename='([^']+\.js)'", html)
    assert "js/apontador/modulos-dependencias.js" in scripts
    assert "js/apontador/jogo-apontador-main.js" in scripts
    assert scripts.index("js/apontador/modulos-dependencias.js") + 1 == scripts.index(
        "js/apontador/jogo-apontador-main.js"
    )


def test_main_valida_dependencias_antes_de_usá_las():
    codigo = MAIN.read_text(encoding="utf-8")
    validacao = codigo.index("window.VTPModulosApontador.validar()")
    bootstrap = codigo.index("window.VolleyTableProApontador.getConfig()")
    assert validacao < bootstrap


def test_manifesto_cobre_modulos_consumidos_pelo_main():
    manifesto = VALIDADOR.read_text(encoding="utf-8")
    codigo = MAIN.read_text(encoding="utf-8")
    consumidos = set(re.findall(r"window\.([A-Z][A-Za-z0-9_]+)", codigo))
    ignorados = {"VTPRealtimeDelta", "VTPRealtimeRenderScheduler", "VTPModulosApontador"}
    consumidos -= ignorados
    faltantes = sorted(nome for nome in consumidos if f'"{nome}"' not in manifesto)
    assert not faltantes, f"Módulos sem declaração de dependência: {faltantes}"
