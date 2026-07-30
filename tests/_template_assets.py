from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
APONTADOR_MAIN = ROOT / "static" / "js" / "apontador" / "jogo-apontador-main.js"
APONTADOR_SOCKET = ROOT / "static" / "js" / "apontador" / "socket-sync.js"
APONTADOR_REALTIME = ROOT / "static" / "js" / "apontador" / "realtime-controller.js"


def ler_template_com_assets(relativo: str) -> str:
    texto = (ROOT / relativo).read_text(encoding="utf-8")
    if relativo.endswith("jogo_apontador.html"):
        texto += "\n" + APONTADOR_SOCKET.read_text(encoding="utf-8")
        texto += "\n" + APONTADOR_REALTIME.read_text(encoding="utf-8")
        texto += "\n" + APONTADOR_MAIN.read_text(encoding="utf-8")
    return texto
