from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
SOCKET_SYNC = ROOT / "static/js/apontador/socket-sync.js"
RUNTIME = ROOT / "templates/apontador/components/runtime_assets.html"
COMPONENTES = ROOT / "templates/apontador/components"


def test_socket_sync_exporta_registro_especifico_do_apontador():
    texto = SOCKET_SYNC.read_text(encoding="utf-8")
    assert "function registrarHandlersApontador" in texto
    assert "registrarHandlersApontador," in texto
    for evento in (
        "estado_partida",
        "estado_jogo_atualizado",
        "estado_partida_delta",
        "recuperacao_partida",
        "estado_partida_local_ok",
        "cronometro_tempo",
    ):
        assert evento in texto


def test_registro_socket_remove_handlers_anteriores_antes_de_reinstalar():
    texto = SOCKET_SYNC.read_text(encoding="utf-8")
    assert "REGISTROS_APONTADOR" in texto
    assert "removerEventos(socket, anteriores)" in texto
    assert "socket.off(nome, handler)" in texto


def test_componentes_do_apontador_nao_repetem_ids_html():
    arquivos = [ROOT / "templates/jogo_apontador.html", *COMPONENTES.glob("*.html")]
    encontrados = {}
    duplicados = {}
    padrao = re.compile(r'\bid=["\']([^"\']+)["\']')

    for arquivo in arquivos:
        texto = arquivo.read_text(encoding="utf-8")
        for match in padrao.finditer(texto):
            html_id = match.group(1)
            local = f"{arquivo.relative_to(ROOT)}:{texto.count(chr(10), 0, match.start()) + 1}"
            if html_id in encontrados:
                duplicados.setdefault(html_id, [encontrados[html_id]]).append(local)
            else:
                encontrados[html_id] = local

    assert not duplicados, duplicados


def test_runtime_carrega_socket_sync_antes_do_main():
    texto = RUNTIME.read_text(encoding="utf-8")
    assert texto.index("js/apontador/socket-sync.js") < texto.index("js/apontador/jogo-apontador-main.js")
