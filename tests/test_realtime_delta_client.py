from pathlib import Path

from tests._template_assets import ler_template_com_assets

ROOT = Path(__file__).resolve().parents[1]


def test_cliente_delta_compartilhado_existe_e_valida_lacuna():
    texto = (ROOT / "static/js/realtime/delta_client.js").read_text(encoding="utf-8")
    assert "VTPRealtimeDelta" in texto
    assert "estado_versao_base" in texto
    assert "lacuna_de_versao" in texto
    assert "onSnapshotRequired" in texto
    assert "__vtp_patch__" in texto
    assert "chaves_removidas" in texto


def test_telas_criticas_assinam_delta_e_carregam_cliente():
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
        assert "js/realtime/delta_client.js" in texto, relativo
        assert "estado_partida_delta" in texto, relativo
        assert "clienteDeltaEstado" in texto, relativo


def test_delta_nao_desliga_eventos_legados_nesta_sprint():
    texto = ler_template_com_assets("templates/jogo_apontador.html")
    assert "registrarHandlersApontador" in texto
    assert "estado_partida" in texto
    assert "estado_jogo_atualizado" in texto

    texto_telao = (ROOT / "templates/placar_profissional.html").read_text(encoding="utf-8")
    assert '"estado_partida"' in texto_telao
    assert '"estado_jogo_atualizado"' in texto_telao
