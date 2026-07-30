from pathlib import Path

from tests._template_assets import ler_template_com_assets


def _conteudo() -> str:
    return ler_template_com_assets("templates/jogo_apontador.html")


def test_payload_envia_versao_base_do_estado():
    conteudo = _conteudo()
    assert "getEstadoVersao: () => estadoVersaoServidor" in conteudo
    assert "estado_versao_base: versao" in conteudo
    assert "estado_versao: versao" in conteudo


def test_ack_trata_conflito_e_snapshot_atrasado():
    conteudo = _conteudo()
    assert "resposta.conflito_versao" in conteudo
    assert "resposta.snapshot_atrasado" in conteudo
    assert 'fonte: "socket_conflito_versao"' in conteudo


def test_estado_antigo_e_ignorado_antes_de_aplicar():
    conteudo = _conteudo()
    assert "estadoRecebidoEstaAtrasado(dados, opcoes)" in conteudo
    assert "if (!opcoes.forcarVersao" in conteudo


def test_versao_nao_e_persistida_entre_recargas():
    conteudo = _conteudo()
    estado_utils = (
        Path(__file__).resolve().parents[1]
        / "static"
        / "js"
        / "apontador"
        / "estado-utils.js"
    ).read_text(encoding="utf-8")

    assert "sessionStorage.setItem" not in conteudo
    assert "sessionStorage.setItem" not in estado_utils
    assert "localStorage.setItem" not in estado_utils
