from pathlib import Path

from services.relatorios.geracao import _placar, _parciais, _status_finalizada


def test_helpers_relatorios_preservam_comportamento_basico():
    partida = {
        "status": "finalizada",
        "pontos_a": 25,
        "pontos_b": 21,
        "set1_a": 25,
        "set1_b": 21,
    }
    assert _status_finalizada(partida) is True
    assert _placar(partida) == "25 x 21"
    assert _parciais(partida) == "25x21"


def test_rota_relatorios_ficou_apenas_com_coordenacao_http():
    conteudo = Path("routes/relatorios.py").read_text(encoding="utf-8")
    assert "def _montar_relatorio" not in conteudo
    assert "def _pdf_response" not in conteudo
    assert "services.relatorios.geracao" in conteudo
    assert "services.relatorios.pdf" in conteudo
