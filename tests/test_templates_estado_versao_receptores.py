from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "templates"


def _conteudo(nome: str) -> str:
    return (TEMPLATES / nome).read_text(encoding="utf-8")


def test_telas_arbitros_descartam_estado_com_versao_antiga():
    for nome in ("arbitro_unico.html", "primeiro_arbitro.html", "segundo_arbitro.html"):
        html = _conteudo(nome)
        assert "let estadoVersaoAplicada" in html
        assert "function extrairVersaoEstado" in html
        assert "function aceitarVersaoEstado" in html
        assert "if (!aceitarVersaoEstado(dados)) return;" in html


def test_placar_profissional_descarta_estado_com_versao_antiga():
    html = _conteudo("placar_profissional.html")
    assert "let estadoVersaoAplicada" in html
    assert "function extrairVersaoEstado" in html
    assert "function aceitarVersaoEstado" in html
    assert "if (!aceitarVersaoEstado(dados)) return;" in html


def test_receptores_mantem_compatibilidade_com_payload_sem_versao():
    for nome in (
        "arbitro_unico.html",
        "primeiro_arbitro.html",
        "segundo_arbitro.html",
        "placar_profissional.html",
    ):
        html = _conteudo(nome)
        assert "if (!recebida) return true" in html
        assert "dados.estado_versao" in html
        assert "interno.estado_versao" in html
