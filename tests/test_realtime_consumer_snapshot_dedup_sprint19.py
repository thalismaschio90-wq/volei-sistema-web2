from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_arbitros_ignoram_snapshot_legado_da_mesma_versao():
    for nome in ("primeiro_arbitro.html", "segundo_arbitro.html", "arbitro_unico.html"):
        texto = (ROOT / "templates" / nome).read_text(encoding="utf-8")
        assert "function aceitarSnapshotLegado" in texto
        assert "if (!aceitarSnapshotLegado(dados)) return;" in texto


def test_placar_profissional_ignora_snapshot_legado_da_mesma_versao():
    texto = (ROOT / "templates" / "placar_profissional.html").read_text(encoding="utf-8")
    assert "function aceitarSnapshotLegado" in texto
    assert "if (!aceitarSnapshotLegado(dados)) return;" in texto


def test_visualizador_publico_ignora_snapshot_ja_aplicado_por_delta():
    texto = (ROOT / "templates" / "visualizador_partida_publica.html").read_text(encoding="utf-8")
    assert "versaoRecebida <= estadoVersaoSocket" in texto
