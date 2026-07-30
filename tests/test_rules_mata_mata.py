from rules.mata_mata import montar_confrontos_mata_mata


def _classificacao(qtd=8):
    return {
        "A": [{"equipe": f"A{i}"} for i in range(1, qtd // 2 + 1)],
        "B": [{"equipe": f"B{i}"} for i in range(1, qtd // 2 + 1)],
    }


def test_quartas_intercaladas():
    r = montar_confrontos_mata_mata("quartas", classificacao=_classificacao())
    assert r["ok"] is True
    assert r["confrontos"] == [("A1", "B4"), ("B2", "A3"), ("B1", "A4"), ("A2", "B3")]


def test_quartas_exige_oito():
    r = montar_confrontos_mata_mata("quartas", classificacao={"A": [{"equipe": "A1"}]})
    assert r["ok"] is False


def test_semifinal_vem_das_quartas():
    quartas = [{"id": i, "ordem": i} for i in range(1, 5)]
    r = montar_confrontos_mata_mata("semifinal", quartas=quartas, resolver_vencedor=lambda p, ph: f"V{p['id']}")
    assert r["confrontos"] == [("V1", "V2"), ("V3", "V4")]


def test_final_e_terceiro_lugar():
    semis = [
        {"id": 1, "ordem": 1, "equipe_a": "A", "equipe_b": "B"},
        {"id": 2, "ordem": 2, "equipe_a": "C", "equipe_b": "D"},
    ]
    vencedor = lambda p, ph: "A" if p["id"] == 1 else "D"
    final = montar_confrontos_mata_mata("final", semifinais=semis, resolver_vencedor=vencedor)
    terceiro = montar_confrontos_mata_mata("terceiro_lugar", semifinais=semis, resolver_vencedor=vencedor)
    assert final["confrontos"] == [("A", "D")]
    assert terceiro["confrontos"] == [("B", "C")]
