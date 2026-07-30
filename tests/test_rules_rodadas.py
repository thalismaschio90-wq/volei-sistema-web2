from rules.rodadas import combinar_data_hora, normalizar_numero_rodada, normalizar_rodada, chave_rodada


def test_combinar_data_hora():
    assert combinar_data_hora("2026-07-27", "14:30") == "2026-07-27T14:30"
    assert combinar_data_hora("2026-07-27", "") == "2026-07-27"
    assert combinar_data_hora("", "14:30") == ""


def test_numero_rodada_seguro():
    assert normalizar_numero_rodada("3") == 3
    assert normalizar_numero_rodada(0) == 1
    assert normalizar_numero_rodada("x") == 1


def test_normalizar_rodada_classificatoria():
    linha = normalizar_rodada("Copa", {"numero": 2, "data": "2026-08-01", "hora": "09:00"})
    assert linha[:6] == ("Copa", "classificatoria", "grupos", "", 2, "Rodada 2")
    assert linha[8] == "2026-08-01T09:00"


def test_chave_rodada():
    assert chave_rodada({"tipo_fase":"AVANCO", "fase":"Final", "serie":"Ouro", "numero_rodada":"1"}) == ("avanco", "final", "ouro", 1)
