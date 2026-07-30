from rules.papeleta import (
    montar_dados_papeleta,
    papeleta_completa,
    rotacao_por_papeleta,
    set_operacional_seguro,
)


def _atletas():
    return [{"id": i, "nome": f"A{i}", "numero": i} for i in range(1, 8)]


def test_set_operacional_avanca_pelos_sets_vencidos():
    assert set_operacional_seguro({"set_atual": 1, "sets_a": 1, "sets_b": 0, "sets_max": 3}) == 2


def test_set_operacional_respeita_limite():
    assert set_operacional_seguro({"set_atual": 5, "sets_a": 3, "sets_b": 2, "sets_max": 5}) == 5


def test_montar_dados_e_rotacao():
    valores = {i: str(i) for i in range(1, 7)}
    dados, erros = montar_dados_papeleta(_atletas(), valores)
    assert erros == []
    assert rotacao_por_papeleta(dados) == ["4", "3", "2", "5", "6", "1"]
    assert papeleta_completa({i: i for i in range(1, 7)})


def test_rejeita_atleta_repetido():
    valores = {1: "1", 2: "1", 3: "3", 4: "4", 5: "5", 6: "6"}
    _, erros = montar_dados_papeleta(_atletas(), valores)
    assert any("repetir atleta" in erro for erro in erros)
