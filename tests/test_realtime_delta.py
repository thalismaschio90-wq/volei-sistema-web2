from realtime.delta import aplicar_delta_estado, criar_delta_estado, delta_compensa


def test_delta_reconstroi_estado_completo():
    anterior = {
        "pontos_a": 10,
        "pontos_b": 8,
        "rotacao_a": ["1", "2", "3", "4", "5", "6"],
        "meta": {"saque": "A", "temporario": True},
        "campo_antigo": "remover",
    }
    atual = {
        "pontos_a": 11,
        "pontos_b": 8,
        "rotacao_a": ["2", "3", "4", "5", "6", "1"],
        "meta": {"saque": "B", "novo": 1},
    }
    delta = criar_delta_estado(7, anterior, atual, versao_base=4, versao=5)
    reconstruido = aplicar_delta_estado(anterior, delta.payload())
    assert reconstruido["pontos_a"] == 11
    assert reconstruido["rotacao_a"] == atual["rotacao_a"]
    assert reconstruido["meta"] == atual["meta"]
    assert "campo_antigo" not in reconstruido
    assert reconstruido["estado_versao"] == 5


def test_delta_vazio_nao_compensa():
    delta = criar_delta_estado(1, {"a": 1}, {"a": 1}, versao_base=1, versao=2)
    assert delta.vazio is True
    assert delta_compensa(delta) is False


def test_delta_pequeno_economiza_em_estado_grande():
    anterior = {"pontos_a": 1, "eventos": [{"id": i, "texto": "x" * 50} for i in range(100)]}
    atual = {"pontos_a": 2, "eventos": anterior["eventos"]}
    delta = criar_delta_estado(1, anterior, atual, versao_base=8, versao=9)
    assert delta.bytes_delta < delta.bytes_estado
    assert delta_compensa(delta, economia_minima_percentual=10)
