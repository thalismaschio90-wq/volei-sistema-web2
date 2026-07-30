from core.n_plus_one import classificar_repeticao, detectar_repeticoes


def test_detecta_repeticao_acima_do_limite():
    itens = [{
        "fingerprint": "abc",
        "operacao": "SELECT",
        "quantidade": 8,
        "duracao_total_ms": 160,
        "duracao_max_ms": 25,
        "origem": "repositories/x.py:func:10",
    }]
    resultado = detectar_repeticoes(itens, limite=4)
    assert len(resultado) == 1
    assert resultado[0]["prioridade"] == "media"
    assert resultado[0]["duracao_media_ms"] == 20.0


def test_ignora_repeticao_abaixo_do_limite():
    assert detectar_repeticoes([{"quantidade": 2}], limite=4) == []


def test_classificacao_alta_por_volume():
    item = classificar_repeticao({
        "fingerprint": "x",
        "quantidade": 25,
        "duracao_total_ms": 100,
        "duracao_max_ms": 5,
    })
    assert item["prioridade"] == "alta"
    assert "JOIN" in item["diagnostico"]
