from core.request_cache import armazenar, escopo_cache, obter


def test_cache_isolado_por_escopo_e_copia_defensiva():
    with escopo_cache():
        armazenar(("x", 1), [{"valor": 10}])
        dado = obter(("x", 1))
        dado[0]["valor"] = 99
        assert obter(("x", 1))[0]["valor"] == 10
    assert obter(("x", 1)) is None


def test_escopos_aninhados_reutilizam_mesmo_cache():
    with escopo_cache():
        armazenar("chave", "valor")
        with escopo_cache():
            assert obter("chave") == "valor"
