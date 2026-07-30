from services.relatorios.cache import gerar_com_cache, invalidar_cache_local, chave_relatorio


def test_chave_muda_com_filtro():
    assert chave_relatorio("ranking", "Copa", equipe="A") != chave_relatorio("ranking", "Copa", equipe="B")


def test_reutiliza_resultado(monkeypatch):
    monkeypatch.setenv("RELATORIOS_CACHE_BACKEND", "local")
    monkeypatch.setenv("RELATORIOS_CACHE_TTL_SECONDS", "120")
    invalidar_cache_local()
    chamadas = {"n": 0}

    def gerar():
        chamadas["n"] += 1
        return "Título", ["linha"]

    a = gerar_com_cache("ranking", "Copa", gerar, equipe="A")
    b = gerar_com_cache("ranking", "Copa", gerar, equipe="A")
    assert chamadas["n"] == 1
    assert not a.cache_hit
    assert b.cache_hit
    assert b.linhas == ["linha"]


def test_ignorar_cache_forca_recalculo(monkeypatch):
    monkeypatch.setenv("RELATORIOS_CACHE_BACKEND", "local")
    invalidar_cache_local()
    chamadas = {"n": 0}

    def gerar():
        chamadas["n"] += 1
        return "T", [chamadas["n"]]

    gerar_com_cache("x", "C", gerar)
    resultado = gerar_com_cache("x", "C", gerar, ignorar_cache=True)
    assert chamadas["n"] == 2
    assert resultado.linhas == [2]
