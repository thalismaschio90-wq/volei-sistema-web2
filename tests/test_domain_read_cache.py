from cache.domain_read import invalidar, limpar_local, obter_ou_carregar


def setup_function():
    limpar_local()


def test_reutiliza_leitura_e_devolve_copia(monkeypatch):
    monkeypatch.setenv("DOMAIN_READ_CACHE_BACKEND", "local")
    monkeypatch.setenv("DOMAIN_READ_CACHE_TTL_SECONDS", "60")
    chamadas = {"n": 0}

    def carregar():
        chamadas["n"] += 1
        return {"dados": {"valor": chamadas["n"]}}

    primeiro = obter_ou_carregar("config", "Copa", "agenda", carregar)
    primeiro["dados"]["valor"] = 999
    segundo = obter_ou_carregar("config", "Copa", "agenda", carregar)

    assert chamadas["n"] == 1
    assert segundo["dados"]["valor"] == 1


def test_invalidacao_isolada_por_entidade(monkeypatch):
    monkeypatch.setenv("DOMAIN_READ_CACHE_BACKEND", "local")
    chamadas = {"A": 0, "B": 0}

    def carregar(nome):
        chamadas[nome] += 1
        return chamadas[nome]

    assert obter_ou_carregar("config", "A", "x", lambda: carregar("A")) == 1
    assert obter_ou_carregar("config", "B", "x", lambda: carregar("B")) == 1
    invalidar("config", "A")
    assert obter_ou_carregar("config", "A", "x", lambda: carregar("A")) == 2
    assert obter_ou_carregar("config", "B", "x", lambda: carregar("B")) == 1


def test_modo_off_nao_reutiliza(monkeypatch):
    monkeypatch.setenv("DOMAIN_READ_CACHE_BACKEND", "off")
    chamadas = {"n": 0}

    def carregar():
        chamadas["n"] += 1
        return chamadas["n"]

    assert obter_ou_carregar("config", "C", "x", carregar) == 1
    assert obter_ou_carregar("config", "C", "x", carregar) == 2
