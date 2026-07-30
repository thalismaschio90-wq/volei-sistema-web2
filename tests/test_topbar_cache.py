from services.ui.topbar import (
    buscar_equipe_topbar,
    invalidar_topbar,
    limpar_cache_topbar_local,
)


def setup_function():
    limpar_cache_topbar_local()


def test_reutiliza_resultado_sem_nova_consulta(monkeypatch):
    monkeypatch.setenv("TOPBAR_CACHE_BACKEND", "local")
    monkeypatch.setenv("TOPBAR_CACHE_TTL_SECONDS", "60")
    chamadas = []

    def buscar(login, competicao):
        chamadas.append((login, competicao))
        return {"nome": "Equipe Teste", "escudo": "logo"}

    primeiro = buscar_equipe_topbar("login", "Copa", buscar)
    segundo = buscar_equipe_topbar("login", "Copa", buscar)

    assert primeiro == segundo
    assert chamadas == [("login", "Copa")]


def test_fallback_global_eh_cacheado(monkeypatch):
    monkeypatch.setenv("TOPBAR_CACHE_BACKEND", "local")
    chamadas = []

    def buscar(login, competicao):
        chamadas.append(competicao)
        return None if competicao else {"nome": "Global"}

    assert buscar_equipe_topbar("login", "Copa", buscar)["nome"] == "Global"
    assert buscar_equipe_topbar("login", "Copa", buscar)["nome"] == "Global"
    assert chamadas == ["Copa", None]


def test_invalidacao_forca_nova_consulta(monkeypatch):
    monkeypatch.setenv("TOPBAR_CACHE_BACKEND", "local")
    contador = {"valor": 0}

    def buscar(login, competicao):
        contador["valor"] += 1
        return {"nome": f"Equipe {contador['valor']}"}

    assert buscar_equipe_topbar("login", None, buscar)["nome"] == "Equipe 1"
    invalidar_topbar()
    assert buscar_equipe_topbar("login", None, buscar)["nome"] == "Equipe 2"


def test_devolve_copia_defensiva(monkeypatch):
    monkeypatch.setenv("TOPBAR_CACHE_BACKEND", "local")

    def buscar(login, competicao):
        return {"nome": "Equipe", "dados": {"x": 1}}

    primeiro = buscar_equipe_topbar("login", None, buscar)
    primeiro["dados"]["x"] = 99
    segundo = buscar_equipe_topbar("login", None, buscar)
    assert segundo["dados"]["x"] == 1
