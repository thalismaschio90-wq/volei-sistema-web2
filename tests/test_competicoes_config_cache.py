from cache.domain_read import limpar_local
from services.competicoes import configuracao


def setup_function():
    limpar_local()


def test_configuracao_avancada_reutiliza_cache(monkeypatch):
    monkeypatch.setenv("DOMAIN_READ_CACHE_BACKEND", "local")
    chamadas = {"n": 0}

    def buscar(nome):
        chamadas["n"] += 1
        return {"nome": nome, "versao": chamadas["n"]}

    monkeypatch.setattr(configuracao.repo, "buscar_configuracao_avancada", buscar)
    assert configuracao.buscar_configuracao_avancada("Copa")["versao"] == 1
    assert configuracao.buscar_configuracao_avancada("Copa")["versao"] == 1
    assert chamadas["n"] == 1


def test_escrita_invalida_cache(monkeypatch):
    monkeypatch.setenv("DOMAIN_READ_CACHE_BACKEND", "local")
    chamadas = {"n": 0}

    def buscar(nome):
        chamadas["n"] += 1
        return {"versao": chamadas["n"]}

    monkeypatch.setattr(configuracao.repo, "buscar_configuracao_agenda", buscar)
    monkeypatch.setattr(configuracao.repo, "atualizar_configuracao_agenda", lambda nome, **dados: True)

    assert configuracao.buscar_configuracao_agenda("Copa")["versao"] == 1
    assert configuracao.atualizar_configuracao_agenda("Copa", modo_distribuicao="x") is True
    assert configuracao.buscar_configuracao_agenda("Copa")["versao"] == 2
