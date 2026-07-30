from services.relatorios.cache import (
    chave_relatorio,
    gerar_com_cache,
    invalidar_cache_competicao,
    invalidar_cache_local,
    versao_cache_competicao,
)


def test_invalidacao_muda_namespace_e_forca_recalculo(monkeypatch):
    monkeypatch.setenv("RELATORIOS_CACHE_BACKEND", "local")
    monkeypatch.setenv("RELATORIOS_CACHE_TTL_SECONDS", "120")
    invalidar_cache_local()
    chamadas = {"n": 0}

    def gerar():
        chamadas["n"] += 1
        return "Ranking", [{"valor": chamadas["n"]}]

    primeiro = gerar_com_cache("ranking", "Copa Serra", gerar)
    segundo = gerar_com_cache("ranking", "Copa Serra", gerar)
    assert primeiro.cache_hit is False
    assert segundo.cache_hit is True
    assert chamadas["n"] == 1

    versao_anterior = versao_cache_competicao("Copa Serra")
    nova_versao = invalidar_cache_competicao("Copa Serra")
    assert nova_versao == versao_anterior + 1

    terceiro = gerar_com_cache("ranking", "Copa Serra", gerar)
    assert terceiro.cache_hit is False
    assert chamadas["n"] == 2


def test_invalidacao_e_isolada_por_competicao(monkeypatch):
    monkeypatch.setenv("RELATORIOS_CACHE_BACKEND", "local")
    invalidar_cache_local()
    chave_a_antes = chave_relatorio("ranking", "Copa A")
    chave_b_antes = chave_relatorio("ranking", "Copa B")

    invalidar_cache_competicao("Copa A")

    assert chave_relatorio("ranking", "Copa A") != chave_a_antes
    assert chave_relatorio("ranking", "Copa B") == chave_b_antes


def test_nome_da_competicao_e_normalizado(monkeypatch):
    monkeypatch.setenv("RELATORIOS_CACHE_BACKEND", "local")
    invalidar_cache_local()
    assert chave_relatorio("ranking", "  Copa   Serra ") == chave_relatorio("ranking", "copa serra")
