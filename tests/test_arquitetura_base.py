from rules.sets import sets_para_vencer, partida_terminou
from rules.cadastro import campos_obrigatorios_atleta
from cache.local_ttl import CacheTTL


def test_sets_para_vencer():
    assert sets_para_vencer(1) == 1
    assert sets_para_vencer(3) == 2
    assert sets_para_vencer(5) == 3
    assert partida_terminou(2, 0, 3)


def test_campos_competicao_rapida():
    campos = campos_obrigatorios_atleta({"competicao_rapida": True})
    assert campos["cpf"] is False


def test_cache_ttl():
    cache = CacheTTL()
    cache.definir("x", 1, 10)
    assert cache.obter("x") == 1
