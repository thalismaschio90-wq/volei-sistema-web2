from realtime.rooms import sala_placar_apontador, salas_partida
from realtime.state_store import LocalEstadoPartidaStore


def test_store_nao_expoe_referencia_mutavel():
    store = LocalEstadoPartidaStore()
    original = {"rotacao_a": ["1", "2", "3", "4", "5", "6"]}
    salvo = store.salvar(10, original)
    original["rotacao_a"][0] = "99"
    obtido = store.obter(10)
    obtido["rotacao_a"][1] = "88"
    assert salvo.versao == 1
    assert store.obter(10)["rotacao_a"] == ["1", "2", "3", "4", "5", "6"]


def test_store_incrementa_versao_monotonica():
    store = LocalEstadoPartidaStore()
    assert store.salvar("7", {"pontos_a": 1}).versao == 1
    assert store.salvar("7", {"pontos_a": 2}).versao == 2
    assert store.versao("7") == 2
    assert store.obter("7")["estado_versao"] == 2


def test_store_remove_estado_e_metadados():
    store = LocalEstadoPartidaStore()
    store.salvar(3, {"ok": True})
    store.remover(3)
    assert store.obter(3) is None
    assert store.versao(3) == 0


def test_salas_sao_estaveis_e_sem_duplicacao():
    salas = salas_partida(12, "Copa")
    assert salas[0] == "12"
    assert "partida:Copa:12" in salas
    assert "arbitros_Copa_12" in salas
    assert len(salas) == len(set(salas))
    assert sala_placar_apontador(" andre ") == "placar_apontador:andre"
