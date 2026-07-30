from realtime.live_state import CoordenadorEstadoPartida
from realtime.state_store import LocalEstadoPartidaStore


def test_preparar_publicacao_reutiliza_estado_ja_salvo_sem_incrementar_versao():
    store = LocalEstadoPartidaStore()
    vivo = CoordenadorEstadoPartida(store)
    estado = {"partida_id": 10, "pontos_a": 1, "pontos_b": 0}

    salvo = vivo.salvar(10, estado, atualizar_origem=True)
    assert salvo is not None
    assert salvo.versao == 1
    assert estado["estado_versao"] == 1

    publicacao = vivo.preparar_publicacao(10, estado)
    assert publicacao is not None
    assert publicacao.alterado is False
    assert publicacao.atual.versao == 1
    assert vivo.versao(10) == 1


def test_preparar_publicacao_incrementa_quando_conteudo_muda():
    store = LocalEstadoPartidaStore()
    vivo = CoordenadorEstadoPartida(store)
    vivo.salvar(20, {"pontos_a": 1, "pontos_b": 0})

    publicacao = vivo.preparar_publicacao(20, {"pontos_a": 2, "pontos_b": 0})
    assert publicacao is not None
    assert publicacao.alterado is True
    assert publicacao.anterior is not None
    assert publicacao.anterior.versao == 1
    assert publicacao.atual.versao == 2
    assert vivo.obter(20)["pontos_a"] == 2


def test_metadados_nao_fazem_snapshot_igual_parecer_diferente():
    store = LocalEstadoPartidaStore()
    vivo = CoordenadorEstadoPartida(store)
    vivo.salvar(30, {"pontos_a": 3, "pontos_b": 2})

    publicacao = vivo.preparar_publicacao(
        30,
        {
            "pontos_a": 3,
            "pontos_b": 2,
            "estado_versao": 999,
            "estado_atualizado_em": 0,
        },
    )
    assert publicacao is not None
    assert publicacao.alterado is False
    assert vivo.versao(30) == 1
