from realtime.event_history import LocalHistoricoDeltaStore
from realtime.recovery import recuperar_estado
from realtime.state_store import LocalEstadoPartidaStore


def _delta(base, versao, valor):
    return {
        "payload_delta": True,
        "partida_id": "10",
        "estado_versao_base": base,
        "estado_versao": versao,
        "patch": {"pontos_a": valor},
        "chaves_removidas": [],
    }


def test_recupera_intervalo_contiguo_sem_snapshot():
    estado = LocalEstadoPartidaStore()
    historico = LocalHistoricoDeltaStore(max_eventos=20)
    for valor in range(1, 4):
        estado.salvar("10", {"pontos_a": valor})
    historico.registrar("10", _delta(1, 2, 2))
    historico.registrar("10", _delta(2, 3, 3))

    resultado = recuperar_estado("10", 1, state_store=estado, history_store=historico, limite=10)

    assert resultado.modo == "eventos"
    assert [item["estado_versao"] for item in resultado.eventos] == [2, 3]
    assert resultado.snapshot is None


def test_usa_snapshot_quando_existe_lacuna():
    estado = LocalEstadoPartidaStore()
    historico = LocalHistoricoDeltaStore(max_eventos=20)
    for valor in range(1, 4):
        estado.salvar("10", {"pontos_a": valor})
    historico.registrar("10", _delta(2, 3, 3))

    resultado = recuperar_estado("10", 1, state_store=estado, history_store=historico, limite=10)

    assert resultado.modo == "snapshot"
    assert resultado.snapshot["pontos_a"] == 3
    assert resultado.snapshot["estado_versao"] == 3


def test_cliente_atualizado_nao_recebe_payload_grande():
    estado = LocalEstadoPartidaStore()
    historico = LocalHistoricoDeltaStore(max_eventos=20)
    salvo = estado.salvar("10", {"pontos_a": 1})

    resultado = recuperar_estado("10", salvo.versao, state_store=estado, history_store=historico)

    assert resultado.modo == "atualizado"
    assert resultado.eventos == ()
    assert resultado.snapshot is None


def test_historico_local_limita_janela_e_copia_dados():
    historico = LocalHistoricoDeltaStore(max_eventos=10)
    for versao in range(1, 13):
        historico.registrar("10", _delta(versao - 1, versao, versao))

    itens = historico.recuperar("10", 0, limite=20)
    assert len(itens) == 10
    assert itens[0]["estado_versao"] == 3
    itens[0]["patch"]["pontos_a"] = 999
    novamente = historico.recuperar("10", 0, limite=20)
    assert novamente[0]["patch"]["pontos_a"] == 3
