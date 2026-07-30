from realtime.rooms import sala_delta, sala_legacy
from realtime.synchronization import normalizar_entrada, salas_para_entrada


def test_cliente_moderno_entra_na_sala_delta_e_nao_na_legacy():
    entrada = normalizar_entrada({
        "partida_id": 12,
        "competicao": "Copa",
        "suporta_delta": True,
        "perfil": "apontador",
    })
    salas = salas_para_entrada(entrada)
    assert sala_delta(12) in salas
    assert sala_legacy(12) not in salas
    assert entrada.suporta_delta is True


def test_cliente_antigo_entra_na_sala_legacy():
    entrada = normalizar_entrada({"partida_id": 12, "competicao": "Copa"})
    salas = salas_para_entrada(entrada)
    assert sala_legacy(12) in salas
    assert sala_delta(12) not in salas
    assert entrada.suporta_delta is False


def test_alias_supports_delta_e_aceito():
    entrada = normalizar_entrada({"partida_id": "9", "supports_delta": "true"})
    assert entrada.suporta_delta is True
    assert sala_delta("9") in salas_para_entrada(entrada)
