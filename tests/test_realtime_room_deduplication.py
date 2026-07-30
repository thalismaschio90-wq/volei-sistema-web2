from realtime.synchronization import normalizar_entrada, salas_para_entrada


def test_cliente_moderno_entra_apenas_na_sala_canonica_e_delta():
    entrada = normalizar_entrada({
        "partida_id": "123",
        "competicao": "Copa",
        "perfil": "apontador",
        "suporta_delta": True,
    })
    assert salas_para_entrada(entrada) == ["123", "delta:123"]


def test_cliente_legado_entra_na_sala_canonica_e_legacy():
    entrada = normalizar_entrada({"partida_id": "123"})
    assert salas_para_entrada(entrada) == ["123", "legacy:123"]


def test_room_extra_nao_duplica_sala_canonica():
    entrada = normalizar_entrada({"partida_id": "123", "room": "123", "suporta_delta": True})
    assert salas_para_entrada(entrada) == ["123", "delta:123"]
