from services.apontadores.rotacao import aplicar_substituicao_estado, rotacao_do_estado, transicao_por_ponto


def test_rotacao_do_estado_aceita_payload_legado():
    estado = {"rotacao": {"equipe_a": [1, 2, 3, 4, 5, 6]}}
    assert rotacao_do_estado(estado, "A") == ["1", "2", "3", "4", "5", "6"]


def test_transicao_por_ponto_resolve_nome_do_saque():
    partida = {
        "saque_atual": "Brancas",
        "equipe_a_operacional": "Azuis",
        "equipe_b_operacional": "Brancas",
    }
    resultado = transicao_por_ponto(
        partida=partida,
        rotacao_a=["4", "3", "2", "5", "6", "1"],
        rotacao_b=["14", "13", "12", "15", "16", "11"],
        equipe_pontuadora="A",
    )
    assert resultado["girou"] is True
    assert resultado["saque_antes"] == "B"
    assert resultado["saque_atual"] == "A"


def test_aplica_substituicao_somente_no_lado_correto():
    estado = {
        "rotacao_a": ["4", "3", "2", "5", "6", "1"],
        "rotacao_b": ["14", "13", "12", "15", "16", "11"],
    }
    novo = aplicar_substituicao_estado(estado, equipe="A", numero_sai="2", numero_entra="9")
    assert novo["rotacao_a"] == ["4", "3", "9", "5", "6", "1"]
    assert novo["rotacao_b"] == estado["rotacao_b"]
