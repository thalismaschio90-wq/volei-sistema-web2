from services.apontadores.papeleta import montar_contexto_papeleta, montar_estado_inicial_jogo


def test_contexto_filtra_atletas_sem_numero():
    contexto = montar_contexto_papeleta(
        competicao="Copa",
        partida={"fase_partida": "papeleta"},
        equipe_a="A",
        equipe_b="B",
        set_atual=1,
        atletas_a=[{"nome": "X", "numero": 1}, {"nome": "Y", "numero": None}],
        atletas_b=[{"nome": "Z", "numero": 2}],
        papeleta_a={i: i for i in range(1, 7)},
        papeleta_b={i: i for i in range(1, 7)},
    )
    assert len(contexto["atletas_a"]) == 1
    assert contexto["fluxo"]["papeleta_a_completa"] is True


def test_estado_inicial_preserva_rotacoes():
    estado = montar_estado_inicial_jogo(
        competicao="Copa",
        partida_id=9,
        partida={"equipe_a": "A", "equipe_b": "B", "sets_a": 1, "sets_b": 0},
        equipe_a="A",
        equipe_b="B",
        set_atual=2,
        rotacao_a=["4", "3", "2", "5", "6", "1"],
        rotacao_b=["10", "9", "8", "11", "12", "7"],
    )
    assert estado["set_atual"] == 2
    assert estado["rotacao_a"][0] == "4"
    assert estado["status_jogo"] == "em_andamento"
