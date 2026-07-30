from services.apontadores.estado_jogo import carregar_contexto_jogo


def test_servico_prioriza_cache_e_completa_rotacao():
    partida = {
        "equipe_a_operacional": "A",
        "equipe_b_operacional": "B",
        "set_atual": 2,
        "pontos_a": 4,
        "pontos_b": 3,
        "sets_a": 1,
        "sets_b": 0,
    }
    pap_a = {i: str(i) for i in range(1, 7)}
    pap_b = {i: str(i + 10) for i in range(1, 7)}

    resultado = carregar_contexto_jogo(
        competicao="Copa",
        partida_id=1,
        partida=partida,
        modo_local=False,
        modo_operacao="simples",
        obter_cache=lambda _id: {"set_atual": 1},
        buscar_estado_banco=lambda *_: {},
        obter_snapshot_local=lambda *_: {},
        aplicar_escudos=lambda estado, *_: estado,
        buscar_papeletas=lambda *_: ("A", "B", 2, pap_a, pap_b),
        listar_atletas=lambda equipe, _comp: [{"numero": 1 if equipe == "A" else 11, "nome": equipe}],
        aplicar_regras=lambda _id, _comp, estado, _partida: estado,
        aplicar_placar_exibicao=lambda estado, _cfg: estado,
        buscar_competicao=lambda _nome: {},
    )

    assert resultado["ok"] is True
    assert resultado["estado"]["set_atual"] == 2
    assert resultado["estado"]["rotacao_a"] == ["4", "3", "2", "5", "6", "1"]
    assert resultado["estado"]["rotacao_b"] == ["14", "13", "12", "15", "16", "11"]
