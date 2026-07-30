from game_engine.contracts import ComandoJogo
from game_engine.events import evento_ponto_registrado
from game_engine.reducer import aplicar_evento
from game_engine.service import comparar_ponto_em_modo_sombra


def test_reducer_ponto_incrementa_apenas_lado_e_define_saque():
    comando = ComandoJogo(
        comando_id="cmd-1",
        partida_id=7,
        competicao="Copa",
        tipo="REGISTRAR_PONTO",
        versao_esperada=10,
        dados={"equipe_pontuadora": "A", "detalhe_lance": "ataque"},
    )
    evento = evento_ponto_registrado(comando, sequencia=11)
    estado = aplicar_evento({"pontos_a": 4, "pontos_b": 3, "saque_atual": "B"}, evento)

    assert estado["pontos_a"] == 5
    assert estado["placar_a"] == 5
    assert estado["pontos_b"] == 3
    assert estado["saque_atual"] == "A"
    assert estado["estado_versao"] == 11


def test_modo_sombra_aprova_estado_oficial_equivalente():
    resultado = comparar_ponto_em_modo_sombra(
        partida_id=7,
        competicao="Copa",
        comando_ponto={"equipe_pontuadora": "B", "detalhe_lance": "erro_saque"},
        estado_anterior={"pontos_a": 8, "pontos_b": 9, "saque_atual": "A", "estado_versao": 20},
        estado_oficial={"pontos_a": 8, "pontos_b": 10, "saque_atual": "B", "estado_versao": 21},
    )

    assert resultado.executado is True
    assert resultado.divergencias == {}
    assert resultado.estado_previsto is not None
    assert resultado.estado_previsto["estado_versao"] == 21


def test_modo_sombra_detecta_divergencia_sem_alterar_estado_oficial():
    oficial = {"pontos_a": 3, "pontos_b": 4, "saque_atual": "B", "estado_versao": 6}
    resultado = comparar_ponto_em_modo_sombra(
        partida_id=8,
        competicao="Copa",
        comando_ponto={"equipe_pontuadora": "A", "detalhe_lance": "ace"},
        estado_anterior={"pontos_a": 3, "pontos_b": 4, "saque_atual": "B", "estado_versao": 5},
        estado_oficial=oficial,
    )

    assert resultado.executado is True
    assert "pontos_a" in resultado.divergencias or "placar_a" in resultado.divergencias
    assert oficial == {"pontos_a": 3, "pontos_b": 4, "saque_atual": "B", "estado_versao": 6}
