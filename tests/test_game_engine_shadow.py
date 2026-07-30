from game_engine.contracts import ComandoJogo
from game_engine.events import evento_de_comando
from game_engine.reducer import aplicar_evento
from game_engine.service import comparar_ponto_em_modo_sombra


def test_reducer_registra_ponto_e_saque_sem_mutar_origem():
    origem = {"pontos_a": 7, "pontos_b": 5, "saque_atual": "B", "estado_versao": 12}
    comando = ComandoJogo(
        tipo="REGISTRAR_PONTO",
        partida_id=10,
        competicao="Copa",
        dados={"equipe_pontuadora": "A", "fundamento": "ataque"},
        versao_esperada=12,
    )
    resultado = aplicar_evento(origem, evento_de_comando(comando))
    assert origem["pontos_a"] == 7
    assert resultado["pontos_a"] == 8
    assert resultado["placar_a"] == 8
    assert resultado["saque_atual"] == "A"
    assert resultado["estado_versao"] == 13


def test_comparacao_sombra_sem_divergencia():
    resultado = comparar_ponto_em_modo_sombra(
        partida_id=10,
        competicao="Copa",
        comando_ponto={"equipe_pontuadora": "B"},
        estado_anterior={"pontos_a": 4, "pontos_b": 6, "saque_atual": "A"},
        estado_oficial={"pontos_a": 4, "pontos_b": 7, "placar_b": 7, "saque_atual": "B"},
    )
    assert resultado.executado is True
    assert resultado.divergiu is False


def test_comparacao_sombra_detecta_divergencia():
    resultado = comparar_ponto_em_modo_sombra(
        partida_id=10,
        competicao="Copa",
        comando_ponto={"equipe_pontuadora": "A"},
        estado_anterior={"pontos_a": 4, "pontos_b": 6},
        estado_oficial={"pontos_a": 4, "pontos_b": 6, "saque_atual": "B"},
    )
    assert resultado.divergiu is True
    assert "pontos_a" in resultado.divergencias
