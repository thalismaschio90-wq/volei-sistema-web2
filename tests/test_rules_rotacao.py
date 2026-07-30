from rules.rotacao import (
    aplicar_recuperacao_saque,
    girar_rotacao,
    normalizar_lado_saque,
    normalizar_rotacao,
    substituir_atleta,
    validar_rotacao,
)


def test_normaliza_dicts_e_completa_seis_posicoes():
    assert normalizar_rotacao([{"numero": 1}, {"camisa": "2"}]) == ["1", "2", "", "", "", ""]


def test_giro_oficial_preserva_ordem_esperada():
    assert girar_rotacao(["4", "3", "2", "5", "6", "1"]) == ["5", "4", "3", "6", "1", "2"]


def test_nao_gira_quando_equipe_mantem_saque():
    resultado = aplicar_recuperacao_saque(
        rotacao_a=["4", "3", "2", "5", "6", "1"],
        rotacao_b=["14", "13", "12", "15", "16", "11"],
        saque_antes="A",
        equipe_pontuadora="A",
    )
    assert resultado["girou"] is False
    assert resultado["rotacao_a"] == ["4", "3", "2", "5", "6", "1"]
    assert resultado["saque_atual"] == "A"


def test_gira_somente_equipe_que_recupera_saque():
    resultado = aplicar_recuperacao_saque(
        rotacao_a=["4", "3", "2", "5", "6", "1"],
        rotacao_b=["14", "13", "12", "15", "16", "11"],
        saque_antes="A",
        equipe_pontuadora="B",
    )
    assert resultado["girou"] is True
    assert resultado["equipe_girou"] == "B"
    assert resultado["rotacao_a"] == ["4", "3", "2", "5", "6", "1"]
    assert resultado["rotacao_b"] == ["15", "14", "13", "16", "11", "12"]


def test_resolve_saque_por_nome_operacional():
    partida = {"equipe_a_operacional": "Time Azul", "equipe_b_operacional": "Time Branco"}
    assert normalizar_lado_saque("Time Branco", partida) == "B"


def test_substituicao_nao_reordena_rotacao():
    assert substituir_atleta(["4", "3", "2", "5", "6", "1"], "2", "9") == ["4", "3", "9", "5", "6", "1"]


def test_validacao_detecta_repetidos():
    validacao = validar_rotacao(["1", "1", "2", "3", "4", "5"])
    assert validacao["ok"] is False
    assert any("Repetidos" in erro for erro in validacao["erros"])
