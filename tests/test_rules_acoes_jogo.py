import pytest

from rules.acoes_jogo import ErroAcaoJogo, aplicar_acao_local, validar_sancao, validar_tempo


def test_tempo_respeita_limite_configurado():
    with pytest.raises(ErroAcaoJogo, match="não possui mais pedidos"):
        validar_tempo("A", {"tempos_a": 1, "limite_tempos": 1})


def test_tempo_incrementa_e_abre_cronometro():
    estado = aplicar_acao_local({"tempos_a": 0, "limite_tempos": 1}, "tempo", "A", {"duracao": 30})
    assert estado["tempos_a"] == 1
    assert estado["tempo_ativo"] == {"equipe": "A", "duracao": 30}


def test_sancao_exige_alvo_e_tipo_valido():
    with pytest.raises(ErroAcaoJogo):
        validar_sancao("B", {"tipo_pessoa": "atleta", "tipo_sancao": "penalidade"})


def test_sancao_local_nao_altera_placar():
    base = {"pontos_a": 12, "pontos_b": 11}
    estado = aplicar_acao_local(
        base,
        "sancao",
        "B",
        {"tipo_pessoa": "atleta", "numero": "7", "tipo_sancao": "advertencia"},
    )
    assert estado["pontos_a"] == 12
    assert estado["pontos_b"] == 11
    assert estado["sancoes_b"][0]["numero"] == "7"


def test_retardamento_mantem_lista_por_equipe():
    estado = aplicar_acao_local({"set_atual": 2}, "retardamento", "A", {})
    assert estado["retardamentos_a"] == [{"equipe": "A", "set": 2, "observacao": ""}]
