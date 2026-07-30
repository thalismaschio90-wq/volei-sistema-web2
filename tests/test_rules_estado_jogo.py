from rules.estado_jogo import (
    aplicar_campos_autoritativos,
    finalizar_estado_operacional,
    mesclar_atletas,
    rotacao_por_papeleta,
)


def test_campos_do_banco_vencem_cache_antigo():
    estado = {"set_atual": 1, "pontos_a": 3, "sets_a": 0, "equipe_a": "Antiga"}
    partida = {
        "equipe_a_operacional": "A",
        "equipe_b_operacional": "B",
        "set_atual": 2,
        "pontos_a": 8,
        "pontos_b": 6,
        "sets_a": 1,
        "sets_b": 0,
    }
    novo = aplicar_campos_autoritativos(estado, partida, "Copa", 10)
    assert novo["set_atual"] == 2
    assert novo["pontos_a"] == 8
    assert novo["sets_a"] == 1
    assert novo["equipe_a"] == "A"


def test_rotacao_fallback_e_modo_operacao():
    pap = {1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6"}
    estado = finalizar_estado_operacional({}, "avancado", pap, pap)
    assert estado["rotacao_a"] == ["4", "3", "2", "5", "6", "1"]
    assert estado["permite_scout"] is True


def test_mescla_elenco_sem_duplicar_numero():
    atletas = [{"nome": "Ana", "numero": 1}]
    pap = {1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6"}
    resultado = mesclar_atletas(atletas, pap, rotacao_por_papeleta(pap))
    assert [a["numero"] for a in resultado] == ["1", "2", "3", "4", "5", "6"]
