from rules.finalizacao import (
    campos_extras_estado_final,
    estado_entre_sets,
    estado_partida_finalizada,
    normalizar_destaque,
    normalizar_estado_final,
    partida_esta_finalizada,
)


def test_normaliza_estado_final_e_placar():
    estado = normalizar_estado_final({"placar_a": "25", "pontos_b": "21", "sets_a": "2", "set_atual": 0})
    assert estado["pontos_a"] == 25
    assert estado["placar_b"] == 21
    assert estado["sets_a"] == 2
    assert estado["set_atual"] == 1


def test_campos_extras_preservam_parciais_e_tipo():
    extras = campos_extras_estado_final({"set1_a": "25", "set1_b": 20, "tipo_encerramento": "normal"})
    assert extras == {"set1_a": 25, "set1_b": 20, "tipo_encerramento": "normal"}


def test_estados_de_fluxo_sao_coerentes():
    entre = estado_entre_sets({"fim_jogo": True})
    final = estado_partida_finalizada({"fim_jogo": False})
    assert entre["status_jogo"] == "entre_sets" and entre["fim_jogo"] is False
    assert final["status_jogo"] == "finalizada" and final["fase_partida"] == "encerrado"


def test_identifica_partida_finalizada_e_wo():
    assert partida_esta_finalizada({"status_jogo": "FINALIZADA"})
    assert partida_esta_finalizada({"tipo_encerramento": "wo"})
    assert not partida_esta_finalizada({"status_jogo": "entre_sets"})


def test_normaliza_destaque():
    destaque = normalizar_destaque({"destaque_lado": " b ", "destaque_nome": " Ana "})
    assert destaque["lado"] == "B"
    assert destaque["nome"] == "Ana"
