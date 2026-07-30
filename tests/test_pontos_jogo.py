import pytest

from rules.pontos_jogo import ErroPonto, normalizar_comando_ponto
from services.apontadores.pontos import (
    montar_payload_socket_ponto,
    montar_resposta_ponto,
    preparar_registro_ponto,
)


def test_ponto_direto_mantem_equipe_e_atleta():
    comando, detalhes = preparar_registro_ponto({
        "equipe": "a",
        "tipo_lance": "ponto",
        "detalhe_lance": "ataque",
        "atleta_numero": "7",
        "atleta_nome": "Ana",
    })
    assert comando["equipe_pontuadora"] == "A"
    assert comando["equipe_scout"] == "A"
    assert detalhes["fundamento"] == "ataque"
    assert detalhes["atleta_numero"] == "7"


def test_erro_da_equipe_a_da_ponto_para_b():
    comando = normalizar_comando_ponto({
        "equipe": "A",
        "tipo_lance": "erro",
        "detalhe_lance": "erro_saque",
    })
    assert comando["equipe_scout"] == "A"
    assert comando["equipe_pontuadora"] == "B"
    assert comando["atleta_numero"] == ""


def test_ponto_simples_nao_exige_atleta():
    comando = normalizar_comando_ponto({"equipe": "B", "tipo_lance": "ponto_simples"})
    assert comando["tipo_lance"] == "ponto"
    assert comando["detalhe_lance"] == "ponto_simples"


def test_ataque_exige_atleta():
    with pytest.raises(ErroPonto, match="Selecione o atleta"):
        normalizar_comando_ponto({"equipe": "A", "tipo_lance": "ponto", "detalhe_lance": "ataque"})


def test_payload_socket_preserva_referencias_cadastro_e_operacional():
    payload = montar_payload_socket_ponto(
        estado={"pontos_a": 5, "ultima_acao": "Ponto A"},
        cache_atual={"pontos_a": 4, "historico": []},
        partida={"equipe_a": "Time 1", "equipe_b": "Time 2", "equipe_a_operacional": "Time 2", "equipe_b_operacional": "Time 1"},
        competicao="Copa",
        partida_id=9,
    )
    assert payload["pontos_a"] == 5
    assert payload["equipe_a_cadastro"] == "Time 1"
    assert payload["equipe_a_operacional"] == "Time 2"


def test_resposta_final_abre_observacoes():
    resposta = montar_resposta_ponto(
        {"partida_finalizada": True, "ultima_acao": "Fim"},
        competicao="Copa",
        partida_id=9,
        url_observacoes="/obs",
    )
    assert resposta["abrir_observacoes"] is True
    assert resposta["url_observacoes"] == "/obs"
