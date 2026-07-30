from rules.pre_jogo import (
    equipe_do_lado,
    fase_fluxo_pre_jogo,
    validar_numeracoes_conferencia,
)
from services.apontadores.pre_jogo import preparar_alteracoes_numeracao


def test_equipe_do_lado_prefere_operacional():
    partida = {"equipe_a": "Original", "equipe_a_operacional": "Operacional"}
    assert equipe_do_lado(partida, "a") == "Operacional"


def test_fase_fluxo_normaliza_agendada():
    assert fase_fluxo_pre_jogo({"status_jogo": "Agendada"}) == "pre_jogo"


def test_validar_numeracoes_detecta_repetidas():
    atletas = {"1": {"nome": "Ana"}, "2": {"nome": "Bia"}}
    _, erros = validar_numeracoes_conferencia(atletas, ["1", "2"], {"1": "7", "2": "7"})
    assert len(erros) == 1
    assert "número 7" in erros[0].lower()


def test_preparar_alteracoes_ignora_numero_igual():
    atletas = [{"id": 1, "numero": 7, "nome": "Ana"}]
    alteracoes, erros = preparar_alteracoes_numeracao(atletas=atletas, ids=["1"], valores={"1": "7"})
    assert erros == []
    assert alteracoes == []
