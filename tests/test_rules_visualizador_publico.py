from rules.visualizador_publico import (
    descricao_evento_publico,
    lado_pontuador_evento_publico,
    lado_responsavel_evento_publico,
    montar_linha_ponto_publico,
    modo_scout_ativo_publico,
)


def _partida():
    return {"equipe_a": "PVC", "equipe_b": "Conexão"}


def test_erro_de_saque_atribui_autoria_ao_adversario_do_pontuador():
    evento = {"tipo": "ponto", "equipe": "A", "fundamento": "erro de saque"}
    lado = lado_pontuador_evento_publico(evento)
    assert lado == "A"
    assert lado_responsavel_evento_publico(evento, lado) == "B"
    assert descricao_evento_publico(evento, _partida(), True) == "Erro de saque da Conexão — ponto para PVC"


def test_ataque_mantem_autoria_na_equipe_pontuadora():
    evento = {"tipo": "ponto", "equipe": "A", "fundamento": "ataque", "numero": 7, "atleta_nome": "Ana"}
    assert descricao_evento_publico(evento, _partida(), True) == "Ataque de #7 Ana (PVC)"


def test_modo_simples_esconde_detalhes_do_scout():
    evento = {"tipo": "ponto", "equipe": "B", "fundamento": "ace"}
    assert descricao_evento_publico(evento, _partida(), False) == "Ponto para Conexão"


def test_linha_do_tempo_separa_sets_e_soma_fundamentos():
    eventos = [
        {"id": 3, "tipo": "ponto", "equipe": "A", "set_numero": 2, "fundamento": "ataque"},
        {"id": 2, "tipo": "ponto", "equipe": "B", "set_numero": 1, "fundamento": "ace"},
        {"id": 1, "tipo": "ponto", "equipe": "A", "set_numero": 1, "fundamento": "ataque"},
    ]
    linhas, evolucao, stats = montar_linha_ponto_publico(_partida(), eventos, True)
    assert len(linhas) == 3
    assert [item["set"] for item in evolucao] == [1, 2]
    assert stats["PVC"]["Ataque"] == 2
    assert stats["Conexão"]["Ace"] == 1


def test_modo_scout_aceita_configuracao_da_partida_ou_competicao():
    assert modo_scout_ativo_publico({"scout_ativo": True}, {}) is True
    assert modo_scout_ativo_publico({}, {"modo_operacao": "avancado"}) is True
    assert modo_scout_ativo_publico({}, {"modo_operacao": "simples"}) is False
