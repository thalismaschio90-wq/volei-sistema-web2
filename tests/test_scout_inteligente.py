from analytics.scout_inteligente import calcular_scout


def ponto(i, equipe, fundamento, atleta=None):
    return {
        "id": i, "categoria": "ponto", "equipe": equipe, "resultado": equipe,
        "fundamento": fundamento, "set_numero": 1, "atleta_nome": atleta,
        "numero": i if atleta else None, "descricao": fundamento,
        "detalhes": {"equipe_pontuadora": equipe},
    }


def test_calcula_pontos_sequencia_e_ranking():
    eventos = [ponto(1, "A", "ataque", "Ana"), ponto(2, "A", "ace", "Ana"), ponto(3, "B", "bloqueio", "Bia")]
    scout = calcular_scout(eventos, {"equipe_a": "Azul", "equipe_b": "Branca"})
    assert scout["placar_reconstruido"] == {"A": 2, "B": 1}
    assert scout["por_equipe"]["A"]["maior_sequencia"] == 2
    assert scout["ranking_atletas"][0]["pontos"] == 2


def test_conta_acoes_nao_pontuacao():
    eventos = [
        {"id": 1, "categoria": "tempo", "equipe": "A", "set_numero": 1},
        {"id": 2, "categoria": "substituicao", "equipe": "B", "set_numero": 1},
        {"id": 3, "categoria": "disciplina", "equipe": "B", "set_numero": 1},
    ]
    scout = calcular_scout(eventos)
    assert scout["por_equipe"]["A"]["tempos"] == 1
    assert scout["por_equipe"]["B"]["substituicoes"] == 1
    assert scout["por_equipe"]["B"]["disciplina"] == 1
