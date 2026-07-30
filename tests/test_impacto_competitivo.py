from analytics.impacto_competitivo import calcular_impacto_competitivo


def ponto(i, equipe, fundamento, set_numero=1, atleta=None, numero=None):
    return {
        "id": i,
        "categoria": "ponto",
        "equipe": equipe,
        "resultado": equipe,
        "fundamento": fundamento,
        "set_numero": set_numero,
        "atleta_nome": atleta,
        "numero": numero,
        "descricao": fundamento,
        "detalhes": {"equipe_pontuadora": equipe},
    }


def test_comparativo_equipes_e_tendencias_por_set():
    eventos = [
        ponto(1, "A", "ataque", 1), ponto(2, "A", "ace", 1), ponto(3, "B", "erro de saque", 1),
        ponto(4, "B", "bloqueio", 2), ponto(5, "B", "ataque", 2), ponto(6, "B", "ace", 2),
    ]
    analise = calcular_impacto_competitivo(eventos, {"equipe_a": "Azul", "equipe_b": "Branca"})
    assert analise["comparativo_equipes"]["A"]["pontos"] == 2
    assert analise["comparativo_equipes"]["A"]["acoes_proprias"] == 2
    assert len(analise["tendencias_por_set"]) == 2
    assert analise["tendencias_por_set"][1]["equipes"]["B"]["maior_sequencia"] == 3


def test_ranking_impacto_prioriza_ace_e_ponto_decisivo():
    eventos = []
    i = 1
    for _ in range(15):
        eventos.append(ponto(i, "A", "ataque", 1, "Ana", 10)); i += 1
        eventos.append(ponto(i, "B", "ataque", 1, "Bia", 8)); i += 1
    eventos.append(ponto(i, "A", "ace", 1, "Ana", 10))
    analise = calcular_impacto_competitivo(eventos)
    assert analise["ranking_impacto"][0]["atleta"] == "#10 Ana"
    assert analise["ranking_impacto"][0]["impacto"] > analise["ranking_impacto"][1]["impacto"]
    assert analise["ranking_impacto"][0]["pontos_decisivos"] >= 1


def test_cobertura_sem_dados_de_atleta():
    analise = calcular_impacto_competitivo([ponto(1, "A", "ataque")])
    assert analise["cobertura"]["pontos_identificados"] == 1
    assert analise["cobertura"]["percentual_pontos_com_atleta"] == 0.0


def test_conta_acoes_operacionais_por_set():
    eventos = [
        {"categoria": "tempo", "equipe": "A", "set_numero": 1},
        {"categoria": "substituicao", "equipe": "A", "set_numero": 1},
        ponto(3, "A", "ataque", 1),
    ]
    analise = calcular_impacto_competitivo(eventos)
    dados = analise["tendencias_por_set"][0]["equipes"]["A"]
    assert dados["tempo"] == 1
    assert dados["substituicao"] == 1
