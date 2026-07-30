
from services.competicoes.classificacao import (
    calcular_classificacao,
    normalizar_criterios_classificacao,
    obter_regras_classificacao,
)

def _grupos():
    return [{"grupo": {"nome": "A"}, "equipes": [{"equipe": "Azul"}, {"equipe": "Branca"}]}]

def test_normaliza_criterios_sem_duplicar():
    criterios = normalizar_criterios_classificacao("pontos,vitorias,pontos,saldo_sets")
    assert criterios[:3] == ["pontos", "vitorias", "saldo_sets"]

def test_classificacao_soma_resultado_finalizado():
    partidas = [{
        "grupo": "A", "equipe_a": "Azul", "equipe_b": "Branca",
        "status": "finalizada", "sets_a": 2, "sets_b": 0,
        "set1_a": 25, "set1_b": 20, "set2_a": 25, "set2_b": 18,
    }]
    comp = {"melhor_de": 3, "pontos_vitoria": 3, "pontos_derrota": 0}
    tabela = calcular_classificacao(partidas, _grupos(), comp)
    assert tabela["A"][0]["equipe"] == "Azul"
    assert tabela["A"][0]["vitorias"] == 1
    assert tabela["A"][0]["pontos_pro"] == 50

def test_regras_respeitam_configuracao():
    regras = obter_regras_classificacao({"pontos_vitoria": 4, "pontos_derrota": 1})
    assert regras["pontos_vitoria"] == 4
    assert regras["pontos_derrota"] == 1
