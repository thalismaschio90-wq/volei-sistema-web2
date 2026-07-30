from rules.grupos_estrutura import (
    distribuir_equipes_balanceado,
    estrutura_grupo_unico,
    nomes_equipes_unicos,
    nomes_grupos_automaticos,
    qtd_grupos_configurada,
    resumo_distribuicao,
)


def test_estrutura_grupo_unico_respeita_configuracao():
    assert estrutura_grupo_unico({"qtd_grupos": 1, "tem_grupos": True}) is True
    assert estrutura_grupo_unico({"qtd_grupos": 4, "tem_grupos": False}) is True
    assert estrutura_grupo_unico({"qtd_grupos": 4, "tem_grupos": True}) is False


def test_nomes_grupos_limitados_a_26():
    nomes = nomes_grupos_automaticos(50)
    assert nomes[0] == "A"
    assert nomes[-1] == "Z"
    assert len(nomes) == 26


def test_qtd_grupos_configurada_normaliza_limites():
    assert qtd_grupos_configurada({"qtd_grupos": 0, "tem_grupos": True}) == 1
    assert qtd_grupos_configurada({"qtd_grupos": 40, "tem_grupos": True}) == 26


def test_equipes_unicas_e_distribuicao_balanceada():
    equipes = nomes_equipes_unicos([
        {"nome": "Time A"},
        {"equipe": "time a"},
        {"nome": "Time B"},
        {"nome": "Time C"},
    ])
    grupos = [{"id": 1, "nome": "A"}, {"id": 2, "nome": "B"}]
    distribuicao = distribuir_equipes_balanceado(equipes, grupos)
    assert equipes == ["Time A", "Time B", "Time C"]
    assert [d["grupo_id"] for d in distribuicao] == [1, 2, 1]
    assert resumo_distribuicao(distribuicao, grupos) == "A: 2, B: 1"
