from services.equipes.conferencia import agrupar_atletas_por_equipe


def test_agrupar_atletas_por_equipe_preserva_ordem():
    agrupado = agrupar_atletas_por_equipe([
        {"id": 1, "equipe": "B", "nome": "Bia"},
        {"id": 2, "equipe": "A", "nome": "Ana"},
        {"id": 3, "equipe": "B", "nome": "Bruna"},
    ])
    assert list(agrupado) == ["B", "A"]
    assert [a["id"] for a in agrupado["B"]] == [1, 3]


def test_agrupar_atletas_sem_equipe():
    agrupado = agrupar_atletas_por_equipe([{"id": 1, "equipe": "", "nome": "Ana"}])
    assert agrupado["Sem equipe"][0]["id"] == 1
