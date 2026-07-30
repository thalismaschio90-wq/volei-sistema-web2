from services.competicoes.geracao_partidas import (
    quadras_permitidas_para_grupo,
    montar_pools_classificatorios,
)


def test_grupo_com_quadra_fixa_nao_invade_outras():
    grupos = [{"nome": "A", "quadra_id": 1}, {"nome": "B", "quadra_id": None}]
    assert quadras_permitidas_para_grupo(grupos, "A", [1, 2], {}) == [1]
    assert quadras_permitidas_para_grupo(grupos, "B", [1, 2], {}) == [2]


def test_configuracao_compartilhada_respeita_quadras_livres():
    grupos = [{"nome": "A", "quadra_id": 1}, {"nome": "B", "quadra_id": None}]
    config = {"grupos_compartilhados": {"B": [1, 2, 3]}}
    assert quadras_permitidas_para_grupo(grupos, "B", [1, 2, 3], config) == [2, 3]


def test_pools_agrupam_grupos_com_mesmas_quadras():
    grupos = [
        {"nome": "A", "quadra_id": None},
        {"nome": "B", "quadra_id": None},
        {"nome": "C", "quadra_id": 3},
    ]
    pools = montar_pools_classificatorios(grupos, [1, 2, 3], {})
    assert pools[(1, 2)] == ["A", "B"]
    assert pools[(3,)] == ["C"]
