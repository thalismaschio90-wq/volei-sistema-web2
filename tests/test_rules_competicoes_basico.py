from rules.competicoes_basico import (
    CRITERIOS_DESEMPATE_PADRAO,
    normalizar_dados_gerais,
    normalizar_estrutura,
    normalizar_pontuacao_desempate,
    normalizar_regras_jogo,
)


def test_dados_gerais_normalizam_textos_sem_inventar_campos():
    assert normalizar_dados_gerais({"nome": "  Copa  ", "cidade": " Caxias ", "extra": 1}) == {
        "nome": "Copa",
        "cidade": "Caxias",
    }


def test_estrutura_e_parcial_e_datas_vazias_viram_none():
    assert normalizar_estrutura({"qtd_quadras": 2, "data_limite_inscricao": ""}) == {
        "qtd_quadras": 2,
        "data_limite_inscricao": None,
    }


def test_regras_jogo_nao_preenchem_campos_ausentes():
    assert normalizar_regras_jogo({"tempos_por_set": 1}) == {"tempos_por_set": 1}


def test_pontuacao_inclui_criterio_padrao():
    cfg = normalizar_pontuacao_desempate({"vitoria_2x0": 3})
    assert cfg["vitoria_2x0"] == 3
    assert cfg["criterios_desempate"] == CRITERIOS_DESEMPATE_PADRAO
