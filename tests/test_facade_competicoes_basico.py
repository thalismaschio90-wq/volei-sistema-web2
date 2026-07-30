import banco


def test_fachadas_basicas_de_competicao_existem():
    for nome in (
        "atualizar_dados_competicao",
        "atualizar_estrutura_competicao",
        "atualizar_regras_jogo",
        "atualizar_pontuacao_desempate",
    ):
        assert callable(getattr(banco, nome))
