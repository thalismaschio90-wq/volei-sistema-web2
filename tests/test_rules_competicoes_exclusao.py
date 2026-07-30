from repositories.competicoes_exclusao import excluir_competicao_persistencia


def test_exclusao_rejeita_nome_vazio():
    assert excluir_competicao_persistencia("") is False
    assert excluir_competicao_persistencia(None) is False
