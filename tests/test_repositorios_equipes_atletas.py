from repositories.atletas import numero_atleta_disponivel


def test_numero_vazio_e_disponivel_sem_acessar_banco():
    assert numero_atleta_disponivel(None, "Equipe", "Competicao") is True
    assert numero_atleta_disponivel("", "Equipe", "Competicao") is True


def test_numero_invalido_nao_e_disponivel():
    assert numero_atleta_disponivel("abc", "Equipe", "Competicao") is False
