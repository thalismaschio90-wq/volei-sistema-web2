from rules.equipes import preparar_equipe_competicao, validar_equipe_competicao


def test_preparar_normaliza_espacos():
    dados = preparar_equipe_competicao("  Equipe   Azul ", " Copa   Serra ")
    assert dados.nome_equipe == "Equipe Azul"
    assert dados.nome_competicao == "Copa Serra"


def test_validar_exige_equipe_e_competicao():
    assert validar_equipe_competicao(preparar_equipe_competicao("", "Copa"))[0] is False
    assert validar_equipe_competicao(preparar_equipe_competicao("Equipe", ""))[0] is False
    assert validar_equipe_competicao(preparar_equipe_competicao("Equipe", "Copa"))[0] is True
