from rules.competicoes_ciclo import normalizar_motivo_travamento, normalizar_nome_competicao, slug_login_organizador


def test_normalizar_nome():
    assert normalizar_nome_competicao("  Copa   Serra  ") == "Copa Serra"


def test_motivo_padrao():
    assert normalizar_motivo_travamento("") == "primeiro_ponto"


def test_slug_login():
    assert slug_login_organizador("Copa Serra 2026") == "copa.serra.2026"
