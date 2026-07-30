from rules.equipes_perfil import preparar_perfil_equipe, validar_renomeacao_equipe


def test_preparar_perfil_normaliza_campos():
    d = preparar_perfil_equipe("  Porto   Alegre ", "  Ana  Silva ", " 51999 ", " a@b.com ", "@time")
    assert d.cidade == "Porto Alegre"
    assert d.responsavel == "Ana Silva"
    assert d.instagram == "time"
    assert d.completo is True


def test_perfil_sem_telefone_incompleto():
    assert preparar_perfil_equipe("Cidade", "Resp", "").completo is False


def test_renomeacao_rejeita_nome_igual():
    ok, _, _ = validar_renomeacao_equipe("Equipe A", "Copa", " equipe a ")
    assert ok is False
