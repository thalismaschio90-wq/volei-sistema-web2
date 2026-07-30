from rules.partidas import normalizar_fase, status_bloqueado, partida_iniciada_ou_finalizada, normalizar_limite, grupo_para_fase


def test_normalizar_fases():
    assert normalizar_fase("Semis") == "semifinal"
    assert normalizar_fase("finais") == "final"
    assert normalizar_fase("grupo") == "grupos"


def test_pre_jogo_sozinho_nao_bloqueia():
    assert status_bloqueado("aguardando", "pre_jogo") is False
    assert status_bloqueado("aguardando", "ao_vivo") is True


def test_partida_com_pontos_bloqueia():
    assert partida_iniciada_ou_finalizada({"pontos_a": 1}) is True
    assert partida_iniciada_ou_finalizada({"status_jogo": "pre_jogo"}) is False


def test_limite_e_grupo():
    assert normalizar_limite(999) == 200
    assert grupo_para_fase("A", "grupos") == "A"
    assert grupo_para_fase("A", "final") is None
