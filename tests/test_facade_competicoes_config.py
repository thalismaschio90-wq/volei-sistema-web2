import banco


def test_fachadas_de_configuracao_existem():
    nomes = [
        "buscar_configuracao_avancada_competicao",
        "atualizar_configuracao_avancada_competicao",
        "inicializar_configuracao_avancada_competicao",
        "buscar_configuracao_agenda_competicao",
        "atualizar_configuracao_agenda_competicao",
        "inicializar_configuracao_agenda_competicao",
    ]
    for nome in nomes:
        assert callable(getattr(banco, nome))


def test_helpers_legados_de_agenda_delegam_regras():
    cfg = banco._agenda_config_padrao()
    assert cfg["modo_distribuicao"] == "automatico_inteligente"
    assert banco._normalizar_json_config_agenda('{"A": 1}', {}) == {"A": 1}
