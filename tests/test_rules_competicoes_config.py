from rules.competicoes_config import (
    configuracao_agenda_padrao,
    fases_padrao_configuracao_avancada,
    normalizar_configuracao_agenda,
    normalizar_fases_config,
)


def test_agenda_padrao_e_independente():
    a = configuracao_agenda_padrao()
    b = configuracao_agenda_padrao()
    a["grupos_compartilhados"]["A"] = [1]
    assert b["grupos_compartilhados"] == {}


def test_normaliza_agenda_limites_e_enums():
    cfg = normalizar_configuracao_agenda({
        "modo_distribuicao": "INVALIDO",
        "rodizio_grupos": "INVALIDO",
        "descanso_minimo_jogos": 99,
        "grupos_compartilhados_json": '{"A": [1, 2]}',
        "quadras_compartilhadas_json": '["Q1"]',
    })
    assert cfg["modo_distribuicao"] == "automatico_inteligente"
    assert cfg["rodizio_grupos"] == "por_rodada"
    assert cfg["descanso_minimo_jogos"] == 5
    assert cfg["grupos_compartilhados"] == {"A": [1, 2]}
    assert cfg["quadras_compartilhadas"] == ["Q1"]


def test_normalizar_fases_config_invalido():
    assert normalizar_fases_config("{invalido") == {}


def test_fases_padrao_respeita_configuracao_principal():
    fases = fases_padrao_configuracao_avancada({
        "tipo_confronto": "cruzado",
        "tipo_classificacao": "geral",
        "cruzamentos_grupos": "A1xB2",
    })
    assert fases["tipo_confronto"] == "cruzado"
    assert fases["tipo_classificacao"] == "geral"
    assert fases["cruzamentos_grupos"] == "A1xB2"
    assert fases["final"]["pontos"] == 25
