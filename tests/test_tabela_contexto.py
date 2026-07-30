from services.competicoes.tabela_contexto import (
    contexto_base,
    montar_pacote_geracao,
    montar_pacote_visualizador,
    normalizar_aba,
    normalizar_fase,
)


def test_normalizacao_aba_e_fase():
    assert normalizar_aba("PARTIDAS") == "partidas"
    assert normalizar_aba("inexistente") == "geracao"
    assert normalizar_fase("Quartas", lambda v: str(v).lower()) == "quartas"
    assert normalizar_fase("outra", lambda v: str(v).lower()) == "classificatorias"


def test_contexto_base_preserva_chaves_do_template():
    ctx = contexto_base(
        competicao={"nome": "Copa"},
        aba="geracao",
        fase_subaba="classificatorias",
        fase_labels={},
        fases_disponiveis={"tem_quartas": True},
        competicao_travada=False,
        grupos_travados=False,
        fase_atual_travada=False,
        fase_banco_ativa="Classificatória",
    )
    assert ctx["competicao"]["nome"] == "Copa"
    assert ctx["partidas"] == []
    assert ctx["classificacao"] == {}
    assert ctx["tem_quartas"] is True


def test_pacote_geracao_carrega_somente_dados_da_aba():
    provedores = {
        "quadras": lambda *_: [{"id": 1, "ativa": True}],
        "grupos": lambda *_: [{"id": 2}],
        "equipes": lambda *_: [{"nome": "A"}],
        "grupos_com_equipes": lambda *_: [{"grupo": {"id": 2}, "equipes": []}],
        "config_agenda": lambda *_: {"modo": "automatico"},
        "estrutura_grupo_unico": lambda *_: True,
    }
    pacote = montar_pacote_geracao({"qtd_quadras": 1}, "Copa", provedores)
    assert pacote["quadra_unica_auto"] is True
    assert pacote["grupo_unico_auto"] is True
    assert pacote["config_geracao"]["modo"] == "automatico"
    assert "partidas" not in pacote


def test_pacote_visualizador_monta_link_curto_e_fallback():
    p_curto = {
        "garantir_codigo_publico": lambda *_: "ABC123",
        "url_publico_curto": lambda codigo: f"/v/{codigo}",
        "url_publico_fallback": lambda nome: f"/visualizador/{nome}",
    }
    curto = montar_pacote_visualizador("Copa", "https://exemplo.com/", p_curto)
    assert curto["link_publico"] == "https://exemplo.com/v/ABC123"

    p_fallback = dict(p_curto)
    p_fallback["garantir_codigo_publico"] = lambda *_: ""
    fallback = montar_pacote_visualizador("Copa", "https://exemplo.com/", p_fallback)
    assert fallback["link_publico_path"] == "/visualizador/Copa"
