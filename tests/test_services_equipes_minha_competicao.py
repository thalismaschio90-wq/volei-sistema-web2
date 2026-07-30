from services.equipes.minha_competicao import (
    montar_contexto_minha_equipe,
    montar_contexto_minhas_partidas,
    resumir_documentacao_atletas,
)


def test_montar_contexto_minha_equipe_normaliza_avisos():
    contexto = montar_contexto_minha_equipe(
        equipe={"nome": "A"},
        erro=None,
        sucesso="ok",
        escudo_padrao="/padrao.svg",
        avisos={"notificacoes_equipe": [{"id": 1}], "notificacoes_nao_lidas": "2"},
    )
    assert contexto["equipe"]["nome"] == "A"
    assert contexto["solicitacoes_equipe"] == []
    assert contexto["notificacoes_nao_lidas"] == 2


def test_montar_contexto_minhas_partidas_preserva_campos():
    contexto = montar_contexto_minhas_partidas(
        equipe={"nome": "A"},
        partidas=[{"id": 10}],
        rodadas_partidas=[{"numero": 1}],
        competicao={"nome": "Copa"},
        grupos=[{"id": 2}],
        classificacao={"2": []},
        criterios_classificacao=["pontos"],
        colunas_classificacao=[{"chave": "pontos"}],
    )
    assert contexto["partidas"][0]["id"] == 10
    assert contexto["competicao"]["nome"] == "Copa"
    assert contexto["criterios_classificacao"] == ["pontos"]


def test_resumir_documentacao_atletas():
    resumo = resumir_documentacao_atletas([
        {"foto": "x", "instagram": "@a", "cpf": "1", "status": "aprovado"},
        {"status": "pendente"},
    ])
    assert resumo == {
        "total": 2,
        "com_foto": 1,
        "com_instagram": 1,
        "com_documento": 1,
        "aprovados": 1,
        "pendentes": 1,
    }
