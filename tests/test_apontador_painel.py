from rules.apontador_painel import fase_normalizada, normalizar_sets_tipo, resolver_modo_operacao, resumo_regra, montar_rodadas_exibicao
from services.apontadores.painel import resolver_cpf_sessao, contexto_home, preparar_partidas_painel


def test_fases_e_sets():
    assert fase_normalizada({"fase": "Semifinais"}) == "semifinal"
    assert normalizar_sets_tipo("md5") == "melhor_de_5"
    assert resumo_regra({"sets_tipo": "melhor_de_5", "pontos_set": 25, "pontos_tiebreak": 15, "modo_operacao": "avancado"}) == "M5 • 25PTS • TB15 • SCOUT"


def test_resolve_modo_avanco():
    cfg = {"fases_config": {"regras_avancadas": {"jogos": {"ouro:F1": {"modo_operacao": "avancado"}}}}}
    assert resolver_modo_operacao({}, cfg, {"origem": "avanco:ouro:F1"}) == "avancado"


def test_cpf_e_contexto_home():
    assert resolver_cpf_sessao({"usuario": "123.456.789-09"}, lambda v: ''.join(c for c in str(v) if c.isdigit())) == "12345678909"
    assert contexto_home({"competicoes": ["Copa"], "pode_jogo_avulso": 1})["competicao_unica"] == "Copa"


def test_preparar_partidas_ordena_e_reduz():
    dados = preparar_partidas_painel(
        [{"id": 2, "rodada": 2, "ordem": 1, "fase": "grupos", "sets_tipo": "melhor_de_5", "texto_grande": "x" * 1000},
         {"id": 1, "rodada": 1, "ordem": 1, "fase": "final"}],
        {}, {}, [{"numero_rodada": 1, "nome": "Abertura"}], 3,
    )
    assert [p["id"] for p in dados["partidas"]] == [1, 2]
    assert dados["sets_max_manual"] == 5
    assert dados["rodadas_exibicao"][0]["nome"] == "Abertura"
    assert "texto_grande" not in dados["partidas"][1]
