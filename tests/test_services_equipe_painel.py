from services.equipes.painel import montar_resumo_painel, proxima_partida, resumir_atletas


def test_resumo_atletas_status_e_percentual():
    resumo = resumir_atletas(
        [
            {"status": "aprovado"},
            {"status": "Pendente"},
            {"status": "reprovado"},
        ],
        6,
    )
    assert resumo["total_atletas"] == 3
    assert resumo["atletas_aprovados"] == 1
    assert resumo["atletas_pendentes"] == 1
    assert resumo["atletas_reprovados"] == 1
    assert resumo["percentual_atletas"] == 50
    assert resumo["status_equipe"] == "Aguardando conferência"


def test_equipe_completa_quando_limite_preenchido_sem_pendencias():
    resumo = resumir_atletas([{"status": "aprovado"}, {"status": "aprovado"}], 2)
    assert resumo["status_equipe"] == "Equipe completa"
    assert resumo["status_classe"] == "tag-aprovado"


def test_proxima_partida_ignora_finalizada_e_jogo_de_outros():
    esperada = {"id": 3, "minha_partida": True, "finalizada": False}
    partidas = [
        {"id": 1, "minha_partida": False, "finalizada": False},
        {"id": 2, "minha_partida": True, "finalizada": True},
        esperada,
    ]
    assert proxima_partida(partidas) is esperada


def test_montar_resumo_painel_reutiliza_dados_recebidos():
    partidas = [
        {"id": 1, "minha_partida": True, "finalizada": False},
        {"id": 2, "minha_partida": False, "finalizada": False},
    ]
    resumo = montar_resumo_painel(
        [{"status": "aprovado"}],
        partidas,
        {"limite_atletas": 10},
    )
    assert resumo["limite_atletas"] == 10
    assert [p["id"] for p in resumo["minhas_partidas"]] == [1]
    assert resumo["proxima_partida"]["id"] == 1
