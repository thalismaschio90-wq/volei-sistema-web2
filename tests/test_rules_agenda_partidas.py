from rules.agenda_partidas import (
    gerar_rodadas_round_robin,
    jogos_rodada_info,
    normalizar_lista_ids,
    gerar_slots_pool_multiquadra,
    gerar_slots_pool_quadra_unica,
    proximo_jogo_sem_conflito,
)


def test_round_robin_par_sem_repetir_equipes_na_rodada():
    rodadas = gerar_rodadas_round_robin(["A", "B", "C", "D"])
    assert len(rodadas) == 3
    confrontos = set()
    for rodada in rodadas:
        usados = set()
        for a, b in rodada["jogos"]:
            assert a not in usados and b not in usados
            usados.update({a, b})
            confrontos.add(frozenset((a, b)))
    assert len(confrontos) == 6


def test_round_robin_impar_gira_folga():
    rodadas = gerar_rodadas_round_robin(["A", "B", "C"])
    assert len(rodadas) == 3
    assert {r["folga"] for r in rodadas} == {"A", "B", "C"}


def test_jogos_rodada_aceita_dicts_e_tuplas():
    assert jogos_rodada_info({"jogos": [("A", "B"), {"equipe_a": "C", "equipe_b": "D"}]}) == [
        ("A", "B"), ("C", "D")
    ]


def test_normalizar_ids_remove_invalidos_e_duplicados():
    assert normalizar_lista_ids('[1, "2", 2, 0, "x"]') == [1, 2]


def test_slots_multiquadra_preservam_numero_da_rodada():
    rodadas = {"A": [{"numero": 1, "jogos": [("T1", "T4"), ("T2", "T3")], "folga": None}]}
    slots = gerar_slots_pool_multiquadra(rodadas, ["A"], [10, 11])
    assert len(slots) == 1
    assert {j["quadra_id"] for j in slots[0]} == {10, 11}
    assert {j["rodada_grupo"] for j in slots[0]} == {1}


def test_slots_quadra_unica_sao_sequenciais_e_mesma_rodada():
    rodadas = {"A": [{"numero": 2, "jogos": [("T1", "T4"), ("T2", "T3")], "folga": None}]}
    slots = gerar_slots_pool_quadra_unica(rodadas, ["A"], 7)
    assert len(slots) == 2
    assert all(slot[0]["quadra_id"] == 7 for slot in slots)
    assert all(slot[0]["rodada_grupo"] == 2 for slot in slots)


def test_proximo_jogo_prefere_equipe_que_nao_jogou_no_slot_anterior():
    jogos = [
        {"equipe_a": "A", "equipe_b": "B"},
        {"equipe_a": "C", "equipe_b": "D"},
    ]
    escolhido = proximo_jogo_sem_conflito(jogos, set(), {"A"})
    assert escolhido == {"equipe_a": "C", "equipe_b": "D"}
