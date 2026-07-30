from rules.partidas_exibicao import (
    fase_partida_normalizada,
    formatar_data_hora,
    mapa_escudos_equipes,
    partida_esta_ao_vivo,
    status_exibicao,
)
from services.competicoes.partidas_exibicao import preparar_partidas


def test_finalizada_tem_prioridade_sobre_status_ao_vivo():
    partida = {"status": "em_andamento", "finalizada": True, "pontos_a": 10}
    assert status_exibicao(partida) == "FINALIZADO"
    assert partida_esta_ao_vivo(partida) is False


def test_fase_e_data_hora_sao_normalizadas():
    assert fase_partida_normalizada({"fase": "Semifinais"}) == "semifinal"
    bruto, entrada, label = formatar_data_hora("2026-07-27 19:30:00")
    assert bruto.startswith("2026-07-27")
    assert entrada == "2026-07-27T19:30"
    assert label == "27/07/2026 19:30"


def test_mapa_escudos_aceita_nome_e_login():
    mapa = mapa_escudos_equipes([{"nome": "Equipe A", "login": "time_a", "escudo": "a.png"}])
    assert mapa["Equipe A"].endswith("/a.png")
    assert mapa["time_a"].endswith("/a.png")


def test_preparar_partidas_ordena_e_preserva_placar_ao_vivo():
    partidas = [
        {"id": 2, "equipe_a": "B", "equipe_b": "C", "data_hora": "2026-07-28 10:00", "status": "aguardando"},
        {"id": 1, "equipe_a": "A", "equipe_b": "B", "data_hora": "2026-07-27 10:00", "status": "em_andamento", "pontos_a": 5, "pontos_b": 4},
    ]

    def placar(partida, _competicao):
        partida["placar_exibicao_a"] = partida.get("sets_a", 0)
        partida["placar_exibicao_b"] = partida.get("sets_b", 0)

    resultado = preparar_partidas(partidas, aplicar_placar_exibicao=placar)
    assert [p["id"] for p in resultado] == [1, 2]
    assert resultado[0]["placar_ao_vivo"] == "5 x 4"
    assert resultado[0]["ao_vivo"] is True
