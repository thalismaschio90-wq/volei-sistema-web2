from services.competicoes import visualizador_publico as svc


def test_obter_estado_publico_prioriza_cache(monkeypatch):
    monkeypatch.setattr(svc, "obter_estado_cache", lambda partida_id: {"competicao": "Copa", "pontos_a": 9})
    monkeypatch.setattr(svc, "buscar_estado_jogo_partida", lambda *args: {"pontos_a": 2})
    estado = svc.obter_estado_publico(1, "Copa")
    assert estado["pontos_a"] == 9


def test_obter_estado_publico_ignora_cache_de_outra_competicao(monkeypatch):
    monkeypatch.setattr(svc, "obter_estado_cache", lambda partida_id: {"competicao": "Outra", "pontos_a": 9})
    monkeypatch.setattr(svc, "buscar_estado_jogo_partida", lambda *args: {"pontos_a": 2})
    estado = svc.obter_estado_publico(1, "Copa")
    assert estado["pontos_a"] == 2


def test_montar_contexto_retorna_none_sem_partida(monkeypatch):
    monkeypatch.setattr(svc, "buscar_competicao_por_nome", lambda nome: {"nome": nome})
    monkeypatch.setattr(svc, "buscar_partida_por_id", lambda *args: None)
    assert svc.montar_contexto_partida_publica("Copa", 99, lambda *args: []) is None


def test_montar_estado_leve(monkeypatch):
    partida = {
        "id": 7,
        "equipe_a": "A",
        "equipe_b": "B",
        "sets_a": 0,
        "sets_b": 0,
    }
    preparada = dict(partida, status_exibicao="AO VIVO", ao_vivo=True, finalizada=False, set_unico=False, set_atual=1, parciais_formatadas="")
    monkeypatch.setattr(svc, "buscar_competicao_por_nome", lambda nome: {"nome": nome})
    monkeypatch.setattr(svc, "buscar_partida_por_id", lambda *args: partida)
    monkeypatch.setattr(svc, "obter_estado_publico", lambda *args: {"pontos_a": 10, "pontos_b": 8, "set_atual": 1})
    monkeypatch.setattr(svc, "buscar_versoes_detalhes", lambda *args: (12, 3))
    monkeypatch.setattr(svc, "obter_estado_versao", lambda *args: 44)

    payload = svc.montar_estado_leve_partida_publica("Copa", 7, lambda *args: [preparada])
    assert payload["partida"]["pontos_a"] == 10
    assert payload["partida"]["pontos_b"] == 8
    assert payload["eventos_versao"] == 12
    assert payload["estado_versao"] == 44
    assert payload["destaque_versao"] == 3
