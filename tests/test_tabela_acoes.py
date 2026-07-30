from services.competicoes import tabela_acoes as acoes


def test_limpar_tabela_bloqueada_nao_chama_repositorio(monkeypatch):
    chamado = {"valor": False}
    monkeypatch.setattr(acoes.partidas_service, "limpar_partidas", lambda nome: chamado.update(valor=True))
    resultado = acoes.limpar_tabela({"nome": "Copa"}, fase_grupos_travada=lambda nome: True)
    assert resultado.ok is False
    assert chamado["valor"] is False


def test_vincular_grupo_quadra_normaliza_grupo(monkeypatch):
    recebido = {}
    monkeypatch.setattr(
        acoes.quadras_service,
        "vincular_grupo_a_quadra",
        lambda comp, grupo, quadra: recebido.update(comp=comp, grupo=grupo, quadra=quadra) or True,
    )
    resultado = acoes.vincular_grupo_quadra(
        {"nome": "Copa"}, " a ", "3", fase_grupos_travada=lambda nome: False
    )
    assert resultado.ok is True
    assert recebido == {"comp": "Copa", "grupo": "A", "quadra": 3}


def test_criar_partida_mata_mata_aceita_a_definir():
    recebido = {}
    resultado = acoes.criar_partida_manual(
        {"nome": "Copa", "tem_grupos": True},
        {"fase": "semifinal", "fase_subaba": "semifinais"},
        fase_pode_ser_alterada=lambda nome, fase: True,
        estrutura_grupo_unico=lambda c: False,
        sincronizar_grupo_unico=lambda c: None,
        listar_grupos=lambda nome: [],
        quadra_padrao_grupo=lambda grupos, grupo: None,
        listar_partidas=lambda nome: [{"ordem": 4}],
        criar_partida=lambda *args, **kwargs: recebido.update(args=args, kwargs=kwargs) or True,
    )
    assert resultado.ok is True
    assert recebido["args"][2:5] == ("A definir", "A definir", 5)


def test_criar_partida_classificatoria_exige_equipes_distintas():
    resultado = acoes.criar_partida_manual(
        {"nome": "Copa"},
        {"fase": "grupos", "grupo": "A", "equipe_a": "Time", "equipe_b": "Time"},
        fase_pode_ser_alterada=lambda nome, fase: True,
        estrutura_grupo_unico=lambda c: False,
        sincronizar_grupo_unico=lambda c: None,
        listar_grupos=lambda nome: [],
        quadra_padrao_grupo=lambda grupos, grupo: None,
        listar_partidas=lambda nome: [],
        criar_partida=lambda *args, **kwargs: True,
    )
    assert resultado.ok is False
    assert "diferentes" in resultado.mensagem


def test_atualizar_partida_preserva_rodada_atual():
    recebido = {}
    resultado = acoes.atualizar_partida_manual(
        {"nome": "Copa"},
        9,
        {"fase": "final", "equipe_a": "A", "equipe_b": "B", "rodada": ""},
        buscar_partida=lambda pid, nome: {"id": pid, "grupo": None, "rodada": 7, "equipe_a": "A", "equipe_b": "B"},
        fase_pode_ser_alterada=lambda nome, fase: True,
        dados_quadra=lambda nome, qid: (2, "Quadra 2"),
        atualizar_partida=lambda *args, **kwargs: recebido.update(args=args, kwargs=kwargs) or True,
    )
    assert resultado.ok is True
    assert recebido["kwargs"]["rodada"] == 7
