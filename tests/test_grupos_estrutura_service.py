from services.competicoes import grupos_estrutura as service


class RNGDeterministico:
    def shuffle(self, valores):
        valores.reverse()


def test_sortear_equipes_salva_em_lote(monkeypatch):
    grupos = [{"id": 1, "nome": "A"}, {"id": 2, "nome": "B"}]
    salvo = {}
    monkeypatch.setattr(service, "garantir_grupos_estrutura", lambda *a, **k: True)
    monkeypatch.setattr(service.grupos_service, "listar_grupos", lambda nome: grupos)
    monkeypatch.setattr(service, "listar_equipes_da_competicao", lambda nome: [
        {"nome": "Um"}, {"nome": "Dois"}, {"nome": "Três"}
    ])
    monkeypatch.setattr(
        service.grupos_service,
        "substituir_distribuicao_equipes",
        lambda nome, distribuicao: salvo.update(nome=nome, distribuicao=distribuicao) or len(distribuicao),
    )
    limpos = []
    resultado = service.sortear_equipes(
        {"nome": "Copa", "qtd_grupos": 2, "tem_grupos": True},
        fase_travada=lambda nome: False,
        limpar_cache=limpos.append,
        rng=RNGDeterministico(),
    )
    assert resultado.ok is True
    assert salvo["nome"] == "Copa"
    assert [x["equipe"] for x in salvo["distribuicao"]] == ["Três", "Dois", "Um"]
    assert limpos == ["Copa"]


def test_sortear_bloqueado_nao_grava(monkeypatch):
    chamado = {"valor": False}
    monkeypatch.setattr(
        service.grupos_service,
        "substituir_distribuicao_equipes",
        lambda *a, **k: chamado.update(valor=True),
    )
    resultado = service.sortear_equipes(
        {"nome": "Copa", "qtd_grupos": 2, "tem_grupos": True},
        fase_travada=lambda nome: True,
    )
    assert resultado.ok is False
    assert chamado["valor"] is False


def test_grupo_unico_cria_a_e_vincula_unica_quadra(monkeypatch):
    grupos = []
    vinculados = []
    monkeypatch.setattr(service.grupos_service, "listar_grupos", lambda nome: list(grupos))
    monkeypatch.setattr(
        service.grupos_service,
        "criar_grupo",
        lambda nome, comp, fase_travada=False: grupos.append({"id": 7, "nome": nome}) or True,
    )
    monkeypatch.setattr(service, "listar_equipes_da_competicao", lambda nome: [{"nome": "Equipe 1"}])
    monkeypatch.setattr(service.grupos_service, "listar_equipes_por_grupo", lambda gid: [])
    monkeypatch.setattr(service.grupos_service, "adicionar_equipe_no_grupo", lambda *a, **k: True)
    monkeypatch.setattr(service.quadras_service, "listar_quadras_competicao", lambda nome: [{"id": 3, "ativa": True}])
    monkeypatch.setattr(
        service.quadras_service,
        "vincular_grupo_a_quadra",
        lambda comp, grupo, qid: vinculados.append((comp, grupo, qid)) or True,
    )
    assert service.sincronizar_grupo_unico(
        {"nome": "Copa", "qtd_grupos": 1, "tem_grupos": True},
        fase_travada=lambda nome: False,
    ) is True
    assert vinculados == [("Copa", "A", 3)]
