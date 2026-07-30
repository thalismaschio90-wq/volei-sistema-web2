from datetime import datetime

from services.replay_partida import (
    carregar_detalhes,
    preparar_evento_replay,
    preparar_linha_tempo,
    resumir_replay,
)


def test_carregar_detalhes_tolera_json_invalido():
    assert carregar_detalhes('{"autor":"Ana"}') == {"autor": "Ana"}
    assert carregar_detalhes("invalido") == {}


def test_preparar_evento_replay_identifica_autor_e_categoria():
    evento = preparar_evento_replay(
        {
            "id": 10,
            "partida_id": 4,
            "set_numero": 2,
            "equipe": "A",
            "tipo": "substituicao",
            "tipo_evento": "substituicao",
            "detalhe": "Sai #7, entra #12",
            "detalhes": '{"operador_nome":"Maria"}',
            "criado_em": datetime(2026, 7, 28, 20, 30, 0),
        }
    )
    assert evento["categoria"] == "substituicao"
    assert evento["autor"] == "Maria"
    assert evento["descricao"] == "Sai #7, entra #12"
    assert evento["criado_em"] == "2026-07-28T20:30:00"


def test_resumo_replay_agrega_categoria_e_set():
    linha = preparar_linha_tempo(
        [
            {"id": 1, "partida_id": 9, "set_numero": 1, "tipo": "ponto", "detalhes": "{}"},
            {"id": 2, "partida_id": 9, "set_numero": 1, "tipo": "fim_set", "detalhes": '{"autor":"João"}'},
        ]
    )
    resumo = resumir_replay(linha)
    assert resumo["total"] == 2
    assert resumo["por_categoria"]["ponto"] == 1
    assert resumo["por_categoria"]["fim_set"] == 1
    assert resumo["por_set"]["1"] == 2
    assert resumo["eventos_com_autor"] == 1
    assert resumo["ultimo_evento_id"] == 2


def test_replay_prioriza_autoria_de_auditoria():
    evento = preparar_evento_replay(
        {
            "id": 11,
            "partida_id": 4,
            "set_numero": 2,
            "tipo": "ponto",
            "detalhes": '{"auditoria":{"nome":"Carlos","usuario":"cpf123","perfil":"apontador","origem":"web","request_id":"abc"}}',
        }
    )
    assert evento["autor"] == "Carlos"
    assert evento["auditoria"]["perfil"] == "apontador"
    assert evento["auditoria"]["request_id"] == "abc"


def test_filtrar_linha_tempo_por_categoria_e_busca():
    from services.replay_partida import filtrar_linha_tempo
    eventos = [
        {"categoria": "ponto", "equipe": "A", "descricao": "Ataque de Ana", "autor": "Maria"},
        {"categoria": "tempo", "equipe": "B", "descricao": "Pedido de tempo", "autor": "João"},
    ]
    assert len(filtrar_linha_tempo(eventos, categoria="ponto")) == 1
    assert len(filtrar_linha_tempo(eventos, busca="ana")) == 1
    assert len(filtrar_linha_tempo(eventos, autor="joão")) == 1


def test_resumo_replay_agrega_equipes_autores_e_percentual():
    resumo = resumir_replay([
        {"id": 1, "categoria": "ponto", "set_numero": 1, "equipe": "A", "autor": "Maria"},
        {"id": 2, "categoria": "tempo", "set_numero": 1, "equipe": "B", "autor": None},
    ])
    assert resumo["por_equipe"] == {"A": 1, "B": 1}
    assert resumo["por_autor"] == {"Maria": 1}
    assert resumo["percentual_com_autor"] == 50.0
    assert resumo["primeiro_evento_id"] == 1
