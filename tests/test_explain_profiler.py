from core.explain import consulta_elegivel, resumir_plano
from core.profiler import finalizar_profile, iniciar_profile, registrar_tempo


def test_consulta_elegivel_apenas_leitura():
    assert consulta_elegivel("SELECT * FROM partidas")
    assert consulta_elegivel("WITH x AS (SELECT 1) SELECT * FROM x")
    assert not consulta_elegivel("UPDATE partidas SET status='x'")


def test_resumir_plano_json():
    payload = [{
        "Plan": {
            "Node Type": "Seq Scan",
            "Relation Name": "partidas",
            "Total Cost": 42.5,
            "Plan Rows": 100,
        },
        "Planning Time": 0.2,
    }]
    resumo = resumir_plano(payload)
    assert resumo["ok"] is True
    assert resumo["operador_dominante"] == "Seq Scan"
    assert resumo["nos"][0]["relacao"] == "partidas"


def test_profiler_agrega_secoes_sem_dados():
    iniciar_profile()
    registrar_tempo("template", 12.5)
    registrar_tempo("template", 2.5)
    resultado = finalizar_profile()
    assert resultado["template"] == 15.0
