from services.dashboard_operacional import montar_dashboard_operacional


def test_dashboard_saudavel_com_redis_e_metricas_baixas():
    dados = montar_dashboard_operacional(
        readiness={"ok": True},
        runtime_config={"workers": 2, "threads": 4, "realtime_state_backend": "redis", "socketio_use_redis": True},
        pool={"tempo_espera_medio_ms": 10},
        performance={"rotas": [{"p95_ms": 200}], "consultas_lentas": [{"max_ms": 300}]},
        realtime={"degradacao": {"modo_atual": "normal", "fila_atual": 0}},
    )
    assert dados["estado_geral"] == "saudavel"


def test_dashboard_critico_quando_multiplos_workers_sem_redis():
    dados = montar_dashboard_operacional(
        readiness={"ok": True},
        runtime_config={"workers": 2, "threads": 4, "realtime_state_backend": "local", "socketio_use_redis": False},
        pool={}, performance={}, realtime={},
    )
    assert dados["estado_geral"] == "critico"
