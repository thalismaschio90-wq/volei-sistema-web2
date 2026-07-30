from core.runtime_config import assert_runtime_safe, load_runtime_config


def test_um_worker_local_e_seguro():
    cfg = load_runtime_config({"GUNICORN_WORKERS": "1", "REALTIME_STATE_BACKEND": "local"})
    assert cfg.errors() == []
    assert cfg.public_dict()["valid"] is True


def test_multiplos_workers_sem_redis_sao_bloqueados():
    cfg = load_runtime_config({"GUNICORN_WORKERS": "2", "REALTIME_STATE_BACKEND": "local"})
    assert len(cfg.errors()) == 2


def test_multiplos_workers_com_redis_sao_permitidos():
    cfg = load_runtime_config({
        "GUNICORN_WORKERS": "2",
        "GUNICORN_THREADS": "4",
        "REALTIME_STATE_BACKEND": "redis",
        "REALTIME_REDIS_REQUIRED": "1",
        "SOCKETIO_USE_REDIS": "1",
        "REDIS_URL": "redis://localhost:6379/0",
    })
    assert cfg.errors() == []
    assert cfg.redis_state_enabled is True
    assert cfg.socket_queue_enabled is True


def test_redis_backend_sem_url_e_invalido():
    cfg = load_runtime_config({"REALTIME_STATE_BACKEND": "redis"})
    assert any("REDIS_URL" in erro for erro in cfg.errors())


def test_assert_runtime_safe_levanta_erro_em_configuracao_insegura():
    try:
        assert_runtime_safe({"GUNICORN_WORKERS": "2"})
    except RuntimeError as exc:
        assert "Configuração de produção insegura" in str(exc)
    else:
        raise AssertionError("Era esperado RuntimeError")


def test_backend_auto_sem_url_resolve_local_com_um_worker():
    cfg = load_runtime_config({
        "GUNICORN_WORKERS": "1",
        "REALTIME_STATE_BACKEND": "auto",
        "SOCKETIO_USE_REDIS": "1",
    })
    assert cfg.errors() == []
    assert cfg.resolved_state_backend == "local"
    assert cfg.redis_state_enabled is False
    assert cfg.public_dict()["resolved_state_backend"] == "local"


def test_backend_auto_com_url_resolve_redis_e_permite_multiplos_workers():
    cfg = load_runtime_config({
        "GUNICORN_WORKERS": "2",
        "REALTIME_STATE_BACKEND": "auto",
        "SOCKETIO_USE_REDIS": "1",
        "REDIS_URL": "redis://localhost:6379/0",
    })
    assert cfg.errors() == []
    assert cfg.resolved_state_backend == "redis"
    assert cfg.redis_state_enabled is True
    assert cfg.socket_queue_enabled is True
