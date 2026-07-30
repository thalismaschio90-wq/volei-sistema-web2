from core.release_readiness import build_release_readiness_report


def base_env():
    return {
        "DATABASE_URL": "postgresql://example.invalid/db",
        "SECRET_KEY": "x" * 48,
        "GUNICORN_WORKERS": "1",
        "GUNICORN_THREADS": "4",
        "REALTIME_STATE_BACKEND": "local",
        "DB_POOL_ENABLED": "1",
        "DB_POOL_MAX": "8",
        "SOCKET_DELTA_ENABLED": "1",
        "SOCKET_LEGACY_STATE_EVENTS": "1",
    }


def test_configuracao_local_segura_aprova_sem_evidencias_remotas():
    report = build_release_readiness_report(base_env())
    assert report.approved is True
    assert any(c.code == "remote_health" and c.status == "warn" for c in report.checks)


def test_multiplos_workers_sem_redis_reprovam():
    env = base_env()
    env["GUNICORN_WORKERS"] = "2"
    report = build_release_readiness_report(env)
    assert report.approved is False
    assert any(c.code == "runtime_config" and c.status == "fail" for c in report.checks)


def test_secret_fraca_reprova():
    env = base_env()
    env["SECRET_KEY"] = "dev"
    report = build_release_readiness_report(env)
    assert report.approved is False
    assert any(c.code == "secret_key" and c.blocking for c in report.checks)


def test_eventos_legados_desligados_sem_guarda_reprovam():
    env = base_env()
    env["SOCKET_LEGACY_STATE_EVENTS"] = "0"
    env["SOCKET_LEGACY_REQUIRE_DELTA_HEALTHY"] = "0"
    report = build_release_readiness_report(env)
    assert report.approved is False
    assert any(c.code == "socket_legacy" and c.status == "fail" for c in report.checks)


def test_markdown_nao_expoe_database_url():
    env = base_env()
    env["DATABASE_URL"] = "postgresql://user:secret@example.com/db"
    text = build_release_readiness_report(env).to_markdown()
    assert "user:secret" not in text
    assert "DATABASE_URL configurada" in text
