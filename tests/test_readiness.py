from core.readiness import build_readiness_report


def test_readiness_ok_com_dependencias_saudaveis(monkeypatch):
    monkeypatch.setenv("GUNICORN_WORKERS", "1")
    monkeypatch.setenv("REALTIME_STATE_BACKEND", "local")
    report = build_readiness_report(
        database_check=lambda: (True, "ok"),
        realtime_check=lambda: (True, "ok", "local"),
    )
    assert report["ok"] is True
    assert report["database"]["ok"] is True
    assert report["realtime"]["backend"] == "local"


def test_readiness_falha_quando_banco_esta_indisponivel(monkeypatch):
    monkeypatch.setenv("GUNICORN_WORKERS", "1")
    monkeypatch.setenv("REALTIME_STATE_BACKEND", "local")
    report = build_readiness_report(
        database_check=lambda: (False, "OperationalError"),
        realtime_check=lambda: (True, "ok", "local"),
    )
    assert report["ok"] is False
    assert report["database"]["detail"] == "OperationalError"


def test_readiness_detecta_runtime_inseguro(monkeypatch):
    monkeypatch.setenv("GUNICORN_WORKERS", "2")
    monkeypatch.setenv("REALTIME_STATE_BACKEND", "local")
    monkeypatch.delenv("REDIS_URL", raising=False)
    report = build_readiness_report(
        database_check=lambda: (True, "ok"),
        realtime_check=lambda: (True, "ok", "local"),
    )
    assert report["ok"] is False
    assert report["runtime"]["errors"]
