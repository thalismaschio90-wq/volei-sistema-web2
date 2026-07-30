from tests.load.config import LoadTestConfig
from tests.load.metrics import MetricCollector, percentile


def test_percentile_nearest_rank():
    assert percentile([1, 2, 3, 4, 5], 95) == 5
    assert percentile([1, 2, 3, 4], 50) == 2
    assert percentile([], 99) == 0


def test_metric_summary():
    metrics = MetricCollector()
    metrics.add("ponto", 100, ok=True)
    metrics.add("ponto", 300, ok=False)
    summary = metrics.summary()["ponto"]
    assert summary["count"] == 2
    assert summary["failed"] == 1
    assert summary["avg_ms"] == 200
    assert summary["p95_ms"] == 300


def test_load_config_blocks_writes_without_authentication():
    config = LoadTestConfig(
        base_url="https://example.test",
        competicao="Homologação",
        partida_id=10,
        allow_writes=True,
    )
    assert any("escrita exige" in error.lower() for error in config.validate())
