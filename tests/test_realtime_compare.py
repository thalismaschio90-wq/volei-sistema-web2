from core.realtime_compare import comparar_snapshots_realtime, gerar_markdown_realtime


def test_compara_bytes_e_eventos():
    antes = {"despacho": {"bytes_emitidos_estimados": 1000, "eventos_por_trafego": [{"evento": "x", "bytes": 800}]}}
    depois = {"despacho": {"bytes_emitidos_estimados": 500, "eventos_por_trafego": [{"evento": "x", "bytes": 300}]}}
    resultado = comparar_snapshots_realtime(antes, depois)
    assert resultado["metricas"]["bytes_emitidos_estimados"]["variacao_percentual"] == -50.0
    assert resultado["eventos"][0]["diferenca"] == -500.0
    assert "Comparação de tráfego" in gerar_markdown_realtime(resultado)
