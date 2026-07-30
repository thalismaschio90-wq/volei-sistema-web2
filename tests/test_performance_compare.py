from core.performance_compare import comparar_snapshots, exportar_markdown_comparacao


def test_compara_rota_e_consulta():
    antes = {
        "rotas": [{
            "endpoint": "painel.inicio", "metodo": "GET", "quantidade": 10,
            "duracao_p95_ms": 1000, "sql_media_ms": 700, "sql_media_consultas": 10,
        }],
        "consultas_lentas": [{
            "fingerprint": "abc", "operacao": "SELECT", "quantidade": 5,
            "duracao_media_ms": 500, "duracao_max_ms": 700,
        }],
    }
    depois = {
        "rotas": [{
            "endpoint": "painel.inicio", "metodo": "GET", "quantidade": 10,
            "duracao_p95_ms": 500, "sql_media_ms": 250, "sql_media_consultas": 4,
        }],
        "consultas_lentas": [{
            "fingerprint": "abc", "operacao": "SELECT", "quantidade": 5,
            "duracao_media_ms": 200, "duracao_max_ms": 300,
        }],
    }
    resultado = comparar_snapshots(antes, depois)
    assert resultado["rotas"][0]["p95_variacao_pct"] == -50.0
    assert resultado["consultas"][0]["variacao_pct"] == -60.0
    assert resultado["resumo"]["rotas_melhoraram"] == 1


def test_markdown_nao_expoe_sql():
    resultado = comparar_snapshots({"rotas": [], "consultas_lentas": []}, {"rotas": [], "consultas_lentas": []})
    texto = exportar_markdown_comparacao(resultado)
    assert "Comparação de performance" in texto
    assert "SELECT * FROM" not in texto
