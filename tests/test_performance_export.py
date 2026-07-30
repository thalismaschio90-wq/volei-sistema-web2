import json

from core.performance_export import exportar_json, exportar_markdown, montar_exportacao


def _snapshot():
    return {
        "backend": "local",
        "iniciado_em": 1.0,
        "limites": {},
        "rotas": [{
            "metodo": "GET", "endpoint": "painel", "rota": "/inicio", "quantidade": 3,
            "erros": 0, "duracao_media_ms": 800.0, "duracao_p95_ms": 950.0,
            "duracao_max_ms": 1000.0, "sql_media_consultas": 8.0, "sql_media_ms": 700.0,
            "sql_max_ms": 400.0,
        }],
        "consultas_lentas": [{
            "fingerprint": "abc123", "operacao": "SELECT", "quantidade": 5,
            "duracao_media_ms": 300.0, "duracao_max_ms": 500.0,
            "rotas": ["painel"], "origens": ["repositories/partidas.py:listar:10"],
            "estrutura": {"tabelas": ["partidas"], "filtros": ["competicao", "status"], "ordenacao": ["rodada"], "agrupamento": []},
            "sugestoes": [{"titulo": "Revisar índice", "detalhe": "Confirmar em homologação."}],
        }],
    }


def test_exportacao_cria_candidato_seguro():
    dados = montar_exportacao(_snapshot())
    candidato = dados["consultas_prioritarias"][0]["indice_candidato"]
    assert "CREATE INDEX CONCURRENTLY" in candidato
    assert "partidas" in candidato
    assert "competicao, status, rodada" in candidato


def test_markdown_nao_expoe_parametros_e_inclui_explain():
    texto = exportar_markdown(_snapshot())
    assert "EXPLAIN (ANALYZE, BUFFERS" in texto
    assert "abc123" in texto
    assert "123.456.789-00" not in texto


def test_json_e_valido():
    dados = json.loads(exportar_json(_snapshot()))
    assert dados["resumo"]["rotas_observadas"] == 1
    assert dados["consultas_prioritarias"][0]["prioridade"] == 1
