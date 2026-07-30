from core.sql_benchmark import ResultadoBenchmark
from core.sql_lab import comparar_resultados, resolver_placeholders


def resultado(nome, media, p95, erro=""):
    return ResultadoBenchmark(nome=nome, tipo="callable", iteracoes=5, aquecimentos=1, media_ms=media, p95_ms=p95, erro=erro)


def test_resolve_placeholders_sem_expor_valor():
    cfg = {"args": ["${VTP_LAB_COMPETICAO}", "${VTP_LAB_PARTIDA_ID}"]}
    out = resolver_placeholders(cfg, {"VTP_LAB_COMPETICAO": "Teste", "VTP_LAB_PARTIDA_ID": "42"})
    assert out == {"args": ["Teste", 42]}


def test_aprova_ganho_real_sem_regressao():
    itens = comparar_resultados([resultado("x", 100, 120)], [resultado("x", 70, 80)])
    assert itens[0].aprovado is True
    assert itens[0].ganho_media_percentual == 30.0


def test_reprova_regressao():
    itens = comparar_resultados([resultado("x", 100, 100)], [resultado("x", 110, 115)])
    assert itens[0].aprovado is False


def test_reprova_quando_falta_cenario():
    itens = comparar_resultados([resultado("x", 100, 100)], [])
    assert itens[0].aprovado is False
