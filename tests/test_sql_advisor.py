from core.sql_advisor import analisar_estrutura_sql, sugerir_otimizacoes


def test_analisa_select_sem_preservar_valores():
    estrutura = analisar_estrutura_sql(
        "SELECT * FROM partidas p WHERE p.competicao = ? AND p.status = ? ORDER BY p.rodada, p.ordem"
    )
    assert estrutura["tabelas"] == ["partidas"]
    assert estrutura["filtros"] == ["competicao", "status"]
    assert estrutura["ordenacao"][:2] == ["rodada", "ordem"]
    assert estrutura["tem_select_star"] is True


def test_sugere_indice_composto_sem_gerar_ddl():
    estrutura = analisar_estrutura_sql(
        "SELECT id FROM partida_eventos WHERE partida_id = ? AND sequencia > ? ORDER BY sequencia"
    )
    sugestoes = sugerir_otimizacoes(estrutura)
    texto = " ".join(item["detalhe"] for item in sugestoes)
    assert "partida_id" in texto
    assert "sequencia" in texto
    assert "CREATE INDEX" not in texto.upper()


def test_update_recomenda_indice_no_filtro():
    estrutura = analisar_estrutura_sql("UPDATE partidas SET status = ? WHERE id = ?")
    sugestoes = sugerir_otimizacoes(estrutura)
    assert any(item["tipo"] == "escrita" for item in sugestoes)
