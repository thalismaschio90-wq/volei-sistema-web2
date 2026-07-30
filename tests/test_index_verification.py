from core.index_verification import (
    construir_verificacoes,
    analisar_plano,
)


def test_construir_verificacoes_nao_expoe_parametros_no_nome():
    itens = construir_verificacoes(competicao="Segredo", equipe="Equipe X", partida_id=99)
    assert len(itens) == 5
    assert all("Segredo" not in item.nome for item in itens)
    assert all("Equipe X" not in item.descricao for item in itens)


def test_analisar_plano_encontra_index_scan():
    verificacao = construir_verificacoes(competicao="C", equipe="E", partida_id=1)[0]
    plano = [{
        "Plan": {
            "Node Type": "Aggregate",
            "Total Cost": 10.5,
            "Plan Rows": 1,
            "Plans": [{
                "Node Type": "Index Only Scan",
                "Index Name": "idx_eventos_competicao_partida",
                "Relation Name": "eventos",
            }],
        },
        "Execution Time": 0.25,
    }]
    resultado = analisar_plano(verificacao, plano)
    assert resultado.indice_usado is True
    assert "idx_eventos_competicao_partida" in resultado.indices_encontrados
    assert resultado.tempo_execucao_ms == 0.25


def test_analisar_plano_registra_seq_scan_sem_falso_positivo():
    verificacao = construir_verificacoes(competicao="C", equipe="E", partida_id=1)[1]
    plano = [{
        "Plan": {
            "Node Type": "Limit",
            "Total Cost": 4.0,
            "Plan Rows": 1,
            "Plans": [{
                "Node Type": "Seq Scan",
                "Relation Name": "equipes_competicoes",
            }],
        }
    }]
    resultado = analisar_plano(verificacao, plano)
    assert resultado.indice_usado is False
    assert resultado.seq_scans == ["equipes_competicoes"]
    assert any("tabelas pequenas" in obs for obs in resultado.observacoes)
