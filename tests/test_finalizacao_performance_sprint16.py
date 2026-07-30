from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BANCO = (ROOT / 'banco.py').read_text(encoding='utf-8')


def _funcao(nome: str) -> str:
    inicio = BANCO.index(f'def {nome}')
    proxima = BANCO.find('\ndef ', inicio + 5)
    return BANCO[inicio: proxima if proxima >= 0 else None]


def test_registrar_resultado_set_nao_executa_ddl_nem_select_asterisco():
    trecho = _funcao('registrar_resultado_set')
    assert 'criar_campos_sets_partida' not in trecho
    assert 'criar_campos_jogo_partida' not in trecho
    assert 'SELECT *' not in trecho.upper()
    assert 'FOR UPDATE' in trecho.upper()


def test_registrar_resultado_set_nao_recalcula_fluxo_em_nova_conexao():
    trecho = _funcao('registrar_resultado_set')
    assert 'buscar_partida_operacional' not in trecho
    assert 'resumir_fluxo_oficial_partida' not in trecho
    assert trecho.count('sincronizar_status_competicao') == 1
    assert 'sincronizar_avanco_automatico_competicao' not in trecho


def test_finalizacao_completa_valida_schema_sem_ddl_runtime():
    trecho = _funcao('finalizar_partida_completa')
    assert 'require_schema' in trecho
    assert 'criar_tabela_destaques_partida()' not in trecho
    assert 'garantir_campos_trava_operacional_partida()' not in trecho
    assert 'SELECT *\n                    FROM partidas' not in trecho
