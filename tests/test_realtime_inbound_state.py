from realtime.inbound_state import (
    aceitar_e_salvar_estado,
    avaliar_estado_recebido,
    progresso_estado,
    versao_recebida,
)
from realtime.state_store import LocalEstadoPartidaStore


def test_versao_recebida_aceita_aliases_legados():
    assert versao_recebida({"estado_versao_base": "7"}) == 7
    assert versao_recebida({"expected_version": 9}) == 9
    assert versao_recebida({}) == 0


def test_rejeita_cliente_com_versao_antiga():
    atual = {"set_atual": 1, "pontos_a": 8, "pontos_b": 7}
    novo = {"set_atual": 1, "pontos_a": 9, "pontos_b": 7}
    resultado = avaliar_estado_recebido(
        atual=atual,
        versao_atual=5,
        novo=novo,
        dados_originais={"estado_versao_base": 3},
    )
    assert not resultado.aceito
    assert resultado.conflito_versao
    assert resultado.estado == atual


def test_cliente_sem_versao_ainda_nao_pode_reduzir_placar():
    resultado = avaliar_estado_recebido(
        atual={"sets_a": 0, "sets_b": 0, "set_atual": 1, "pontos_a": 12, "pontos_b": 10},
        versao_atual=4,
        novo={"sets_a": 0, "sets_b": 0, "set_atual": 1, "pontos_a": 8, "pontos_b": 10},
    )
    assert not resultado.aceito
    assert resultado.snapshot_atrasado


def test_desfazer_explicitamente_permitem_reducao():
    resultado = avaliar_estado_recebido(
        atual={"set_atual": 1, "pontos_a": 12, "pontos_b": 10},
        versao_atual=4,
        novo={"set_atual": 1, "pontos_a": 11, "pontos_b": 10},
        dados_originais={"estado_versao_base": 4, "origem": "desfazer_ponto"},
    )
    assert resultado.aceito


def test_aceitacao_e_gravacao_sao_atomicas_no_store():
    store = LocalEstadoPartidaStore()
    primeiro = aceitar_e_salvar_estado(
        store=store,
        partida_id=10,
        novo={"set_atual": 1, "pontos_a": 1, "pontos_b": 0},
    )
    assert primeiro.aceito and primeiro.versao_atual == 1

    segundo = aceitar_e_salvar_estado(
        store=store,
        partida_id=10,
        novo={"set_atual": 1, "pontos_a": 2, "pontos_b": 0},
        dados_originais={"estado_versao_base": 1},
    )
    assert segundo.aceito and segundo.versao_atual == 2

    atrasado = aceitar_e_salvar_estado(
        store=store,
        partida_id=10,
        novo={"set_atual": 1, "pontos_a": 3, "pontos_b": 0},
        dados_originais={"estado_versao_base": 1},
    )
    assert not atrasado.aceito
    assert atrasado.conflito_versao
    assert store.obter(10)["pontos_a"] == 2
    assert store.versao(10) == 2


def test_progresso_prioriza_sets_set_e_pontos():
    assert progresso_estado({"sets_a": 1, "sets_b": 0, "set_atual": 2, "pontos_a": 0}) > progresso_estado({"sets_a": 0, "sets_b": 0, "set_atual": 1, "pontos_a": 25})
