import pytest

from rules.substituicoes import (
    ErroSubstituicao,
    aplicar_substituicao_excepcional,
    aplicar_substituicao_normal,
)


def estado_base():
    return {
        'rotacao_a': ['1', '2', '3', '4', '5', '6'],
        'rotacao_b': ['7', '8', '9', '10', '11', '12'],
        'titulares_iniciais_a': ['1', '2', '3', '4', '5', '6'],
        'titulares_iniciais_b': ['7', '8', '9', '10', '11', '12'],
        'status_jogadores_a': {},
        'status_jogadores_b': {},
        'subs_a': 0,
        'subs_b': 0,
        'limite_substituicoes': 6,
        'set_atual': 1,
    }


def elenco(*numeros):
    return {str(n): {'numero': str(n), 'libero': False} for n in numeros}


def test_titular_sai_e_reserva_entra_sem_reordenar():
    novo = aplicar_substituicao_normal(
        estado_base(), 'A', '1', '13', atletas_validos=elenco(*range(1, 14))
    )
    assert novo['rotacao_a'] == ['13', '2', '3', '4', '5', '6']
    assert novo['rotacao_b'] == ['7', '8', '9', '10', '11', '12']
    assert novo['subs_a'] == 1
    assert novo['vinculos_titular_reserva_a'] == {'1': '13'}
    assert novo['vinculos_reserva_titular_a'] == {'13': '1'}


def test_reserva_so_pode_sair_para_titular_vinculado():
    primeiro = aplicar_substituicao_normal(
        estado_base(), 'A', '1', '13', atletas_validos=elenco(*range(1, 15))
    )
    with pytest.raises(ErroSubstituicao):
        aplicar_substituicao_normal(
            primeiro, 'A', '13', '2', atletas_validos=elenco(*range(1, 15))
        )


def test_retorno_encerra_dupla_no_set():
    primeiro = aplicar_substituicao_normal(
        estado_base(), 'A', '1', '13', atletas_validos=elenco(*range(1, 14))
    )
    retorno = aplicar_substituicao_normal(
        primeiro, 'A', '13', '1', atletas_validos=elenco(*range(1, 14))
    )
    assert retorno['rotacao_a'][0] == '1'
    assert retorno['subs_a'] == 2
    with pytest.raises(ErroSubstituicao):
        aplicar_substituicao_normal(
            retorno, 'A', '1', '13', atletas_validos=elenco(*range(1, 14))
        )


def test_limite_e_respeitado():
    estado = estado_base()
    estado['subs_a'] = 6
    with pytest.raises(ErroSubstituicao, match='Limite'):
        aplicar_substituicao_normal(
            estado, 'A', '1', '13', atletas_validos=elenco(*range(1, 14))
        )


def test_libero_nao_entra_em_substituicao_normal():
    atletas = elenco(*range(1, 14))
    atletas['13']['libero'] = True
    with pytest.raises(ErroSubstituicao, match='Líbero'):
        aplicar_substituicao_normal(estado_base(), 'A', '1', '13', atletas_validos=atletas)


def test_excepcional_troca_posicao_e_registra_historico():
    novo = aplicar_substituicao_excepcional(
        estado_base(), 'B', '7', '13', atletas_validos=elenco(*range(1, 14)), motivo='lesao'
    )
    assert novo['rotacao_b'] == ['13', '8', '9', '10', '11', '12']
    assert novo['rotacao_a'] == ['1', '2', '3', '4', '5', '6']
    assert novo['subs_excepcionais'][-1]['motivo'] == 'lesao'
