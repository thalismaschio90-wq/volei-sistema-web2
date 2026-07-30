from rules.grupos import (
    dados_grupo_validos,
    normalizar_nome_equipe,
    normalizar_nome_grupo,
    vinculo_grupo_valido,
)


def test_normalizacoes_grupos():
    assert normalizar_nome_grupo('  Grupo   A ') == 'Grupo A'
    assert normalizar_nome_equipe('  Time   Azul ') == 'Time Azul'


def test_validacoes_grupos():
    assert dados_grupo_validos('A', 'Copa')
    assert not dados_grupo_validos('', 'Copa')
    assert vinculo_grupo_valido('2', 'Equipe', 'Copa')
    assert not vinculo_grupo_valido(None, 'Equipe', 'Copa')
