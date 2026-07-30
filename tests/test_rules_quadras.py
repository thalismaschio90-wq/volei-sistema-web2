from rules.quadras import (
    formatar_quadra_exibicao,
    normalizar_lista_quadras,
    normalizar_pin_arbitragem,
    normalizar_quantidade_quadras,
    quadra_matches_texto,
)


def test_pin_arbitragem():
    assert normalizar_pin_arbitragem('12-34') == '1234'
    assert normalizar_pin_arbitragem('123') == ''


def test_formatar_quadra():
    assert formatar_quadra_exibicao({'nome':'Quadra 1','local':'Ginásio'}) == 'Quadra 1 — Ginásio'
    assert formatar_quadra_exibicao({'nome':'Quadra Ginásio','local':'Ginásio'}) == 'Quadra Ginásio'


def test_matches_texto():
    q={'id':7,'nome':'Quadra 2','local':'Apollo','ordem':2}
    assert quadra_matches_texto(q,'7')
    assert quadra_matches_texto(q,'q2')
    assert quadra_matches_texto(q,'Quadra 2 — Apollo')


def test_normalizar_lista_mantem_uma_ativa():
    dados=normalizar_lista_quadras([{'nome':' Principal  ','ativa':False}])
    assert dados[0]['nome']=='Principal'
    assert dados[0]['ativa'] is True


def test_quantidade_minima():
    assert normalizar_quantidade_quadras(0)==1
    assert normalizar_quantidade_quadras('3')==3
