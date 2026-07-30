from pathlib import Path


def test_repositorio_atletas_nao_usa_select_asterisco():
    fonte = Path('repositories/atletas.py').read_text(encoding='utf-8').upper()
    assert 'SELECT *' not in fonte
    for coluna in ('FOTO_ATLETA', 'INSTAGRAM', 'CAPITAO_PADRAO', 'LIBERO'):
        assert coluna in fonte


def test_agrupamento_de_atletas_da_competicao_nao_usa_select_asterisco():
    fonte = Path('routes/equipes.py').read_text(encoding='utf-8').upper()
    inicio = fonte.index('DEF _LISTAR_ATLETAS_COMPETICAO_AGRUPADOS')
    fim = fonte.index('_EXTENSOES_ESCUDO_PERMITIDAS', inicio)
    bloco = fonte[inicio:fim]
    assert 'SELECT *' not in bloco
    for coluna in ('EQUIPE_LOGIN', 'EQUIPE_ID', 'NUMERO', 'NOME'):
        assert coluna in bloco
