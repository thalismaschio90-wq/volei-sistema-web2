from pathlib import Path


def test_repositorio_nao_executa_ddl_em_consultas():
    texto = Path('repositories/conferencia_atletas.py').read_text(encoding='utf-8').upper()
    assert 'ALTER TABLE' not in texto
    assert 'CREATE TABLE' not in texto


def test_rota_nao_garante_schema_durante_request():
    texto = Path('routes/equipes.py').read_text(encoding='utf-8')
    bloco = texto[texto.index('@equipes_bp.route("/conferencia-atletas")'):]
    assert 'criar_campos_conferencia_atletas()' not in bloco
