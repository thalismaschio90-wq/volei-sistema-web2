from pathlib import Path


def test_repositorios_estruturais_exigem_force_para_ddl():
    alvos = [
        Path("repositories/partidas.py"),
        Path("repositories/grupos.py"),
        Path("repositories/rodadas.py"),
        Path("repositories/quadras.py"),
    ]
    for caminho in alvos:
        texto = caminho.read_text(encoding="utf-8")
        assert "if not force:" in texto, caminho
        assert "require_schema" in texto, caminho


def test_migracoes_incluem_partidas_e_grupos():
    texto = Path("core/schema_migrations.py").read_text(encoding="utf-8")
    assert '"banco:criar_tabela_partidas"' in texto
    assert '"banco:criar_tabelas_grupos"' in texto
