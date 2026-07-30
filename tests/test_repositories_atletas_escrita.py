import pytest

from repositories.atletas_escrita import _dependencias


def test_dependencias_rejeita_mapa_incompleto():
    with pytest.raises(RuntimeError) as exc:
        _dependencias({"conectar": object()})
    assert "Dependências ausentes" in str(exc.value)
    assert "cpf_valido" in str(exc.value)


def test_dependencias_aceita_contrato_completo():
    nomes = (
        "conectar", "somente_digitos", "formatar_cpf", "cpf_valido",
        "criar_tabela_atletas", "criar_campos_controle_inscricao_competicoes",
        "criar_campos_liberacao_extra_equipes", "criar_campos_conferencia_atletas",
        "cpf_sql_limpo", "salvar_atleta_global",
    )
    deps = {nome: object() for nome in nomes}
    assert _dependencias(deps) == deps
