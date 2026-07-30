from pathlib import Path

from core.schema_migrations import executar_migracoes, listar_migracoes


def test_dry_run_nao_exige_banco():
    resultado = executar_migracoes(dry_run=True)
    assert len(resultado) >= 9
    assert all(item["status"] == "pendente_desconhecido" for item in resultado)


def test_versoes_de_migracao_sao_unicas():
    versoes = [step.version for step in listar_migracoes()]
    assert len(versoes) == len(set(versoes))


def test_app_nao_executa_ddl_no_import():
    app_source = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
    chamadas_proibidas = (
        "garantir_schema_runtime()",
        "criar_estrutura_rotacao_profissional()",
        "criar_tabela_atalhos_apontador()",
        "criar_tabela_equipes_competicoes()",
        "criar_campos_perfil_equipe()",
        "criar_campo_escudo_equipes()",
        "criar_campos_quadro_tecnico_equipes()",
        "criar_campos_liberacao_extra_equipes()",
        "garantir_campos_trava_operacional_partida()",
    )
    assert not any(call in app_source for call in chamadas_proibidas)
