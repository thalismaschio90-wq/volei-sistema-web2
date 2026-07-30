import banco


def test_fachadas_grupos_existem():
    nomes = [
        'criar_tabelas_grupos', 'listar_grupos', 'criar_grupo',
        'adicionar_equipe_no_grupo', 'listar_equipes_por_grupo',
        'listar_equipes_por_grupos_competicao', 'buscar_grupo_por_id',
        'atualizar_grupo', 'remover_equipe_do_grupo', 'excluir_grupo',
    ]
    for nome in nomes:
        assert callable(getattr(banco, nome))
