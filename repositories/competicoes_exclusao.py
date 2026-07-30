"""Exclusão transacional e segura de competições.

Preserva cadastros permanentes e remove apenas dados e vínculos pertencentes
à competição. A rotina permanece introspectiva para tolerar versões diferentes
do esquema durante a migração.
"""
from __future__ import annotations

import re

from repositories.conexao import conectar


def excluir_competicao_persistencia(nome):
    """Exclui uma competição sem apagar cadastros permanentes.

    Mantém:
    - atletas cadastrados;
    - equipes cadastradas;
    - usuários superadmin, equipe e apontador;
    - oficiais/apontadores globais.

    Remove:
    - competição principal;
    - partidas;
    - grupos;
    - vínculos da competição;
    - papeletas;
    - eventos;
    - scouts/destaques/sanções/históricos/pins/quadras/configurações da competição.

    Importante:
    O usuário organizador NÃO pode ser apagado antes da linha de competicoes,
    porque competicoes.organizador_login pode referenciar usuarios.login.
    Por isso primeiro limpamos os dados operacionais, depois apagamos
    competicoes e só no final removemos o organizador daquela competição.
    """
    if not nome:
        return False

    def _valor_linha(linha, chave, indice=0):
        return linha.get(chave) if isinstance(linha, dict) else linha[indice]

    def _identificador(nome_identificador):
        """Aspas seguras para nomes vindos do information_schema."""
        nome_identificador = str(nome_identificador or "")
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", nome_identificador):
            raise ValueError(f"Identificador SQL inválido: {nome_identificador}")
        return '"' + nome_identificador.replace('"', '""') + '"'

    def _tabela_existe(tabelas, tabela):
        return tabela in tabelas

    def _tem_coluna(colunas_por_tabela, tabela, coluna):
        return coluna in colunas_por_tabela.get(tabela, set())

    def _delete_por_coluna(cur, tabelas_existentes, colunas_por_tabela, tabela, coluna, valor):
        if _tabela_existe(tabelas_existentes, tabela) and _tem_coluna(colunas_por_tabela, tabela, coluna):
            cur.execute(
                f"DELETE FROM {_identificador(tabela)} WHERE {_identificador(coluna)} = %s",
                (valor,),
            )

    def _update_vinculo_null(cur, tabelas_existentes, colunas_por_tabela, tabela, coluna_filtro, valor_filtro, campos_null):
        if not _tabela_existe(tabelas_existentes, tabela):
            return
        if not _tem_coluna(colunas_por_tabela, tabela, coluna_filtro):
            return

        campos_validos = [c for c in campos_null if _tem_coluna(colunas_por_tabela, tabela, c)]
        if not campos_validos:
            return

        # IMPORTANTE:
        # No banco atual, atletas.equipe e atletas.competicao são NOT NULL.
        # Por isso não podemos remover o vínculo usando NULL nesses campos.
        # Usamos string vazia para soltar o atleta/equipe da competição sem
        # apagar o cadastro permanente.
        sets_partes = []
        valores_update = []

        for c in campos_validos:
            if tabela == "atletas" and c in {"competicao", "equipe"}:
                sets_partes.append(f"{_identificador(c)} = %s")
                valores_update.append("")
            elif tabela == "equipes" and c == "competicao":
                sets_partes.append(f"{_identificador(c)} = %s")
                valores_update.append("")
            else:
                sets_partes.append(f"{_identificador(c)} = NULL")

        valores_update.append(valor_filtro)
        sets = ", ".join(sets_partes)
        cur.execute(
            f"UPDATE {_identificador(tabela)} SET {sets} WHERE {_identificador(coluna_filtro)} = %s",
            tuple(valores_update),
        )

    # Tabelas que NUNCA devem ser apagadas inteiras nesta rotina.
    # Elas só podem receber UPDATE/limpeza de vínculo quando necessário.
    tabelas_preservadas = {
        "atletas",
        "equipes",
        "usuarios",
        "oficiais",
        "apontadores_acesso",
        "demos_temporarias",
        "configuracoes_sistema",
    }

    # Alguns bancos antigos/novos usam nomes diferentes para a coluna da competição.
    colunas_competicao_possiveis = (
        "competicao",
        "nome_competicao",
        "competicao_nome",
    )

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                print(">>> EXCLUINDO COMPETIÇÃO COMPLETA:", nome, flush=True)

                cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_type = 'BASE TABLE'
                """)
                tabelas_existentes = {
                    _valor_linha(linha, "table_name")
                    for linha in cur.fetchall()
                }

                cur.execute("""
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                """)
                colunas_por_tabela = {}
                for linha in cur.fetchall():
                    tabela = _valor_linha(linha, "table_name", 0)
                    coluna = _valor_linha(linha, "column_name", 1)
                    colunas_por_tabela.setdefault(tabela, set()).add(coluna)

                # Corrige a trava única de atletas antes de soltar os vínculos.
                # Bancos antigos podem ter o índice antigo que não permite vários atletas
                # com competicao vazia. A exclusão precisa preservar atletas e limpar o
                # vínculo, então o índice deve valer somente para atletas vinculados a
                # uma competição real.
                if _tabela_existe(tabelas_existentes, "atletas"):
                    cur.execute("DROP INDEX IF EXISTS uq_atletas_cpf_competicao")
                    cur.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS uq_atletas_cpf_competicao
                        ON atletas (
                            REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g'),
                            COALESCE(competicao, '')
                        )
                        WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') <> ''
                          AND COALESCE(competicao, '') <> ''
                    """)

                # No modo rápido, atletas são descartáveis; no modo normal, permanecem.
                competicao_rapida = False
                if _tabela_existe(tabelas_existentes, "competicoes") and _tem_coluna(colunas_por_tabela, "competicoes", "tipo_competicao"):
                    cur.execute("SELECT COALESCE(tipo_competicao, 'normal') AS tipo FROM competicoes WHERE nome=%s LIMIT 1", (nome,))
                    row_tipo = cur.fetchone() or {}
                    competicao_rapida = str(_valor_linha(row_tipo, "tipo") or "normal").lower() == "rapida"

                # Guarda o login do organizador antes de apagar a competição.
                organizador_login = None
                if _tabela_existe(tabelas_existentes, "competicoes"):
                    col_competicoes = colunas_por_tabela.get("competicoes", set())
                    if "organizador_login" in col_competicoes:
                        cur.execute("""
                            SELECT organizador_login
                            FROM competicoes
                            WHERE nome = %s
                            LIMIT 1
                        """, (nome,))
                        row_org = cur.fetchone()
                        if row_org:
                            organizador_login = _valor_linha(row_org, "organizador_login")

                # IDs das partidas da competição. Isso permite apagar primeiro tudo
                # que depende de partida_id, antes de remover partidas.
                partida_ids = []
                if _tabela_existe(tabelas_existentes, "partidas") and _tem_coluna(colunas_por_tabela, "partidas", "competicao"):
                    cur.execute("SELECT id FROM partidas WHERE competicao = %s", (nome,))
                    partida_ids = [_valor_linha(linha, "id") for linha in cur.fetchall()]

                # Usuários permanentes ficam. Só perdem o vínculo com a competição.
                # NÃO apagar o organizador aqui, porque competicoes.organizador_login
                # ainda pode estar apontando para usuarios.login.
                if _tabela_existe(tabelas_existentes, "usuarios"):
                    col_usuarios = colunas_por_tabela.get("usuarios", set())
                    if "competicao_vinculada" in col_usuarios:
                        cur.execute("""
                            UPDATE usuarios
                            SET competicao_vinculada = NULL
                            WHERE competicao_vinculada = %s
                              AND COALESCE(perfil, '') IN ('apontador', 'equipe')
                        """, (nome,))

                # Atletas rápidos são apagados; atletas normais apenas perdem o vínculo.
                if competicao_rapida:
                    _delete_por_coluna(cur, tabelas_existentes, colunas_por_tabela, "atletas", "competicao", nome)
                else:
                    _update_vinculo_null(
                        cur, tabelas_existentes, colunas_por_tabela, "atletas", "competicao", nome,
                        ["competicao", "equipe", "equipe_login", "equipe_id", "numero"],
                    )

                # Equipes ficam cadastradas. Se ainda houver coluna antiga competicao,
                # remove só o vínculo antigo sem apagar a equipe global.
                _update_vinculo_null(
                    cur,
                    tabelas_existentes,
                    colunas_por_tabela,
                    "equipes",
                    "competicao",
                    nome,
                    ["competicao"],
                )

                # 1) Apaga primeiro qualquer tabela filha que tenha partida_id.
                # Isso cobre eventos, scouts, papeletas, sanções, destaques,
                # histórico de rotação e tabelas novas que forem criadas depois.
                if partida_ids:
                    for tabela in sorted(tabelas_existentes):
                        if tabela in tabelas_preservadas or tabela in {"partidas", "competicoes"}:
                            continue
                        colunas = colunas_por_tabela.get(tabela, set())
                        if "partida_id" not in colunas:
                            continue
                        cur.execute(
                            f"DELETE FROM {_identificador(tabela)} WHERE partida_id = ANY(%s)",
                            (partida_ids,),
                        )

                # 2) Ordem explícita para tabelas que podem ter dependência entre si.
                # Ex.: grupos_equipes pode depender de grupos; por isso sai antes.
                ordem_explicita = [
                    "eventos_partida",
                    "eventos",
                    "historico_rotacao",
                    "sancoes_partida",
                    "destaques_partida",
                    "papeletas_sets",
                    "papeletas",
                    "classificacao_cache",
                    "equipe_conferencia",
                    "grupo_equipes",
                    "grupos_equipes",
                    "grupos",
                    "competicao_quadras",
                    "competicao_agenda_config",
                    "competicao_oficiais",
                    "competicao_pins_operacionais",
                    "solicitacoes_treinador",
                    "equipes_competicoes",
                    "partidas",
                ]

                tabelas_ja_limpas = set()
                for tabela in ordem_explicita:
                    if tabela in tabelas_preservadas or tabela == "competicoes":
                        continue
                    if not _tabela_existe(tabelas_existentes, tabela):
                        continue
                    colunas = colunas_por_tabela.get(tabela, set())
                    for coluna_comp in colunas_competicao_possiveis:
                        if coluna_comp in colunas:
                            cur.execute(
                                f"DELETE FROM {_identificador(tabela)} WHERE {_identificador(coluna_comp)} = %s",
                                (nome,),
                            )
                            tabelas_ja_limpas.add(tabela)
                            break

                # 3) Apaga tudo que possui coluna de competição, exceto cadastros preservados,
                # competicoes e tabelas já limpas acima. Isso deixa a rotina preparada para
                # tabelas novas criadas no futuro.
                for tabela in sorted(tabelas_existentes):
                    if tabela in tabelas_preservadas or tabela == "competicoes" or tabela in tabelas_ja_limpas:
                        continue
                    colunas = colunas_por_tabela.get(tabela, set())
                    for coluna_comp in colunas_competicao_possiveis:
                        if coluna_comp in colunas:
                            cur.execute(
                                f"DELETE FROM {_identificador(tabela)} WHERE {_identificador(coluna_comp)} = %s",
                                (nome,),
                            )
                            break

                # 4) Remove a competição principal.
                if _tabela_existe(tabelas_existentes, "competicoes"):
                    cur.execute("DELETE FROM competicoes WHERE nome = %s", (nome,))

                # 5) Agora sim remove o usuário organizador gerado para a competição.
                # SUPERADMIN, equipe e apontador continuam preservados.
                if _tabela_existe(tabelas_existentes, "usuarios"):
                    col_usuarios = colunas_por_tabela.get("usuarios", set())

                    if "competicao_vinculada" in col_usuarios:
                        cur.execute("""
                            DELETE FROM usuarios
                            WHERE competicao_vinculada = %s
                              AND COALESCE(perfil, '') NOT IN ('superadmin', 'apontador', 'equipe')
                        """, (nome,))

                    if organizador_login and "login" in col_usuarios:
                        cur.execute("""
                            DELETE FROM usuarios
                            WHERE login = %s
                              AND COALESCE(perfil, '') NOT IN ('superadmin', 'apontador', 'equipe')
                        """, (organizador_login,))

            conn.commit()

        print(">>> COMPETIÇÃO EXCLUÍDA COMPLETAMENTE", flush=True)
        return True

    except Exception as e:
        print("ERRO REAL AO EXCLUIR COMPETIÇÃO:", repr(e), flush=True)
        return False

