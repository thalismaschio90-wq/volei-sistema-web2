"""Persistência de nome, perfil e identidade visual das equipes.

Mantém o contrato legado enquanto retira SQL e normalização do banco.py.
"""
from __future__ import annotations

from rules.equipes_perfil import preparar_perfil_equipe, validar_renomeacao_equipe
from repositories.conexao import conectar
from repositories.equipes_contexto import colunas_equipes
from core.schema_inspection import buscar_colunas_tabela


def atualizar_nome_equipe_persistencia(nome_atual, nome_competicao, novo_nome):
    valido, _mensagem, nomes = validar_renomeacao_equipe(nome_atual, nome_competicao, novo_nome)
    nome_atual, nome_competicao, novo_nome = nomes
    if not valido:
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(travada, FALSE) AS travada FROM competicoes WHERE nome = %s LIMIT 1",
                (nome_competicao,),
            )
            competicao = cur.fetchone() or {}
            if bool(competicao.get("travada")):
                return False
            cur.execute("""
                SELECT e.login, e.nome AS nome_global, ec.equipe_nome AS nome_vinculo
                FROM equipes_competicoes ec
                JOIN equipes e
                  ON e.login = ec.equipe_login
                  OR LOWER(TRIM(e.nome)) = LOWER(TRIM(ec.equipe_nome))
                WHERE ec.competicao = %s
                  AND (
                        LOWER(TRIM(ec.equipe_nome)) = LOWER(TRIM(%s))
                     OR LOWER(TRIM(e.nome)) = LOWER(TRIM(%s))
                     OR LOWER(TRIM(e.login)) = LOWER(TRIM(%s))
                  )
                LIMIT 1
            """, (nome_competicao, nome_atual, nome_atual, nome_atual))
            equipe = cur.fetchone()
            if not equipe:
                return False

            login_equipe = (equipe.get("login") or "").strip()
            nomes_antigos = []
            for valor in (nome_atual, equipe.get("nome_global"), equipe.get("nome_vinculo")):
                valor = str(valor or "").strip()
                if valor and valor.casefold() not in {v.casefold() for v in nomes_antigos}:
                    nomes_antigos.append(valor)

            cur.execute("UPDATE equipes SET nome = %s WHERE login = %s", (novo_nome, login_equipe))
            cur.execute("""
                UPDATE equipes_competicoes
                SET equipe_nome = %s, equipe_login = %s
                WHERE competicao = %s
                  AND (equipe_login = %s OR LOWER(TRIM(equipe_nome)) = ANY(%s))
            """, (novo_nome, login_equipe, nome_competicao, login_equipe, [n.lower() for n in nomes_antigos]))
            cur.execute("""
                UPDATE usuarios SET nome = %s, equipe = %s
                WHERE login = %s AND perfil = 'equipe'
            """, (novo_nome, novo_nome, login_equipe))

            for nome_antigo in nomes_antigos:
                cur.execute("""
                    UPDATE grupos_equipes SET equipe = %s
                    WHERE competicao = %s AND LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                """, (novo_nome, nome_competicao, nome_antigo))
                cur.execute("""
                    UPDATE partidas
                    SET equipe_a = CASE WHEN LOWER(TRIM(COALESCE(equipe_a, ''))) = LOWER(TRIM(%s)) THEN %s ELSE equipe_a END,
                        equipe_b = CASE WHEN LOWER(TRIM(COALESCE(equipe_b, ''))) = LOWER(TRIM(%s)) THEN %s ELSE equipe_b END,
                        equipe_a_operacional = CASE WHEN LOWER(TRIM(COALESCE(equipe_a_operacional, ''))) = LOWER(TRIM(%s)) THEN %s ELSE equipe_a_operacional END,
                        equipe_b_operacional = CASE WHEN LOWER(TRIM(COALESCE(equipe_b_operacional, ''))) = LOWER(TRIM(%s)) THEN %s ELSE equipe_b_operacional END,
                        lado_esquerdo = CASE WHEN LOWER(TRIM(COALESCE(lado_esquerdo, ''))) = LOWER(TRIM(%s)) THEN %s ELSE lado_esquerdo END,
                        saque_inicial = CASE WHEN LOWER(TRIM(COALESCE(saque_inicial, ''))) = LOWER(TRIM(%s)) THEN %s ELSE saque_inicial END,
                        sorteio_vencedor = CASE WHEN LOWER(TRIM(COALESCE(sorteio_vencedor, ''))) = LOWER(TRIM(%s)) THEN %s ELSE sorteio_vencedor END,
                        vencedor = CASE WHEN LOWER(TRIM(COALESCE(vencedor, ''))) = LOWER(TRIM(%s)) THEN %s ELSE vencedor END
                    WHERE competicao = %s AND (
                        LOWER(TRIM(COALESCE(equipe_a, ''))) = LOWER(TRIM(%s)) OR
                        LOWER(TRIM(COALESCE(equipe_b, ''))) = LOWER(TRIM(%s)) OR
                        LOWER(TRIM(COALESCE(equipe_a_operacional, ''))) = LOWER(TRIM(%s)) OR
                        LOWER(TRIM(COALESCE(equipe_b_operacional, ''))) = LOWER(TRIM(%s)) OR
                        LOWER(TRIM(COALESCE(lado_esquerdo, ''))) = LOWER(TRIM(%s)) OR
                        LOWER(TRIM(COALESCE(saque_inicial, ''))) = LOWER(TRIM(%s)) OR
                        LOWER(TRIM(COALESCE(sorteio_vencedor, ''))) = LOWER(TRIM(%s)) OR
                        LOWER(TRIM(COALESCE(vencedor, ''))) = LOWER(TRIM(%s))
                    )
                """, (
                    nome_antigo, novo_nome, nome_antigo, novo_nome,
                    nome_antigo, novo_nome, nome_antigo, novo_nome,
                    nome_antigo, novo_nome, nome_antigo, novo_nome,
                    nome_antigo, novo_nome, nome_antigo, novo_nome,
                    nome_competicao,
                    nome_antigo, nome_antigo, nome_antigo, nome_antigo,
                    nome_antigo, nome_antigo, nome_antigo, nome_antigo,
                ))
                cur.execute("""
                    UPDATE atletas SET equipe = %s
                    WHERE competicao = %s AND LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                """, (novo_nome, nome_competicao, nome_antigo))

                for tabela, campo in (("papeletas", "equipe"), ("papeleta", "equipe"), ("eventos_partida", "equipe"), ("eventos", "equipe"), ("historico_rotacao", "equipe")):
                    try:
                        colunas = buscar_colunas_tabela(tabela)
                        if campo in colunas and "competicao" in colunas:
                            cur.execute(
                                f"UPDATE {tabela} SET {campo} = %s WHERE competicao = %s AND LOWER(TRIM({campo})) = LOWER(TRIM(%s))",
                                (novo_nome, nome_competicao, nome_antigo),
                            )
                    except Exception as exc:
                        print(f"AVISO atualizar_nome_equipe/{tabela}.{campo}:", repr(exc))
        conn.commit()
    return True, "Atualizado com sucesso!"


def _atualizar_por_login_com_fallback(cur, set_sql, valores_base, login):
    cur.execute(f"UPDATE equipes SET {set_sql} WHERE login = %s", tuple(valores_base + [login]))
    alteradas = cur.rowcount or 0
    if alteradas <= 0:
        cur.execute("SELECT equipe FROM usuarios WHERE login = %s LIMIT 1", (login,))
        row = cur.fetchone()
        nome = ""
        try:
            nome = (row.get("equipe") if hasattr(row, "get") else row[0]) or ""
        except Exception:
            nome = ""
        nome = str(nome).strip()
        if nome:
            cur.execute(
                f"UPDATE equipes SET {set_sql} WHERE LOWER(TRIM(nome)) = LOWER(TRIM(%s))",
                tuple(valores_base + [nome]),
            )
            alteradas = cur.rowcount or 0
    if alteradas <= 0:
        try:
            cur.execute(
                f"""
                UPDATE equipes e SET {set_sql}
                FROM equipes_competicoes ec
                WHERE (ec.equipe_login = %s OR LOWER(TRIM(ec.equipe_nome)) = LOWER(TRIM(e.nome)))
                  AND (e.login = ec.equipe_login OR LOWER(TRIM(e.nome)) = LOWER(TRIM(ec.equipe_nome)))
                """,
                tuple(valores_base + [login]),
            )
            alteradas = cur.rowcount or 0
        except Exception as exc:
            print("AVISO atualizar equipe/fallback_vinculo:", repr(exc))
    return alteradas


def atualizar_escudo_equipe_por_login_persistencia(login, escudo, escudo_blob=None):
    login = str(login or "").strip()
    escudo = str(escudo or "").strip()
    escudo_blob = escudo if escudo_blob is None else str(escudo_blob or "").strip()
    if not login:
        return False
    colunas = colunas_equipes()
    sets, valores = [], []
    if "escudo" in colunas:
        sets.append("escudo = %s"); valores.append(escudo)
    if "escudo_blob" in colunas:
        sets.append("escudo_blob = %s"); valores.append(escudo_blob)
    if not sets:
        return False
    with conectar() as conn:
        with conn.cursor() as cur:
            alteradas = _atualizar_por_login_com_fallback(cur, ", ".join(sets), valores, login)
        conn.commit()
    return alteradas > 0


def perfil_equipe_incompleto_por_login_consulta(login, conn=None):
    login = str(login or "").strip()
    if not login:
        return False
    sql = """
        SELECT nome, cidade, responsavel, telefone, email, instagram,
               COALESCE(perfil_completo, FALSE) AS perfil_completo
        FROM equipes WHERE login = %s LIMIT 1
    """
    if conn is None:
        with conectar() as cnx:
            return perfil_equipe_incompleto_por_login_consulta(login, conn=cnx)
    with conn.cursor() as cur:
        cur.execute(sql, (login,))
        equipe = cur.fetchone()
    if not equipe:
        return False
    return not all(str(equipe.get(campo) or "").strip() for campo in ("cidade", "responsavel", "telefone"))


def salvar_perfil_equipe_por_login_persistencia(login, cidade="", responsavel="", telefone="", email="", instagram="", escudo=None):
    login = str(login or "").strip()
    if not login:
        return False
    dados = preparar_perfil_equipe(cidade, responsavel, telefone, email, instagram)
    sets = ["cidade = %s", "responsavel = %s", "telefone = %s", "email = %s", "instagram = %s", "perfil_completo = %s"]
    valores = [dados.cidade, dados.responsavel, dados.telefone, dados.email, dados.instagram, dados.completo]
    if escudo is not None:
        valor = str(escudo or "").strip()
        sets.append("escudo = %s"); valores.append(valor)
        if "escudo_blob" in colunas_equipes():
            sets.append("escudo_blob = %s"); valores.append(valor)
    with conectar() as conn:
        with conn.cursor() as cur:
            alteradas = _atualizar_por_login_com_fallback(cur, ", ".join(sets), valores, login)
        conn.commit()
    return alteradas > 0
