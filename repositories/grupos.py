"""Persistência dos grupos e dos vínculos de equipes.

Este módulo somente consulta e grava dados. As decisões de bloqueio da fase são
recebidas por parâmetro para evitar dependência circular com ``banco.py``.
"""
from repositories.conexao import conectar
from rules.grupos import (
    dados_grupo_validos,
    normalizar_nome_equipe,
    normalizar_nome_grupo,
    vinculo_grupo_valido,
)


def criar_tabelas_grupos(*, cache_colunas=None, force=False):
    if not force:
        from core.schema_requirements import require_schema
        require_schema(
            tables=("grupos", "grupos_equipes"),
            columns={"grupos": ("competicao", "quadra_id", "quadra_nome")},
            context="grupos das competições",
        )
        return
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS grupos (
                    id SERIAL PRIMARY KEY,
                    nome VARCHAR(30),
                    competicao TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS grupos_equipes (
                    id SERIAL PRIMARY KEY,
                    grupo_id INTEGER,
                    equipe TEXT,
                    competicao TEXT
                )
            """)
            cur.execute("ALTER TABLE grupos ADD COLUMN IF NOT EXISTS quadra_id INTEGER")
            cur.execute("ALTER TABLE grupos ADD COLUMN IF NOT EXISTS quadra_nome TEXT DEFAULT ''")
        conn.commit()
    if cache_colunas is not None:
        cache_colunas.pop("grupos", None)


def listar_grupos(competicao):
    competicao = str(competicao or "").strip()
    if not competicao:
        return []
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, nome, competicao, quadra_id, quadra_nome
                FROM grupos
                WHERE competicao = %s
                ORDER BY nome
            """, (competicao,))
            return cur.fetchall() or []


def criar_grupo(nome, competicao, *, fase_travada=False):
    nome = normalizar_nome_grupo(nome)
    competicao = str(competicao or "").strip()
    if fase_travada or not dados_grupo_validos(nome, competicao):
        return False
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM grupos
                WHERE UPPER(TRIM(nome)) = UPPER(TRIM(%s))
                  AND competicao = %s
                LIMIT 1
            """, (nome, competicao))
            if cur.fetchone():
                return False
            cur.execute("INSERT INTO grupos (nome, competicao) VALUES (%s, %s)", (nome, competicao))
        conn.commit()
    return True


def adicionar_equipe_no_grupo(grupo_id, equipe, competicao, *, fase_travada=False):
    equipe = normalizar_nome_equipe(equipe)
    competicao = str(competicao or "").strip()
    if fase_travada or not vinculo_grupo_valido(grupo_id, equipe, competicao):
        return False
    grupo_id = int(grupo_id)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM grupos
                WHERE id = %s AND competicao = %s
                LIMIT 1
            """, (grupo_id, competicao))
            if not cur.fetchone():
                return False
            cur.execute("""
                DELETE FROM grupos_equipes
                WHERE competicao = %s
                  AND LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                  AND grupo_id <> %s
            """, (competicao, equipe, grupo_id))
            cur.execute("""
                SELECT id FROM grupos_equipes
                WHERE grupo_id = %s
                  AND LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                  AND competicao = %s
                LIMIT 1
            """, (grupo_id, equipe, competicao))
            if cur.fetchone():
                return False
            cur.execute("""
                INSERT INTO grupos_equipes (grupo_id, equipe, competicao)
                VALUES (%s, %s, %s)
            """, (grupo_id, equipe, competicao))
        conn.commit()
    return True


def listar_equipes_por_grupo(grupo_id):
    try:
        grupo_id = int(grupo_id)
    except (TypeError, ValueError):
        return []
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, grupo_id, equipe, competicao
                FROM grupos_equipes
                WHERE grupo_id = %s
                ORDER BY equipe
            """, (grupo_id,))
            return cur.fetchall() or []


def listar_equipes_por_grupos_competicao(competicao):
    competicao = str(competicao or "").strip()
    if not competicao:
        return {}
    resultado = {}
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ge.id, ge.grupo_id, ge.equipe, ge.competicao
                FROM grupos_equipes ge
                JOIN grupos g ON g.id = ge.grupo_id
                WHERE ge.competicao = %s AND g.competicao = %s
                ORDER BY g.nome, ge.equipe
            """, (competicao, competicao))
            for row in cur.fetchall() or []:
                resultado.setdefault(row.get("grupo_id"), []).append(row)
    return resultado


def buscar_grupo_por_id(grupo_id, competicao):
    try:
        grupo_id = int(grupo_id)
    except (TypeError, ValueError):
        return None
    competicao = str(competicao or "").strip()
    if not competicao:
        return None
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, nome, competicao, quadra_id, quadra_nome
                FROM grupos
                WHERE id = %s AND competicao = %s
                LIMIT 1
            """, (grupo_id, competicao))
            return cur.fetchone()


def atualizar_grupo(grupo_id, novo_nome, competicao):
    try:
        grupo_id = int(grupo_id)
    except (TypeError, ValueError):
        return False
    novo_nome = normalizar_nome_grupo(novo_nome)
    competicao = str(competicao or "").strip()
    if not dados_grupo_validos(novo_nome, competicao):
        return False
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nome FROM grupos WHERE id = %s AND competicao = %s LIMIT 1", (grupo_id, competicao))
            atual = cur.fetchone()
            if not atual:
                return False
            nome_antigo = atual["nome"]
            cur.execute("""
                SELECT id FROM grupos
                WHERE UPPER(TRIM(nome)) = UPPER(TRIM(%s))
                  AND competicao = %s AND id <> %s
                LIMIT 1
            """, (novo_nome, competicao, grupo_id))
            if cur.fetchone():
                return False
            cur.execute("UPDATE grupos SET nome = %s WHERE id = %s AND competicao = %s", (novo_nome, grupo_id, competicao))
            cur.execute("""
                UPDATE partidas SET grupo = %s
                WHERE competicao = %s AND grupo = %s
            """, (novo_nome, competicao, nome_antigo))
        conn.commit()
    return True


def remover_equipe_do_grupo(grupo_id, equipe, competicao, *, fase_travada=False):
    equipe = normalizar_nome_equipe(equipe)
    competicao = str(competicao or "").strip()
    if fase_travada or not vinculo_grupo_valido(grupo_id, equipe, competicao):
        return False
    grupo_id = int(grupo_id)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nome FROM grupos WHERE id = %s AND competicao = %s LIMIT 1", (grupo_id, competicao))
            grupo = cur.fetchone()
            if not grupo:
                return False
            cur.execute("""
                DELETE FROM grupos_equipes
                WHERE grupo_id = %s AND equipe = %s AND competicao = %s
            """, (grupo_id, equipe, competicao))
            cur.execute("""
                DELETE FROM partidas
                WHERE competicao = %s AND grupo = %s
                  AND (equipe_a = %s OR equipe_b = %s)
            """, (competicao, grupo["nome"], equipe, equipe))
        conn.commit()
    return True


def excluir_grupo(grupo_id, competicao, *, fase_travada=False):
    try:
        grupo_id = int(grupo_id)
    except (TypeError, ValueError):
        return False
    competicao = str(competicao or "").strip()
    if fase_travada or not competicao:
        return False
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nome FROM grupos WHERE id = %s AND competicao = %s LIMIT 1", (grupo_id, competicao))
            grupo = cur.fetchone()
            if not grupo:
                return False
            cur.execute("DELETE FROM partidas WHERE competicao = %s AND grupo = %s", (competicao, grupo["nome"]))
            cur.execute("DELETE FROM grupos_equipes WHERE grupo_id = %s AND competicao = %s", (grupo_id, competicao))
            cur.execute("DELETE FROM grupos WHERE id = %s AND competicao = %s", (grupo_id, competicao))
        conn.commit()
    return True


def limpar_vinculos_competicao(competicao, *, conn=None):
    """Remove os vínculos dos grupos da competição numa transação curta."""
    competicao = str(competicao or "").strip()
    if not competicao:
        return 0
    if conn is not None:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM grupos_equipes WHERE competicao = %s", (competicao,))
            return cur.rowcount or 0
    with conectar() as conexao:
        with conexao.cursor() as cur:
            cur.execute("DELETE FROM grupos_equipes WHERE competicao = %s", (competicao,))
            total = cur.rowcount or 0
        conexao.commit()
    return total


def substituir_distribuicao_equipes(competicao, distribuicao):
    """Substitui todos os vínculos da competição em uma única transação."""
    competicao = str(competicao or "").strip()
    registros = []
    for item in distribuicao or []:
        try:
            grupo_id = int((item or {}).get("grupo_id"))
        except (TypeError, ValueError):
            continue
        equipe = normalizar_nome_equipe((item or {}).get("equipe"))
        if grupo_id > 0 and equipe:
            registros.append((grupo_id, equipe, competicao))
    if not competicao:
        return 0
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM grupos_equipes WHERE competicao = %s", (competicao,))
            if registros:
                cur.executemany(
                    """
                    INSERT INTO grupos_equipes (grupo_id, equipe, competicao)
                    VALUES (%s, %s, %s)
                    """,
                    registros,
                )
        conn.commit()
    return len(registros)
