"""Persistência de quadras, PINs e vínculos com grupos/partidas."""
import random
from repositories.conexao import conectar
from rules.quadras import (
    formatar_quadra_exibicao,
    normalizar_lista_quadras,
    normalizar_nome_competicao,
    normalizar_pin_arbitragem,
    normalizar_quantidade_quadras,
    quadra_matches_texto,
)


def _tabela_existe_cur(cur, nome):
    cur.execute("""SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=%s LIMIT 1""", (nome,))
    return cur.fetchone() is not None


def _colunas_cur(cur, tabela):
    cur.execute("""SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name=%s""", (tabela,))
    return {r["column_name"] if isinstance(r, dict) else r[0] for r in (cur.fetchall() or [])}


def criar_tabela_competicao_quadras(*, cache_colunas=None, marcar_schema=None, schema_pronto=None, force=False):
    if not force:
        from core.schema_requirements import require_schema
        require_schema(
            tables=("competicao_quadras",),
            columns={"competicao_quadras": ("competicao", "nome", "ordem", "pin_arbitragem")},
            context="quadras das competições",
        )
        if marcar_schema:
            marcar_schema("tabela_competicao_quadras")
        return
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS competicao_quadras (
                id SERIAL PRIMARY KEY, competicao TEXT NOT NULL, nome TEXT NOT NULL,
                local TEXT DEFAULT '', ordem INTEGER DEFAULT 1, ativa BOOLEAN DEFAULT TRUE,
                criado_em TIMESTAMP DEFAULT NOW(), atualizado_em TIMESTAMP DEFAULT NOW(),
                pin_arbitragem VARCHAR(4), pin_arbitragem_criado_em TIMESTAMP)""")
            cur.execute("""ALTER TABLE competicao_quadras
                ADD COLUMN IF NOT EXISTS competicao TEXT NOT NULL,
                ADD COLUMN IF NOT EXISTS nome TEXT NOT NULL DEFAULT 'Quadra',
                ADD COLUMN IF NOT EXISTS local TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS ordem INTEGER DEFAULT 1,
                ADD COLUMN IF NOT EXISTS ativa BOOLEAN DEFAULT TRUE,
                ADD COLUMN IF NOT EXISTS criado_em TIMESTAMP DEFAULT NOW(),
                ADD COLUMN IF NOT EXISTS atualizado_em TIMESTAMP DEFAULT NOW(),
                ADD COLUMN IF NOT EXISTS pin_arbitragem VARCHAR(4),
                ADD COLUMN IF NOT EXISTS pin_arbitragem_criado_em TIMESTAMP""")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_competicao_quadras_competicao ON competicao_quadras (competicao)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_competicao_quadras_ordem ON competicao_quadras (competicao, ordem)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_competicao_quadras_pin_arbitragem ON competicao_quadras (pin_arbitragem)")
            if _tabela_existe_cur(cur, "partidas"):
                cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS quadra_id INTEGER")
                cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS quadra_nome TEXT DEFAULT ''")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_partidas_competicao_quadra ON partidas (competicao, quadra_id)")
            if _tabela_existe_cur(cur, "grupos"):
                cur.execute("ALTER TABLE grupos ADD COLUMN IF NOT EXISTS quadra_id INTEGER")
                cur.execute("ALTER TABLE grupos ADD COLUMN IF NOT EXISTS quadra_nome TEXT DEFAULT ''")
        conn.commit()
    if cache_colunas is not None:
        for tabela in ("competicao_quadras", "partidas", "grupos"):
            cache_colunas.pop(tabela, None)
    if marcar_schema:
        try: marcar_schema("tabela_competicao_quadras")
        except Exception: pass


def _gerar_pin_unico_cur(cur):
    for _ in range(60):
        pin = str(random.randint(1000, 9999))
        cur.execute("SELECT id FROM competicao_quadras WHERE pin_arbitragem=%s LIMIT 1", (pin,))
        if not cur.fetchone(): return pin
    return str(random.randint(1000, 9999))


def garantir_pins_arbitragem_quadras(nome_competicao):
    nome_competicao = normalizar_nome_competicao(nome_competicao)
    if not nome_competicao: return []
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM competicao_quadras WHERE competicao=%s AND COALESCE(ativa,TRUE)=TRUE LIMIT 1", (nome_competicao,))
            if not cur.fetchone():
                cur.execute("""SELECT COALESCE(NULLIF(TRIM(quadra),''),'1') quadra,
                    COALESCE(NULLIF(TRIM(quadra_nome),''),NULLIF(TRIM(quadra),''),'Quadra 1') quadra_nome
                    FROM partidas WHERE competicao=%s GROUP BY 1,2 ORDER BY 1""", (nome_competicao,))
                linhas = cur.fetchall() or [{"quadra":"1","quadra_nome":"Quadra 1"}]
                for idx, linha in enumerate(linhas, 1):
                    numero = str(linha.get("quadra") or idx).strip()
                    nome = str(linha.get("quadra_nome") or f"Quadra {numero}").strip()
                    cur.execute("INSERT INTO competicao_quadras (competicao,nome,local,ordem,ativa) VALUES (%s,%s,'',%s,TRUE)", (nome_competicao,nome,idx))
            cur.execute("SELECT id,pin_arbitragem FROM competicao_quadras WHERE competicao=%s AND COALESCE(ativa,TRUE)=TRUE ORDER BY COALESCE(ordem,9999),id", (nome_competicao,))
            for q in cur.fetchall() or []:
                if normalizar_pin_arbitragem(q.get("pin_arbitragem")): continue
                cur.execute("UPDATE competicao_quadras SET pin_arbitragem=%s,pin_arbitragem_criado_em=COALESCE(pin_arbitragem_criado_em,NOW()),atualizado_em=NOW() WHERE id=%s", (_gerar_pin_unico_cur(cur),q["id"]))
        conn.commit()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id,competicao,nome,local,ordem,ativa,pin_arbitragem FROM competicao_quadras WHERE competicao=%s AND COALESCE(ativa,TRUE)=TRUE ORDER BY COALESCE(ordem,9999),id", (nome_competicao,))
            return cur.fetchall() or []


def buscar_vinculo_arbitragem_por_pin(pin):
    pin = normalizar_pin_arbitragem(pin)
    if not pin: return None
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id,competicao,nome,local,ordem,ativa,pin_arbitragem FROM competicao_quadras WHERE pin_arbitragem=%s AND COALESCE(ativa,TRUE)=TRUE LIMIT 1", (pin,))
            return cur.fetchone()


def listar_quadras_competicao(nome_competicao, somente_ativas=False):
    nome_competicao = normalizar_nome_competicao(nome_competicao)
    if not nome_competicao: return []
    sql = "SELECT id,competicao,nome,local,ordem,ativa FROM competicao_quadras WHERE competicao=%s"
    if somente_ativas: sql += " AND COALESCE(ativa,TRUE)=TRUE"
    sql += " ORDER BY COALESCE(ordem,9999),id"
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (nome_competicao,))
            linhas = cur.fetchall() or []
    for linha in linhas:
        linha["nome_exibicao"] = formatar_quadra_exibicao(linha)
        linha["quadra_label"] = linha["nome_exibicao"]
    return linhas


def buscar_quadra_competicao_por_texto(nome_competicao, texto):
    for quadra in listar_quadras_competicao(nome_competicao):
        if quadra_matches_texto(quadra, texto): return quadra
    return None


def buscar_quadra_competicao_por_id(nome_competicao, quadra_id):
    try: quadra_id = int(quadra_id)
    except (TypeError, ValueError): return None
    nome_competicao = normalizar_nome_competicao(nome_competicao)
    if not nome_competicao: return None
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id,competicao,nome,local,ordem,ativa FROM competicao_quadras WHERE competicao=%s AND id=%s LIMIT 1", (nome_competicao,quadra_id))
            q = cur.fetchone()
    if q:
        q["nome_exibicao"] = formatar_quadra_exibicao(q); q["quadra_label"] = q["nome_exibicao"]
    return q


def garantir_quadras_competicao(nome_competicao, qtd_quadras=1):
    nome_competicao = normalizar_nome_competicao(nome_competicao)
    if not nome_competicao: return []
    qtd_quadras = normalizar_quantidade_quadras(qtd_quadras)
    existentes = listar_quadras_competicao(nome_competicao, True)
    if len(existentes) >= qtd_quadras: return existentes
    with conectar() as conn:
        with conn.cursor() as cur:
            for ordem in range(len(existentes)+1, qtd_quadras+1):
                cur.execute("INSERT INTO competicao_quadras (competicao,nome,local,ordem,ativa) VALUES (%s,%s,'',%s,TRUE)", (nome_competicao,f"Quadra {ordem}",ordem))
        conn.commit()
    return listar_quadras_competicao(nome_competicao, True)


def normalizar_vinculos_quadras_competicao(nome_competicao):
    nome_competicao = normalizar_nome_competicao(nome_competicao)
    if not nome_competicao: return False
    quadras = listar_quadras_competicao(nome_competicao)
    if not quadras: return False
    mapa = {int(q["id"]): q for q in quadras if q.get("id")}
    def localizar(qid, texto):
        try:
            q = mapa.get(int(qid or 0))
            if q: return q
        except Exception: pass
        for q in quadras:
            if quadra_matches_texto(q, texto): return q
        return None
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id,quadra_id,quadra_nome FROM grupos WHERE competicao=%s", (nome_competicao,))
            for g in cur.fetchall() or []:
                q=localizar(g.get("quadra_id"),g.get("quadra_nome"))
                if q:
                    cur.execute("UPDATE grupos SET quadra_id=%s,quadra_nome=%s WHERE id=%s", (q["id"],formatar_quadra_exibicao(q),g["id"]))
            cur.execute("SELECT id,quadra,quadra_id,quadra_nome FROM partidas WHERE competicao=%s", (nome_competicao,))
            for partida in cur.fetchall() or []:
                q=localizar(partida.get("quadra_id"),partida.get("quadra_nome") or partida.get("quadra"))
                if q:
                    cur.execute("UPDATE partidas SET quadra_id=%s,quadra_nome=%s,quadra=%s WHERE id=%s", (q["id"],formatar_quadra_exibicao(q),str(q["id"]),partida["id"]))
        conn.commit()
    return True


def salvar_quadras_competicao(nome_competicao, quadras):
    nome_competicao = normalizar_nome_competicao(nome_competicao)
    if not nome_competicao: return []
    dados = normalizar_lista_quadras(quadras); ids=[]
    with conectar() as conn:
        with conn.cursor() as cur:
            for q in dados:
                if q["id"]:
                    cur.execute("UPDATE competicao_quadras SET nome=%s,local=%s,ordem=%s,ativa=%s,atualizado_em=NOW() WHERE id=%s AND competicao=%s RETURNING id", (q["nome"],q["local"],q["ordem"],q["ativa"],q["id"],nome_competicao))
                    row=cur.fetchone()
                else: row=None
                if not row:
                    cur.execute("INSERT INTO competicao_quadras (competicao,nome,local,ordem,ativa) VALUES (%s,%s,%s,%s,%s) RETURNING id", (nome_competicao,q["nome"],q["local"],q["ordem"],q["ativa"]))
                    row=cur.fetchone()
                if row: ids.append(int(row["id"]))
            if ids:
                cur.execute("UPDATE competicao_quadras SET ativa=FALSE,atualizado_em=NOW() WHERE competicao=%s AND NOT (id=ANY(%s))", (nome_competicao,ids))
            cur.execute("UPDATE competicoes SET qtd_quadras=%s WHERE nome=%s", (max(1,sum(1 for q in dados if q["ativa"])),nome_competicao))
        conn.commit()
    normalizar_vinculos_quadras_competicao(nome_competicao)
    return listar_quadras_competicao(nome_competicao, True)


def vincular_grupo_a_quadra(nome_competicao, grupo_nome, quadra_id):
    q=buscar_quadra_competicao_por_id(nome_competicao,quadra_id)
    if not q: return False
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE grupos SET quadra_id=%s,quadra_nome=%s WHERE competicao=%s AND nome=%s",
                (q["id"], formatar_quadra_exibicao(q), normalizar_nome_competicao(nome_competicao), grupo_nome),
            )
        conn.commit()
    return True


def aplicar_quadra_em_partida(nome_competicao, partida_id, quadra_id):
    q=buscar_quadra_competicao_por_id(nome_competicao,quadra_id)
    try: partida_id=int(partida_id)
    except (TypeError,ValueError): return False
    if not q: return False
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE partidas SET quadra_id=%s,quadra_nome=%s,quadra=%s WHERE competicao=%s AND id=%s",
                (q["id"], formatar_quadra_exibicao(q), str(q["id"]), normalizar_nome_competicao(nome_competicao), partida_id),
            )
        conn.commit()
    return True
