"""Persistência do cadastro básico de partidas.

Este módulo não contém regras de voleibol nem emite Socket.IO. Ele somente
consulta e grava o cadastro/agenda das partidas.
"""
from repositories.conexao import conectar
from rules.partidas import normalizar_limite, texto


def criar_tabela_partidas(*, force=False):
    if not force:
        from core.schema_requirements import require_schema
        require_schema(
            tables=("partidas",),
            columns={"partidas": ("competicao", "rodada", "ordem", "status", "fase")},
            context="cadastro de partidas",
        )
        return
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS partidas (
                    id SERIAL PRIMARY KEY, competicao TEXT NOT NULL, grupo TEXT,
                    equipe_a TEXT, equipe_b TEXT, fase TEXT DEFAULT 'grupos',
                    ordem INTEGER, status TEXT DEFAULT 'aguardando'
                )
            """)
            colunas = [
                "rodada INTEGER", "quadra TEXT", "quadra_id INTEGER",
                "quadra_nome TEXT DEFAULT ''", "data_hora TEXT", "origem TEXT DEFAULT 'manual'",
                "sets_a INTEGER DEFAULT 0", "sets_b INTEGER DEFAULT 0",
                "set1_a INTEGER", "set1_b INTEGER", "set2_a INTEGER", "set2_b INTEGER",
                "set3_a INTEGER", "set3_b INTEGER", "set4_a INTEGER", "set4_b INTEGER",
                "set5_a INTEGER", "set5_b INTEGER", "origem_resultado TEXT DEFAULT 'apontada'",
                "scout_preenchido BOOLEAN DEFAULT FALSE", "vencedor TEXT",
                "operador_login TEXT", "operador_nome TEXT", "status_operacao TEXT DEFAULT 'livre'",
                "reservado_em TIMESTAMP", "pre_jogo_iniciado_em TIMESTAMP", "apontador_login TEXT",
                "apontador_nome TEXT", "arbitro_1_cpf TEXT", "arbitro_1_nome TEXT",
                "arbitro_2_cpf TEXT", "arbitro_2_nome TEXT", "sorteio_vencedor TEXT",
                "sorteio_escolha TEXT", "saque_inicial TEXT", "lado_esquerdo TEXT",
                "equipe_a_operacional TEXT", "equipe_b_operacional TEXT", "capitao_a_id INTEGER",
                "capitao_a_nome TEXT", "capitao_a_numero INTEGER", "capitao_b_id INTEGER",
                "capitao_b_nome TEXT", "capitao_b_numero INTEGER",
            ]
            for definicao in colunas:
                cur.execute(f"ALTER TABLE partidas ADD COLUMN IF NOT EXISTS {definicao}")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_partidas_competicao_ordem ON partidas (competicao, rodada, ordem, id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_partidas_competicao_fase ON partidas (competicao, fase)")
        conn.commit()


def _formatar_quadras(linhas, formatar_quadra=None):
    for linha in linhas or []:
        if not formatar_quadra or not linha.get("quadra_id") or not linha.get("quadra_nome_cadastro"):
            continue
        try:
            nome = formatar_quadra({
                "nome": linha.get("quadra_nome_cadastro"),
                "local": linha.get("quadra_local_cadastro"),
                "ordem": linha.get("quadra_id"),
            })
            linha["quadra_nome"] = nome
            linha["quadra_label"] = nome
        except Exception:
            continue
    return linhas



_CAMPOS_LISTA_LEVE = """
    p.id, p.competicao, p.grupo, p.equipe_a, p.equipe_b,
    p.equipe_a_operacional, p.equipe_b_operacional,
    p.fase, p.ordem, p.rodada, p.quadra, p.quadra_id, p.quadra_nome, p.data_hora, p.origem,
    p.status, p.status_jogo, p.status_operacao, p.fase_partida,
    p.sets_a, p.sets_b, p.pontos_a, p.pontos_b,
    p.set1_a, p.set1_b, p.set2_a, p.set2_b, p.set3_a, p.set3_b,
    p.set4_a, p.set4_b, p.set5_a, p.set5_b,
    p.vencedor, p.scout_preenchido, p.modo_operacao,
    p.pre_jogo_iniciado_em, p.pre_jogo_finalizado,
    p.operador_login, p.operador_nome, p.apontador_login, p.apontador_nome,
    p.tiebreak_pendente, p.tiebreak_definido
"""


def listar_partidas_leve(competicao, *, limite=500, offset=0, formatar_quadra=None, incluir_escudos=True):
    """Lista operacional leve e paginada para painéis e bootstrap.

    Não agrega eventos e não usa ``p.*``. O contrato contém apenas campos de
    agenda, placar, status e operação necessários para as listagens.
    """
    competicao = texto(competicao)
    if not competicao:
        return []
    limite = max(1, min(int(limite or 500), 2000))
    offset = max(0, int(offset or 0))
    joins_escudos = ""
    campos_escudos = "'' AS escudo_a, '' AS escudo_b"
    if incluir_escudos:
        joins_escudos = """
            LEFT JOIN equipes_competicoes eca ON eca.competicao=p.competicao
                AND LOWER(TRIM(eca.equipe_nome))=LOWER(TRIM(p.equipe_a))
            LEFT JOIN equipes ea ON ea.login=eca.equipe_login
            LEFT JOIN equipes_competicoes ecb ON ecb.competicao=p.competicao
                AND LOWER(TRIM(ecb.equipe_nome))=LOWER(TRIM(p.equipe_b))
            LEFT JOIN equipes eb ON eb.login=ecb.equipe_login
        """
        campos_escudos = "COALESCE(ea.escudo, '') AS escudo_a, COALESCE(eb.escudo, '') AS escudo_b"
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT {_CAMPOS_LISTA_LEVE},
                    COALESCE(cq.nome, '') AS quadra_nome_cadastro,
                    COALESCE(cq.local, '') AS quadra_local_cadastro,
                    {campos_escudos}
                FROM partidas p
                LEFT JOIN competicao_quadras cq
                    ON cq.competicao=p.competicao AND cq.id=p.quadra_id
                {joins_escudos}
                WHERE p.competicao=%s
                ORDER BY COALESCE(p.rodada,999999), COALESCE(p.ordem,999999), p.id
                LIMIT %s OFFSET %s
            """, (competicao, limite, offset))
            return _formatar_quadras(cur.fetchall() or [], formatar_quadra)


def proxima_ordem_partida(competicao):
    """Retorna a próxima ordem sem carregar todas as partidas."""
    competicao = texto(competicao)
    if not competicao:
        return 1
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(ordem), 0) + 1 AS proxima_ordem FROM partidas WHERE competicao=%s",
                (competicao,),
            )
            row = cur.fetchone() or {}
            try:
                return max(1, int(row.get("proxima_ordem") or 1))
            except (TypeError, ValueError):
                return 1

def listar_partidas(competicao, *, formatar_quadra=None):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.*, COALESCE(cq.nome, '') AS quadra_nome_cadastro,
                    COALESCE(cq.local, '') AS quadra_local_cadastro,
                    COALESCE(ea.escudo, '') AS escudo_a, COALESCE(eb.escudo, '') AS escudo_b,
                    COALESCE(ev.eventos_total, 0) AS eventos_total
                FROM partidas p
                LEFT JOIN (SELECT partida_id, COUNT(*) AS eventos_total FROM eventos
                    WHERE competicao = %s GROUP BY partida_id) ev ON ev.partida_id = p.id
                LEFT JOIN competicao_quadras cq ON cq.competicao=p.competicao AND cq.id=p.quadra_id
                LEFT JOIN equipes_competicoes eca ON eca.competicao=p.competicao
                    AND LOWER(TRIM(eca.equipe_nome))=LOWER(TRIM(p.equipe_a))
                LEFT JOIN equipes ea ON ea.login=eca.equipe_login
                LEFT JOIN equipes_competicoes ecb ON ecb.competicao=p.competicao
                    AND LOWER(TRIM(ecb.equipe_nome))=LOWER(TRIM(p.equipe_b))
                LEFT JOIN equipes eb ON eb.login=ecb.equipe_login
                WHERE p.competicao=%s
                ORDER BY COALESCE(p.rodada,999999), p.ordem, p.id
            """, (competicao, competicao))
            return _formatar_quadras(cur.fetchall() or [], formatar_quadra)


def listar_partidas_da_equipe(competicao, equipe, limite=50, *, formatar_quadra=None):
    competicao, equipe = texto(competicao), texto(equipe)
    if not competicao or not equipe:
        return []
    limite = normalizar_limite(limite)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.*, COALESCE(cq.nome, '') AS quadra_nome_cadastro,
                    COALESCE(cq.local, '') AS quadra_local_cadastro,
                    COALESCE(ea.escudo, '') AS escudo_a, COALESCE(eb.escudo, '') AS escudo_b
                FROM partidas p
                LEFT JOIN competicao_quadras cq ON cq.competicao=p.competicao AND cq.id=p.quadra_id
                LEFT JOIN equipes_competicoes eca ON eca.competicao=p.competicao
                    AND LOWER(TRIM(eca.equipe_nome))=LOWER(TRIM(p.equipe_a))
                LEFT JOIN equipes ea ON ea.login=eca.equipe_login
                LEFT JOIN equipes_competicoes ecb ON ecb.competicao=p.competicao
                    AND LOWER(TRIM(ecb.equipe_nome))=LOWER(TRIM(p.equipe_b))
                LEFT JOIN equipes eb ON eb.login=ecb.equipe_login
                WHERE p.competicao=%s AND (
                    LOWER(TRIM(p.equipe_a))=LOWER(TRIM(%s)) OR LOWER(TRIM(p.equipe_b))=LOWER(TRIM(%s)))
                ORDER BY CASE
                    WHEN LOWER(COALESCE(p.status_jogo,p.status_operacao,p.status,'')) IN ('ao_vivo','em_andamento','andamento','jogo') THEN 1
                    WHEN LOWER(COALESCE(p.status_jogo,p.status_operacao,p.status,'')) IN ('pre_jogo','papeleta','papeleta_pronta') THEN 2
                    WHEN LOWER(COALESCE(p.status_jogo,p.status_operacao,p.status,'')) IN ('finalizada','finalizado','encerrada','encerrado') THEN 4
                    ELSE 3 END,
                    COALESCE(p.rodada,999999), COALESCE(p.ordem,999999), p.id LIMIT %s
            """, (competicao, equipe, equipe, limite))
            return _formatar_quadras(cur.fetchall() or [], formatar_quadra)


def buscar_partida_por_id(partida_id, competicao, *, formatar_quadra=None):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.*, COALESCE(cq.nome, '') AS quadra_nome_cadastro,
                    COALESCE(cq.local, '') AS quadra_local_cadastro,
                    COALESCE(ea.escudo, '') AS escudo_a, COALESCE(eb.escudo, '') AS escudo_b
                FROM partidas p
                LEFT JOIN competicao_quadras cq ON cq.competicao=p.competicao AND cq.id=p.quadra_id
                LEFT JOIN equipes_competicoes eca ON eca.competicao=p.competicao
                    AND LOWER(TRIM(eca.equipe_nome))=LOWER(TRIM(p.equipe_a))
                LEFT JOIN equipes ea ON ea.login=eca.equipe_login
                LEFT JOIN equipes_competicoes ecb ON ecb.competicao=p.competicao
                    AND LOWER(TRIM(ecb.equipe_nome))=LOWER(TRIM(p.equipe_b))
                LEFT JOIN equipes eb ON eb.login=ecb.equipe_login
                WHERE p.id=%s AND p.competicao=%s LIMIT 1
            """, (partida_id, competicao))
            linha = cur.fetchone()
            if not linha:
                return None
            linhas = _formatar_quadras([linha], formatar_quadra)
            return linhas[0]


def competicao_tem_partida_iniciada_por_fase(nome_competicao, fase=None):
    sql_fase = "AND COALESCE(fase, 'grupos') = %s" if fase else ""
    params = [nome_competicao] + ([fase] if fase else [])
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""SELECT id FROM partidas WHERE competicao=%s {sql_fase} AND (
                    COALESCE(pontos_a,0)>0 OR COALESCE(pontos_b,0)>0 OR COALESCE(sets_a,0)>0 OR COALESCE(sets_b,0)>0
                    OR pre_jogo_iniciado_em IS NOT NULL OR COALESCE(pre_jogo_finalizado,FALSE)=TRUE
                    OR LOWER(REPLACE(COALESCE(status_jogo,''),'-','_')) IN ('em_andamento','em andamento','andamento','entre_sets','tiebreak_sorteio','finalizada','finalizado','encerrada','encerrado','ao_vivo','ao vivo')
                    OR LOWER(REPLACE(COALESCE(status,''),'-','_')) IN ('em_andamento','em andamento','andamento','iniciada','iniciado','finalizada','finalizado','encerrada','encerrado','ao_vivo','ao vivo')) LIMIT 1""", tuple(params))
                return cur.fetchone() is not None
    except Exception:
        return False


def inserir_partida(dados, *, buscar_colunas):
    with conectar() as conn:
        with conn.cursor() as cur:
            colunas = buscar_colunas("partidas")
            campos = ["competicao","grupo","equipe_a","equipe_b","fase","ordem","quadra","quadra_id","quadra_nome","data_hora","rodada","origem","status"]
            valores = [dados.get(c) for c in campos]
            if "status_jogo" in colunas:
                campos.append("status_jogo"); valores.append("aguardando")
            if "fase_partida" in colunas:
                campos.append("fase_partida"); valores.append("aguardando")
            for campo in ("sets_a", "sets_b"):
                if campo in colunas: campos.append(campo); valores.append(0)
            for campo in ("set1_a","set1_b","set2_a","set2_b","set3_a","set3_b"):
                if campo in colunas: campos.append(campo); valores.append(None)
            cur.execute(f"INSERT INTO partidas ({', '.join(campos)}) VALUES ({', '.join(['%s']*len(valores))})", tuple(valores))
        conn.commit()
    return True


def atualizar_partida(partida_id, competicao, dados):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""UPDATE partidas SET grupo=%s,fase=%s,equipe_a=%s,equipe_b=%s,
                quadra=%s,quadra_id=%s,quadra_nome=%s,data_hora=%s,status=%s,status_jogo=%s,
                fase_partida=%s,rodada=%s WHERE id=%s AND competicao=%s""", (
                dados.get("grupo"),dados.get("fase"),dados.get("equipe_a"),dados.get("equipe_b"),
                dados.get("quadra"),dados.get("quadra_id"),dados.get("quadra_nome") or dados.get("quadra") or "",
                dados.get("data_hora"),dados.get("status"),dados.get("status"),dados.get("status"),
                dados.get("rodada"),partida_id,competicao))
            alteradas = cur.rowcount
        conn.commit()
    return alteradas > 0


def excluir_partida(partida_id, competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM partidas WHERE id=%s AND competicao=%s", (partida_id, competicao))
            alteradas = cur.rowcount
        conn.commit()
    return alteradas > 0


def limpar_partidas(competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM partidas WHERE competicao=%s", (competicao,))
        conn.commit()
    return True


def limpar_partidas_por_fase(competicao, fase):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM partidas WHERE competicao=%s AND COALESCE(fase,'grupos')=%s", (competicao, fase))
        conn.commit()
    return True


def inserir_partidas_em_lote(partidas, *, buscar_colunas_tabela=None):
    """Insere várias partidas com um único roundtrip e um único commit.

    Mantém compatibilidade com bancos antigos selecionando apenas as colunas
    existentes quando ``buscar_colunas_tabela`` é informado.
    """
    partidas = [p for p in (partidas or []) if p]
    if not partidas:
        return 0

    if buscar_colunas_tabela is None:
        from core.schema_inspection import buscar_colunas_tabela

    colunas_partidas = buscar_colunas_tabela("partidas") or set()

    campos_base = [
        "competicao", "grupo", "equipe_a", "equipe_b", "fase", "ordem",
        "quadra", "quadra_id", "quadra_nome", "origem", "rodada", "data_hora", "status",
    ]
    extras_possiveis = [
        "status_jogo", "fase_partida", "status_operacao",
        "sets_a", "sets_b", "pontos_a", "pontos_b",
    ]

    campos = [c for c in campos_base if c in colunas_partidas]
    campos.extend([c for c in extras_possiveis if c in colunas_partidas and c not in campos])
    if not campos:
        raise RuntimeError("Não foi possível identificar as colunas da tabela partidas.")

    def _int_ou_none(valor):
        try:
            return int(valor) if valor not in (None, "") else None
        except (TypeError, ValueError):
            return None

    valores = []
    for partida in partidas:
        quadra_id = _int_ou_none(partida.get("quadra_id"))
        mapa_valores = {
            "competicao": partida.get("competicao"),
            "grupo": partida.get("grupo"),
            "equipe_a": partida.get("equipe_a"),
            "equipe_b": partida.get("equipe_b"),
            "fase": partida.get("fase") or "grupos",
            "ordem": int(partida.get("ordem") or 0),
            "quadra": str(quadra_id) if quadra_id else None,
            "quadra_id": quadra_id,
            "quadra_nome": partida.get("quadra_nome") or "",
            "origem": partida.get("origem") or "automatica",
            "rodada": partida.get("rodada"),
            "data_hora": partida.get("data_hora"),
            "status": "aguardando",
            "status_jogo": "aguardando",
            "fase_partida": "aguardando",
            "status_operacao": "livre",
            "sets_a": 0,
            "sets_b": 0,
            "pontos_a": 0,
            "pontos_b": 0,
        }
        valores.append(tuple(mapa_valores.get(c) for c in campos))

    placeholders = ", ".join(["%s"] * len(campos))
    from repositories.conexao import conectar

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                f'INSERT INTO partidas ({", ".join(campos)}) VALUES ({placeholders})',
                valores,
            )
        conn.commit()

    return len(valores)
