"""Persistência de rodadas e agenda programada de competições."""
from repositories.conexao import conectar
from rules.rodadas import chave_rodada, normalizar_numero_rodada, normalizar_rodada, texto


def criar_tabela_competicao_rodadas(*, force=False, schema_pronto=None, marcar_schema=None, cache_colunas=None):
    if not force:
        from core.schema_requirements import require_schema
        require_schema(
            tables=("competicao_rodadas",),
            columns={"competicao_rodadas": ("competicao", "tipo_fase", "fase", "numero_rodada")},
            context="rodadas das competições",
        )
        if marcar_schema:
            marcar_schema("tabela_competicao_rodadas")
        return
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""CREATE TABLE IF NOT EXISTS competicao_rodadas (
                id SERIAL PRIMARY KEY, competicao TEXT NOT NULL,
                tipo_fase TEXT NOT NULL DEFAULT 'classificatoria', fase TEXT NOT NULL DEFAULT 'grupos',
                serie TEXT DEFAULT '', numero_rodada INTEGER NOT NULL DEFAULT 1, nome TEXT DEFAULT '',
                data TEXT DEFAULT '', hora TEXT DEFAULT '', data_hora TEXT DEFAULT '', ativo BOOLEAN DEFAULT TRUE,
                atualizado_em TIMESTAMP DEFAULT NOW())""")
            cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS uq_competicao_rodadas_chave
                ON competicao_rodadas (competicao, tipo_fase, fase, COALESCE(serie, ''), numero_rodada)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_competicao_rodadas_competicao
                ON competicao_rodadas (competicao, tipo_fase, fase, serie, numero_rodada)""")
        conn.commit()
    if cache_colunas is not None:
        cache_colunas.pop("competicao_rodadas", None)
    if marcar_schema:
        try:
            marcar_schema("tabela_competicao_rodadas")
        except Exception:
            pass


def listar_rodadas_competicao(nome_competicao):
    nome = texto(nome_competicao)
    if not nome:
        return []
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    competicao,
                    tipo_fase,
                    fase,
                    serie,
                    numero_rodada,
                    nome,
                    data,
                    hora,
                    data_hora,
                    ativo,
                    atualizado_em
                FROM competicao_rodadas
                WHERE competicao = %s
                ORDER BY CASE WHEN tipo_fase = 'classificatoria' THEN 0 ELSE 1 END,
                         fase, serie, numero_rodada, id
            """, (nome,))
            return cur.fetchall() or []


def salvar_rodadas_competicao(nome_competicao, rodadas):
    nome = texto(nome_competicao)
    if not nome:
        return False
    linhas = [normalizar_rodada(nome, r) for r in (rodadas or [])]
    linhas = [r for r in linhas if r is not None]
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM competicao_rodadas WHERE competicao=%s", (nome,))
            if linhas:
                cur.executemany("""INSERT INTO competicao_rodadas
                    (competicao,tipo_fase,fase,serie,numero_rodada,nome,data,hora,data_hora,ativo,atualizado_em)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())""", linhas)
        conn.commit()
    return True


def mapa_rodadas_competicao(nome_competicao):
    return {chave_rodada(r): r for r in listar_rodadas_competicao(nome_competicao)}


def buscar_data_hora_rodada_programada(nome_competicao, tipo_fase="classificatoria", fase="grupos", serie="", numero_rodada=1):
    alvo = (texto(tipo_fase).lower(), texto(fase).lower(), texto(serie).lower(), normalizar_numero_rodada(numero_rodada))
    rodada = mapa_rodadas_competicao(nome_competicao).get(alvo)
    if not rodada:
        return None
    return texto(rodada.get("data_hora")) or None
