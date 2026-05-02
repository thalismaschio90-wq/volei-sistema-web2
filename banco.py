import os
import random
import re
import string
import json
from datetime import datetime
from threading import Lock

from dotenv import load_dotenv
load_dotenv()

from psycopg import connect
from psycopg.rows import dict_row

ConnectionPool = None

# --- ESSA LINHA ABAIXO É A QUE ESTÁ FALTANDO ---
_CACHE_COLUNAS = {} 
# -----------------------------------------------

DATABASE_URL_PADRAO = ""


ARQUIVO_DADOS = "dados.json"


_SCHEMA_FLAGS = {
    "campos_sets_partida": False,
    "campos_jogo_partida": False,
    "campos_rotacao_partidas": False,
    "tabela_eventos": False,
    "tabela_historico_rotacao": False,
    "indices_desempenho": False,
    "campos_quadro_tecnico_equipes": False,
    "campos_liberacao_extra_equipes": False,
    "campos_controle_inscricao_competicoes": False,
    "tabela_atletas": False,
}
_SCHEMA_LOCK = Lock()
_POOL_LOCK = Lock()
_DB_POOL = None


def _schema_ja_pronto(chave, force=False):
    if force:
        return False

    if _SCHEMA_FLAGS.get(chave):
        return True

    with _SCHEMA_LOCK:
        if _SCHEMA_FLAGS.get(chave):
            return True
        return False


def _marcar_schema_pronto(chave):
    with _SCHEMA_LOCK:
        _SCHEMA_FLAGS[chave] = True



# =========================================================
# ARQUIVO LOCAL (COMPATIBILIDADE)
# =========================================================
def obter_dados():
    if not os.path.exists(ARQUIVO_DADOS):
        return {"usuarios": {}, "competicoes": {}, "equipes": {}, "atletas": []}

    try:
        with open(ARQUIVO_DADOS, "r", encoding="utf-8") as f:
            dados = json.load(f)
    except Exception:
        return {"usuarios": {}, "competicoes": {}, "equipes": {}, "atletas": []}

    if not isinstance(dados, dict):
        return {"usuarios": {}, "competicoes": {}, "equipes": {}, "atletas": []}

    dados.setdefault("usuarios", {})
    dados.setdefault("competicoes", {})
    dados.setdefault("equipes", {})
    dados.setdefault("atletas", [])
    return dados


def salvar_dados(dados):
    with open(ARQUIVO_DADOS, "w", encoding="utf-8") as f:
        json.dump(dados, f, ensure_ascii=False, indent=4)


# =========================================================
# CONEXÃO
# =========================================================
def _obter_database_url():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL não configurada no ambiente.")
    return url


def _obter_pool():
    global _DB_POOL

    if ConnectionPool is None:
        return None

    if _DB_POOL is not None:
        return _DB_POOL

    with _POOL_LOCK:
        if _DB_POOL is not None:
            return _DB_POOL

        _DB_POOL = ConnectionPool(
            conninfo=_obter_database_url(),
            kwargs={
                "row_factory": dict_row,
                "sslmode": "require",
                "connect_timeout": 5,
                "prepare_threshold": None,
            },
            min_size=int(os.environ.get("DB_POOL_MIN_SIZE", 2)),
            max_size=int(os.environ.get("DB_POOL_MAX_SIZE", 30)),
            timeout=float(os.environ.get("DB_POOL_TIMEOUT", 5)),
            max_idle=60,
            max_lifetime=300,
            reconnect_timeout=5,
            open=True,
        )

        return _DB_POOL


def _fechar_pool_quebrado():
    global _DB_POOL

    try:
        if _DB_POOL is not None:
            _DB_POOL.close(timeout=2)
    except Exception:
        pass

    _DB_POOL = None


def conectar():
    pool = _obter_pool()

    if pool is not None:
        try:
            return pool.connection(timeout=float(os.environ.get("DB_POOL_TIMEOUT", 20)))
        except Exception as e:
            print("ERRO AO PEGAR CONEXAO DO POOL, RECRIANDO:", repr(e))
            _fechar_pool_quebrado()
            pool = _obter_pool()
            return pool.connection(timeout=float(os.environ.get("DB_POOL_TIMEOUT", 20)))

    return connect(
        _obter_database_url(),
        row_factory=dict_row,
        sslmode="require",
        connect_timeout=5,
        prepare_threshold=None,
    )


# =========================================================
# HELPERS
# =========================================================
def _normalizar_texto_base(texto):
    texto = (texto or "").lower().strip()
    texto = re.sub(r"[^\w\s]", "", texto)
    texto = re.sub(r"\s+", "_", texto)
    texto = texto[:24].strip("_")

    if not texto:
        texto = "cadastro"

    return texto


def _normalizar_login_organizador(nome_competicao):
    return f"org_{_normalizar_texto_base(nome_competicao)}"


def _normalizar_login_equipe(nome_equipe):
    return f"eq_{_normalizar_texto_base(nome_equipe)}"


def _normalizar_login_mesario(nome_mesario):
    return f"mes_{_normalizar_texto_base(nome_mesario)}"


def _gerar_login_unico(base):
    login = base
    contador = 1

    while usuario_existe(login):
        contador += 1
        login = f"{base}_{contador}"

    return login


def _gerar_senha_aleatoria(tamanho=8):
    caracteres = string.ascii_uppercase + string.digits
    return "".join(random.choice(caracteres) for _ in range(tamanho))


def _buscar_colunas_tabela(nome_tabela):
    # Se já buscou uma vez, retorna da memória sem abrir conexão
    if nome_tabela in _CACHE_COLUNAS:
        return _CACHE_COLUNAS[nome_tabela]

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
            """, (nome_tabela,))
            rows = cur.fetchall()
            colunas = {row["column_name"] for row in rows}
            
            if colunas:
                _CACHE_COLUNAS[nome_tabela] = colunas
            return colunas


def _campo_ou_alias(colunas, campo, alias_sql):
    if campo in colunas:
        return campo
    return alias_sql


def _campos_competicao(prefixo="", incluir_senha_organizador=False):
    colunas = _buscar_colunas_tabela("competicoes")
    p = f"{prefixo}." if prefixo else ""

    campos = [
        f"{p}nome",
        f"{p}data",
        f"{p}status",
        f"{p}organizador_login",
        _campo_ou_alias(colunas, "cidade", "'' AS cidade") if not prefixo else (
            f"{p}cidade" if "cidade" in colunas else "'' AS cidade"
        ),
        _campo_ou_alias(colunas, "ginasio", "'' AS ginasio") if not prefixo else (
            f"{p}ginasio" if "ginasio" in colunas else "'' AS ginasio"
        ),
        _campo_ou_alias(colunas, "categoria", "'' AS categoria") if not prefixo else (
            f"{p}categoria" if "categoria" in colunas else "'' AS categoria"
        ),
        _campo_ou_alias(colunas, "sexo", "'' AS sexo") if not prefixo else (
            f"{p}sexo" if "sexo" in colunas else "'' AS sexo"
        ),
        _campo_ou_alias(colunas, "divisao", "'' AS divisao") if not prefixo else (
            f"{p}divisao" if "divisao" in colunas else "'' AS divisao"
        ),
        _campo_ou_alias(colunas, "qtd_equipes", "0 AS qtd_equipes") if not prefixo else (
            f"{p}qtd_equipes" if "qtd_equipes" in colunas else "0 AS qtd_equipes"
        ),
        _campo_ou_alias(colunas, "formato", "'' AS formato") if not prefixo else (
            f"{p}formato" if "formato" in colunas else "'' AS formato"
        ),
        _campo_ou_alias(colunas, "tem_grupos", "FALSE AS tem_grupos") if not prefixo else (
            f"{p}tem_grupos" if "tem_grupos" in colunas else "FALSE AS tem_grupos"
        ),
        _campo_ou_alias(colunas, "qtd_grupos", "0 AS qtd_grupos") if not prefixo else (
            f"{p}qtd_grupos" if "qtd_grupos" in colunas else "0 AS qtd_grupos"
        ),
        _campo_ou_alias(colunas, "qtd_quadras", "1 AS qtd_quadras") if not prefixo else (
            f"{p}qtd_quadras" if "qtd_quadras" in colunas else "1 AS qtd_quadras"
        ),
        _campo_ou_alias(colunas, "modo_operacao", "'simples' AS modo_operacao") if not prefixo else (
            f"{p}modo_operacao" if "modo_operacao" in colunas else "'simples' AS modo_operacao"
        ),
        _campo_ou_alias(colunas, "sets_tipo", "'melhor_de_3' AS sets_tipo") if not prefixo else (
            f"{p}sets_tipo" if "sets_tipo" in colunas else "'melhor_de_3' AS sets_tipo"
        ),
        _campo_ou_alias(colunas, "pontos_set", "25 AS pontos_set") if not prefixo else (
            f"{p}pontos_set" if "pontos_set" in colunas else "25 AS pontos_set"
        ),
        _campo_ou_alias(colunas, "tem_tiebreak", "TRUE AS tem_tiebreak") if not prefixo else (
            f"{p}tem_tiebreak" if "tem_tiebreak" in colunas else "TRUE AS tem_tiebreak"
        ),
        _campo_ou_alias(colunas, "pontos_tiebreak", "15 AS pontos_tiebreak") if not prefixo else (
            f"{p}pontos_tiebreak" if "pontos_tiebreak" in colunas else "15 AS pontos_tiebreak"
        ),
        _campo_ou_alias(colunas, "diferenca_minima", "2 AS diferenca_minima") if not prefixo else (
            f"{p}diferenca_minima" if "diferenca_minima" in colunas else "2 AS diferenca_minima"
        ),
        _campo_ou_alias(colunas, "tempos_por_set", "2 AS tempos_por_set") if not prefixo else (
            f"{p}tempos_por_set" if "tempos_por_set" in colunas else "2 AS tempos_por_set"
        ),
        _campo_ou_alias(colunas, "substituicoes_por_set", "6 AS substituicoes_por_set") if not prefixo else (
            f"{p}substituicoes_por_set" if "substituicoes_por_set" in colunas else "6 AS substituicoes_por_set"
        ),
        _campo_ou_alias(colunas, "vitoria_set_unico", "2 AS vitoria_set_unico") if not prefixo else (
            f"{p}vitoria_set_unico" if "vitoria_set_unico" in colunas else "2 AS vitoria_set_unico"
        ),
        _campo_ou_alias(colunas, "derrota_set_unico", "0 AS derrota_set_unico") if not prefixo else (
            f"{p}derrota_set_unico" if "derrota_set_unico" in colunas else "0 AS derrota_set_unico"
        ),
        _campo_ou_alias(colunas, "vitoria_2x0", "3 AS vitoria_2x0") if not prefixo else (
            f"{p}vitoria_2x0" if "vitoria_2x0" in colunas else "3 AS vitoria_2x0"
        ),
        _campo_ou_alias(colunas, "vitoria_2x1", "2 AS vitoria_2x1") if not prefixo else (
            f"{p}vitoria_2x1" if "vitoria_2x1" in colunas else "2 AS vitoria_2x1"
        ),
        _campo_ou_alias(colunas, "derrota_1x2", "1 AS derrota_1x2") if not prefixo else (
            f"{p}derrota_1x2" if "derrota_1x2" in colunas else "1 AS derrota_1x2"
        ),
        _campo_ou_alias(colunas, "derrota_0x2", "0 AS derrota_0x2") if not prefixo else (
            f"{p}derrota_0x2" if "derrota_0x2" in colunas else "0 AS derrota_0x2"
        ),
        _campo_ou_alias(colunas, "vitoria_3x0", "3 AS vitoria_3x0") if not prefixo else (
            f"{p}vitoria_3x0" if "vitoria_3x0" in colunas else "3 AS vitoria_3x0"
        ),
        _campo_ou_alias(colunas, "vitoria_3x1", "3 AS vitoria_3x1") if not prefixo else (
            f"{p}vitoria_3x1" if "vitoria_3x1" in colunas else "3 AS vitoria_3x1"
        ),
        _campo_ou_alias(colunas, "vitoria_3x2", "2 AS vitoria_3x2") if not prefixo else (
            f"{p}vitoria_3x2" if "vitoria_3x2" in colunas else "2 AS vitoria_3x2"
        ),
        _campo_ou_alias(colunas, "derrota_2x3", "1 AS derrota_2x3") if not prefixo else (
            f"{p}derrota_2x3" if "derrota_2x3" in colunas else "1 AS derrota_2x3"
        ),
        _campo_ou_alias(colunas, "derrota_1x3", "0 AS derrota_1x3") if not prefixo else (
            f"{p}derrota_1x3" if "derrota_1x3" in colunas else "0 AS derrota_1x3"
        ),
        _campo_ou_alias(colunas, "derrota_0x3", "0 AS derrota_0x3") if not prefixo else (
            f"{p}derrota_0x3" if "derrota_0x3" in colunas else "0 AS derrota_0x3"
        ),
        _campo_ou_alias(
            colunas,
            "criterios_desempate",
            "'vitorias,pontos,saldo_sets,sets_pro,sets_contra,saldo_pontos,pontos_pro,pontos_contra,confronto_direto,coef_sets,coef_pontos,fair_play,sorteio' AS criterios_desempate"
        ) if not prefixo else (
            f"{p}criterios_desempate" if "criterios_desempate" in colunas else
            "'vitorias,pontos,saldo_sets,sets_pro,sets_contra,saldo_pontos,pontos_pro,pontos_contra,confronto_direto,coef_sets,coef_pontos,fair_play,sorteio' AS criterios_desempate"
        ),
        _campo_ou_alias(colunas, "limite_atletas", "0 AS limite_atletas") if not prefixo else (
            f"{p}limite_atletas" if "limite_atletas" in colunas else "0 AS limite_atletas"
        ),
        _campo_ou_alias(colunas, "permitir_edicao_pos_prazo", "FALSE AS permitir_edicao_pos_prazo") if not prefixo else (
            f"{p}permitir_edicao_pos_prazo" if "permitir_edicao_pos_prazo" in colunas else "FALSE AS permitir_edicao_pos_prazo"
        ),
        _campo_ou_alias(colunas, "travada", "FALSE AS travada") if not prefixo else (
            f"{p}travada" if "travada" in colunas else "FALSE AS travada"
        ),
        _campo_ou_alias(colunas, "motivo_travamento", "'' AS motivo_travamento") if not prefixo else (
            f"{p}motivo_travamento" if "motivo_travamento" in colunas else "'' AS motivo_travamento"
        ),
        _campo_ou_alias(colunas, "travada_em", "NULL::timestamp AS travada_em") if not prefixo else (
            f"{p}travada_em" if "travada_em" in colunas else "NULL::timestamp AS travada_em"
        ),
    ]

    campos.extend([
        _campo_ou_alias(colunas, "tipo_classificacao", "'grupo' AS tipo_classificacao") if not prefixo else (
            f"{p}tipo_classificacao" if "tipo_classificacao" in colunas else "'grupo' AS tipo_classificacao"
        ),
        _campo_ou_alias(colunas, "qtd_classificados", "0 AS qtd_classificados") if not prefixo else (
            f"{p}qtd_classificados" if "qtd_classificados" in colunas else "0 AS qtd_classificados"
        ),
        _campo_ou_alias(colunas, "formato_finais", "'mata_mata' AS formato_finais") if not prefixo else (
            f"{p}formato_finais" if "formato_finais" in colunas else "'mata_mata' AS formato_finais"
        ),
        _campo_ou_alias(colunas, "possui_bye", "FALSE AS possui_bye") if not prefixo else (
            f"{p}possui_bye" if "possui_bye" in colunas else "FALSE AS possui_bye"
        ),
        _campo_ou_alias(colunas, "qtd_bye", "0 AS qtd_bye") if not prefixo else (
            f"{p}qtd_bye" if "qtd_bye" in colunas else "0 AS qtd_bye"
        ),
        _campo_ou_alias(colunas, "fases_config", "'{}' AS fases_config") if not prefixo else (
            f"{p}fases_config" if "fases_config" in colunas else "'{}' AS fases_config"
        ),
        _campo_ou_alias(colunas, "tipo_confronto", "'grupo_interno' AS tipo_confronto") if not prefixo else (
            f"{p}tipo_confronto" if "tipo_confronto" in colunas else "'grupo_interno' AS tipo_confronto"
        ),
        _campo_ou_alias(colunas, "cruzamentos_grupos", "'' AS cruzamentos_grupos") if not prefixo else (
            f"{p}cruzamentos_grupos" if "cruzamentos_grupos" in colunas else "'' AS cruzamentos_grupos"
        ),
        _campo_ou_alias(colunas, "data_limite_inscricao", "NULL AS data_limite_inscricao") if not prefixo else (
            f"{p}data_limite_inscricao" if "data_limite_inscricao" in colunas else "NULL AS data_limite_inscricao"
        ),
        _campo_ou_alias(colunas, "hora_limite_inscricao", "NULL AS hora_limite_inscricao") if not prefixo else (
            f"{p}hora_limite_inscricao" if "hora_limite_inscricao" in colunas else "NULL AS hora_limite_inscricao"
        ),
        _campo_ou_alias(colunas, "bloquear_apos_inicio", "FALSE AS bloquear_apos_inicio") if not prefixo else (
            f"{p}bloquear_apos_inicio" if "bloquear_apos_inicio" in colunas else "FALSE AS bloquear_apos_inicio"
        ),
    ])

    if incluir_senha_organizador:
        campos.append("u.senha AS organizador_senha")

    return campos


# =========================================================
# USUÁRIOS
# =========================================================
def buscar_usuario_por_login(login, conn=None):
    if conn is not None:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login, nome, senha, perfil, ativo, equipe, competicao_vinculada
                FROM usuarios
                WHERE login = %s
                LIMIT 1
            """, (login,))
            return cur.fetchone()

    with conectar() as conn:
        return buscar_usuario_por_login(login, conn)


def usuario_existe(login, conn=None):
    if conn is not None:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login
                FROM usuarios
                WHERE login = %s
                LIMIT 1
            """, (login,))
            return cur.fetchone() is not None

    with conectar() as conn:
        return usuario_existe(login, conn)


def atualizar_login_usuario(login_atual, novo_login):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login
                FROM usuarios
                WHERE login = %s
                LIMIT 1
            """, (novo_login,))

            if cur.fetchone():
                return False

            cur.execute("""
                UPDATE usuarios
                SET login = %s
                WHERE login = %s
            """, (novo_login, login_atual))

            cur.execute("""
                UPDATE equipes
                SET login = %s
                WHERE login = %s
            """, (novo_login, login_atual))

            cur.execute("""
                UPDATE competicoes
                SET organizador_login = %s
                WHERE organizador_login = %s
            """, (novo_login, login_atual))

        conn.commit()

    return True


def atualizar_senha_usuario(login, nova_senha):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE usuarios
                SET senha = %s
                WHERE login = %s
            """, (nova_senha, login))

            cur.execute("""
                UPDATE equipes
                SET senha = %s
                WHERE login = %s
            """, (nova_senha, login))

        conn.commit()

    return True

# =========================================================
# COMPETIÇÕES
# =========================================================

def criar_campos_regras_operacionais_competicoes():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS tempos_por_set INTEGER DEFAULT 2
            """)
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS substituicoes_por_set INTEGER DEFAULT 6
            """)
        conn.commit()

def criar_campos_travamento_competicoes():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS travada BOOLEAN DEFAULT FALSE
            """)
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS motivo_travamento TEXT DEFAULT ''
            """)
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS travada_em TIMESTAMP
            """)
        conn.commit()


def criar_campos_conferencia_atletas():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS conferencia_liberada BOOLEAN DEFAULT FALSE,
                ADD COLUMN IF NOT EXISTS conferencia_encerrada BOOLEAN DEFAULT FALSE,
                ADD COLUMN IF NOT EXISTS conferencia_prazo TEXT,
                ADD COLUMN IF NOT EXISTS conferencia_link TEXT;
            """)
        conn.commit()


def listar_competicoes():
    campos = _campos_competicao(prefixo="c", incluir_senha_organizador=True)

    sql = f"""
        SELECT {", ".join(campos)}
        FROM competicoes c
        LEFT JOIN usuarios u
            ON u.login = c.organizador_login
        ORDER BY c.nome
    """

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()


def listar_competicoes_do_organizador(login_organizador):
    campos = _campos_competicao()

    sql = f"""
        SELECT {", ".join(campos)}
        FROM competicoes
        WHERE organizador_login = %s
        ORDER BY nome
    """

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (login_organizador,))
            return cur.fetchall()


def buscar_competicao_por_organizador(login_organizador):
    campos = _campos_competicao()

    sql = f"""
        SELECT {", ".join(campos)}
        FROM competicoes
        WHERE organizador_login = %s
        LIMIT 1
    """

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (login_organizador,))
            return cur.fetchone()


def competicao_existe(nome):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nome
                FROM competicoes
                WHERE nome = %s
                LIMIT 1
            """, (nome,))
            return cur.fetchone() is not None


def criar_competicao_com_organizador(nome, data, status, modo_operacao="simples", tempos_por_set=2, substituicoes_por_set=6):
    login_organizador = _gerar_login_unico(_normalizar_login_organizador(nome))
    senha_organizador = _gerar_senha_aleatoria(8)

    colunas = _buscar_colunas_tabela("competicoes")

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO usuarios (
                    login, nome, senha, perfil, ativo, equipe, competicao_vinculada
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                login_organizador,
                f"Organizador - {nome}",
                senha_organizador,
                "organizador",
                True,
                None,
                nome
            ))

            campos = ["nome", "data", "status", "organizador_login"]
            valores = [nome, data, status, login_organizador]

            mapa_defaults = {
                "cidade": "",
                "ginasio": "",
                "categoria": "",
                "sexo": "",
                "divisao": "",
                "qtd_equipes": 0,
                "formato": "grupos",
                "tem_grupos": False,
                "qtd_grupos": 0,
                "qtd_quadras": 1,
                "modo_operacao": modo_operacao or "simples",
                "tempos_por_set": tempos_por_set,
                "substituicoes_por_set": substituicoes_por_set,
                "sets_tipo": "melhor_de_3",
                "pontos_set": 25,
                "tem_tiebreak": True,
                "pontos_tiebreak": 15,
                "diferenca_minima": 2,
                "vitoria_set_unico": 2,
                "derrota_set_unico": 0,
                "vitoria_2x0": 3,
                "vitoria_2x1": 2,
                "derrota_1x2": 1,
                "derrota_0x2": 0,
                "vitoria_3x0": 3,
                "vitoria_3x1": 3,
                "vitoria_3x2": 2,
                "derrota_2x3": 1,
                "derrota_1x3": 0,
                "derrota_0x3": 0,
                "criterios_desempate": "vitorias,pontos,saldo_sets,sets_pro,sets_contra,saldo_pontos,pontos_pro,pontos_contra,confronto_direto,coef_sets,coef_pontos,fair_play,sorteio",
                "tipo_classificacao": "grupo",
                "qtd_classificados": 0,
                "formato_finais": "mata_mata",
                "possui_bye": False,
                "qtd_bye": 0,
                "fases_config": json.dumps({}, ensure_ascii=False),
                "tipo_confronto": "grupo_interno",
                "cruzamentos_grupos": "",
                "data_limite_inscricao": None,
                "hora_limite_inscricao": None,
                "bloquear_apos_inicio": False,
                "limite_atletas": 0,
                "permitir_edicao_pos_prazo": False,
                "travada": False,
                "motivo_travamento": "",
                "travada_em": None,
            }

            for campo, default in mapa_defaults.items():
                if campo in colunas:
                    campos.append(campo)
                    valores.append(default)

            placeholders = ", ".join(["%s"] * len(valores))

            cur.execute(
                f"""
                INSERT INTO competicoes ({", ".join(campos)})
                VALUES ({placeholders})
                """,
                tuple(valores)
            )

        conn.commit()

    return {
        "login": login_organizador,
        "senha": senha_organizador
    }


def competicao_esta_travada(nome_competicao):
    colunas = _buscar_colunas_tabela("competicoes")
    if "travada" not in colunas:
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(travada, FALSE) AS travada
                FROM competicoes
                WHERE nome = %s
                LIMIT 1
            """, (nome_competicao,))
            row = cur.fetchone()
            return bool(row and row.get("travada"))


def travar_competicao(nome_competicao, motivo="primeiro_ponto"):
    colunas = _buscar_colunas_tabela("competicoes")
    if "travada" not in colunas:
        criar_campos_travamento_competicoes()
        colunas = _buscar_colunas_tabela("competicoes")

    sets = []
    if "travada" in colunas:
        sets.append("travada = TRUE")
    if "motivo_travamento" in colunas:
        sets.append("motivo_travamento = %s")
    if "travada_em" in colunas:
        sets.append("travada_em = NOW()")

    if not sets:
        return False

    valores = []
    if "motivo_travamento" in colunas:
        valores.append((motivo or "primeiro_ponto").strip())
    valores.append(nome_competicao)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE competicoes
                SET {', '.join(sets)}
                WHERE nome = %s
                  AND COALESCE(travada, FALSE) = FALSE
            """, tuple(valores))
            alteradas = cur.rowcount
        conn.commit()

    return alteradas > 0


def destravar_competicao(nome_competicao):
    colunas = _buscar_colunas_tabela("competicoes")
    if "travada" not in colunas:
        return True

    sets = ["travada = FALSE"]
    if "motivo_travamento" in colunas:
        sets.append("motivo_travamento = ''")
    if "travada_em" in colunas:
        sets.append("travada_em = NULL")

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE competicoes
                SET {', '.join(sets)}
                WHERE nome = %s
            """, (nome_competicao,))
        conn.commit()

    return True


def equipe_tem_partida_iniciada(nome_competicao, nome_equipe):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM partidas
                WHERE competicao = %s
                  AND (equipe_a = %s OR equipe_b = %s OR equipe_a_operacional = %s OR equipe_b_operacional = %s)
                  AND (
                        COALESCE(pontos_a, 0) > 0
                     OR COALESCE(pontos_b, 0) > 0
                     OR LOWER(COALESCE(status_jogo, '')) IN ('em_andamento', 'entre_sets', 'tiebreak_sorteio', 'finalizada', 'encerrado')
                     OR LOWER(COALESCE(status, '')) IN ('em_andamento', 'andamento', 'iniciada', 'iniciado', 'finalizada')
                  )
                LIMIT 1
            """, (nome_competicao, nome_equipe, nome_equipe, nome_equipe, nome_equipe))
            return cur.fetchone() is not None


def validar_edicao_atletas_equipe(nome_competicao, nome_equipe):
    if not competicao_esta_travada(nome_competicao):
        return True, ""

    if equipe_tem_partida_iniciada(nome_competicao, nome_equipe):
        return False, "A competição está travada e esta equipe já iniciou seus jogos. Alterações de atletas foram bloqueadas."

    return True, "Competição travada, mas esta equipe ainda não iniciou seus jogos. Alterações de atletas seguem liberadas até a estreia da equipe."


def validar_competicao_editavel(nome_competicao, escopo="alteração"):
    if competicao_esta_travada(nome_competicao):
        return False, f"A competição está travada. Não é permitido realizar esta {escopo}."
    return True, ""


def atualizar_dados_competicao(nome_original, dados):
    ok_edicao, _ = validar_competicao_editavel(nome_original, "edição")
    if not ok_edicao:
        return False

    colunas = _buscar_colunas_tabela("competicoes")

    sets = []
    valores = []

    mapa = {
        "nome": dados.get("nome"),
        "data": dados.get("data"),
        "status": dados.get("status"),
    }

    if "cidade" in colunas:
        mapa["cidade"] = dados.get("cidade", "")
    if "ginasio" in colunas:
        mapa["ginasio"] = dados.get("ginasio", "")
    if "categoria" in colunas:
        mapa["categoria"] = dados.get("categoria", "")
    if "sexo" in colunas:
        mapa["sexo"] = dados.get("sexo", "")
    if "divisao" in colunas:
        mapa["divisao"] = dados.get("divisao", "")

    for campo, valor in mapa.items():
        sets.append(f"{campo} = %s")
        valores.append(valor)

    valores.append(nome_original)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE competicoes
                SET {", ".join(sets)}
                WHERE nome = %s
                """,
                tuple(valores)
            )

            novo_nome = dados.get("nome")
            if novo_nome and novo_nome != nome_original:
                cur.execute("""
                    UPDATE usuarios
                    SET competicao_vinculada = %s
                    WHERE competicao_vinculada = %s
                """, (novo_nome, nome_original))

                cur.execute("""
                    UPDATE equipes
                    SET competicao = %s
                    WHERE competicao = %s
                """, (novo_nome, nome_original))

        conn.commit()

    return True


def atualizar_estrutura_competicao(nome_competicao, dados):
    ok_edicao, _ = validar_competicao_editavel(nome_competicao, "alteração estrutural")
    if not ok_edicao:
        return False

    colunas = _buscar_colunas_tabela("competicoes")
    sets = []
    valores = []

    if "qtd_equipes" in colunas:
        sets.append("qtd_equipes = %s")
        valores.append(dados.get("qtd_equipes", 0))
    if "formato" in colunas:
        sets.append("formato = %s")
        valores.append(dados.get("formato", ""))
    if "tem_grupos" in colunas:
        sets.append("tem_grupos = %s")
        valores.append(dados.get("tem_grupos", False))
    if "qtd_grupos" in colunas:
        sets.append("qtd_grupos = %s")
        valores.append(dados.get("qtd_grupos", 0))
    if "qtd_quadras" in colunas:
        sets.append("qtd_quadras = %s")
        valores.append(dados.get("qtd_quadras", 1))
    if "modo_operacao" in colunas:
        sets.append("modo_operacao = %s")
        valores.append(dados.get("modo_operacao", "simples"))
    if "tipo_confronto" in colunas:
        sets.append("tipo_confronto = %s")
        valores.append(dados.get("tipo_confronto", "grupo_interno"))
    if "tipo_classificacao" in colunas:
        sets.append("tipo_classificacao = %s")
        valores.append(dados.get("tipo_classificacao", "grupo"))
    if "cruzamentos_grupos" in colunas:
        sets.append("cruzamentos_grupos = %s")
        valores.append(dados.get("cruzamentos_grupos", ""))
    if "data_limite_inscricao" in colunas:
        sets.append("data_limite_inscricao = %s")
        valores.append(dados.get("data_limite_inscricao") or None)
    if "hora_limite_inscricao" in colunas:
        sets.append("hora_limite_inscricao = %s")
        valores.append(dados.get("hora_limite_inscricao") or None)
    if "bloquear_apos_inicio" in colunas:
        sets.append("bloquear_apos_inicio = %s")
        valores.append(dados.get("bloquear_apos_inicio", False))
    if "limite_atletas" in colunas:
        sets.append("limite_atletas = %s")
        valores.append(dados.get("limite_atletas", 0))
    if "permitir_edicao_pos_prazo" in colunas:
        sets.append("permitir_edicao_pos_prazo = %s")
        valores.append(dados.get("permitir_edicao_pos_prazo", False))

    if not sets:
        return True

    valores.append(nome_competicao)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE competicoes
                SET {", ".join(sets)}
                WHERE nome = %s
                """,
                tuple(valores)
            )
        conn.commit()

    return True


def atualizar_regras_jogo(nome_competicao, dados):
    ok_edicao, _ = validar_competicao_editavel(nome_competicao, "alteração de regras")
    if not ok_edicao:
        return False

    colunas = _buscar_colunas_tabela("competicoes")
    sets = []
    valores = []

    if "sets_tipo" in colunas:
        sets.append("sets_tipo = %s")
        valores.append(dados.get("sets_tipo"))
    if "pontos_set" in colunas:
        sets.append("pontos_set = %s")
        valores.append(dados.get("pontos_set"))
    if "tem_tiebreak" in colunas:
        sets.append("tem_tiebreak = %s")
        valores.append(dados.get("tem_tiebreak"))
    if "pontos_tiebreak" in colunas:
        sets.append("pontos_tiebreak = %s")
        valores.append(dados.get("pontos_tiebreak"))
    if "diferenca_minima" in colunas:
        sets.append("diferenca_minima = %s")
        valores.append(dados.get("diferenca_minima"))
    if "tempos_por_set" in colunas and "tempos_por_set" in dados:
        sets.append("tempos_por_set = %s")
        valores.append(dados.get("tempos_por_set"))
    if "substituicoes_por_set" in colunas and "substituicoes_por_set" in dados:
        sets.append("substituicoes_por_set = %s")
        valores.append(dados.get("substituicoes_por_set"))

    if not sets:
        return True

    valores.append(nome_competicao)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE competicoes
                SET {", ".join(sets)}
                WHERE nome = %s
                """,
                tuple(valores)
            )
        conn.commit()

    return True


def atualizar_pontuacao_desempate(nome_competicao, dados):
    ok_edicao, _ = validar_competicao_editavel(nome_competicao, "alteração de pontuação e desempate")
    if not ok_edicao:
        return False

    colunas = _buscar_colunas_tabela("competicoes")
    sets = []
    valores = []

    campos_pontuacao = [
        "vitoria_set_unico", "derrota_set_unico", "vitoria_2x0", "vitoria_2x1",
        "derrota_1x2", "derrota_0x2", "vitoria_3x0", "vitoria_3x1",
        "vitoria_3x2", "derrota_2x3", "derrota_1x3", "derrota_0x3",
    ]

    for campo in campos_pontuacao:
        if campo in colunas and campo in dados:
            sets.append(f"{campo} = %s")
            valores.append(dados.get(campo))

    if "criterios_desempate" in colunas:
        sets.append("criterios_desempate = %s")
        valores.append(
            dados.get(
                "criterios_desempate",
                "vitorias,pontos,saldo_sets,sets_pro,sets_contra,saldo_pontos,pontos_pro,pontos_contra,confronto_direto,coef_sets,coef_pontos,fair_play,sorteio"
            )
        )

    if not sets:
        return True

    valores.append(nome_competicao)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE competicoes
                SET {", ".join(sets)}
                WHERE nome = %s
                """,
                tuple(valores)
            )
        conn.commit()

    return True


def excluir_competicao(nome):
    try:
        with conectar() as conn:
            with conn.cursor() as cur:

                print(">>> EXCLUINDO COMPETIÇÃO:", nome)

                # 1. DESVINCULAR APONTADORES
                cur.execute("""
                    UPDATE usuarios
                    SET competicao_vinculada = NULL
                    WHERE competicao_vinculada = %s
                      AND perfil = 'apontador'
                """, (nome,))

                # 2. PARTIDAS
                cur.execute("DELETE FROM partidas WHERE competicao = %s", (nome,))

                # 3. GRUPOS
                try:
                    cur.execute("DELETE FROM grupo_equipes WHERE competicao = %s", (nome,))
                except:
                    pass

                try:
                    cur.execute("DELETE FROM grupos WHERE competicao = %s", (nome,))
                except:
                    pass

                # 4. ATLETAS
                cur.execute("DELETE FROM atletas WHERE competicao = %s", (nome,))

                # 5. EQUIPES
                cur.execute("DELETE FROM equipes WHERE competicao = %s", (nome,))

                # 6. USUÁRIOS (menos apontador e superadmin)
                cur.execute("""
                    DELETE FROM usuarios
                    WHERE competicao_vinculada = %s
                      AND perfil NOT IN ('superadmin', 'apontador')
                """, (nome,))

                # 7. COMPETIÇÃO
                cur.execute("DELETE FROM competicoes WHERE nome = %s", (nome,))

            conn.commit()

        print(">>> FINALIZADO COM SUCESSO")
        return True

    except Exception as e:
        print("ERRO REAL:", e)
        return False
        

def redefinir_senha_organizador(login_organizador):
    nova_senha = _gerar_senha_aleatoria(8)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE usuarios
                SET senha = %s
                WHERE login = %s
                  AND perfil = 'organizador'
            """, (nova_senha, login_organizador))
        conn.commit()

    return {"login": login_organizador, "senha": nova_senha}


# =========================================================
# CONTROLE DE INSCRIÇÃO DA COMPETIÇÃO
# =========================================================
def criar_campos_controle_inscricao_competicoes(force=False):
    chave = "campos_controle_inscricao_competicoes"
    if _schema_ja_pronto(chave, force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS data_limite_inscricao TEXT
            """)
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS hora_limite_inscricao TEXT
            """)
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS bloquear_apos_inicio BOOLEAN DEFAULT TRUE
            """)
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS limite_atletas INTEGER DEFAULT 0
            """)
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS permitir_edicao_pos_prazo BOOLEAN DEFAULT FALSE
            """)
        conn.commit()

    _marcar_schema_pronto(chave)

def obter_controle_inscricao_competicao(nome_competicao):
    criar_campos_controle_inscricao_competicoes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    nome,
                    data_limite_inscricao,
                    hora_limite_inscricao,
                    bloquear_apos_inicio,
                    limite_atletas,
                    permitir_edicao_pos_prazo
                FROM competicoes
                WHERE nome = %s
                LIMIT 1
            """, (nome_competicao,))
            return cur.fetchone()


def salvar_controle_inscricao_competicao(
    nome_competicao,
    data_limite_inscricao,
    hora_limite_inscricao,
    bloquear_apos_inicio,
    limite_atletas=0,
    permitir_edicao_pos_prazo=False
):
    criar_campos_controle_inscricao_competicoes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE competicoes
                SET
                    data_limite_inscricao = %s,
                    hora_limite_inscricao = %s,
                    bloquear_apos_inicio = %s,
                    limite_atletas = %s,
                    permitir_edicao_pos_prazo = %s
                WHERE nome = %s
            """, (
                data_limite_inscricao or None,
                hora_limite_inscricao or None,
                bloquear_apos_inicio,
                limite_atletas,
                permitir_edicao_pos_prazo,
                nome_competicao
            ))
        conn.commit()

    return True


def competicao_tem_partida_iniciada(nome_competicao):
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id
                    FROM partidas
                    WHERE competicao = %s
                      AND LOWER(COALESCE(status, '')) IN ('em_andamento', 'andamento', 'iniciada', 'iniciado')
                    LIMIT 1
                """, (nome_competicao,))
                return cur.fetchone() is not None
    except Exception:
        return False




def competicao_em_andamento(nome_competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1
                FROM partidas
                WHERE competicao = %s
                  AND LOWER(COALESCE(status, '')) IN ('em_andamento', 'andamento', 'iniciada', 'iniciado', 'finalizada')
                LIMIT 1
            """, (nome_competicao,))
            return cur.fetchone() is not None

def inscricao_e_edicao_liberadas(nome_competicao):
    controle = obter_controle_inscricao_competicao(nome_competicao)

    if not controle:
        return True, ""

    data_limite = (controle.get("data_limite_inscricao") or "").strip()
    hora_limite = (controle.get("hora_limite_inscricao") or "").strip()
    bloquear_apos_inicio = bool(controle.get("bloquear_apos_inicio"))

    if bloquear_apos_inicio and competicao_tem_partida_iniciada(nome_competicao):
        return False, "Inscrições e edições bloqueadas porque a competição já iniciou."

    if not data_limite:
        return True, ""

    try:
        if hora_limite:
            limite = datetime.strptime(f"{data_limite} {hora_limite}", "%Y-%m-%d %H:%M")
        else:
            limite = datetime.strptime(f"{data_limite} 23:59", "%Y-%m-%d %H:%M")
    except ValueError:
        return True, ""

    agora = datetime.now()

    if agora > limite:
        return False, "O prazo de inscrição e edição de atletas já foi encerrado."

    return True, ""




def criar_campos_liberacao_extra_equipes(force=False):
    chave = "campos_liberacao_extra_equipes"
    if _schema_ja_pronto(chave, force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE equipes
                ADD COLUMN IF NOT EXISTS liberacao_extra_inscricao BOOLEAN DEFAULT FALSE
            """)
            cur.execute("""
                ALTER TABLE equipes
                ADD COLUMN IF NOT EXISTS liberacao_extra_data TEXT
            """)
            cur.execute("""
                ALTER TABLE equipes
                ADD COLUMN IF NOT EXISTS liberacao_extra_hora TEXT
            """)
        conn.commit()

    _marcar_schema_pronto(chave)

def controle_inscricao_para_equipe(nome_competicao, nome_equipe):
    controle = obter_controle_inscricao_competicao(nome_competicao)

    liberado_atletas, motivo_travamento = validar_edicao_atletas_equipe(nome_competicao, nome_equipe)
    if not liberado_atletas:
        return {
            "aberta": False,
            "liberado": False,
            "motivo": motivo_travamento,
            "origem": "competicao_travada"
        }

    if not controle:
        return {
            "aberta": True,
            "liberado": True,
            "motivo": motivo_travamento,
            "origem": "competicao_aberta"
        }

    equipe = buscar_equipe_por_nome_e_competicao(nome_equipe, nome_competicao)
    if equipe:
        liberacao_extra = bool(equipe.get("liberacao_extra_inscricao"))
        data_extra = (equipe.get("liberacao_extra_data") or "").strip()
        hora_extra = (equipe.get("liberacao_extra_hora") or "").strip()

        if liberacao_extra:
            if not data_extra:
                return {
                    "aberta": True,
                    "liberado": True,
                    "motivo": "Equipe com liberação especial após o prazo.",
                    "origem": "liberacao_especial"
                }

            try:
                if hora_extra:
                    limite_extra = datetime.strptime(f"{data_extra} {hora_extra}", "%Y-%m-%d %H:%M")
                else:
                    limite_extra = datetime.strptime(f"{data_extra} 23:59", "%Y-%m-%d %H:%M")

                if datetime.now() <= limite_extra:
                    return {
                        "aberta": True,
                        "liberado": True,
                        "motivo": "Equipe com liberação especial dentro do prazo extra.",
                        "origem": "liberacao_especial"
                    }
            except ValueError:
                return {
                    "aberta": True,
                    "liberado": True,
                    "motivo": "Equipe com liberação especial.",
                    "origem": "liberacao_especial"
                }

    liberado, motivo = inscricao_e_edicao_liberadas(nome_competicao)

    if liberado:
        return {
            "aberta": True,
            "liberado": True,
            "motivo": "",
            "origem": "competicao_aberta"
        }

    return {
        "aberta": False,
        "liberado": False,
        "motivo": motivo or "Inscrição/edição bloqueada para esta equipe.",
        "origem": "bloqueado"
    }


def salvar_liberacao_extra_equipe(
    nome_equipe,
    nome_competicao,
    liberacao_extra_inscricao,
    liberacao_extra_data="",
    liberacao_extra_hora=""
):
    criar_campos_liberacao_extra_equipes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE equipes
                SET
                    liberacao_extra_inscricao = %s,
                    liberacao_extra_data = %s,
                    liberacao_extra_hora = %s
                WHERE nome = %s
                  AND competicao = %s
            """, (
                bool(liberacao_extra_inscricao),
                (liberacao_extra_data or "").strip() or None,
                (liberacao_extra_hora or "").strip() or None,
                nome_equipe,
                nome_competicao
            ))
        conn.commit()

    return True, "Atualizado com sucesso!"
    # ou
    return False, "Erro ao atualizar."

# =========================================================
# EQUIPES
# =========================================================
def criar_campos_quadro_tecnico_equipes(force=False):
    chave = "campos_quadro_tecnico_equipes"
    if _schema_ja_pronto(chave, force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE equipes
                ADD COLUMN IF NOT EXISTS treinador TEXT
            """)
            cur.execute("""
                ALTER TABLE equipes
                ADD COLUMN IF NOT EXISTS auxiliar_tecnico TEXT
            """)
            cur.execute("""
                ALTER TABLE equipes
                ADD COLUMN IF NOT EXISTS preparador_fisico TEXT
            """)
            cur.execute("""
                ALTER TABLE equipes
                ADD COLUMN IF NOT EXISTS medico TEXT
            """)
        conn.commit()

    _marcar_schema_pronto(chave)

def listar_equipes_da_competicao(nome_competicao):
    criar_campos_quadro_tecnico_equipes()
    criar_campos_liberacao_extra_equipes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    nome,
                    login,
                    senha,
                    competicao,
                    treinador,
                    auxiliar_tecnico,
                    preparador_fisico,
                    medico,
                    liberacao_extra_inscricao,
                    liberacao_extra_data,
                    liberacao_extra_hora
                FROM equipes
                WHERE competicao = %s
                ORDER BY nome
            """, (nome_competicao,))
            return cur.fetchall()


def buscar_equipe_por_nome_e_competicao(nome_equipe, nome_competicao):
    criar_campos_quadro_tecnico_equipes()
    criar_campos_liberacao_extra_equipes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    nome,
                    login,
                    senha,
                    competicao,
                    treinador,
                    auxiliar_tecnico,
                    preparador_fisico,
                    medico,
                    liberacao_extra_inscricao,
                    liberacao_extra_data,
                    liberacao_extra_hora
                FROM equipes
                WHERE nome = %s
                  AND competicao = %s
                LIMIT 1
            """, (nome_equipe, nome_competicao))
            return cur.fetchone()


def buscar_equipe_por_login(login):
    criar_campos_quadro_tecnico_equipes()
    criar_campos_liberacao_extra_equipes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    nome,
                    login,
                    senha,
                    competicao,
                    treinador,
                    auxiliar_tecnico,
                    preparador_fisico,
                    medico,
                    liberacao_extra_inscricao,
                    liberacao_extra_data,
                    liberacao_extra_hora
                FROM equipes
                WHERE login = %s
                LIMIT 1
            """, (login,))
            return cur.fetchone()


def atualizar_quadro_tecnico_equipe(nome_equipe, competicao, treinador, auxiliar_tecnico, preparador_fisico, medico):
    criar_campos_quadro_tecnico_equipes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE equipes
                SET treinador = %s,
                    auxiliar_tecnico = %s,
                    preparador_fisico = %s,
                    medico = %s
                WHERE nome = %s
                  AND competicao = %s
            """, (
                treinador,
                auxiliar_tecnico,
                preparador_fisico,
                medico,
                nome_equipe,
                competicao
            ))
        conn.commit()

    return True, "Atualizado com sucesso!"
    # ou
    return False, "Erro ao atualizar."


def equipe_existe_na_competicao(nome_equipe, nome_competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nome
                FROM equipes
                WHERE LOWER(nome) = LOWER(%s)
                  AND competicao = %s
                LIMIT 1
            """, (nome_equipe, nome_competicao))
            return cur.fetchone() is not None


def criar_equipe_com_credenciais(nome_equipe, nome_competicao):
    criar_campos_quadro_tecnico_equipes()
    criar_campos_liberacao_extra_equipes()

    login_equipe = _gerar_login_unico(_normalizar_login_equipe(nome_equipe))
    senha_equipe = _gerar_senha_aleatoria(8)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO equipes (
                    nome, login, senha, competicao,
                    treinador, auxiliar_tecnico, preparador_fisico, medico,
                    liberacao_extra_inscricao, liberacao_extra_data, liberacao_extra_hora
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                nome_equipe,
                login_equipe,
                senha_equipe,
                nome_competicao,
                "",
                "",
                "",
                "",
                False,
                None,
                None
            ))

            cur.execute("""
                INSERT INTO usuarios (
                    login, nome, senha, perfil, ativo, equipe, competicao_vinculada
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                login_equipe,
                nome_equipe,
                senha_equipe,
                "equipe",
                True,
                nome_equipe,
                nome_competicao
            ))

        conn.commit()

    return {
        "login": login_equipe,
        "senha": senha_equipe
    }


def atualizar_nome_equipe(nome_atual, nome_competicao, novo_nome):
    ok_edicao, _ = validar_competicao_editavel(nome_competicao, "alteração estrutural")
    if not ok_edicao:
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login
                FROM equipes
                WHERE nome = %s
                  AND competicao = %s
                LIMIT 1
            """, (nome_atual, nome_competicao))
            equipe = cur.fetchone()

            if not equipe:
                return False

            login_equipe = equipe["login"]

            cur.execute("""
                UPDATE equipes
                SET nome = %s
                WHERE nome = %s
                  AND competicao = %s
            """, (novo_nome, nome_atual, nome_competicao))

            cur.execute("""
                UPDATE usuarios
                SET nome = %s,
                    equipe = %s
                WHERE login = %s
                  AND perfil = 'equipe'
                  AND competicao_vinculada = %s
            """, (novo_nome, novo_nome, login_equipe, nome_competicao))

            cur.execute("""
                UPDATE atletas
                SET equipe = %s
                WHERE equipe = %s
                  AND competicao = %s
            """, (novo_nome, nome_atual, nome_competicao))

        conn.commit()

    return True, "Atualizado com sucesso!"
    # ou
    return False, "Erro ao atualizar."


def redefinir_senha_da_equipe(nome_equipe, nome_competicao):
    nova_senha = _gerar_senha_aleatoria(8)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login
                FROM equipes
                WHERE nome = %s
                  AND competicao = %s
                LIMIT 1
            """, (nome_equipe, nome_competicao))
            equipe = cur.fetchone()

            if not equipe:
                return None

            login_equipe = equipe["login"]

            cur.execute("""
                UPDATE equipes
                SET senha = %s
                WHERE nome = %s
                  AND competicao = %s
            """, (nova_senha, nome_equipe, nome_competicao))

            cur.execute("""
                UPDATE usuarios
                SET senha = %s
                WHERE login = %s
                  AND perfil = 'equipe'
                  AND competicao_vinculada = %s
            """, (nova_senha, login_equipe, nome_competicao))

        conn.commit()

    return {
        "login": login_equipe,
        "senha": nova_senha
    }


def excluir_equipe(nome_equipe, nome_competicao):
    ok_edicao, _ = validar_competicao_editavel(nome_competicao, "alteração estrutural")
    if not ok_edicao:
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login
                FROM equipes
                WHERE nome = %s
                  AND competicao = %s
                LIMIT 1
            """, (nome_equipe, nome_competicao))
            equipe = cur.fetchone()

            if not equipe:
                return False

            login_equipe = equipe["login"]

            cur.execute("""
                DELETE FROM equipes
                WHERE nome = %s
                  AND competicao = %s
            """, (nome_equipe, nome_competicao))

            cur.execute("""
                DELETE FROM usuarios
                WHERE login = %s
                  AND perfil = 'equipe'
                  AND competicao_vinculada = %s
            """, (login_equipe, nome_competicao))

            cur.execute("""
                DELETE FROM atletas
                WHERE equipe = %s
                  AND competicao = %s
            """, (nome_equipe, nome_competicao))

        conn.commit()

    return True


# =========================================================
# MESÁRIOS
# =========================================================
def listar_mesarios_da_competicao(nome_competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login, nome, senha, perfil, ativo, competicao_vinculada
                FROM usuarios
                WHERE perfil = 'mesario'
                  AND competicao_vinculada = %s
                ORDER BY nome
            """, (nome_competicao,))
            return cur.fetchall()


def mesario_existe_na_competicao(nome_mesario, nome_competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login
                FROM usuarios
                WHERE perfil = 'mesario'
                  AND LOWER(nome) = LOWER(%s)
                  AND competicao_vinculada = %s
                LIMIT 1
            """, (nome_mesario, nome_competicao))
            return cur.fetchone() is not None


def criar_mesario_com_credenciais(nome_mesario, nome_competicao):
    login_mesario = _gerar_login_unico(_normalizar_login_mesario(nome_mesario))
    senha_mesario = _gerar_senha_aleatoria(8)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO usuarios (
                    login, nome, senha, perfil, ativo, equipe, competicao_vinculada
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                login_mesario,
                nome_mesario,
                senha_mesario,
                "mesario",
                True,
                None,
                nome_competicao
            ))

        conn.commit()

    return {"login": login_mesario, "senha": senha_mesario}


def redefinir_senha_do_mesario(nome_mesario, nome_competicao):
    nova_senha = _gerar_senha_aleatoria(8)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login
                FROM usuarios
                WHERE perfil = 'mesario'
                  AND nome = %s
                  AND competicao_vinculada = %s
                LIMIT 1
            """, (nome_mesario, nome_competicao))
            mesario = cur.fetchone()

            if not mesario:
                return None

            login_mesario = mesario["login"]

            cur.execute("""
                UPDATE usuarios
                SET senha = %s
                WHERE login = %s
                  AND perfil = 'mesario'
                  AND competicao_vinculada = %s
            """, (nova_senha, login_mesario, nome_competicao))

        conn.commit()

    return {"login": login_mesario, "senha": nova_senha}


def excluir_mesario(nome_mesario, nome_competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM usuarios
                WHERE perfil = 'mesario'
                  AND nome = %s
                  AND competicao_vinculada = %s
            """, (nome_mesario, nome_competicao))
            apagados = cur.rowcount

        conn.commit()

    return apagados > 0


# =========================================================
# DASHBOARD
# =========================================================
def contar_competicoes():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total
                FROM competicoes
            """)
            row = cur.fetchone()
            return row["total"] if row else 0


def contar_equipes():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total
                FROM equipes
            """)
            row = cur.fetchone()
            return row["total"] if row else 0


def contar_partidas():
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) AS total
                    FROM partidas
                """)
                row = cur.fetchone()
                return row["total"] if row else 0
    except Exception:
        return 0


def criar_indices_desempenho(force=False):
    chave = "indices_desempenho"
    if _schema_ja_pronto(chave, force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_atletas_equipe_competicao ON atletas (equipe, competicao)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_atletas_competicao_status_nome ON atletas (competicao, status, nome)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_atletas_equipe_competicao_numero ON atletas (equipe, competicao, numero)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_equipes_nome_competicao ON equipes (nome, competicao)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_equipes_login ON equipes (login)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_partidas_competicao_status ON partidas (competicao, status)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_partidas_competicao_equipes ON partidas (competicao, equipe_a, equipe_b)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_partidas_competicao_id ON partidas (competicao, id)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_partidas_competicao_ordem ON partidas (competicao, ordem)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_eventos_partida_competicao ON eventos (partida_id, competicao)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_papeletas_partida_competicao_set ON papeletas (partida_id, competicao, set_numero)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_usuarios_login_perfil ON usuarios (login, perfil)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_competicoes_nome ON competicoes (nome)""")
        conn.commit()

    _marcar_schema_pronto(chave)


# =========================================================
# ATLETAS
# =========================================================
def criar_tabela_atletas(force=False):
    chave = "tabela_atletas"
    if _schema_ja_pronto(chave, force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS atletas (
                    id SERIAL PRIMARY KEY,
                    nome TEXT NOT NULL,
                    cpf TEXT UNIQUE NOT NULL,
                    data_nascimento TEXT,
                    numero INTEGER,
                    equipe TEXT,
                    competicao TEXT,
                    status TEXT DEFAULT 'pendente'
                )
            """)
            cur.execute("ALTER TABLE atletas ADD COLUMN IF NOT EXISTS capitao_padrao BOOLEAN DEFAULT FALSE")
        conn.commit()

    _marcar_schema_pronto(chave)
    # criar_indices_desempenho()

def atleta_existe_por_cpf(cpf):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM atletas
                WHERE cpf = %s
                LIMIT 1
            """, (cpf,))
            return cur.fetchone() is not None


def cadastrar_atleta(nome, cpf, data_nascimento, numero, equipe, competicao):
    nome = (nome or "").strip()
    cpf = (cpf or "").strip()
    equipe = (equipe or "").strip()
    competicao = (competicao or "").strip()

    if not nome or not cpf:
        return False, "Informe nome e CPF do atleta."

    numero_final = None
    if numero not in (None, ""):
        try:
            numero_final = int(numero)
        except (TypeError, ValueError):
            return False, "Número inválido."

    criar_tabela_atletas()
    criar_campos_controle_inscricao_competicoes()
    criar_campos_liberacao_extra_equipes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM atletas
                WHERE cpf = %s
                LIMIT 1
            """, (cpf,))
            if cur.fetchone() is not None:
                return False, "Já existe um atleta cadastrado com este CPF."

            cur.execute("""
                SELECT
                    c.nome,
                    c.data_limite_inscricao,
                    c.hora_limite_inscricao,
                    COALESCE(c.bloquear_apos_inicio, TRUE) AS bloquear_apos_inicio,
                    COALESCE(c.limite_atletas, 0) AS limite_atletas,
                    COALESCE(c.travada, FALSE) AS travada,
                    COALESCE(e.liberacao_extra_inscricao, FALSE) AS liberacao_extra_inscricao,
                    e.liberacao_extra_data,
                    e.liberacao_extra_hora
                FROM competicoes c
                LEFT JOIN equipes e
                  ON e.competicao = c.nome
                 AND e.nome = %s
                WHERE c.nome = %s
                LIMIT 1
            """, (equipe, competicao))
            controle = cur.fetchone() or {}

            if controle.get("travada"):
                cur.execute("""
                    SELECT id
                    FROM partidas
                    WHERE competicao = %s
                      AND (equipe_a = %s OR equipe_b = %s OR equipe_a_operacional = %s OR equipe_b_operacional = %s)
                      AND (
                            COALESCE(pontos_a, 0) > 0
                         OR COALESCE(pontos_b, 0) > 0
                         OR LOWER(COALESCE(status_jogo, '')) IN ('em_andamento', 'entre_sets', 'tiebreak_sorteio', 'finalizada', 'encerrado')
                         OR LOWER(COALESCE(status, '')) IN ('em_andamento', 'andamento', 'iniciada', 'iniciado', 'finalizada')
                      )
                    LIMIT 1
                """, (competicao, equipe, equipe, equipe, equipe))
                if cur.fetchone() is not None:
                    return False, "A competição está travada e esta equipe já iniciou seus jogos. Alterações de atletas foram bloqueadas."

            prazo_liberado_por_extra = False
            if bool(controle.get("liberacao_extra_inscricao")):
                data_extra = (controle.get("liberacao_extra_data") or "").strip()
                hora_extra = (controle.get("liberacao_extra_hora") or "").strip() or "23:59"
                if not data_extra:
                    prazo_liberado_por_extra = True
                else:
                    try:
                        prazo_liberado_por_extra = datetime.now() <= datetime.strptime(f"{data_extra} {hora_extra}", "%Y-%m-%d %H:%M")
                    except ValueError:
                        prazo_liberado_por_extra = True

            if not prazo_liberado_por_extra:
                if bool(controle.get("bloquear_apos_inicio")):
                    cur.execute("""
                        SELECT id
                        FROM partidas
                        WHERE competicao = %s
                          AND LOWER(COALESCE(status, '')) IN ('em_andamento', 'andamento', 'iniciada', 'iniciado')
                        LIMIT 1
                    """, (competicao,))
                    if cur.fetchone() is not None:
                        return False, "Inscrições e edições bloqueadas porque a competição já iniciou."

                data_limite = (controle.get("data_limite_inscricao") or "").strip()
                hora_limite = (controle.get("hora_limite_inscricao") or "").strip() or "23:59"
                if data_limite:
                    try:
                        if datetime.now() > datetime.strptime(f"{data_limite} {hora_limite}", "%Y-%m-%d %H:%M"):
                            return False, "O prazo de inscrição e edição de atletas já foi encerrado."
                    except ValueError:
                        pass

            limite = int(controle.get("limite_atletas") or 0)
            if limite > 0:
                cur.execute("""
                    SELECT COUNT(*) AS total
                    FROM atletas
                    WHERE equipe = %s
                      AND competicao = %s
                """, (equipe, competicao))
                row = cur.fetchone() or {}
                if int(row.get("total") or 0) >= limite:
                    return False, "O limite de atletas da equipe já foi atingido."

            if numero_final is not None:
                cur.execute("""
                    SELECT id
                    FROM atletas
                    WHERE equipe = %s
                      AND competicao = %s
                      AND numero = %s
                    LIMIT 1
                """, (equipe, competicao, numero_final))
                if cur.fetchone() is not None:
                    return False, "Já existe outro atleta com essa numeração nesta equipe."

            cur.execute("""
                INSERT INTO atletas (
                    nome, cpf, data_nascimento, numero, equipe, competicao, status
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'pendente')
            """, (nome, cpf, data_nascimento, numero_final, equipe, competicao))
        conn.commit()

    return True, "Atleta cadastrado com sucesso."

def listar_atletas_da_equipe(equipe, competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM atletas
                WHERE equipe = %s
                  AND competicao = %s
                ORDER BY nome
            """, (equipe, competicao))
            return cur.fetchall()


def excluir_atleta(id_atleta):
    try:
        # Abre UMA ÚNICA conexão para fazer todo o trabalho
        with conectar() as conn:
            with conn.cursor() as cur:
                # 1. Busca os dados do atleta
                cur.execute("SELECT equipe, competicao FROM atletas WHERE id = %s", (id_atleta,))
                atleta = cur.fetchone()
                
                if not atleta:
                    return False, "Atleta não encontrado."

                nome_equipe = atleta["equipe"]
                nome_competicao = atleta["competicao"]

                # 2. Verifica se a competição está travada direto no banco (sem abrir outra conexão)
                cur.execute("""
                    SELECT COALESCE(travada, FALSE) AS travada
                    FROM competicoes
                    WHERE nome = %s
                """, (nome_competicao,))
                comp = cur.fetchone()

                if comp and comp.get("travada"):
                    # 3. Se estiver travada, verifica se a equipe já jogou (sem abrir outra conexão)
                    cur.execute("""
                        SELECT id FROM partidas
                        WHERE competicao = %s
                          AND (equipe_a = %s OR equipe_b = %s OR equipe_a_operacional = %s OR equipe_b_operacional = %s)
                          AND (
                              COALESCE(pontos_a, 0) > 0 OR COALESCE(pontos_b, 0) > 0
                              OR LOWER(COALESCE(status_jogo, '')) IN ('em_andamento', 'entre_sets', 'tiebreak_sorteio', 'finalizada', 'encerrado')
                              OR LOWER(COALESCE(status, '')) IN ('em_andamento', 'andamento', 'iniciada', 'iniciado', 'finalizada')
                          )
                        LIMIT 1
                    """, (nome_competicao, nome_equipe, nome_equipe, nome_equipe, nome_equipe))
                    
                    if cur.fetchone():
                        return False, "Competição travada: esta equipe já iniciou jogos. Exclusão bloqueada."

                # 4. Passou nas validações? Deleta o atleta!
                cur.execute("DELETE FROM atletas WHERE id = %s", (id_atleta,))
            
            # Salva as alterações no banco!
            conn.commit()

        return True, "Atleta removido com sucesso."
    
    except Exception as e:
        # 5. Captura erros do banco (ex: atleta que já tem ponto na súmula)
        erro_str = str(e).lower()
        if "foreign key" in erro_str or "violates foreign key" in erro_str:
            return False, "Este atleta já jogou ou está em uma súmula e não pode ser excluído."
        return False, f"Erro ao excluir atleta: {str(e)}"


# =========================================================
# ATLETAS - ORGANIZADOR
# =========================================================
def listar_atletas_da_competicao(nome_competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, nome, cpf, equipe, status
                FROM atletas
                WHERE competicao = %s
                ORDER BY status, nome
            """, (nome_competicao,))
            return cur.fetchall()


def atualizar_status_atleta(id_atleta, novo_status):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, equipe, competicao
                FROM atletas
                WHERE id = %s
                LIMIT 1
            """, (id_atleta,))
            atleta = cur.fetchone()

            if not atleta:
                return False, "Atleta não encontrado."

            ok_edicao, mensagem = validar_edicao_atletas_equipe(atleta["competicao"], atleta["equipe"])
            if not ok_edicao:
                return False, mensagem

            cur.execute("""
                UPDATE atletas
                SET status = %s
                WHERE id = %s
            """, (novo_status, id_atleta))
        conn.commit()

    return True, "Status do atleta atualizado com sucesso."


# =========================================================
# TABELA - GRUPOS
# =========================================================
def criar_tabelas_grupos():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS grupos (
                    id SERIAL PRIMARY KEY,
                    nome VARCHAR(10),
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

        conn.commit()


def listar_grupos(competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM grupos
                WHERE competicao = %s
                ORDER BY nome
            """, (competicao,))
            return cur.fetchall()


def criar_grupo(nome, competicao):
    if fase_grupos_esta_travada_por_jogo(competicao):
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM grupos
                WHERE UPPER(nome) = UPPER(%s)
                  AND competicao = %s
                LIMIT 1
            """, (nome, competicao))
            existente = cur.fetchone()
            if existente:
                return False

            cur.execute("""
                INSERT INTO grupos (nome, competicao)
                VALUES (%s, %s)
            """, (nome, competicao))
        conn.commit()
    return True


def adicionar_equipe_no_grupo(grupo_id, equipe, competicao):
    if fase_grupos_esta_travada_por_jogo(competicao):
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM grupos_equipes
                WHERE grupo_id = %s
                  AND equipe = %s
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
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM grupos_equipes
                WHERE grupo_id = %s
            """, (grupo_id,))
            return cur.fetchall()


def buscar_grupo_por_id(grupo_id, competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM grupos
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (grupo_id, competicao))
            return cur.fetchone()


def atualizar_grupo(grupo_id, novo_nome, competicao):
    grupo_atual = buscar_grupo_por_id(grupo_id, competicao)
    if not grupo_atual:
        return False

    nome_antigo = grupo_atual["nome"]

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM grupos
                WHERE UPPER(nome) = UPPER(%s)
                  AND competicao = %s
                  AND id <> %s
                LIMIT 1
            """, (novo_nome, competicao, grupo_id))
            if cur.fetchone():
                return False

            cur.execute("""
                UPDATE grupos
                SET nome = %s
                WHERE id = %s
                  AND competicao = %s
            """, (novo_nome, grupo_id, competicao))

            cur.execute("""
                UPDATE partidas
                SET grupo = %s
                WHERE competicao = %s
                  AND grupo = %s
            """, (novo_nome, competicao, nome_antigo))
        conn.commit()
    return True


def remover_equipe_do_grupo(grupo_id, equipe, competicao):
    if fase_grupos_esta_travada_por_jogo(competicao):
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM grupos_equipes
                WHERE grupo_id = %s
                  AND equipe = %s
                  AND competicao = %s
            """, (grupo_id, equipe, competicao))

            grupo = buscar_grupo_por_id(grupo_id, competicao)
            if grupo:
                cur.execute("""
                    DELETE FROM partidas
                    WHERE competicao = %s
                      AND grupo = %s
                      AND (equipe_a = %s OR equipe_b = %s)
                """, (competicao, grupo["nome"], equipe, equipe))
        conn.commit()
    return True


def excluir_grupo(grupo_id, competicao):
    if fase_grupos_esta_travada_por_jogo(competicao):
        return False

    grupo = buscar_grupo_por_id(grupo_id, competicao)
    if not grupo:
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM partidas
                WHERE competicao = %s
                  AND grupo = %s
            """, (competicao, grupo["nome"]))

            cur.execute("""
                DELETE FROM grupos_equipes
                WHERE grupo_id = %s
                  AND competicao = %s
            """, (grupo_id, competicao))

            cur.execute("""
                DELETE FROM grupos
                WHERE id = %s
                  AND competicao = %s
            """, (grupo_id, competicao))
        conn.commit()
    return True


# =========================================================
# PARTIDAS (TABELA DE JOGOS)
# =========================================================
def criar_tabela_partidas():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS partidas (
                    id SERIAL PRIMARY KEY,
                    competicao TEXT NOT NULL,
                    grupo TEXT,
                    equipe_a TEXT,
                    equipe_b TEXT,
                    fase TEXT DEFAULT 'grupos',
                    ordem INTEGER,
                    status TEXT DEFAULT 'agendada'
                )
            """)

            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS rodada INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS quadra TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS data_hora TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS origem TEXT DEFAULT 'manual'")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS sets_a INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS sets_b INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set1_a INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set1_b INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set2_a INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set2_b INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set3_a INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set3_b INTEGER")

            # operação do apontador / pré-jogo
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS operador_login TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS operador_nome TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS status_operacao TEXT DEFAULT 'livre'")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS reservado_em TIMESTAMP")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS pre_jogo_iniciado_em TIMESTAMP")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS apontador_login TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS apontador_nome TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS arbitro_1_cpf TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS arbitro_1_nome TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS arbitro_2_cpf TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS arbitro_2_nome TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS sorteio_vencedor TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS sorteio_escolha TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS saque_inicial TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS lado_esquerdo TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS equipe_a_operacional TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS equipe_b_operacional TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS capitao_a_id INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS capitao_a_nome TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS capitao_a_numero INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS capitao_b_id INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS capitao_b_nome TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS capitao_b_numero INTEGER")

        conn.commit()


def listar_partidas(competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM partidas
                WHERE competicao = %s
                ORDER BY COALESCE(rodada, 999999), ordem, id
            """, (competicao,))
            return cur.fetchall()


def buscar_partida_por_id(partida_id, competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))
            return cur.fetchone()



def _normalizar_fase_partida(fase):
    fase = (fase or "grupos").strip().lower()
    if fase in {"classificatorias", "classificatória", "classificatorias", "grupo"}:
        return "grupos"
    if fase in {"semifinais", "semi", "semis"}:
        return "semifinal"
    if fase in {"finais", "finalíssima", "finalissima"}:
        return "final"
    return fase or "grupos"


def _status_partida_bloqueado(status, status_jogo=None):
    status = (status or "").strip().lower()
    status_jogo = (status_jogo or "").strip().lower()
    bloqueados = {
        "pre_jogo", "em_andamento", "em andamento", "andamento",
        "entre_sets", "tiebreak_sorteio", "finalizada", "finalizado",
        "encerrada", "encerrado", "iniciada", "iniciado",
    }
    return status in bloqueados or status_jogo in bloqueados


def partida_ja_iniciou_ou_finalizou(partida):
    if not partida:
        return False
    try:
        pontos_a = int(partida.get("pontos_a") or 0)
        pontos_b = int(partida.get("pontos_b") or 0)
        sets_a = int(partida.get("sets_a") or 0)
        sets_b = int(partida.get("sets_b") or 0)
    except (TypeError, ValueError):
        pontos_a = pontos_b = sets_a = sets_b = 0

    return (
        pontos_a > 0
        or pontos_b > 0
        or sets_a > 0
        or sets_b > 0
        or bool(partida.get("pre_jogo_iniciado_em"))
        or bool(partida.get("pre_jogo_finalizado"))
        or _status_partida_bloqueado(partida.get("status"), partida.get("status_jogo"))
    )


def competicao_tem_partida_iniciada_por_fase(nome_competicao, fase=None):
    fase = _normalizar_fase_partida(fase) if fase else None
    sql_fase = "AND COALESCE(fase, 'grupos') = %s" if fase else ""
    params = [nome_competicao]
    if fase:
        params.append(fase)

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT id
                    FROM partidas
                    WHERE competicao = %s
                      {sql_fase}
                      AND (
                            COALESCE(pontos_a, 0) > 0
                         OR COALESCE(pontos_b, 0) > 0
                         OR COALESCE(sets_a, 0) > 0
                         OR COALESCE(sets_b, 0) > 0
                         OR pre_jogo_iniciado_em IS NOT NULL
                         OR COALESCE(pre_jogo_finalizado, FALSE) = TRUE
                         OR LOWER(COALESCE(status_jogo, '')) IN ('pre_jogo', 'em_andamento', 'em andamento', 'andamento', 'entre_sets', 'tiebreak_sorteio', 'finalizada', 'finalizado', 'encerrada', 'encerrado')
                         OR LOWER(COALESCE(status, '')) IN ('pre_jogo', 'em_andamento', 'em andamento', 'andamento', 'iniciada', 'iniciado', 'finalizada', 'finalizado', 'encerrada', 'encerrado')
                      )
                    LIMIT 1
                """, tuple(params))
                return cur.fetchone() is not None
    except Exception:
        return False


def fase_grupos_esta_travada_por_jogo(nome_competicao):
    return competicao_tem_partida_iniciada_por_fase(nome_competicao, "grupos")


def fase_tem_partida_iniciada(nome_competicao, fase):
    return competicao_tem_partida_iniciada_por_fase(nome_competicao, fase)


def fase_partidas_pode_ser_alterada(nome_competicao, fase):
    fase = _normalizar_fase_partida(fase)
    if fase == "grupos":
        return not fase_grupos_esta_travada_por_jogo(nome_competicao)
    return not fase_tem_partida_iniciada(nome_competicao, fase)

def criar_partida(competicao, grupo, equipe_a, equipe_b, ordem, quadra=None, fase='grupos', data_hora=None, rodada=None, origem='manual'):
    fase = _normalizar_fase_partida(fase)
    grupo = grupo if fase == "grupos" else None

    if not fase_partidas_pode_ser_alterada(competicao, fase):
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO partidas (
                    competicao, grupo, equipe_a, equipe_b, fase, ordem, quadra, data_hora, rodada, origem, status,
                    sets_a, sets_b, set1_a, set1_b, set2_a, set2_b, set3_a, set3_b
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'agendada', 0, 0, NULL, NULL, NULL, NULL, NULL, NULL)
            """, (competicao, grupo, equipe_a, equipe_b, fase, ordem, quadra, data_hora, rodada, origem))
        conn.commit()


def atualizar_partida(partida_id, competicao, grupo, fase, equipe_a, equipe_b, quadra=None, data_hora=None, status='agendada', rodada=None):
    fase = _normalizar_fase_partida(fase)
    grupo = grupo if fase == "grupos" else None

    partida_atual = buscar_partida_por_id(partida_id, competicao)
    if partida_ja_iniciou_ou_finalizou(partida_atual):
        return False
    if not fase_partidas_pode_ser_alterada(competicao, fase):
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE partidas
                SET grupo = %s,
                    fase = %s,
                    equipe_a = %s,
                    equipe_b = %s,
                    quadra = %s,
                    data_hora = %s,
                    status = %s,
                    rodada = %s
                WHERE id = %s
                  AND competicao = %s
            """, (grupo, fase, equipe_a, equipe_b, quadra, data_hora, status, rodada, partida_id, competicao))
        conn.commit()
    return True


def excluir_partida(partida_id, competicao):
    partida = buscar_partida_por_id(partida_id, competicao)
    if not partida:
        return False, "Partida não encontrada."

    if partida_ja_iniciou_ou_finalizou(partida):
        return False, "Não é possível excluir uma partida que já iniciou, teve pré-jogo aberto ou foi finalizada."

    fase = _normalizar_fase_partida(partida.get("fase"))
    if not fase_partidas_pode_ser_alterada(competicao, fase):
        return False, "Esta fase já iniciou. Não é possível excluir partidas dela."

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM partidas
                WHERE id = %s
                  AND competicao = %s
            """, (partida_id, competicao))
        conn.commit()
    return True, "Partida excluída com sucesso."

def limpar_partidas(competicao):
    if competicao_tem_partida_iniciada_por_fase(competicao):
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM partidas
                WHERE competicao = %s
            """, (competicao,))
        conn.commit()


def limpar_partidas_por_fase(competicao, fase):
    fase = _normalizar_fase_partida(fase)
    if not fase_partidas_pode_ser_alterada(competicao, fase):
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM partidas
                WHERE competicao = %s
                  AND COALESCE(fase, 'grupos') = %s
            """, (competicao, fase))
            conn.commit()


# =========================================================
# ROTAÇÃO OFICIAL / HISTÓRICO / VALIDAÇÃO
# =========================================================

def criar_campos_rotacao_partidas(force=False):
    chave = "campos_rotacao_partidas"
    if _schema_ja_pronto(chave, force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE partidas
                ADD COLUMN IF NOT EXISTS rotacao_a TEXT[],
                ADD COLUMN IF NOT EXISTS rotacao_b TEXT[],
                ADD COLUMN IF NOT EXISTS saque_atual TEXT,
                ADD COLUMN IF NOT EXISTS saque_inicial TEXT,
                ADD COLUMN IF NOT EXISTS rotacao_validacao_ativa BOOLEAN DEFAULT TRUE
            """)
        conn.commit()

    _marcar_schema_pronto(chave)


def criar_tabela_historico_rotacao(force=False):
    chave = "tabela_historico_rotacao"
    if _schema_ja_pronto(chave, force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS historico_rotacao (
                    id SERIAL PRIMARY KEY,
                    partida_id INTEGER NOT NULL,
                    competicao TEXT NOT NULL,
                    set_numero INTEGER DEFAULT 1,

                    ponto_a INTEGER DEFAULT 0,
                    ponto_b INTEGER DEFAULT 0,

                    equipe_ponto TEXT,
                    saque_antes TEXT,
                    saque_depois TEXT,

                    girou BOOLEAN DEFAULT FALSE,
                    equipe_girou TEXT,

                    rotacao_a_antes TEXT[],
                    rotacao_b_antes TEXT[],
                    rotacao_a_depois TEXT[],
                    rotacao_b_depois TEXT[],

                    irregularidade BOOLEAN DEFAULT FALSE,
                    tipo_irregularidade TEXT,
                    mensagem TEXT,

                    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

        conn.commit()

    _marcar_schema_pronto(chave)


def criar_estrutura_rotacao_profissional(force=False):
    criar_campos_rotacao_partidas(force=force)
    criar_tabela_historico_rotacao(force=force)


# ------------------------------
# HELPERS
# ------------------------------

def _normalizar_rotacao_oficial(rotacao):
    if not isinstance(rotacao, list):
        rotacao = []

    rotacao = [str(x or "").strip() for x in rotacao]

    while len(rotacao) < 6:
        rotacao.append("")

    return rotacao[:6]


def _rotacao_tem_6_validos(rotacao):
    rotacao = _normalizar_rotacao_oficial(rotacao)
    preenchidos = [x for x in rotacao if x]
    return len(preenchidos) == 6 and len(set(preenchidos)) == 6


def _rotacao_valida_ou_padrao(rotacao):
    r = _normalizar_rotacao_oficial(rotacao)
    if not _rotacao_tem_6_validos(r):
        return ["", "", "", "", "", ""]
    return r


def girar_rotacao_oficial(rotacao):
    """
    Ordem interna/visual usada no sistema:
    [IV, III, II, V, VI, I]

    Giro oficial:
    II vai para I (sacador)
    I vai para VI
    VI vai para V
    V vai para IV
    IV vai para III
    III vai para II
    """
    rotacao = _normalizar_rotacao_oficial(rotacao)

    if not _rotacao_tem_6_validos(rotacao):
        return rotacao

    return [
        rotacao[3],  # novo IV  = antigo V
        rotacao[0],  # novo III = antigo IV
        rotacao[1],  # novo II  = antigo III
        rotacao[4],  # novo V   = antigo VI
        rotacao[5],  # novo VI  = antigo I
        rotacao[2],  # novo I   = antigo II (sacador)
    ]


def validar_rotacao_oficial(rotacao, atletas_validos=None):
    rotacao = _normalizar_rotacao_oficial(rotacao)
    erros = []

    preenchidos = [x for x in rotacao if x]

    if len(preenchidos) != 6:
        erros.append("A rotação precisa ter 6 atletas.")

    repetidos = sorted({x for x in preenchidos if preenchidos.count(x) > 1})
    if repetidos:
        erros.append("Repetidos: " + ", ".join(repetidos))

    if atletas_validos:
        validos = {str(x).strip() for x in atletas_validos}
        invalidos = [x for x in preenchidos if x not in validos]
        if invalidos:
            erros.append("Inválidos: " + ", ".join(invalidos))

    return {"ok": not erros, "erros": erros}


# ------------------------------
# CORE DO SISTEMA
# ------------------------------

def aplicar_rotacao_por_ponto(partida_id, competicao, equipe_ponto):
    criar_estrutura_rotacao_profissional()

    equipe_ponto = str(equipe_ponto or "").strip().upper()
    if equipe_ponto not in {"A", "B"}:
        return False, {"mensagem": "Equipe inválida"}

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM partidas
                WHERE id = %s AND competicao = %s
                FOR UPDATE
            """, (partida_id, competicao))
            partida = cur.fetchone()

            if not partida:
                return False, {"mensagem": "Partida não encontrada"}

            estado = buscar_estado_jogo_partida(partida_id, competicao) or {}

            ponto_a = int(partida.get("pontos_a") or 0)
            ponto_b = int(partida.get("pontos_b") or 0)
            set_atual = int(partida.get("set_atual") or 1)

            rotacao_a = _rotacao_valida_ou_padrao(
                estado.get("rotacao_a") or partida.get("rotacao_a")
            )
            rotacao_b = _rotacao_valida_ou_padrao(
                estado.get("rotacao_b") or partida.get("rotacao_b")
            )

            saque_antes = (
                estado.get("saque_atual")
                or partida.get("saque_atual")
                or partida.get("saque_inicial")
                or ""
            ).strip().upper()

            rotacao_a_antes = list(rotacao_a)
            rotacao_b_antes = list(rotacao_b)

            girou = False
            equipe_girou = ""

            # 🔥 REGRA OFICIAL
            if saque_antes != equipe_ponto:
                girou = True
                equipe_girou = equipe_ponto

                if equipe_ponto == "A":
                    rotacao_a = girar_rotacao_oficial(rotacao_a)
                else:
                    rotacao_b = girar_rotacao_oficial(rotacao_b)

            saque_depois = equipe_ponto

            cur.execute("""
                UPDATE partidas
                SET rotacao_a=%s, rotacao_b=%s, saque_atual=%s
                WHERE id=%s AND competicao=%s
            """, (rotacao_a, rotacao_b, saque_depois, partida_id, competicao))

            cur.execute("""
                INSERT INTO historico_rotacao (
                    partida_id, competicao, set_numero,
                    ponto_a, ponto_b,
                    equipe_ponto,
                    saque_antes, saque_depois,
                    girou, equipe_girou,
                    rotacao_a_antes, rotacao_b_antes,
                    rotacao_a_depois, rotacao_b_depois
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                partida_id, competicao, set_atual,
                ponto_a, ponto_b,
                equipe_ponto,
                saque_antes, saque_depois,
                girou, equipe_girou,
                rotacao_a_antes, rotacao_b_antes,
                rotacao_a, rotacao_b
            ))

        conn.commit()

    try:
        estado_atual = buscar_estado_jogo_partida(partida_id, competicao) or {}

        estado_atual.update({
            "rotacao_a": rotacao_a,
            "rotacao_b": rotacao_b,
            "saque_atual": saque_depois,
            "pontos_a": ponto_a,
            "pontos_b": ponto_b
        })

        _salvar_snapshot_estado_jogo(partida_id, competicao, estado_atual)

    except Exception as e:
        print("ERRO snapshot:", e)

    return True, {
        "rotacao_a": rotacao_a,
        "rotacao_b": rotacao_b,
        "saque_atual": saque_depois,
        "girou": girou,
        "equipe_girou": equipe_girou
    }


# =========================================================
# OFICIAIS (ÁRBITROS E APONTADORES)
# =========================================================
def criar_tabelas_oficiais():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS oficiais (
                id SERIAL PRIMARY KEY,
                nome TEXT NOT NULL,
                cpf TEXT UNIQUE NOT NULL,
                criado_em TIMESTAMP DEFAULT NOW()
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS apontadores_acesso (
                id SERIAL PRIMARY KEY,
                cpf TEXT UNIQUE NOT NULL,
                senha TEXT,
                ativo BOOLEAN DEFAULT TRUE,
                primeiro_acesso BOOLEAN DEFAULT TRUE
            )
            """)

            cur.execute("""
            CREATE TABLE IF NOT EXISTS competicao_oficiais (
                id SERIAL PRIMARY KEY,
                competicao TEXT NOT NULL,
                cpf TEXT NOT NULL,
                funcao TEXT NOT NULL,
                criado_em TIMESTAMP DEFAULT NOW()
            )
            """)

            cur.execute("""
                ALTER TABLE apontadores_acesso
                ADD COLUMN IF NOT EXISTS primeiro_acesso BOOLEAN DEFAULT TRUE
            """)

        conn.commit()


# =========================================================
# OFICIAIS - BUSCA E CADASTRO
# =========================================================
def buscar_oficial_por_cpf(cpf):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM oficiais
                WHERE cpf = %s
                LIMIT 1
            """, (cpf,))
            return cur.fetchone()


def oficial_existe(cpf):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM oficiais
                WHERE cpf = %s
                LIMIT 1
            """, (cpf,))
            return cur.fetchone() is not None


def cadastrar_oficial(nome, cpf):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO oficiais (nome, cpf)
                VALUES (%s, %s)
                ON CONFLICT (cpf) DO NOTHING
            """, (nome, cpf))
        conn.commit()

    return True


# =========================================================
# APONTADOR - ACESSO
# =========================================================
def apontador_existe(cpf):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM apontadores_acesso
                WHERE cpf = %s
                LIMIT 1
            """, (cpf,))
            return cur.fetchone() is not None


def criar_apontador(cpf):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO apontadores_acesso (cpf, senha, ativo, primeiro_acesso)
                VALUES (%s, NULL, TRUE, TRUE)
                ON CONFLICT (cpf) DO NOTHING
            """, (cpf,))
        conn.commit()

    return True


def buscar_apontador(cpf):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.*, o.nome
                FROM apontadores_acesso a
                LEFT JOIN oficiais o ON o.cpf = a.cpf
                WHERE a.cpf = %s
                LIMIT 1
            """, (cpf,))
            return cur.fetchone()


def definir_senha_apontador(cpf, senha):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE apontadores_acesso
                SET senha = %s,
                    primeiro_acesso = FALSE
                WHERE cpf = %s
            """, (senha, cpf))
        conn.commit()

    return True


def atualizar_status_apontador(cpf, ativo):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE apontadores_acesso
                SET ativo = %s
                WHERE cpf = %s
            """, (ativo, cpf))
        conn.commit()

    return True


def autenticar_apontador(cpf, senha):
    apontador = buscar_apontador(cpf)

    if not apontador:
        return None

    if not apontador.get("ativo", True):
        return None

    senha_salva = apontador.get("senha")

    if not senha_salva:
        return apontador

    if senha_salva != senha:
        return False

    return apontador


# =========================================================
# VÍNCULO COM COMPETIÇÃO
# =========================================================
def vincular_oficial_competicao(competicao, cpf, funcao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM competicao_oficiais
                WHERE TRIM(LOWER(competicao)) = TRIM(LOWER(%s))
                  AND TRIM(cpf) = TRIM(%s)
                  AND TRIM(LOWER(funcao)) = TRIM(LOWER(%s))
                LIMIT 1
            """, (competicao, cpf, funcao))
            existente = cur.fetchone()

            if existente:
                return True

            cur.execute("""
                INSERT INTO competicao_oficiais (competicao, cpf, funcao)
                VALUES (%s, %s, %s)
            """, (competicao, cpf, funcao))
        conn.commit()

    return True


def listar_oficiais_competicao(competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    c.id,
                    c.competicao,
                    c.cpf,
                    c.funcao,
                    o.nome
                FROM competicao_oficiais c
                JOIN oficiais o ON o.cpf = c.cpf
                WHERE c.competicao = %s
                ORDER BY c.funcao, o.nome
            """, (competicao,))
            return cur.fetchall()


def excluir_oficial_competicao(id_vinculo):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM competicao_oficiais
                WHERE id = %s
            """, (id_vinculo,))
        conn.commit()

    return True


# =========================================================


def remover_apontador_da_competicao(cpf, competicao):
    """
    Organizador: remove o apontador apenas da competição atual.
    Não apaga o cadastro do oficial nem o acesso global do apontador.
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM competicao_oficiais
                WHERE TRIM(LOWER(competicao)) = TRIM(LOWER(%s))
                  AND REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') =
                      REGEXP_REPLACE(COALESCE(%s, ''), '\\D', '', 'g')
                  AND TRIM(LOWER(funcao)) = 'apontador'
            """, (competicao, cpf))
        conn.commit()

    return True


def excluir_apontador_global(cpf):
    """
    Superadmin: exclui o apontador do sistema inteiro.
    Mantém partidas, eventos e placares históricos intactos.
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM competicao_oficiais
                WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') =
                      REGEXP_REPLACE(COALESCE(%s, ''), '\\D', '', 'g')
                  AND TRIM(LOWER(funcao)) = 'apontador'
            """, (cpf,))

            cur.execute("""
                DELETE FROM apontadores_acesso
                WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') =
                      REGEXP_REPLACE(COALESCE(%s, ''), '\\D', '', 'g')
            """, (cpf,))

            cur.execute("""
                DELETE FROM oficiais o
                WHERE REGEXP_REPLACE(COALESCE(o.cpf, ''), '\\D', '', 'g') =
                      REGEXP_REPLACE(COALESCE(%s, ''), '\\D', '', 'g')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM competicao_oficiais c
                      WHERE REGEXP_REPLACE(COALESCE(c.cpf, ''), '\\D', '', 'g') =
                            REGEXP_REPLACE(COALESCE(o.cpf, ''), '\\D', '', 'g')
                  )
            """, (cpf,))
        conn.commit()

    return True
# APONTADOR - COMPETIÇÕES ATIVAS
# =========================================================
# =========================================================
# APONTADOR - COMPETIÇÕES ATIVAS
# =========================================================
def listar_competicoes_apontador(cpf):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT
                    c.competicao,
                    comp.data,
                    comp.status
                FROM competicao_oficiais c
                LEFT JOIN competicoes comp
                    ON TRIM(LOWER(comp.nome)) = TRIM(LOWER(c.competicao))
                WHERE REGEXP_REPLACE(COALESCE(c.cpf, ''), '\\D', '', 'g')
                      = REGEXP_REPLACE(COALESCE(%s, ''), '\\D', '', 'g')
                  AND TRIM(LOWER(c.funcao)) = 'apontador'
                ORDER BY c.competicao
            """, (cpf,))
            return cur.fetchall()
            

# =========================================================
# CONFIGURAÇÃO AVANÇADA DA COMPETIÇÃO
# =========================================================
def buscar_configuracao_avancada_competicao(nome_competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    nome,
                    tipo_classificacao,
                    qtd_classificados,
                    formato_finais,
                    possui_bye,
                    qtd_bye,
                    fases_config,
                    tipo_confronto,
                    cruzamentos_grupos,
                    data_limite_inscricao,
                    hora_limite_inscricao,
                    bloquear_apos_inicio
                FROM competicoes
                WHERE nome = %s
                LIMIT 1
            """, (nome_competicao,))
            row = cur.fetchone()

    if not row:
        return None

    fases_config = row.get("fases_config")
    if isinstance(fases_config, str):
        try:
            fases_config = json.loads(fases_config)
        except Exception:
            fases_config = {}

    row["fases_config"] = fases_config or {}
    return row


def atualizar_configuracao_avancada_competicao(
    nome_competicao,
    tipo_classificacao,
    qtd_classificados,
    formato_finais,
    possui_bye,
    qtd_bye,
    fases_config,
    tipo_confronto="grupo_interno",
    cruzamentos_grupos="",
    data_limite_inscricao=None,
    hora_limite_inscricao=None,
    bloquear_apos_inicio=False,
):
    ok_edicao, _ = validar_competicao_editavel(nome_competicao, "alteração de formato")
    if not ok_edicao:
        return False

    if not isinstance(fases_config, str):
        fases_config = json.dumps(fases_config or {}, ensure_ascii=False)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE competicoes
                SET
                    tipo_classificacao = %s,
                    qtd_classificados = %s,
                    formato_finais = %s,
                    possui_bye = %s,
                    qtd_bye = %s,
                    fases_config = %s::jsonb,
                    tipo_confronto = %s,
                    cruzamentos_grupos = %s,
                    data_limite_inscricao = %s,
                    hora_limite_inscricao = %s,
                    bloquear_apos_inicio = %s
                WHERE nome = %s
            """, (
                tipo_classificacao,
                qtd_classificados,
                formato_finais,
                possui_bye,
                qtd_bye,
                fases_config,
                tipo_confronto,
                cruzamentos_grupos,
                data_limite_inscricao,
                hora_limite_inscricao,
                bloquear_apos_inicio,
                nome_competicao,
            ))
        conn.commit()

    return True


def inicializar_configuracao_avancada_competicao(nome_competicao):
    config = buscar_configuracao_avancada_competicao(nome_competicao)
    if not config:
        return False

    fases_config = config.get("fases_config") or {}
    if fases_config:
        return True

    fases_padrao = {
        "tipo_confronto": config.get("tipo_confronto") or "grupo_interno",
        "tipo_classificacao": config.get("tipo_classificacao") or "grupo",
        "cruzamentos_grupos": config.get("cruzamentos_grupos") or "",
        "grupos": {
            "tipo_jogo": "set_unico",
            "pontos": 25,
            "tem_tiebreak": False,
            "pontos_tiebreak": 15
        },
        "grupos_especificos": {
            "A": {"tipo_jogo": "", "pontos": ""},
            "B": {"tipo_jogo": "", "pontos": ""},
            "C": {"tipo_jogo": "", "pontos": ""},
            "D": {"tipo_jogo": "", "pontos": ""},
        },
        "quartas": {
            "tipo_jogo": "melhor_de_3",
            "pontos": 21,
            "tem_tiebreak": True,
            "pontos_tiebreak": 15
        },
        "semifinal": {
            "tipo_jogo": "melhor_de_3",
            "pontos": 21,
            "tem_tiebreak": True,
            "pontos_tiebreak": 15
        },
        "final": {
            "tipo_jogo": "melhor_de_3",
            "pontos": 25,
            "tem_tiebreak": True,
            "pontos_tiebreak": 15
        }
    }

    return atualizar_configuracao_avancada_competicao(
        nome_competicao=nome_competicao,
        tipo_classificacao=config.get("tipo_classificacao") or "grupo",
        qtd_classificados=config.get("qtd_classificados") or 0,
        formato_finais=config.get("formato_finais") or "mata_mata",
        possui_bye=config.get("possui_bye") or False,
        qtd_bye=config.get("qtd_bye") or 0,
        fases_config=fases_padrao,
        tipo_confronto=config.get("tipo_confronto") or "grupo_interno",
        cruzamentos_grupos=config.get("cruzamentos_grupos") or "",
        data_limite_inscricao=config.get("data_limite_inscricao"),
        hora_limite_inscricao=config.get("hora_limite_inscricao"),
        bloquear_apos_inicio=config.get("bloquear_apos_inicio") or False,
    )


# =========================================================
# ATLETAS - NUMERAÇÃO E PRAZO
# =========================================================
def numero_atleta_disponivel(numero, equipe, competicao, id_atleta=None, atleta_id=None):
    if id_atleta is None and atleta_id is not None:
        id_atleta = atleta_id

    if numero in (None, ""):
        return True

    try:
        numero = int(numero)
    except (TypeError, ValueError):
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            if id_atleta is not None:
                cur.execute("""
                    SELECT id
                    FROM atletas
                    WHERE equipe = %s
                      AND competicao = %s
                      AND numero = %s
                      AND id <> %s
                    LIMIT 1
                """, (equipe, competicao, numero, id_atleta))
            else:
                cur.execute("""
                    SELECT id
                    FROM atletas
                    WHERE equipe = %s
                      AND competicao = %s
                      AND numero = %s
                    LIMIT 1
                """, (equipe, competicao, numero))
            return cur.fetchone() is None


def listar_atletas_aprovados_da_equipe(equipe, competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM atletas
                WHERE equipe = %s
                  AND competicao = %s
                  AND status = 'aprovado'
                ORDER BY nome
            """, (equipe, competicao))
            return cur.fetchall()


def buscar_capitao_padrao_equipe(equipe, competicao, conn=None):
    """Retorna o capitão padrão já salvo para a equipe na competição."""
    if not equipe or not competicao:
        return None

    def _executar(c):
        with c.cursor() as cur:
            cur.execute("ALTER TABLE atletas ADD COLUMN IF NOT EXISTS capitao_padrao BOOLEAN DEFAULT FALSE")
            cur.execute("""
                SELECT *
                FROM atletas
                WHERE equipe = %s
                  AND competicao = %s
                  AND status = 'aprovado'
                  AND COALESCE(capitao_padrao, FALSE) = TRUE
                ORDER BY id DESC
                LIMIT 1
            """, (equipe, competicao))
            return cur.fetchone()

    if conn is not None:
        return _executar(conn)

    with conectar() as conn:
        return _executar(conn)


def _aplicar_capitao_em_partida(cur, lado, atleta, partida_id, competicao):
    if not atleta:
        return

    numero = atleta.get("numero")
    if numero in (None, ""):
        return

    campo_id = "capitao_a_id" if lado == "A" else "capitao_b_id"
    campo_nome = "capitao_a_nome" if lado == "A" else "capitao_b_nome"
    campo_numero = "capitao_a_numero" if lado == "A" else "capitao_b_numero"

    cur.execute(f"""
        UPDATE partidas
        SET {campo_id} = %s,
            {campo_nome} = %s,
            {campo_numero} = %s
        WHERE id = %s
          AND competicao = %s
          AND ({campo_id} IS NULL OR {campo_id} = 0)
    """, (atleta.get("id"), atleta.get("nome"), numero, partida_id, competicao))


def aplicar_capitaes_padrao_partida(partida_id, competicao):
    """
    Preenche automaticamente capitães da partida usando o capitão padrão
    da equipe, sem sobrescrever capitão escolhido manualmente na partida.
    """
    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return partida

    equipe_a = partida.get("equipe_a_operacional")
    equipe_b = partida.get("equipe_b_operacional")

    if not equipe_a and not equipe_b:
        return partida

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE atletas ADD COLUMN IF NOT EXISTS capitao_padrao BOOLEAN DEFAULT FALSE")

            cap_a = buscar_capitao_padrao_equipe(equipe_a, competicao, conn=conn) if equipe_a else None
            cap_b = buscar_capitao_padrao_equipe(equipe_b, competicao, conn=conn) if equipe_b else None

            if not partida.get("capitao_a_id"):
                _aplicar_capitao_em_partida(cur, "A", cap_a, partida_id, competicao)

            if not partida.get("capitao_b_id"):
                _aplicar_capitao_em_partida(cur, "B", cap_b, partida_id, competicao)

        conn.commit()

    return buscar_partida_operacional(partida_id, competicao)


def atualizar_numero_atleta(id_atleta, numero):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, equipe, competicao, status
                FROM atletas
                WHERE id = %s
                LIMIT 1
            """, (id_atleta,))
            atleta = cur.fetchone()

            if not atleta or atleta.get("status") != "aprovado":
                return False, "Somente atletas aprovados podem receber numeração."

            ok_edicao, mensagem = validar_edicao_atletas_equipe(atleta["competicao"], atleta["equipe"])
            if not ok_edicao:
                return False, mensagem

            if numero not in (None, ""):
                try:
                    numero = int(numero)
                except ValueError:
                    return False, "Número inválido."

                if not numero_atleta_disponivel(numero, atleta["equipe"], atleta["competicao"], id_atleta=id_atleta):
                    return False, "Já existe outro atleta com essa numeração nesta equipe."
            else:
                numero = None

            cur.execute("""
                UPDATE atletas
                SET numero = %s
                WHERE id = %s
            """, (numero, id_atleta))

        conn.commit()

    return True, "Numeração atualizada com sucesso."


def competicao_tem_partida_iniciada(nome_competicao):
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id
                    FROM partidas
                    WHERE competicao = %s
                      AND status IN ('em_andamento', 'finalizada', 'encerrada')
                    LIMIT 1
                """, (nome_competicao,))
                return cur.fetchone() is not None
    except Exception:
        return False


def inscricao_aberta_competicao(nome_competicao):
    config = buscar_configuracao_avancada_competicao(nome_competicao)
    if not config:
        return True

    if config.get("bloquear_apos_inicio") and competicao_tem_partida_iniciada(nome_competicao):
        return False

    data_limite = config.get("data_limite_inscricao")
    hora_limite = config.get("hora_limite_inscricao")

    if not data_limite:
        return True

    try:
        data_str = str(data_limite)
        hora_str = str(hora_limite or "23:59")
        limite = datetime.fromisoformat(f"{data_str} {hora_str}")
    except Exception:
        return True

    return datetime.now() <= limite


def contar_atletas_da_equipe(equipe, competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total
                FROM atletas
                WHERE equipe = %s
                  AND competicao = %s
            """, (equipe, competicao))
            row = cur.fetchone()
            return row["total"] if row else 0


def buscar_competicao_por_nome(nome_competicao):
    campos = _campos_competicao()
    sql = f"""
        SELECT {', '.join(campos)}
        FROM competicoes
        WHERE nome = %s
        LIMIT 1
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (nome_competicao,))
            return cur.fetchone()


# =========================================================
# APONTADOR - RESOLUÇÃO DE CPF E PRÉ-JOGO
# =========================================================
def buscar_cpf_oficial_por_login(login):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT o.cpf, o.nome
                FROM usuarios u
                JOIN oficiais o
                  ON TRIM(LOWER(o.nome)) = TRIM(LOWER(u.nome))
                WHERE u.login = %s
                LIMIT 1
            """, (login,))
            return cur.fetchone()


def listar_arbitros_competicao(competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    c.id,
                    c.competicao,
                    c.cpf,
                    c.funcao,
                    o.nome
                FROM competicao_oficiais c
                JOIN oficiais o ON o.cpf = c.cpf
                WHERE c.competicao = %s
                  AND (
                        LOWER(c.funcao) LIKE '%%arbitro%%'
                        OR LOWER(c.funcao) LIKE '%%árbitro%%'
                  )
                ORDER BY o.nome
            """, (competicao,))
            return cur.fetchall()


def buscar_partida_operacional(partida_id, competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))
            return cur.fetchone()


def assumir_partida_operacional(partida_id, competicao, operador_login, operador_nome):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT operador_login, status_operacao
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))
            atual = cur.fetchone()

            if not atual:
                return False, "Partida não encontrada."

            dono = atual.get("operador_login")
            status_operacao = (atual.get("status_operacao") or "livre").lower()

            if dono and dono != operador_login and status_operacao in {"reservado", "pre_jogo", "em_andamento"}:
                return False, "Esta partida já está em operação por outro apontador."

            cur.execute("""
                UPDATE partidas
                SET operador_login = %s,
                    operador_nome = %s,
                    apontador_login = %s,
                    apontador_nome = %s,
                    status_operacao = 'reservado',
                    reservado_em = NOW()
                WHERE id = %s
                  AND competicao = %s
            """, (operador_login, operador_nome, operador_login, operador_nome, partida_id, competicao))
        conn.commit()

    return True, "Partida assumida com sucesso."


def abandonar_partida_operacional(partida_id, competicao, operador_login):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status_operacao, operador_login
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))
            atual = cur.fetchone()

            if not atual:
                return False, "Partida não encontrada."

            if atual.get("operador_login") != operador_login:
                return False, "Você não é o operador desta partida."

            if (atual.get("status_operacao") or "livre").lower() != "reservado":
                return False, "A partida já iniciou o pré-jogo e não pode mais ser abandonada dessa forma."

            cur.execute("""
                UPDATE partidas
                SET operador_login = NULL,
                    operador_nome = NULL,
                    status_operacao = 'livre',
                    reservado_em = NULL,
                    apontador_login = NULL,
                    apontador_nome = NULL
                WHERE id = %s
                  AND competicao = %s
            """, (partida_id, competicao))
        conn.commit()

    return True, "Partida abandonada com sucesso."


def salvar_pre_jogo_partida(
    partida_id,
    competicao,
    operador_login,
    arbitro_1_cpf,
    arbitro_2_cpf,
    sorteio_vencedor,
    sorteio_escolha,
    saque_inicial,
    lado_esquerdo,
):
    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return False, "Partida não encontrada."

    if partida.get("operador_login") != operador_login:
        return False, "Esta partida não está sob sua operação."

    fase_atual = (partida.get("fase_partida") or "pre_jogo").strip().lower()
    if fase_atual not in {"pre_jogo", "", "reservado"}:
        return False, "O pré-jogo inicial já foi finalizado e não pode mais ser alterado."

    equipe_a_cadastro = partida.get("equipe_a")
    equipe_b_cadastro = partida.get("equipe_b")

    if lado_esquerdo == equipe_a_cadastro:
        equipe_a_operacional = equipe_a_cadastro
        equipe_b_operacional = equipe_b_cadastro
    elif lado_esquerdo == equipe_b_cadastro:
        equipe_a_operacional = equipe_b_cadastro
        equipe_b_operacional = equipe_a_cadastro
    else:
        return False, "Equipe do lado esquerdo inválida."

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nome FROM oficiais WHERE cpf = %s LIMIT 1", (arbitro_1_cpf,))
            a1 = cur.fetchone()

            cur.execute("SELECT nome FROM oficiais WHERE cpf = %s LIMIT 1", (arbitro_2_cpf,))
            a2 = cur.fetchone()

            if not a1 or not a2:
                return False, "Árbitros inválidos."

            cur.execute("""
                UPDATE partidas
                SET arbitro_1_cpf = %s,
                    arbitro_1_nome = %s,
                    arbitro_2_cpf = %s,
                    arbitro_2_nome = %s,
                    sorteio_vencedor = %s,
                    sorteio_escolha = %s,
                    saque_inicial = %s,
                    saque_atual = %s,
                    lado_esquerdo = %s,
                    equipe_a_operacional = %s,
                    equipe_b_operacional = %s,
                    status_operacao = 'pre_jogo',
                    status = 'pre_jogo',
                    fase_partida = 'papeleta',
                    pre_jogo_finalizado = TRUE,
                    pre_jogo_iniciado_em = COALESCE(pre_jogo_iniciado_em, NOW()),
                    pre_jogo_finalizado_em = NOW()
                WHERE id = %s
                  AND competicao = %s
            """, (
                arbitro_1_cpf,
                a1["nome"],
                arbitro_2_cpf,
                a2["nome"],
                sorteio_vencedor,
                sorteio_escolha,
                saque_inicial,
                saque_inicial,
                lado_esquerdo,
                equipe_a_operacional,
                equipe_b_operacional,
                partida_id,
                competicao,
            ))

        conn.commit()

    aplicar_capitaes_padrao_partida(partida_id, competicao)

    return True, "Pré-jogo salvo com sucesso."


def salvar_sorteio_tiebreak_partida(
    partida_id,
    competicao,
    operador_login,
    sorteio_vencedor,
    sorteio_escolha,
    saque_tiebreak,
    lado_esquerdo_tiebreak,
):
    criar_campos_sets_partida()

    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return False, "Partida não encontrada."

    if partida.get("operador_login") != operador_login:
        return False, "Esta partida não está sob sua operação."

    fluxo = resumir_fluxo_oficial_partida(partida_id, competicao, partida=partida) or {}
    if fluxo.get("fase_partida") != "tiebreak_sorteio":
        return False, "O sorteio do tie-break não está liberado neste momento."

    equipe_a_cadastro = partida.get("equipe_a")
    equipe_b_cadastro = partida.get("equipe_b")

    if lado_esquerdo_tiebreak not in {equipe_a_cadastro, equipe_b_cadastro}:
        return False, "Equipe do lado esquerdo do tie-break inválida."

    if sorteio_vencedor not in {equipe_a_cadastro, equipe_b_cadastro}:
        return False, "Equipe vencedora do sorteio do tie-break inválida."

    if saque_tiebreak not in {equipe_a_cadastro, equipe_b_cadastro, "A", "B"}:
        return False, "Equipe do saque inicial do tie-break inválida."

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE partidas
                SET sorteio_tiebreak_vencedor = %s,
                    sorteio_tiebreak_escolha = %s,
                    saque_tiebreak = %s,
                    lado_esquerdo_tiebreak = %s,
                    tiebreak_pendente = FALSE,
                    tiebreak_definido = TRUE,
                    fase_partida = 'papeleta',
                    status_jogo = 'entre_sets',
                    status_operacao = 'pre_jogo'
                WHERE id = %s
                  AND competicao = %s
            """, (
                sorteio_vencedor,
                sorteio_escolha,
                saque_tiebreak,
                lado_esquerdo_tiebreak,
                partida_id,
                competicao,
            ))
        conn.commit()

    return True, "Sorteio do tie-break salvo com sucesso."


# =========================================================
# CONFERÊNCIA DE EQUIPES (PRÉ-JOGO)
# =========================================================
def criar_tabela_conferencia_equipes():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS equipe_conferencia (
                    id SERIAL PRIMARY KEY,
                    competicao TEXT NOT NULL,
                    equipe TEXT NOT NULL,
                    conferido BOOLEAN DEFAULT TRUE,
                    atualizado_em TIMESTAMP DEFAULT NOW(),
                    UNIQUE(competicao, equipe)
                )
            """)
        conn.commit()


def equipe_ja_conferida(competicao, equipe):
    criar_tabela_conferencia_equipes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT conferido
                FROM equipe_conferencia
                WHERE competicao = %s
                  AND equipe = %s
                LIMIT 1
            """, (competicao, equipe))
            row = cur.fetchone()
            return bool(row and row.get("conferido"))


def marcar_equipe_conferida(competicao, equipe):
    criar_tabela_conferencia_equipes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO equipe_conferencia (competicao, equipe, conferido)
                VALUES (%s, %s, TRUE)
                ON CONFLICT (competicao, equipe)
                DO UPDATE SET conferido = TRUE, atualizado_em = NOW()
            """, (competicao, equipe))
        conn.commit()


# =========================================================
# PAPELETA
# =========================================================
def criar_tabela_papeleta():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS papeletas (
                    id SERIAL PRIMARY KEY,
                    partida_id INTEGER,
                    competicao TEXT,
                    equipe TEXT,
                    set_numero INTEGER DEFAULT 1,
                    posicao INTEGER,
                    atleta_id INTEGER,
                    numero INTEGER,
                    nome TEXT
                )
            """)

            cur.execute("""
                ALTER TABLE papeletas
                ADD COLUMN IF NOT EXISTS set_numero INTEGER DEFAULT 1
            """)

        conn.commit()


def salvar_papeleta(partida_id, competicao, equipe, set_numero, dados):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM papeletas
                WHERE partida_id = %s
                  AND competicao = %s
                  AND equipe = %s
                  AND set_numero = %s
            """, (partida_id, competicao, equipe, set_numero))

            registros = []

            for posicao, atleta in dados.items():
                registros.append((
                    partida_id,
                    competicao,
                    equipe,
                    set_numero,
                    posicao,
                    atleta.get("id"),
                    atleta.get("numero"),
                    atleta.get("nome")
                ))

            if registros:
                cur.executemany("""
                    INSERT INTO papeletas (
                        partida_id, competicao, equipe, set_numero,
                        posicao, atleta_id, numero, nome
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, registros)

        conn.commit()


def listar_papeleta(partida_id, competicao, equipe, set_numero):
    criar_tabela_papeleta()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM papeletas
                WHERE partida_id = %s
                  AND competicao = %s
                  AND equipe = %s
                  AND set_numero = %s
                ORDER BY posicao
            """, (partida_id, competicao, equipe, set_numero))
            return cur.fetchall()


# =========================================================
# SETS DA PARTIDA
# =========================================================
def criar_campos_sets_partida(force=False):
    if _schema_ja_pronto("campos_sets_partida", force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:

            # =============================
            # CONTROLE DE SETS
            # =============================
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set_atual INTEGER DEFAULT 1")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS sets_a INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS sets_b INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS sets_max INTEGER DEFAULT 3")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS sets_para_vencer INTEGER DEFAULT 2")

            # =============================
            # CONTROLE DE FASE (CRÍTICO)
            # =============================
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS fase_partida TEXT DEFAULT 'pre_jogo'")

            # 🔥 GARANTE CONSISTÊNCIA NAS ANTIGAS
            cur.execute("""
                UPDATE partidas
                SET fase_partida = 'pre_jogo'
                WHERE fase_partida IS NULL
            """)

            # =============================
            # PRÉ-JOGO (ESSENCIAL PRO TEU FLUXO)
            # =============================
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS pre_jogo_finalizado BOOLEAN DEFAULT FALSE")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS pre_jogo_iniciado_em TIMESTAMP")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS pre_jogo_finalizado_em TIMESTAMP")

            # =============================
            # TIEBREAK
            # =============================
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS tiebreak_pendente BOOLEAN DEFAULT FALSE")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS tiebreak_definido BOOLEAN DEFAULT FALSE")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS sorteio_tiebreak_vencedor TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS sorteio_tiebreak_escolha TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS saque_tiebreak TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS lado_esquerdo_tiebreak TEXT")

            # =============================
            # FINALIZAÇÃO / WO
            # =============================
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS observacoes TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS data_fim TIMESTAMP")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS tipo_encerramento TEXT")

        conn.commit()

    _marcar_schema_pronto("campos_sets_partida")


def _normalizar_formato_sets(formato):
    formato = (formato or 'melhor_de_3').strip().lower()
    if formato in {'set_unico', 'melhor_de_3', 'melhor_de_5'}:
        return formato
    return 'melhor_de_3'


def calcular_sets_max(formato):
    formato = _normalizar_formato_sets(formato)
    if formato == 'set_unico':
        return 1
    if formato == 'melhor_de_5':
        return 5
    return 3


def calcular_sets_para_vencer(formato):
    formato = _normalizar_formato_sets(formato)
    if formato == 'melhor_de_5':
        return 3
    if formato == 'melhor_de_3':
        return 2
    return 1


def set_eh_tiebreak(formato, set_numero):
    formato = _normalizar_formato_sets(formato)
    try:
        set_numero = int(set_numero or 1)
    except (TypeError, ValueError):
        set_numero = 1

    if formato == 'melhor_de_3':
        return set_numero == 3
    if formato == 'melhor_de_5':
        return set_numero == 5
    return False


def set_deve_inverter_lados(formato, set_numero):
    formato = _normalizar_formato_sets(formato)
    try:
        set_numero = int(set_numero or 1)
    except (TypeError, ValueError):
        set_numero = 1

    if formato == 'set_unico':
        return False

    return set_numero % 2 == 0


def papeleta_set_esta_completa(partida_id, competicao, equipe, set_numero):
    if not equipe:
        return False
    try:
        rows = listar_papeleta(partida_id, competicao, equipe, int(set_numero or 1)) or []
    except Exception:
        return False
    return len(rows) == 6


def _inferir_fase_partida(partida, formato=None):
    if not partida:
        return 'pre_jogo'

    formato = _normalizar_formato_sets(formato)
    status_partida = (partida.get('status') or '').strip().lower()
    status_jogo = (partida.get('status_jogo') or '').strip().lower()
    status_operacao = (partida.get('status_operacao') or '').strip().lower()

    if status_partida == 'finalizada' or status_jogo == 'finalizada' or status_operacao == 'finalizada':
        return 'encerrado'

    if status_jogo == 'em_andamento':
        return 'jogo'

    if status_jogo == 'tiebreak_sorteio' or status_operacao == 'tiebreak_sorteio':
        return 'tiebreak_sorteio'

    if status_jogo == 'entre_sets':
        return 'intervalo_set'

    if not (partida.get('equipe_a_operacional') and partida.get('equipe_b_operacional')):
        return 'pre_jogo'

    set_atual = int(partida.get('set_atual') or 1)
    papeleta_a_ok = papeleta_set_esta_completa(partida.get('id'), partida.get('competicao'), partida.get('equipe_a_operacional'), set_atual)
    papeleta_b_ok = papeleta_set_esta_completa(partida.get('id'), partida.get('competicao'), partida.get('equipe_b_operacional'), set_atual)

    if papeleta_a_ok and papeleta_b_ok:
        return 'papeleta_pronta'

    return 'papeleta'


def resumir_fluxo_oficial_partida(partida_id, competicao, partida=None):
    if not partida:
        partida = buscar_partida_operacional(partida_id, competicao)

    if not partida:
        return None

    comp = buscar_competicao_por_nome(competicao) or {}
    formato = _normalizar_formato_sets(comp.get('sets_tipo'))
    sets_max = calcular_sets_max(formato)
    sets_para_vencer = calcular_sets_para_vencer(formato)
    fase_partida = _inferir_fase_partida(partida, formato=formato)
    set_atual = int(partida.get('set_atual') or 1)
    papeleta_a_ok = papeleta_set_esta_completa(partida_id, competicao, partida.get('equipe_a_operacional'), set_atual)
    papeleta_b_ok = papeleta_set_esta_completa(partida_id, competicao, partida.get('equipe_b_operacional'), set_atual)

    if fase_partida == 'encerrado':
        proxima_etapa = 'encerrado'
    elif fase_partida == 'pre_jogo':
        proxima_etapa = 'pre_jogo'
    elif fase_partida == 'tiebreak_sorteio':
        proxima_etapa = 'tiebreak_sorteio'
    elif fase_partida in {'papeleta', 'intervalo_set'}:
        proxima_etapa = 'papeleta'
    elif fase_partida == 'papeleta_pronta':
        proxima_etapa = 'jogo'
    else:
        proxima_etapa = 'jogo'

    return {
        'formato': formato,
        'sets_max': sets_max,
        'sets_para_vencer': sets_para_vencer,
        'fase_partida': fase_partida,
        'proxima_etapa': proxima_etapa,
        'set_atual': set_atual,
        'set_deve_inverter_lados': set_deve_inverter_lados(formato, set_atual),
        'set_eh_tiebreak': set_eh_tiebreak(formato, set_atual),
        'tiebreak_pendente': bool(partida.get('tiebreak_pendente')),
        'tiebreak_definido': bool(partida.get('tiebreak_definido')),
        'papeleta_a_completa': papeleta_a_ok,
        'papeleta_b_completa': papeleta_b_ok,
    }


def inicializar_sets_partida(partida_id, competicao):
    criar_campos_sets_partida()

    comp = buscar_competicao_por_nome(competicao) or {}
    formato = _normalizar_formato_sets(comp.get("sets_tipo"))
    sets_max = calcular_sets_max(formato)
    sets_para_vencer = calcular_sets_para_vencer(formato)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE partidas
                SET set_atual = COALESCE(set_atual, 1),
                    sets_a = COALESCE(sets_a, 0),
                    sets_b = COALESCE(sets_b, 0),
                    sets_max = COALESCE(sets_max, %s),
                    sets_para_vencer = COALESCE(sets_para_vencer, %s),
                    fase_partida = COALESCE(fase_partida, 'pre_jogo')
                WHERE id = %s
                  AND competicao = %s
            """, (sets_max, sets_para_vencer, partida_id, competicao))
        conn.commit()

    partida = buscar_partida_operacional(partida_id, competicao)
    fluxo = resumir_fluxo_oficial_partida(partida_id, competicao, partida=partida)
    if fluxo:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE partidas
                    SET fase_partida = %s,
                        sets_max = %s,
                        sets_para_vencer = %s
                    WHERE id = %s
                      AND competicao = %s
                """, (
                    fluxo["fase_partida"],
                    fluxo["sets_max"],
                    fluxo["sets_para_vencer"],
                    partida_id,
                    competicao,
                ))
            conn.commit()


def registrar_resultado_set(partida_id, competicao, vencedor):
    criar_campos_sets_partida()
    criar_campos_jogo_partida()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))
            partida = cur.fetchone()

            if not partida:
                return False, "Partida não encontrada."

            comp = buscar_competicao_por_nome(competicao) or {}
            formato = _normalizar_formato_sets(comp.get("sets_tipo"))
            sets_max = calcular_sets_max(formato)
            sets_para_vencer = calcular_sets_para_vencer(formato)

            sets_a = int(partida.get("sets_a") or 0)
            sets_b = int(partida.get("sets_b") or 0)
            set_atual = int(partida.get("set_atual") or 1)

            if vencedor == "A":
                sets_a += 1
            elif vencedor == "B":
                sets_b += 1
            else:
                return False, "Vencedor inválido."

            acabou = sets_a >= sets_para_vencer or sets_b >= sets_para_vencer or set_atual >= sets_max

            if acabou:
                cur.execute("""
                    UPDATE partidas
                    SET sets_a = %s,
                        sets_b = %s,
                        sets_max = %s,
                        sets_para_vencer = %s,
                        fase_partida = 'encerrado',
                        status = 'finalizada',
                        status_jogo = 'finalizada',
                        status_operacao = 'finalizada',
                        tiebreak_pendente = FALSE
                    WHERE id = %s
                      AND competicao = %s
                """, (sets_a, sets_b, sets_max, sets_para_vencer, partida_id, competicao))
            else:
                proximo_set = set_atual + 1
                precisa_tiebreak = set_eh_tiebreak(formato, proximo_set)

                if precisa_tiebreak:
                    cur.execute("""
                        UPDATE partidas
                        SET sets_a = %s,
                            sets_b = %s,
                            set_atual = %s,
                            pontos_a = 0,
                            pontos_b = 0,
                            saque_atual = NULL,
                            sets_max = %s,
                            sets_para_vencer = %s,
                            fase_partida = 'tiebreak_sorteio',
                            status_jogo = 'tiebreak_sorteio',
                            status_operacao = 'tiebreak_sorteio',
                            tiebreak_pendente = TRUE,
                            tiebreak_definido = FALSE,
                            sorteio_tiebreak_vencedor = NULL,
                            sorteio_tiebreak_escolha = NULL,
                            saque_tiebreak = NULL,
                            lado_esquerdo_tiebreak = NULL
                        WHERE id = %s
                          AND competicao = %s
                    """, (sets_a, sets_b, proximo_set, sets_max, sets_para_vencer, partida_id, competicao))
                else:
                    cur.execute("""
                        UPDATE partidas
                        SET sets_a = %s,
                            sets_b = %s,
                            set_atual = %s,
                            pontos_a = 0,
                            pontos_b = 0,
                            saque_atual = NULL,
                            sets_max = %s,
                            sets_para_vencer = %s,
                            fase_partida = 'intervalo_set',
                            status_jogo = 'entre_sets',
                            tiebreak_pendente = FALSE
                        WHERE id = %s
                          AND competicao = %s
                    """, (sets_a, sets_b, proximo_set, sets_max, sets_para_vencer, partida_id, competicao))

        conn.commit()

    partida_atualizada = buscar_partida_operacional(partida_id, competicao)
    fluxo = resumir_fluxo_oficial_partida(partida_id, competicao, partida=partida_atualizada)
    if fluxo:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE partidas
                    SET fase_partida = %s,
                        sets_max = %s,
                        sets_para_vencer = %s
                    WHERE id = %s
                      AND competicao = %s
                """, (
                    fluxo["fase_partida"],
                    fluxo["sets_max"],
                    fluxo["sets_para_vencer"],
                    partida_id,
                    competicao,
                ))
            conn.commit()

    if fluxo and fluxo["fase_partida"] == "encerrado":
        return True, "Partida finalizada com sucesso."
    return True, "Set atualizado com sucesso."

# =========================================================
# EVENTOS DA PARTIDA (AO VIVO)
# =========================================================
# =========================================================
# CAPITÃO NO PRÉ-JOGO
# =========================================================
def salvar_capitao_partida(partida_id, competicao, operador_login, lado, atleta_id):
    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return False, "Partida não encontrada."

    if partida.get("operador_login") != operador_login:
        return False, "Somente o operador da partida pode definir o capitão."

    lado = (lado or "").strip().upper()
    if lado not in {"A", "B"}:
        return False, "Lado inválido para capitão."

    equipe = partida.get("equipe_a_operacional") if lado == "A" else partida.get("equipe_b_operacional")
    if not equipe:
        return False, "Equipe operacional ainda não definida."

    atletas = listar_atletas_aprovados_da_equipe(equipe, competicao)
    atleta = next((a for a in atletas if str(a.get("id")) == str(atleta_id)), None)

    if not atleta:
        return False, "Atleta inválido para esta equipe."

    numero = atleta.get("numero")
    if numero in (None, ""):
        return False, "Só é possível definir como capitão um atleta já numerado."

    campo_id = "capitao_a_id" if lado == "A" else "capitao_b_id"
    campo_nome = "capitao_a_nome" if lado == "A" else "capitao_b_nome"
    campo_numero = "capitao_a_numero" if lado == "A" else "capitao_b_numero"

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE atletas ADD COLUMN IF NOT EXISTS capitao_padrao BOOLEAN DEFAULT FALSE")

            # Mantém apenas um capitão padrão por equipe/competição.
            cur.execute("""
                UPDATE atletas
                SET capitao_padrao = FALSE
                WHERE equipe = %s
                  AND competicao = %s
            """, (equipe, competicao))

            cur.execute("""
                UPDATE atletas
                SET capitao_padrao = TRUE
                WHERE id = %s
            """, (atleta.get("id"),))

            cur.execute(f"""
                UPDATE partidas
                SET {campo_id} = %s,
                    {campo_nome} = %s,
                    {campo_numero} = %s
                WHERE id = %s
                  AND competicao = %s
            """, (atleta.get("id"), atleta.get("nome"), numero, partida_id, competicao))
        conn.commit()

    return True, "Capitão definido com sucesso e salvo como padrão da equipe."


# =========================================================
# JOGO AO VIVO - ETAPA 1
# =========================================================
def criar_campos_jogo_partida(force=False):
    if _schema_ja_pronto("campos_jogo_partida", force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS pontos_a INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS pontos_b INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS saque_atual TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS status_jogo TEXT DEFAULT 'pre_jogo'")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS rotacao_a_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS rotacao_b_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS status_jogadores_a_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS status_jogadores_b_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS subs_a INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS subs_b INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS titulares_iniciais_a_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS titulares_iniciais_b_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS vinculos_titular_reserva_a_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS vinculos_titular_reserva_b_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS vinculos_reserva_titular_a_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS vinculos_reserva_titular_b_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS sancoes_a_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS sancoes_b_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS cartoes_verdes_a_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS cartoes_verdes_b_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS bloqueios_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS substituicao_forcada_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS retardamentos_a_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS retardamentos_b_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS subs_excepcionais_json TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS pre_jogo_finalizado BOOLEAN DEFAULT FALSE")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS pre_jogo_finalizado_em TIMESTAMP")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS fase_partida TEXT DEFAULT 'pre_jogo'")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS pre_jogo_finalizado BOOLEAN DEFAULT FALSE")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS pre_jogo_finalizado_em TIMESTAMP")
        conn.commit()

    _marcar_schema_pronto("campos_jogo_partida")

    
def criar_tabela_sancoes_partida():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sancoes_partida (
                    id SERIAL PRIMARY KEY,
                    partida_id INTEGER NOT NULL,
                    competicao TEXT NOT NULL,
                    equipe TEXT NOT NULL,
                    tipo_pessoa TEXT,
                    numero TEXT,
                    nome TEXT,
                    tipo TEXT NOT NULL,
                    escopo TEXT,
                    set_aplicado INTEGER DEFAULT 1,
                    observacao TEXT,
                    criado_em TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                ALTER TABLE sancoes_partida
                ADD COLUMN IF NOT EXISTS tipo_pessoa TEXT
            """)
            cur.execute("""
                ALTER TABLE sancoes_partida
                ADD COLUMN IF NOT EXISTS numero TEXT
            """)
            cur.execute("""
                ALTER TABLE sancoes_partida
                ADD COLUMN IF NOT EXISTS nome TEXT
            """)
            cur.execute("""
                ALTER TABLE sancoes_partida
                ADD COLUMN IF NOT EXISTS observacao TEXT
            """)
        conn.commit()


def _tipo_progressivo_sancao(partida_id, competicao, equipe, tipo_pessoa='', numero='', nome=''):
    criar_tabela_sancoes_partida()
    chave_numero = str(numero or '').strip()
    chave_nome = (nome or '').strip().lower()
    tipo_pessoa = (tipo_pessoa or '').strip().lower()

    historico = []
    with conectar() as conn:
        with conn.cursor() as cur:
            if tipo_pessoa == 'atleta' and chave_numero:
                cur.execute("""
                    SELECT tipo
                    FROM sancoes_partida
                    WHERE partida_id = %s
                      AND competicao = %s
                      AND equipe = %s
                      AND LOWER(COALESCE(tipo_pessoa, '')) = %s
                      AND COALESCE(numero, '') = %s
                    ORDER BY id ASC
                """, (partida_id, competicao, equipe, tipo_pessoa, chave_numero))
            else:
                cur.execute("""
                    SELECT tipo
                    FROM sancoes_partida
                    WHERE partida_id = %s
                      AND competicao = %s
                      AND equipe = %s
                      AND LOWER(COALESCE(tipo_pessoa, '')) = %s
                      AND LOWER(COALESCE(nome, '')) = %s
                    ORDER BY id ASC
                """, (partida_id, competicao, equipe, tipo_pessoa, chave_nome))
            historico = cur.fetchall() or []

    ordem = ['advertencia', 'penalidade', 'expulsao', 'desqualificacao']
    maior_idx = -1
    for row in historico:
        t = (row.get('tipo') or '').strip().lower()
        if t in ordem:
            maior_idx = max(maior_idx, ordem.index(t))

    return ordem, maior_idx


def _registrar_linha_sancao_partida(partida_id, competicao, equipe, tipo_pessoa='', numero='', nome='', tipo='', escopo='', set_aplicado=1, observacao=''):
    criar_tabela_sancoes_partida()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sancoes_partida (
                    partida_id, competicao, equipe, tipo_pessoa, numero, nome, tipo, escopo, set_aplicado, observacao
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                partida_id,
                competicao,
                equipe,
                (tipo_pessoa or '').strip().lower(),
                str(numero or '').strip(),
                (nome or '').strip(),
                (tipo or '').strip().lower(),
                (escopo or '').strip().lower(),
                int(set_aplicado or 1),
                (observacao or '').strip(),
            ))
        conn.commit()


def inicializar_jogo_partida(partida_id, competicao):
    """
    Inicializa o jogo somente quando necessário.
    Não recria tabelas nem reconstrói snapshot toda vez que abre a tela.
    """

    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return None

    status_jogo = (partida.get("status_jogo") or "").strip().lower()

    if status_jogo in {"em_andamento", "entre_sets", "finalizada"}:
        return partida

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE partidas
                SET set_atual = COALESCE(set_atual, 1),
                    sets_a = COALESCE(sets_a, 0),
                    sets_b = COALESCE(sets_b, 0),
                    pontos_a = COALESCE(pontos_a, 0),
                    pontos_b = COALESCE(pontos_b, 0),
                    status_jogo = COALESCE(status_jogo, 'pre_jogo'),
                    fase_partida = COALESCE(fase_partida, 'pre_jogo')
                WHERE id = %s
                  AND competicao = %s
            """, (partida_id, competicao))
        conn.commit()

    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return None

    try:
        estado = buscar_estado_jogo_partida(partida_id, competicao)
    except Exception:
        estado = None

    if not estado:
        _reconstruir_e_salvar_snapshot(partida_id, competicao, partida)

    return buscar_partida_operacional(partida_id, competicao)

def _saque_inicial_lado_operacional(partida):
    comp = buscar_competicao_por_nome(partida.get("competicao")) or {}
    formato = _normalizar_formato_sets(comp.get("sets_tipo"))
    set_atual = int(partida.get("set_atual") or 1)

    usar_tiebreak = set_eh_tiebreak(formato, set_atual) and bool(partida.get("tiebreak_definido"))
    campo_saque = "saque_tiebreak" if usar_tiebreak else "saque_inicial"
    saque_inicial = (partida.get(campo_saque) or "").strip()
    if not saque_inicial:
        return ""

    if saque_inicial in {"A", "B"}:
        return saque_inicial

    if saque_inicial == partida.get("equipe_a_operacional"):
        return "A"
    if saque_inicial == partida.get("equipe_b_operacional"):
        return "B"

    return ""


def _posicoes_base_papeleta(partida_id, competicao, equipe, set_numero):
    posicoes = {1: "", 2: "", 3: "", 4: "", 5: "", 6: ""}

    if not equipe:
        return posicoes

    rows = listar_papeleta(partida_id, competicao, equipe, set_numero) or []
    for row in rows:
        try:
            posicao = int(row.get("posicao") or 0)
        except (TypeError, ValueError):
            continue

        if posicao in posicoes:
            numero = row.get("numero")
            posicoes[posicao] = "" if numero in (None, "") else str(numero)

    return posicoes


def _girar_posicoes_horario(posicoes):
    posicoes = posicoes or {}
    return {
        1: posicoes.get(2, ""),
        2: posicoes.get(3, ""),
        3: posicoes.get(4, ""),
        4: posicoes.get(5, ""),
        5: posicoes.get(6, ""),
        6: posicoes.get(1, ""),
    }


def _posicoes_para_quadra(posicoes):
    posicoes = posicoes or {}
    return [
        posicoes.get(4, ""),
        posicoes.get(3, ""),
        posicoes.get(2, ""),
        posicoes.get(5, ""),
        posicoes.get(6, ""),
        posicoes.get(1, ""),
    ]


def _calcular_rotacoes_partida(partida_id, competicao, partida=None):
    if not partida:
        partida = buscar_partida_operacional(partida_id, competicao)

    vazio = ["", "", "", "", "", ""]
    vazio_pos = {1: "", 2: "", 3: "", 4: "", 5: "", 6: ""}

    if not partida:
        return {
            "posicoes_a": dict(vazio_pos),
            "posicoes_b": dict(vazio_pos),
            "rotacao_a": vazio[:],
            "rotacao_b": vazio[:],
            "saque_calculado": "",
            "subs_a": 0,
            "subs_b": 0,
            "titulares_iniciais_a": [],
            "titulares_iniciais_b": [],
            "vinculos_titular_reserva_a": {},
            "vinculos_titular_reserva_b": {},
            "vinculos_reserva_titular_a": {},
            "vinculos_reserva_titular_b": {},
        }

    set_atual = int(partida.get("set_atual") or 1)
    equipe_a = partida.get("equipe_a_operacional")
    equipe_b = partida.get("equipe_b_operacional")

    posicoes_a = _posicoes_base_papeleta(partida_id, competicao, equipe_a, set_atual)
    posicoes_b = _posicoes_base_papeleta(partida_id, competicao, equipe_b, set_atual)

    titulares_iniciais_a = {str(numero) for numero in posicoes_a.values() if str(numero).strip()}
    titulares_iniciais_b = {str(numero) for numero in posicoes_b.values() if str(numero).strip()}
    vinculos_titular_reserva_a = {}
    vinculos_titular_reserva_b = {}
    vinculos_reserva_titular_a = {}
    vinculos_reserva_titular_b = {}
    subs_a = 0
    subs_b = 0

    saque_corrente = _saque_inicial_lado_operacional(partida)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, equipe, tipo, fundamento, resultado
                FROM eventos
                WHERE partida_id = %s
                  AND competicao = %s
                  AND set_numero = %s
                  AND tipo IN ('ponto', 'substituicao', 'substituicao_excepcional')
                ORDER BY id ASC
            """, (partida_id, competicao, set_atual))
            eventos = cur.fetchall()

    for evento in eventos:
        tipo = (evento.get("tipo") or "").strip().lower()
        equipe_evento = (evento.get("equipe") or "").strip().upper()
        if equipe_evento not in {"A", "B"}:
            continue

        if tipo == 'ponto':
            if saque_corrente in {"A", "B"} and equipe_evento != saque_corrente:
                if equipe_evento == "A":
                    posicoes_a = _girar_posicoes_horario(posicoes_a)
                else:
                    posicoes_b = _girar_posicoes_horario(posicoes_b)
            saque_corrente = equipe_evento
            continue

        if tipo not in {'substituicao', 'substituicao_excepcional'}:
            continue

        detalhes = evento.get('detalhes')
        if isinstance(detalhes, str):
            try:
                detalhes = json.loads(detalhes)
            except Exception:
                detalhes = {}
        if not isinstance(detalhes, dict):
            detalhes = {}

        numero_sai = str(detalhes.get('numero_sai') or detalhes.get('sai') or '').strip()
        numero_entra = str(detalhes.get('numero_entra') or detalhes.get('entra') or '').strip()
        if not numero_sai or not numero_entra:
            continue

        if equipe_evento == 'A':
            alvo_posicoes = posicoes_a
            titulares_iniciais = titulares_iniciais_a
            vinc_tit_res = vinculos_titular_reserva_a
            vinc_res_tit = vinculos_reserva_titular_a
            if tipo == 'substituicao':
                subs_a += 1
        else:
            alvo_posicoes = posicoes_b
            titulares_iniciais = titulares_iniciais_b
            vinc_tit_res = vinculos_titular_reserva_b
            vinc_res_tit = vinculos_reserva_titular_b
            if tipo == 'substituicao':
                subs_b += 1

        for posicao in [1, 2, 3, 4, 5, 6]:
            if str(alvo_posicoes.get(posicao, '')).strip() == numero_sai:
                alvo_posicoes[posicao] = numero_entra
                break

        sai_titular = numero_sai in titulares_iniciais
        entra_titular = numero_entra in titulares_iniciais

        if sai_titular and not entra_titular:
            vinc_tit_res[numero_sai] = numero_entra
            vinc_res_tit[numero_entra] = numero_sai
        elif (not sai_titular) and entra_titular:
            titular = vinc_res_tit.pop(numero_sai, None)
            if titular:
                vinc_tit_res.pop(titular, None)

    return {
        "posicoes_a": posicoes_a,
        "posicoes_b": posicoes_b,
        "rotacao_a": _posicoes_para_quadra(posicoes_a),
        "rotacao_b": _posicoes_para_quadra(posicoes_b),
        "saque_calculado": saque_corrente,
        "subs_a": subs_a,
        "subs_b": subs_b,
        "titulares_iniciais_a": sorted(titulares_iniciais_a, key=lambda x: int(x) if str(x).isdigit() else str(x)),
        "titulares_iniciais_b": sorted(titulares_iniciais_b, key=lambda x: int(x) if str(x).isdigit() else str(x)),
        "vinculos_titular_reserva_a": vinculos_titular_reserva_a,
        "vinculos_titular_reserva_b": vinculos_titular_reserva_b,
        "vinculos_reserva_titular_a": vinculos_reserva_titular_a,
        "vinculos_reserva_titular_b": vinculos_reserva_titular_b,
    }



def _regras_jogo_competicao(competicao):
    comp = buscar_competicao_por_nome(competicao) or {}
    sets_tipo = (comp.get("sets_tipo") or "melhor_de_3").strip().lower()
    pontos_set = int(comp.get("pontos_set") or 25)
    pontos_tiebreak = int(comp.get("pontos_tiebreak") or 15)
    diferenca_minima = int(comp.get("diferenca_minima") or 2)
    modo_operacao = (comp.get("modo_operacao") or "simples").strip().lower()

    if sets_tipo == "set_unico":
        sets_para_vencer = 1
    elif sets_tipo == "melhor_de_5":
        sets_para_vencer = 3
    else:
        sets_para_vencer = 2

    return {
        "sets_tipo": sets_tipo,
        "pontos_set": pontos_set,
        "pontos_tiebreak": pontos_tiebreak,
        "diferenca_minima": diferenca_minima,
        "modo_operacao": modo_operacao,
        "sets_para_vencer": sets_para_vencer,
    }


def _set_atual_e_tiebreak(sets_tipo, set_atual):
    return (sets_tipo == "melhor_de_3" and set_atual == 3) or (sets_tipo == "melhor_de_5" and set_atual == 5)


def registrar_evento_partida(
    partida_id,
    competicao,
    set_numero,
    equipe,
    tipo,
    fundamento=None,
    resultado=None,
    detalhe=None,
    atleta_nome=None,
    numero=None,
    atleta_id=None,
    tipo_evento=None,
    detalhes=None
):
    criar_tabela_eventos()

    equipe = (equipe or '').strip().upper() if equipe is not None else None
    tipo = (tipo or '').strip()
    fundamento = (fundamento or '').strip() if fundamento is not None else None
    resultado = (resultado or '').strip() if resultado is not None else None
    detalhe = (detalhe or '').strip() if detalhe is not None else None
    atleta_nome = (atleta_nome or '').strip() if atleta_nome is not None else None
    tipo_evento = (tipo_evento or tipo or '').strip() if tipo_evento is not None or tipo else None

    numero_final = None
    if numero not in (None, ''):
        try:
            numero_final = int(str(numero).strip())
        except (ValueError, TypeError):
            numero_final = None

    atleta_id_final = None
    if atleta_id not in (None, ''):
        try:
            atleta_id_final = int(str(atleta_id).strip())
        except (ValueError, TypeError):
            atleta_id_final = None

    detalhes_json = None
    if isinstance(detalhes, dict):
        try:
            detalhes_json = json.dumps(detalhes, ensure_ascii=False)
        except Exception:
            detalhes_json = None
    elif isinstance(detalhes, str) and detalhes.strip():
        detalhes_json = detalhes.strip()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO eventos (
                    partida_id,
                    competicao,
                    set_numero,
                    equipe,
                    tipo,
                    tipo_evento,
                    fundamento,
                    resultado,
                    detalhe,
                    atleta_id,
                    atleta_nome,
                    numero,
                    detalhes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                partida_id,
                competicao,
                set_numero,
                equipe,
                tipo,
                tipo_evento,
                fundamento,
                resultado,
                detalhe,
                atleta_id_final,
                atleta_nome,
                numero_final,
                detalhes_json,
            ))
        conn.commit()



def _json_load_text(valor, padrao):
    if valor in (None, ""):
        return padrao
    try:
        return json.loads(valor)
    except Exception:
        return padrao


def _detalhes_evento_dict(detalhes):
    if isinstance(detalhes, dict):
        return dict(detalhes)
    if isinstance(detalhes, str):
        try:
            valor = json.loads(detalhes)
            if isinstance(valor, dict):
                return valor
        except Exception:
            pass
    return {}


def _nome_equipe_por_lado(partida, lado):
    lado = (lado or '').strip().upper()
    if lado == 'A':
        return (partida.get('equipe_a_operacional') or partida.get('equipe_a') or 'Equipe A').strip()
    if lado == 'B':
        return (partida.get('equipe_b_operacional') or partida.get('equipe_b') or 'Equipe B').strip()
    return 'Equipe'


def _descricao_alvo_evento(tipo_pessoa='', numero='', nome=''):
    tipo_pessoa = (tipo_pessoa or '').strip().lower()
    numero = str(numero or '').strip()
    nome = (nome or '').strip()

    if tipo_pessoa == 'atleta':
        if numero and nome:
            return f'#{numero} - {nome}'
        if numero:
            return f'#{numero}'
        if nome:
            return nome
        return 'Atleta'

    if nome:
        return nome

    rotulos = {
        'tecnico': 'Técnico',
        'auxiliar': 'Auxiliar',
        'membro': 'Membro',
    }
    return rotulos.get(tipo_pessoa, 'Membro')


def _montar_ultima_acao_partida(partida, tipo, equipe=None, detalhes=None):
    detalhes = _detalhes_evento_dict(detalhes)
    equipe = (equipe or '').strip().upper()
    nome_equipe = _nome_equipe_por_lado(partida, equipe) if equipe in {'A', 'B'} else 'Equipe'

    if tipo in {'ponto', 'retardamento_penalidade'}:
        detalhe = (detalhes.get('detalhe_lance') or detalhes.get('fundamento') or detalhes.get('resultado') or 'ponto')
        detalhe = str(detalhe).replace('_', ' ').strip()
        atleta_label = (detalhes.get('atleta_label') or '').strip()
        atleta_nome = (detalhes.get('atleta_nome') or '').strip()
        atleta_numero = str(detalhes.get('atleta_numero') or '').strip()
        if not atleta_label:
            if atleta_numero and atleta_nome:
                atleta_label = f'#{atleta_numero} - {atleta_nome}'
            elif atleta_numero:
                atleta_label = f'#{atleta_numero}'
            elif atleta_nome:
                atleta_label = atleta_nome
        texto = f'Ponto {nome_equipe} • {detalhe.title()}'
        if atleta_label:
            texto += f' • {atleta_label}'
        return texto

    if tipo == 'sancao':
        alvo = _descricao_alvo_evento(detalhes.get('tipo_pessoa'), detalhes.get('numero'), detalhes.get('nome'))
        tipo_sancao = str(detalhes.get('tipo_sancao') or 'sancao').replace('_', ' ').strip().title()
        return f'Sanção {nome_equipe} • {tipo_sancao} • {alvo}'

    if tipo == 'cartao_verde':
        alvo = _descricao_alvo_evento(detalhes.get('tipo_pessoa'), detalhes.get('numero'), detalhes.get('nome'))
        return f'Cartão verde {nome_equipe} • {alvo}'

    if tipo == 'retardamento':
        tipo_ret = str(detalhes.get('tipo_retardamento') or '').strip().lower()
        rotulo = 'penalidade' if tipo_ret == 'penalidade' else 'advertência'
        return f'Retardamento {nome_equipe} • {rotulo}'

    if tipo == 'substituicao_excepcional':
        numero_sai = str(detalhes.get('numero_sai') or '').strip()
        numero_entra = str(detalhes.get('numero_entra') or '').strip()
        return f'Subst. excepcional {nome_equipe} • sai {numero_sai or "-"} / entra {numero_entra or "-"}'

    if tipo == 'substituicao':
        numero_sai = str(detalhes.get('numero_sai') or '').strip()
        numero_entra = str(detalhes.get('numero_entra') or '').strip()
        return f'Substituição {nome_equipe} • sai {numero_sai or "-"} / entra {numero_entra or "-"}'

    return ''


def _girar_rotacao_visual_horario(rotacao):
    rot = list(rotacao or ["", "", "", "", "", ""])
    while len(rot) < 6:
        rot.append("")
    return [rot[3], rot[0], rot[1], rot[4], rot[5], rot[2]]


def _snapshot_estado_partida(partida, competicao):
    comp = buscar_competicao_por_nome(competicao) or {}
    return {
        "id": partida.get("id"),
        "competicao": partida.get("competicao"),
        "equipe_a": partida.get("equipe_a"),
        "equipe_b": partida.get("equipe_b"),
        "equipe_a_operacional": partida.get("equipe_a_operacional"),
        "equipe_b_operacional": partida.get("equipe_b_operacional"),
        "pontos_a": int(partida.get("pontos_a") or 0),
        "pontos_b": int(partida.get("pontos_b") or 0),
        "sets_a": int(partida.get("sets_a") or 0),
        "sets_b": int(partida.get("sets_b") or 0),
        "set_atual": int(partida.get("set_atual") or 1),
        "fase_partida": partida.get("fase_partida") or "pre_jogo",
        "sets_max": int(partida.get("sets_max") or calcular_sets_max(comp.get("sets_tipo"))),
        "sets_para_vencer": int(partida.get("sets_para_vencer") or calcular_sets_para_vencer(comp.get("sets_tipo"))),
        "saque_atual": partida.get("saque_atual") or "",
        "saque_inicial": partida.get("saque_inicial") or "",
        "status_jogo": partida.get("status_jogo") or "pre_jogo",
        "status": partida.get("status") or "",
        "rotacao_a": _json_load_text(partida.get("rotacao_a_json"), ["", "", "", "", "", ""]),
        "rotacao_b": _json_load_text(partida.get("rotacao_b_json"), ["", "", "", "", "", ""]),
        "status_jogadores_a": _json_load_text(partida.get("status_jogadores_a_json"), {}),
        "status_jogadores_b": _json_load_text(partida.get("status_jogadores_b_json"), {}),
        "subs_a": int(partida.get("subs_a") or 0),
        "subs_b": int(partida.get("subs_b") or 0),
        "titulares_iniciais_a": _json_load_text(partida.get("titulares_iniciais_a_json"), []),
        "titulares_iniciais_b": _json_load_text(partida.get("titulares_iniciais_b_json"), []),
        "vinculos_titular_reserva_a": _json_load_text(partida.get("vinculos_titular_reserva_a_json"), {}),
        "vinculos_titular_reserva_b": _json_load_text(partida.get("vinculos_titular_reserva_b_json"), {}),
        "vinculos_reserva_titular_a": _json_load_text(partida.get("vinculos_reserva_titular_a_json"), {}),
        "vinculos_reserva_titular_b": _json_load_text(partida.get("vinculos_reserva_titular_b_json"), {}),
        "sancoes_a": _json_load_text(partida.get("sancoes_a_json"), []),
        "sancoes_b": _json_load_text(partida.get("sancoes_b_json"), []),
        "cartoes_verdes_a": _json_load_text(partida.get("cartoes_verdes_a_json"), []),
        "cartoes_verdes_b": _json_load_text(partida.get("cartoes_verdes_b_json"), []),
        "bloqueios": _json_load_text(partida.get("bloqueios_json"), {}),
        "substituicao_forcada": _json_load_text(partida.get("substituicao_forcada_json"), {}),
        "retardamentos_a": _json_load_text(partida.get("retardamentos_a_json"), []),
        "retardamentos_b": _json_load_text(partida.get("retardamentos_b_json"), []),
        "subs_excepcionais": _json_load_text(partida.get("subs_excepcionais_json"), []),
        "limite_substituicoes": int(comp.get("substituicoes_por_set") or 6),
    }


def _salvar_snapshot_estado_jogo(partida_id, competicao, estado):
    criar_campos_jogo_partida()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE partidas
                SET saque_atual = %s,
                    status_jogo = %s,
                    fase_partida = %s,
                    rotacao_a_json = %s,
                    rotacao_b_json = %s,
                    status_jogadores_a_json = %s,
                    status_jogadores_b_json = %s,
                    subs_a = %s,
                    subs_b = %s,
                    titulares_iniciais_a_json = %s,
                    titulares_iniciais_b_json = %s,
                    vinculos_titular_reserva_a_json = %s,
                    vinculos_titular_reserva_b_json = %s,
                    vinculos_reserva_titular_a_json = %s,
                    vinculos_reserva_titular_b_json = %s,
                    sancoes_a_json = %s,
                    sancoes_b_json = %s,
                    cartoes_verdes_a_json = %s,
                    cartoes_verdes_b_json = %s,
                    bloqueios_json = %s,
                    substituicao_forcada_json = %s,
                    retardamentos_a_json = %s,
                    retardamentos_b_json = %s,
                    subs_excepcionais_json = %s
                WHERE id = %s
                  AND competicao = %s
            """, (
                estado.get("saque_atual") or None,
                estado.get("status_jogo") or "pre_jogo",
                estado.get("fase_partida") or 'jogo',
                json.dumps(estado.get("rotacao_a", ["", "", "", "", "", ""]), ensure_ascii=False),
                json.dumps(estado.get("rotacao_b", ["", "", "", "", "", ""]), ensure_ascii=False),
                json.dumps(estado.get("status_jogadores_a", {}), ensure_ascii=False),
                json.dumps(estado.get("status_jogadores_b", {}), ensure_ascii=False),
                int(estado.get("subs_a") or 0),
                int(estado.get("subs_b") or 0),
                json.dumps(estado.get("titulares_iniciais_a", []), ensure_ascii=False),
                json.dumps(estado.get("titulares_iniciais_b", []), ensure_ascii=False),
                json.dumps(estado.get("vinculos_titular_reserva_a", {}), ensure_ascii=False),
                json.dumps(estado.get("vinculos_titular_reserva_b", {}), ensure_ascii=False),
                json.dumps(estado.get("vinculos_reserva_titular_a", {}), ensure_ascii=False),
                json.dumps(estado.get("vinculos_reserva_titular_b", {}), ensure_ascii=False),
                json.dumps(estado.get("sancoes_a", []), ensure_ascii=False),
                json.dumps(estado.get("sancoes_b", []), ensure_ascii=False),
                json.dumps(estado.get("cartoes_verdes_a", []), ensure_ascii=False),
                json.dumps(estado.get("cartoes_verdes_b", []), ensure_ascii=False),
                json.dumps(estado.get("bloqueios", {}), ensure_ascii=False),
                json.dumps(estado.get("substituicao_forcada", {}), ensure_ascii=False),
                json.dumps(estado.get("retardamentos_a", []), ensure_ascii=False),
                json.dumps(estado.get("retardamentos_b", []), ensure_ascii=False),
                json.dumps(estado.get("subs_excepcionais", []), ensure_ascii=False),
                partida_id,
                competicao
            ))
        conn.commit()


def _aplicar_eventos_disciplinares_snapshot(partida_id, competicao, partida, estado_base):
    estado = dict(estado_base or {})
    estado.setdefault("sancoes_a", [])
    estado.setdefault("sancoes_b", [])
    estado.setdefault("cartoes_verdes_a", [])
    estado.setdefault("cartoes_verdes_b", [])
    estado.setdefault("bloqueios", {})
    estado.setdefault("substituicao_forcada", {})
    estado.setdefault("retardamentos_a", [])
    estado.setdefault("retardamentos_b", [])
    estado.setdefault("subs_excepcionais", [])

    set_atual = int(partida.get("set_atual") or estado.get("set_atual") or 1)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, equipe, tipo, detalhes, set_numero
                FROM eventos
                WHERE partida_id = %s
                  AND competicao = %s
                  AND tipo IN ('sancao', 'cartao_verde', 'retardamento', 'substituicao_excepcional')
                ORDER BY id ASC
            """, (partida_id, competicao))
            eventos = cur.fetchall()

    for evento in eventos:
        tipo = (evento.get("tipo") or "").strip().lower()
        equipe = (evento.get("equipe") or "").strip().upper()
        detalhes = evento.get("detalhes")
        if isinstance(detalhes, str):
            try:
                detalhes = json.loads(detalhes)
            except Exception:
                detalhes = {}
        if not isinstance(detalhes, dict):
            detalhes = {}

        if equipe not in {"A", "B"}:
            continue

        if tipo == 'retardamento':
            item = {
                'tipo_retardamento': detalhes.get('tipo_retardamento') or '',
                'observacao': detalhes.get('observacao') or '',
                'set_numero': int(evento.get('set_numero') or 1),
            }
            chave = 'retardamentos_a' if equipe == 'A' else 'retardamentos_b'
            estado[chave].append(item)
            continue

        if tipo == 'substituicao_excepcional':
            item = {
                'numero_sai': str(detalhes.get('numero_sai') or '').strip(),
                'numero_entra': str(detalhes.get('numero_entra') or '').strip(),
                'motivo': detalhes.get('motivo') or '',
                'observacao': detalhes.get('observacao') or '',
                'set_numero': int(evento.get('set_numero') or 1),
                'equipe': equipe,
            }
            estado['subs_excepcionais'].append(item)
            if item['numero_sai']:
                estado['bloqueios'][item['numero_sai']] = {'tipo': 'substituicao_excepcional', 'escopo': 'partida', 'set_numero': item['set_numero']}
            continue

        if tipo == 'cartao_verde':
            item = {
                'tipo_pessoa': detalhes.get('tipo_pessoa') or '',
                'numero': str(detalhes.get('numero') or '').strip(),
                'nome': detalhes.get('nome') or '',
                'observacao': detalhes.get('observacao') or '',
            }
            chave = 'cartoes_verdes_a' if equipe == 'A' else 'cartoes_verdes_b'
            estado[chave].append(item)
            continue

        if tipo != 'sancao':
            continue

        item = {
            'tipo_pessoa': detalhes.get('tipo_pessoa') or '',
            'numero': str(detalhes.get('numero') or '').strip(),
            'nome': detalhes.get('nome') or '',
            'tipo_sancao': detalhes.get('tipo_sancao') or '',
            'set_numero': int(evento.get('set_numero') or 1),
            'observacao': detalhes.get('observacao') or '',
        }
        chave = 'sancoes_a' if equipe == 'A' else 'sancoes_b'
        estado[chave].append(item)

        numero = item['numero']
        tipo_pessoa = (item['tipo_pessoa'] or '').strip().lower()
        tipo_sancao = (item['tipo_sancao'] or '').strip().lower()
        if tipo_pessoa == 'atleta' and numero:
            if tipo_sancao == 'desqualificacao':
                estado['bloqueios'][numero] = {'tipo': 'desqualificacao', 'escopo': 'partida', 'set_numero': item['set_numero']}
            elif tipo_sancao == 'expulsao' and item['set_numero'] == set_atual:
                estado['bloqueios'][numero] = {'tipo': 'expulsao', 'escopo': 'set', 'set_numero': item['set_numero']}

    return estado


def atleta_bloqueado(numero, estado, set_atual=None):
    numero = str(numero or '').strip()
    if not numero:
        return False
    bloqueios = dict((estado or {}).get('bloqueios') or {})
    info = bloqueios.get(numero)
    if not info:
        return False
    escopo = (info.get('escopo') or '').strip().lower()
    if escopo == 'partida':
        return True
    if escopo == 'set':
        return int(info.get('set_numero') or 0) == int(set_atual or 0)
    return True


def registrar_cartao_verde_partida(partida_id, competicao, equipe, tipo_pessoa='', numero='', nome='', observacao=''):
    criar_tabela_eventos()
    criar_campos_jogo_partida()

    equipe = (equipe or '').strip().upper()
    tipo_pessoa = (tipo_pessoa or '').strip().lower()
    numero = str(numero or '').strip()
    nome = (nome or '').strip()
    observacao = (observacao or '').strip()

    if equipe not in {'A', 'B'}:
        return False, 'Equipe inválida.'

    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return False, 'Partida não encontrada.'

    set_atual = int(partida.get('set_atual') or 1)

    detalhe = 'cartão verde'
    if tipo_pessoa == 'atleta' and numero:
        detalhe += f" | atleta #{numero}"
    elif nome:
        detalhe += f" | {nome}"
    if observacao:
        detalhe += f" | obs: {observacao}"

    registrar_evento_partida(
        partida_id,
        competicao,
        set_atual,
        equipe,
        "cartao_verde",
        fundamento=tipo_pessoa or None,
        detalhe=detalhe,
        atleta_nome=nome or None,
        numero=numero or None
    )

    estado = _reconstruir_e_salvar_snapshot(partida_id, competicao, partida)
    tempos = buscar_tempos_restantes_partida(partida_id, competicao)
    estado['tempos_a'] = tempos.get('tempos_a')
    estado['tempos_b'] = tempos.get('tempos_b')
    estado['mensagem'] = 'Cartão verde registrado.'
    estado['ultima_acao'] = _montar_ultima_acao_partida(
        partida,
        'cartao_verde',
        equipe=equipe,
        detalhes={
            'tipo_pessoa': tipo_pessoa,
            'numero': numero,
            'nome': nome,
            'observacao': observacao,
        }
    )
    estado['partida_finalizada'] = (estado.get('status_jogo') or '').lower() == 'finalizada'
    return True, estado


def registrar_sancao_partida(partida_id, competicao, equipe, tipo_pessoa='', numero='', nome='', tipo_sancao='', observacao=''):
    criar_tabela_eventos()
    criar_tabela_sancoes_partida()
    criar_campos_jogo_partida()
    criar_campos_sets_partida()

    equipe = (equipe or '').strip().upper()
    tipo_sancao = (tipo_sancao or '').strip().lower()
    tipo_pessoa = (tipo_pessoa or '').strip().lower()
    numero = str(numero or '').strip()
    nome = (nome or '').strip()
    observacao = (observacao or '').strip()

    if equipe not in {'A', 'B'}:
        return False, 'Equipe inválida.'

    if tipo_sancao not in {'advertencia', 'penalidade', 'expulsao', 'desqualificacao'}:
        return False, 'Tipo de sanção inválido.'

    if tipo_pessoa == 'atleta' and not numero:
        return False, 'Número do atleta não informado.'

    if tipo_pessoa != 'atleta' and not nome:
        return False, 'Nome do alvo não informado.'

    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return False, 'Partida não encontrada.'

    estado = _buscar_estado_jogo_partida_base(partida_id, competicao, garantir=False, permitir_reconstrucao=False)
    if not estado:
        return False, 'Estado da partida não encontrado.'

    set_atual = int(partida.get('set_atual') or 1)

    ordem, maior_idx = _tipo_progressivo_sancao(
        partida_id,
        competicao,
        equipe,
        tipo_pessoa=tipo_pessoa,
        numero=numero,
        nome=nome
    )
    idx_solicitado = ordem.index(tipo_sancao)

    tipo_final = tipo_sancao
    mensagem_progressao = ''

    if maior_idx >= idx_solicitado:
        proximo_idx = min(maior_idx + 1, len(ordem) - 1)
        tipo_final = ordem[proximo_idx]

        if tipo_final != tipo_sancao:
            mensagem_progressao = f'Sanção ajustada progressivamente para {tipo_final}.'
        elif maior_idx == len(ordem) - 1:
            tipo_final = ordem[-1]
            mensagem_progressao = 'O alvo já estava no limite máximo de sanção; mantida desqualificação.'

    escopo = 'partida' if tipo_final == 'desqualificacao' else 'set' if tipo_final == 'expulsao' else 'progressiva'

    detalhe = tipo_final
    if tipo_pessoa == 'atleta' and numero:
        detalhe += f" | atleta #{numero}"
    elif nome:
        detalhe += f" | {nome}"
    if observacao:
        detalhe += f" | obs: {observacao}"

    registrar_evento_partida(
        partida_id,
        competicao,
        set_atual,
        equipe,
        "sancao",
        fundamento=tipo_pessoa,
        resultado=tipo_final,
        detalhe=detalhe,
        atleta_nome=nome or None,
        numero=numero or None
    )

    _registrar_linha_sancao_partida(
        partida_id=partida_id,
        competicao=competicao,
        equipe=equipe,
        tipo_pessoa=tipo_pessoa,
        numero=numero,
        nome=nome,
        tipo=tipo_final,
        escopo=escopo,
        set_aplicado=set_atual,
        observacao=observacao,
    )

    if tipo_final == 'penalidade':
        equipe_ponto = 'B' if equipe == 'A' else 'A'

        detalhes_penalidade = {
            'origem_sancao': True,
            'tipo_pessoa': tipo_pessoa,
            'numero': numero,
            'nome': nome,
            'tipo_sancao': tipo_final,
            'tipo_sancao_solicitado': tipo_sancao,
            'observacao': observacao,
        }

        ok, resultado = registrar_ponto_partida(
            partida_id, competicao, adversario,
            tipo='retardamento_penalidade',
            detalhes={
                'origem_retardamento': True,
                'tipo_lance': 'falta',
                'detalhe_lance': 'retardamento',
                'fundamento': 'retardamento',
                'resultado': 'erro',
                'responsavel_lado': equipe,
                'observacao': observacao,
            }
        )
        
        if not ok:
            return False, resultado

        resultado['mensagem'] = mensagem_progressao or 'Penalidade registrada.'
        return True, resultado

    estado_reconstruido = _reconstruir_e_salvar_snapshot(partida_id, competicao, partida)

    if tipo_pessoa == 'atleta' and numero and tipo_final in {'expulsao', 'desqualificacao'}:
        rotacao_lado = (
            estado_reconstruido.get('rotacao_a')
            if equipe == 'A'
            else estado_reconstruido.get('rotacao_b')
        ) or []

        em_quadra = numero in [str(x).strip() for x in rotacao_lado]

        if em_quadra:
            estado_reconstruido['substituicao_forcada'] = {
                'equipe': equipe,
                'numero': numero,
                'tipo_sancao': tipo_final,
                'set_numero': set_atual,
            }
            _salvar_snapshot_estado_jogo(partida_id, competicao, estado_reconstruido)

    tempos = buscar_tempos_restantes_partida(partida_id, competicao)
    estado_reconstruido['tempos_a'] = tempos.get('tempos_a')
    estado_reconstruido['tempos_b'] = tempos.get('tempos_b')
    estado_reconstruido['mensagem'] = mensagem_progressao or 'Sanção registrada.'
    estado_reconstruido['partida_finalizada'] = (estado_reconstruido.get('status_jogo') or '').lower() == 'finalizada'

    return True, estado_reconstruido

def _reconstruir_e_salvar_snapshot(partida_id, competicao, partida):
    rotacoes = _calcular_rotacoes_partida(partida_id, competicao, partida)
    estado = {
        "saque_atual": partida.get("saque_atual") or rotacoes.get("saque_calculado") or "",
        "status_jogo": partida.get("status_jogo") or "pre_jogo",
        "rotacao_a": rotacoes.get("rotacao_a", ["", "", "", "", "", ""]),
        "rotacao_b": rotacoes.get("rotacao_b", ["", "", "", "", "", ""]),
        "subs_a": int(rotacoes.get("subs_a") or 0),
        "subs_b": int(rotacoes.get("subs_b") or 0),
        "titulares_iniciais_a": rotacoes.get("titulares_iniciais_a", []),
        "titulares_iniciais_b": rotacoes.get("titulares_iniciais_b", []),
        "vinculos_titular_reserva_a": rotacoes.get("vinculos_titular_reserva_a", {}),
        "vinculos_titular_reserva_b": rotacoes.get("vinculos_titular_reserva_b", {}),
        "vinculos_reserva_titular_a": rotacoes.get("vinculos_reserva_titular_a", {}),
        "vinculos_reserva_titular_b": rotacoes.get("vinculos_reserva_titular_b", {}),
        "status_jogadores_a": {str(res): {"tipo": "substituto", "vinculo": str(tit)} for res, tit in (rotacoes.get("vinculos_reserva_titular_a", {}) or {}).items()},
        "status_jogadores_b": {str(res): {"tipo": "substituto", "vinculo": str(tit)} for res, tit in (rotacoes.get("vinculos_reserva_titular_b", {}) or {}).items()},
        "sancoes_a": [],
        "sancoes_b": [],
        "cartoes_verdes_a": [],
        "cartoes_verdes_b": [],
        "bloqueios": {},
        "substituicao_forcada": {},
        "retardamentos_a": [],
        "retardamentos_b": [],
        "subs_excepcionais": [],
    }
    estado = _aplicar_eventos_disciplinares_snapshot(partida_id, competicao, partida, estado)
    _salvar_snapshot_estado_jogo(partida_id, competicao, estado)
    partida_atualizada = buscar_partida_operacional(partida_id, competicao) or partida
    estado_completo = _snapshot_estado_partida(partida_atualizada, competicao)
    if not (estado_completo.get("saque_atual") or "").strip() and rotacoes.get("saque_calculado"):
        estado_completo["saque_atual"] = rotacoes.get("saque_calculado") or ""
    return estado_completo

def _buscar_estado_jogo_partida_base(partida_id, competicao, garantir=False, permitir_reconstrucao=True):
    if garantir:
        garantir_estado_partida(partida_id, competicao)

    criar_campos_jogo_partida()
    criar_campos_sets_partida()
    criar_tabela_eventos()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))
            partida = cur.fetchone()

    if not partida:
        return None

    estado = _snapshot_estado_partida(partida, competicao)
    fluxo = resumir_fluxo_oficial_partida(partida_id, competicao, partida=partida) or {}
    estado.update(fluxo)

    if not permitir_reconstrucao:
        return estado

    rot_a = estado.get("rotacao_a") or []
    rot_b = estado.get("rotacao_b") or []

    rotacao_valida = (
        len(rot_a) == 6
        and len(rot_b) == 6
        and (
            any(str(x).strip() for x in rot_a)
            or any(str(x).strip() for x in rot_b)
            or estado.get("status_jogo") == "pre_jogo"
        )
    )

    if rotacao_valida:
        return estado

    estado = _reconstruir_e_salvar_snapshot(partida_id, competicao, partida)
    fluxo = resumir_fluxo_oficial_partida(
        partida_id,
        competicao,
        partida=buscar_partida_operacional(partida_id, competicao)
    ) or {}
    estado.update(fluxo)
    return estado


def buscar_estado_jogo_partida(partida_id, competicao):
    return _buscar_estado_jogo_partida_base(
        partida_id,
        competicao,
        garantir=False,
        permitir_reconstrucao=False,
    )


def _montar_historico_resumido_partida(partida_id, competicao, limite=5):
    eventos = listar_eventos_partida(partida_id, competicao, limite=limite) or []
    historico = []

    for ev in eventos:
        descricao = str(ev.get("descricao") or "").strip() or "Ação registrada"
        historico.append({"descricao": descricao})

    return historico



def _emitir_estado_tempo_real(partida_id, competicao):
    estado = buscar_estado_jogo_partida(partida_id, competicao) or {}

    payload = {
        "placar_a": int(estado.get("pontos_a") or estado.get("placar_a") or 0),
        "placar_b": int(estado.get("pontos_b") or estado.get("placar_b") or 0),
        "sets_a": int(estado.get("sets_a") or 0),
        "sets_b": int(estado.get("sets_b") or 0),
        "saque_atual": estado.get("saque_atual") or "",
        "tempos_a": estado.get("tempos_a"),
        "tempos_b": estado.get("tempos_b"),
        "subs_a": int(estado.get("subs_a") or 0),
        "subs_b": int(estado.get("subs_b") or 0),
        "rotacao": {
            "equipe_a": list(estado.get("rotacao_a") or ["", "", "", "", "", ""]),
            "equipe_b": list(estado.get("rotacao_b") or ["", "", "", "", "", ""]),
        },
        "status_jogo": estado.get("status_jogo") or "",
        "set_atual": int(estado.get("set_atual") or 1),
    }

    from socket_events import emitir_estado_partida
    emitir_estado_partida(partida_id, payload)

    return True
    


def registrar_ponto_partida(partida_id, competicao, equipe, tipo='ponto', detalhes=None):
    print("🔥 registrar_ponto_partida INICIO", flush=True)

    criar_estrutura_rotacao_profissional()
    criar_tabela_eventos()
    criar_campos_jogo_partida()
    criar_campos_sets_partida()

    def _carregar_rotacao_real(partida, lado):
        campo_array = f"rotacao_{lado}"
        campo_json = f"rotacao_{lado}_json"

        rotacao = partida.get(campo_array)

        if isinstance(rotacao, list) and _rotacao_tem_6_validos(rotacao):
            return _normalizar_rotacao_oficial(rotacao)

        try:
            rotacao_json = json.loads(partida.get(campo_json) or "[]")
        except Exception:
            rotacao_json = []

        if _rotacao_tem_6_validos(rotacao_json):
            return _normalizar_rotacao_oficial(rotacao_json)

        return ["", "", "", "", "", ""]

    def _lado_saque(valor, partida):
        valor = str(valor or "").strip()
        if not valor:
            return ""

        valor_upper = valor.upper()
        if valor_upper in {"A", "B"}:
            return valor_upper

        equipe_a_nome = str(partida.get("equipe_a") or partida.get("equipe_a_operacional") or "").strip().lower()
        equipe_b_nome = str(partida.get("equipe_b") or partida.get("equipe_b_operacional") or "").strip().lower()

        if valor.lower() == equipe_a_nome:
            return "A"

        if valor.lower() == equipe_b_nome:
            return "B"

        return ""

    equipe = (equipe or "").strip().upper()
    print("➡️ equipe:", equipe, flush=True)

    if equipe not in {"A", "B"}:
        return False, "Equipe inválida."

    detalhes = detalhes or {}
    if not isinstance(detalhes, dict):
        detalhes = {}

    def _oposto(lado):
        return "B" if lado == "A" else "A"

    equipe_pontuadora = str(detalhes.get("equipe_pontuadora") or equipe or "").strip().upper()
    resultado_tmp = str(detalhes.get("resultado") or detalhes.get("tipo_lance") or tipo or "").strip().lower()

    equipe_scout_raw = str(
        detalhes.get("equipe_scout")
        or detalhes.get("responsavel_lado")
        or ""
    ).strip().upper()

    if equipe_scout_raw in {"A", "B"}:
        equipe_scout = equipe_scout_raw
    elif resultado_tmp in {"erro", "falta"}:
        equipe_scout = _oposto(equipe_pontuadora)
    else:
        equipe_scout = equipe_pontuadora

    if equipe_pontuadora not in {"A", "B"}:
        return False, "Equipe pontuadora inválida."

    if equipe_scout not in {"A", "B"}:
        equipe_scout = equipe_pontuadora

    detalhes["equipe_pontuadora"] = equipe_pontuadora
    detalhes["equipe_scout"] = equipe_scout
    detalhes["responsavel_lado"] = equipe_scout

    regras = _regras_jogo_competicao(competicao)

    fim_set = False
    fim_jogo = False
    vencedor_set = None
    vencedor_partida = None

    girou = False
    equipe_girou = ""
    saque_antes = ""
    saque_depois = equipe_pontuadora

    partida = None
    pontos_a = 0
    pontos_b = 0
    sets_a = 0
    sets_b = 0
    set_atual = 1
    rotacao_a = ["", "", "", "", "", ""]
    rotacao_b = ["", "", "", "", "", ""]

    print("➡️ antes conectar", flush=True)
    with conectar() as conn:
        print("✅ conectou", flush=True)

        with conn.cursor() as cur:
            print("➡️ antes SELECT partida", flush=True)
            cur.execute("""
                SELECT *
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                FOR UPDATE
            """, (partida_id, competicao))

            partida = cur.fetchone()
            print("✅ depois SELECT partida:", bool(partida), flush=True)

            if not partida:
                return False, "Partida não encontrada."

            if (partida.get("status_jogo") or "").lower() == "finalizada":
                return False, "Partida já finalizada."

            pontos_a = int(partida.get("pontos_a") or 0)
            pontos_b = int(partida.get("pontos_b") or 0)
            sets_a = int(partida.get("sets_a") or 0)
            sets_b = int(partida.get("sets_b") or 0)
            set_atual = int(partida.get("set_atual") or 1)

            rotacao_a = _carregar_rotacao_real(partida, "a")
            rotacao_b = _carregar_rotacao_real(partida, "b")

            saque_antes = _lado_saque(
                partida.get("saque_atual") or partida.get("saque_inicial"),
                partida
            )

            if not saque_antes:
                saque_antes = equipe_pontuadora

            rotacao_a_antes = list(rotacao_a)
            rotacao_b_antes = list(rotacao_b)

            if saque_antes != equipe_pontuadora:
                girou = True
                equipe_girou = equipe_pontuadora

                if equipe_pontuadora == "A":
                    rotacao_a = girar_rotacao_oficial(rotacao_a)
                else:
                    rotacao_b = girar_rotacao_oficial(rotacao_b)

            saque_depois = equipe_pontuadora

            if equipe_pontuadora == "A":
                pontos_a += 1
            else:
                pontos_b += 1

            pontos_set = int(regras.get("pontos_set") or 21)
            diferenca_minima = int(regras.get("diferenca_minima") or 2)
            sets_para_vencer = int(regras.get("sets_para_vencer") or 2)

            fundamento = (
                detalhes.get("fundamento")
                or detalhes.get("detalhe_lance")
                or detalhes.get("tipo_erro")
                or ""
            )
            resultado = detalhes.get("resultado") or detalhes.get("tipo_lance") or tipo or "ponto"
            detalhe = (
                detalhes.get("detalhe_lance")
                or detalhes.get("tipo_erro")
                or detalhes.get("detalhe")
                or fundamento
                or ""
            )

            atleta_nome = detalhes.get("atleta_nome") or ""
            numero = detalhes.get("atleta_numero") or detalhes.get("numero") or None
            atleta_id = detalhes.get("atleta_id") or None

            numero_final = None
            if numero not in (None, ""):
                try:
                    numero_final = int(str(numero).strip())
                except Exception:
                    numero_final = None

            atleta_id_final = None
            if atleta_id not in (None, ""):
                try:
                    atleta_id_final = int(str(atleta_id).strip())
                except Exception:
                    atleta_id_final = None

            try:
                detalhes_json = json.dumps(detalhes, ensure_ascii=False)
            except Exception:
                detalhes_json = "{}"

            print("➡️ antes INSERT evento", flush=True)
            cur.execute("""
                INSERT INTO eventos (
                    partida_id, competicao, set_numero, equipe,
                    tipo, tipo_evento, fundamento, resultado, detalhe,
                    atleta_id, atleta_nome, numero, detalhes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                partida_id,
                competicao,
                set_atual,
                equipe_scout,
                tipo or "ponto",
                tipo or "ponto",
                str(fundamento or "").strip(),
                str(resultado or "").strip(),
                str(detalhe or "").strip(),
                atleta_id_final,
                str(atleta_nome or "").strip(),
                numero_final,
                detalhes_json,
            ))
            print("✅ depois INSERT evento", flush=True)

            if (pontos_a >= pontos_set or pontos_b >= pontos_set) and abs(pontos_a - pontos_b) >= diferenca_minima:
                fim_set = True
                vencedor_set = "A" if pontos_a > pontos_b else "B"

                if vencedor_set == "A":
                    sets_a += 1
                else:
                    sets_b += 1

                fim_jogo = sets_a >= sets_para_vencer or sets_b >= sets_para_vencer

                if fim_jogo:
                    vencedor_partida = "A" if sets_a > sets_b else "B"

                if set_atual < 1:
                    set_atual = 1
                if set_atual > 3:
                    set_atual = 3

                coluna_a = f"set{set_atual}_a"
                coluna_b = f"set{set_atual}_b"

                if fim_jogo:
                    print("➡️ antes UPDATE finaliza jogo", flush=True)
                    cur.execute(f"""
                        UPDATE partidas
                        SET pontos_a = %s,
                            pontos_b = %s,
                            sets_a = %s,
                            sets_b = %s,
                            {coluna_a} = %s,
                            {coluna_b} = %s,
                            saque_atual = %s,
                            rotacao_a = %s,
                            rotacao_b = %s,
                            rotacao_a_json = %s,
                            rotacao_b_json = %s,
                            status = 'finalizada',
                            status_jogo = 'finalizada',
                            fase_partida = 'encerrado',
                            status_operacao = 'finalizada',
                            vencedor = %s,
                            data_fim = NOW(),
                            tipo_encerramento = 'normal'
                        WHERE id = %s
                          AND competicao = %s
                    """, (
                        pontos_a,
                        pontos_b,
                        sets_a,
                        sets_b,
                        pontos_a,
                        pontos_b,
                        saque_depois,
                        rotacao_a,
                        rotacao_b,
                        json.dumps(rotacao_a, ensure_ascii=False),
                        json.dumps(rotacao_b, ensure_ascii=False),
                        vencedor_partida,
                        partida_id,
                        competicao,
                    ))
                    print("✅ depois UPDATE finaliza jogo", flush=True)
                else:
                    print("➡️ antes UPDATE finaliza set", flush=True)
                    cur.execute(f"""
                        UPDATE partidas
                        SET pontos_a = 0,
                            pontos_b = 0,
                            sets_a = %s,
                            sets_b = %s,
                            set_atual = %s,
                            {coluna_a} = %s,
                            {coluna_b} = %s,
                            saque_atual = %s,
                            rotacao_a = %s,
                            rotacao_b = %s,
                            rotacao_a_json = %s,
                            rotacao_b_json = %s,
                            status_jogo = 'em_andamento',
                            fase_partida = 'jogo'
                        WHERE id = %s
                          AND competicao = %s
                    """, (
                        sets_a,
                        sets_b,
                        set_atual + 1,
                        pontos_a,
                        pontos_b,
                        saque_depois,
                        rotacao_a,
                        rotacao_b,
                        json.dumps(rotacao_a, ensure_ascii=False),
                        json.dumps(rotacao_b, ensure_ascii=False),
                        partida_id,
                        competicao,
                    ))
                    print("✅ depois UPDATE finaliza set", flush=True)

                    pontos_a = 0
                    pontos_b = 0
                    set_atual += 1
            else:
                print("➡️ antes UPDATE ponto normal", flush=True)
                cur.execute("""
                    UPDATE partidas
                    SET pontos_a = %s,
                        pontos_b = %s,
                        saque_atual = %s,
                        rotacao_a = %s,
                        rotacao_b = %s,
                        rotacao_a_json = %s,
                        rotacao_b_json = %s,
                        status_jogo = 'em_andamento',
                        fase_partida = 'jogo'
                    WHERE id = %s
                      AND competicao = %s
                """, (
                    pontos_a,
                    pontos_b,
                    saque_depois,
                    rotacao_a,
                    rotacao_b,
                    json.dumps(rotacao_a, ensure_ascii=False),
                    json.dumps(rotacao_b, ensure_ascii=False),
                    partida_id,
                    competicao,
                ))
                print("✅ depois UPDATE ponto normal", flush=True)

            try:
                validacao_a = validar_rotacao_oficial(rotacao_a)
                validacao_b = validar_rotacao_oficial(rotacao_b)

                irregularidade = not validacao_a.get("ok") or not validacao_b.get("ok")
                mensagens = []

                if not validacao_a.get("ok"):
                    mensagens.extend([f"Equipe A: {e}" for e in validacao_a.get("erros", [])])

                if not validacao_b.get("ok"):
                    mensagens.extend([f"Equipe B: {e}" for e in validacao_b.get("erros", [])])

                mensagem_rotacao = " | ".join(mensagens)

                print("➡️ antes INSERT historico_rotacao", flush=True)
                cur.execute("""
                    INSERT INTO historico_rotacao (
                        partida_id, competicao, set_numero,
                        ponto_a, ponto_b,
                        equipe_ponto,
                        saque_antes, saque_depois,
                        girou, equipe_girou,
                        rotacao_a_antes, rotacao_b_antes,
                        rotacao_a_depois, rotacao_b_depois,
                        irregularidade, tipo_irregularidade, mensagem
                    )
                    VALUES (
                        %s, %s, %s,
                        %s, %s,
                        %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s
                    )
                """, (
                    partida_id,
                    competicao,
                    set_atual,
                    pontos_a,
                    pontos_b,
                    equipe_pontuadora,
                    saque_antes,
                    saque_depois,
                    girou,
                    equipe_girou,
                    rotacao_a_antes,
                    rotacao_b_antes,
                    rotacao_a,
                    rotacao_b,
                    irregularidade,
                    "rotacao_invalida" if irregularidade else "",
                    mensagem_rotacao or ("Giro realizado." if girou else "Equipe manteve o saque."),
                ))
                print("✅ depois INSERT historico_rotacao", flush=True)
            except Exception as e:
                print("⚠️ erro historico_rotacao:", repr(e), flush=True)

        print("➡️ antes COMMIT", flush=True)
        conn.commit()
        print("✅ depois COMMIT", flush=True)

    historico = []
    try:
        historico = _montar_historico_resumido_partida(partida_id, competicao, limite=5)
    except Exception as e:
        print("⚠️ erro montar historico:", repr(e), flush=True)

    return True, {
        "mensagem": "Jogo finalizado." if fim_jogo else ("Set finalizado." if fim_set else "Ponto registrado."),
        "competicao": competicao,
        "partida_id": partida_id,
        "equipe_a": partida.get("equipe_a") or partida.get("equipe_a_operacional") or "",
        "equipe_b": partida.get("equipe_b") or partida.get("equipe_b_operacional") or "",
        "pontos_a": pontos_a,
        "pontos_b": pontos_b,
        "placar_a": pontos_a,
        "placar_b": pontos_b,
        "sets_a": sets_a,
        "sets_b": sets_b,
        "set_atual": set_atual,
        "set1_a": partida.get("set1_a"),
        "set1_b": partida.get("set1_b"),
        "set2_a": partida.get("set2_a"),
        "set2_b": partida.get("set2_b"),
        "set3_a": partida.get("set3_a"),
        "set3_b": partida.get("set3_b"),
        "saque_atual": saque_depois,
        "status_jogo": "finalizada" if fim_jogo else "em_andamento",
        "fase_partida": "encerrado" if fim_jogo else "jogo",
        "fim_set": fim_set,
        "fim_jogo": fim_jogo,
        "set_finalizado": fim_set,
        "partida_finalizada": fim_jogo,
        "abrir_observacoes": fim_jogo,
        "tipo_encerramento": "normal" if fim_jogo else None,
        "vencedor_set": vencedor_set,
        "vencedor_partida": vencedor_partida,
        "rotacao_a": rotacao_a,
        "rotacao_b": rotacao_b,
        "girou": girou,
        "equipe_girou": equipe_girou,
        "ultima_acao": "Jogo finalizado" if fim_jogo else ("Set finalizado" if fim_set else "Ponto registrado"),
        "historico": historico,
    }

def registrar_wo_partida(partida_id, competicao, vencedor_lado):
    print("🟢 WO - entrou registrar_wo_partida", flush=True)

    criar_campos_jogo_partida()
    criar_campos_sets_partida()

    vencedor_lado = (vencedor_lado or "").strip().upper()

    if vencedor_lado not in {"A", "B"}:
        return False, "Vencedor inválido."

    regras = _regras_jogo_competicao(competicao)
    sets_para_vencer = int(regras.get("sets_para_vencer") or 2)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    equipe_a,
                    equipe_b,
                    equipe_a_operacional,
                    equipe_b_operacional,
                    status_jogo
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))

            partida = cur.fetchone()

            if not partida:
                return False, "Partida não encontrada."

            if (partida.get("status_jogo") or "").lower() == "finalizada":
                return False, "Partida já finalizada."

            equipe_a_nome = partida.get("equipe_a") or partida.get("equipe_a_operacional") or ""
            equipe_b_nome = partida.get("equipe_b") or partida.get("equipe_b_operacional") or ""

            if vencedor_lado == "A":
                vencedor_nome = equipe_a_nome
                sets_a = sets_para_vencer
                sets_b = 0

                set1_a, set1_b = 21, 0
                set2_a, set2_b = (21, 0) if sets_para_vencer >= 2 else (None, None)
                set3_a, set3_b = None, None

            else:
                vencedor_nome = equipe_b_nome
                sets_a = 0
                sets_b = sets_para_vencer

                set1_a, set1_b = 0, 21
                set2_a, set2_b = (0, 21) if sets_para_vencer >= 2 else (None, None)
                set3_a, set3_b = None, None

            cur.execute("""
                UPDATE partidas
                SET
                    pontos_a = 0,
                    pontos_b = 0,

                    sets_a = %s,
                    sets_b = %s,
                    set_atual = 1,

                    set1_a = %s,
                    set1_b = %s,
                    set2_a = %s,
                    set2_b = %s,
                    set3_a = %s,
                    set3_b = %s,

                    status = 'finalizada',
                    status_jogo = 'finalizada',
                    fase_partida = 'encerrado',
                    status_operacao = 'finalizada',

                    vencedor = %s,
                    data_fim = NOW(),
                    tipo_encerramento = 'WO'
                WHERE id = %s
                  AND competicao = %s
            """, (
                sets_a,
                sets_b,
                set1_a,
                set1_b,
                set2_a,
                set2_b,
                set3_a,
                set3_b,
                vencedor_nome,
                partida_id,
                competicao
            ))

        conn.commit()

    return True, {
        "mensagem": "Partida encerrada por WO.",

        "competicao": competicao,
        "partida_id": partida_id,

        "equipe_a": equipe_a_nome,
        "equipe_b": equipe_b_nome,

        "pontos_a": 0,
        "pontos_b": 0,
        "placar_a": 0,
        "placar_b": 0,

        "sets_a": sets_a,
        "sets_b": sets_b,
        "set_atual": 1,

        "set1_a": set1_a,
        "set1_b": set1_b,
        "set2_a": set2_a,
        "set2_b": set2_b,
        "set3_a": set3_a,
        "set3_b": set3_b,

        "saque_atual": "",
        "status_jogo": "finalizada",

        "fim_set": True,
        "fim_jogo": True,
        "set_finalizado": True,
        "partida_finalizada": True,
        "abrir_observacoes": True,

        "tipo_encerramento": "WO",
        "vencedor_partida": vencedor_nome,
        "vencedor_lado": vencedor_lado,

        "rotacao_a": ["", "", "", "", "", ""],
        "rotacao_b": ["", "", "", "", "", ""],

        "ultima_acao": "Partida encerrada por WO",
        "historico": _montar_historico_resumido_partida(partida_id, competicao, limite=5),
    }


def registrar_substituicao_partida(partida_id, competicao, equipe, numero_sai, numero_entra):
    criar_tabela_eventos()
    criar_campos_jogo_partida()
    criar_campos_sets_partida()

    equipe = (equipe or '').strip().upper()
    if equipe not in {'A', 'B'}:
        return False, 'Equipe inválida.'

    numero_sai = str(numero_sai or '').strip()
    numero_entra = str(numero_entra or '').strip()

    if not numero_sai or not numero_entra:
        return False, 'Informe corretamente quem sai e quem entra.'

    if numero_sai == numero_entra:
        return False, 'O atleta que entra deve ser diferente do atleta que sai.'

    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return False, 'Partida não encontrada.'

    estado = buscar_estado_jogo_partida(partida_id, competicao)
    if not estado:
        return False, 'Estado da partida não encontrado.'

    limite = int(estado.get('limite_substituicoes') or 6)
    subs_usadas = int(estado.get('subs_a') or 0) if equipe == 'A' else int(estado.get('subs_b') or 0)

    if subs_usadas >= limite:
        return False, 'Limite de substituições atingido neste set.'

    equipe_nome = partida.get('equipe_a_operacional') if equipe == 'A' else partida.get('equipe_b_operacional')

    elenco = listar_atletas_aprovados_da_equipe(equipe_nome, competicao) if equipe_nome else []
    atletas_validos = {}

    for atleta in elenco:
        numero = atleta.get('numero')
        if numero in (None, ''):
            continue
        atletas_validos[str(numero).strip()] = atleta

    if numero_sai not in atletas_validos:
        return False, 'O atleta que sai não pertence à equipe ou não possui número válido.'

    if numero_entra not in atletas_validos:
        return False, 'O atleta que entra não pertence à equipe ou não possui número válido.'

    set_atual = int(partida.get('set_atual') or 1)

    if atleta_bloqueado(numero_entra, estado, set_atual):
        return False, 'Esse atleta está bloqueado por sanção e não pode entrar.'

    rotacao_atual = list(estado.get('rotacao_a') or []) if equipe == 'A' else list(estado.get('rotacao_b') or [])
    rotacao_str = [str(x).strip() for x in rotacao_atual if str(x).strip()]

    if len(rotacao_str) < 6:
        try:
            contexto = reconstruir_contexto_rotacao_set(partida_id, competicao) or {}
            rotacao_atual = list(contexto.get('rotacao_a') or []) if equipe == 'A' else list(contexto.get('rotacao_b') or [])
            rotacao_str = [str(x).strip() for x in rotacao_atual if str(x).strip()]
        except Exception:
            pass

    if len(rotacao_str) < 6:
        try:
            papeleta = listar_papeleta(partida_id, competicao, equipe_nome, set_atual) or []
            mapa = {
                int(row['posicao']): str(row['numero']).strip()
                for row in papeleta
                if row.get('numero') not in (None, '')
            }

            rotacao_atual = [
                mapa.get(4, ''),
                mapa.get(3, ''),
                mapa.get(2, ''),
                mapa.get(5, ''),
                mapa.get(6, ''),
                mapa.get(1, ''),
            ]

            rotacao_str = [str(x).strip() for x in rotacao_atual if str(x).strip()]
        except Exception:
            pass

    if numero_sai not in rotacao_str:
        return False, 'O atleta que sai não está em quadra.'

    if numero_entra in rotacao_str:
        return False, 'O atleta que entra já está em quadra.'

    while len(rotacao_atual) < 6:
        rotacao_atual.append('')

    pos_real = None
    for i, valor in enumerate(rotacao_atual):
        if str(valor).strip() == numero_sai:
            pos_real = i
            break

    if pos_real is None:
        return False, 'Não foi possível identificar a posição do atleta em quadra.'

    rotacao_atual[pos_real] = numero_entra

    status_jogadores_a = dict(estado.get('status_jogadores_a') or {})
    status_jogadores_b = dict(estado.get('status_jogadores_b') or {})

    status_alvo = status_jogadores_a if equipe == 'A' else status_jogadores_b

    status_sai = dict(status_alvo.get(numero_sai) or {})
    status_entra = dict(status_alvo.get(numero_entra) or {})

    titulares_iniciais = set(
        str(x).strip()
        for x in (
            estado.get('titulares_iniciais_a', []) if equipe == 'A'
            else estado.get('titulares_iniciais_b', [])
        )
        if str(x).strip()
    )

    # Quem entra fica marcado como substituto: vermelho na quadra
    status_entra['em_quadra'] = True
    status_entra['tipo'] = 'substituto'
    status_entra['vinculo'] = numero_sai

    # Quem sai fica marcado como retorno se era titular inicial: verde quando voltar
    status_sai['em_quadra'] = False
    status_sai['tipo'] = 'retorno' if numero_sai in titulares_iniciais else ''
    status_sai['vinculo'] = numero_entra

    status_alvo[numero_sai] = status_sai
    status_alvo[numero_entra] = status_entra

    subs_a = int(estado.get('subs_a') or 0)
    subs_b = int(estado.get('subs_b') or 0)

    if equipe == 'A':
        subs_a += 1
        nova_rotacao_a = rotacao_atual
        nova_rotacao_b = list(estado.get('rotacao_b') or [])
    else:
        subs_b += 1
        nova_rotacao_a = list(estado.get('rotacao_a') or [])
        nova_rotacao_b = rotacao_atual

    registrar_evento_partida(
        partida_id,
        competicao,
        set_atual,
        equipe,
        'substituicao',
        detalhe=f'{numero_sai}>{numero_entra}',
        numero=numero_entra
    )

    snapshot = {
        'saque_atual': estado.get('saque_atual'),
        'status_jogo': estado.get('status_jogo'),
        'fase_partida': estado.get('fase_partida') or 'jogo',
        'rotacao_a': nova_rotacao_a,
        'rotacao_b': nova_rotacao_b,
        'status_jogadores_a': status_jogadores_a,
        'status_jogadores_b': status_jogadores_b,
        'subs_a': subs_a,
        'subs_b': subs_b,
        'titulares_iniciais_a': estado.get('titulares_iniciais_a', []),
        'titulares_iniciais_b': estado.get('titulares_iniciais_b', []),
        'vinculos_titular_reserva_a': estado.get('vinculos_titular_reserva_a', {}),
        'vinculos_titular_reserva_b': estado.get('vinculos_titular_reserva_b', {}),
        'vinculos_reserva_titular_a': estado.get('vinculos_reserva_titular_a', {}),
        'vinculos_reserva_titular_b': estado.get('vinculos_reserva_titular_b', {}),
        'substituicao_forcada': estado.get('substituicao_forcada', {}),
        'bloqueios': estado.get('bloqueios', {}),
        'retardamentos_a': estado.get('retardamentos_a', []),
        'retardamentos_b': estado.get('retardamentos_b', []),
        'subs_excepcionais': estado.get('subs_excepcionais', []),
        'sancoes_a': estado.get('sancoes_a', []),
        'sancoes_b': estado.get('sancoes_b', []),
        'cartoes_verdes_a': estado.get('cartoes_verdes_a', []),
        'cartoes_verdes_b': estado.get('cartoes_verdes_b', []),
    }

    _salvar_snapshot_estado_jogo(partida_id, competicao, snapshot)

    tempos = buscar_tempos_restantes_partida(partida_id, competicao)

    historico = []
    ultima_acao = f'Substituição {equipe}: #{numero_sai} → #{numero_entra}'

    try:
        eventos = listar_eventos_partida(partida_id, competicao, limite=5) or []

        for ev in eventos:
            descricao = (ev.get("descricao") or "").strip()

            if not descricao:
                tipo_evento = str(ev.get("tipo_evento") or ev.get("tipo") or "").strip()
                equipe_ev = str(ev.get("equipe") or "").strip()
                detalhe_ev = str(ev.get("detalhe") or ev.get("detalhes") or "").strip()
                numero_ev = str(ev.get("numero") or "").strip()

                partes = []
                if tipo_evento:
                    partes.append(tipo_evento.replace("_", " ").title())
                if equipe_ev:
                    partes.append(f"Equipe {equipe_ev}")
                if detalhe_ev:
                    partes.append(detalhe_ev.replace("_", " "))
                if numero_ev:
                    partes.append(f"#{numero_ev}")

                descricao = " • ".join([p for p in partes if p]) or "Ação registrada"

            historico.append({"descricao": descricao})

        if historico:
            ultima_acao = historico[0]["descricao"]

    except Exception:
        historico = [{"descricao": ultima_acao}]

    resposta = {
        'mensagem': 'Substituição registrada.',
        'pontos_a': int(partida.get('pontos_a') or 0),
        'pontos_b': int(partida.get('pontos_b') or 0),
        'sets_a': int(partida.get('sets_a') or 0),
        'sets_b': int(partida.get('sets_b') or 0),
        'set_atual': set_atual,
        'saque_atual': estado.get('saque_atual') or '',
        'status_jogo': estado.get('status_jogo') or 'em_andamento',
        'fase_partida': estado.get('fase_partida') or 'jogo',
        'partida_finalizada': False,
        'rotacao_a': nova_rotacao_a,
        'rotacao_b': nova_rotacao_b,
        'tempos_a': tempos.get('tempos_a'),
        'tempos_b': tempos.get('tempos_b'),
        'subs_a': subs_a,
        'subs_b': subs_b,
        'limite_substituicoes': limite,
        'status_jogadores_a': status_jogadores_a,
        'status_jogadores_b': status_jogadores_b,
        'sancoes_a': estado.get('sancoes_a', []),
        'sancoes_b': estado.get('sancoes_b', []),
        'cartoes_verdes_a': estado.get('cartoes_verdes_a', []),
        'cartoes_verdes_b': estado.get('cartoes_verdes_b', []),
        'bloqueios': estado.get('bloqueios', {}),
        'substituicao_forcada': estado.get('substituicao_forcada', {}),
        'retardamentos_a': estado.get('retardamentos_a', []),
        'retardamentos_b': estado.get('retardamentos_b', []),
        'subs_excepcionais': estado.get('subs_excepcionais', []),
        'historico': historico,
        'ultima_acao': ultima_acao,
    }

    _emitir_estado_tempo_real(partida_id, competicao)

    return True, resposta
    
        
def registrar_substituicao_excepcional_partida(partida_id, competicao, equipe, numero_sai, numero_entra, motivo='', observacao=''):
    criar_tabela_eventos()
    criar_campos_jogo_partida()
    criar_campos_sets_partida()

    equipe = (equipe or '').strip().upper()
    numero_sai = str(numero_sai or '').strip()
    numero_entra = str(numero_entra or '').strip()
    motivo = (motivo or '').strip().lower()
    observacao = (observacao or '').strip()

    if equipe not in {'A', 'B'}:
        return False, 'Equipe inválida.'
    if not numero_sai or not numero_entra:
        return False, 'Informe quem sai e quem entra.'
    if numero_sai == numero_entra:
        return False, 'A troca excepcional precisa envolver atletas diferentes.'

    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return False, 'Partida não encontrada.'

    estado = buscar_estado_jogo_partida(partida_id, competicao)
    if not estado:
        return False, 'Estado da partida não encontrado.'

    set_atual = int(partida.get('set_atual') or 1)
    rotacao_atual = list(estado.get('rotacao_a') if equipe == 'A' else estado.get('rotacao_b') or ["", "", "", "", "", ""])
    status_jogadores = dict(estado.get('status_jogadores_a') if equipe == 'A' else estado.get('status_jogadores_b') or {})

    if numero_sai not in [str(x).strip() for x in rotacao_atual]:
        return False, 'O atleta que sai precisa estar em quadra.'
    if numero_entra in [str(x).strip() for x in rotacao_atual]:
        return False, 'O atleta que entra precisa estar fora de quadra.'
    if atleta_bloqueado(numero_entra, estado, set_atual):
        return False, 'O atleta que entra está bloqueado para este jogo.'

    equipe_nome = partida.get('equipe_a_operacional') if equipe == 'A' else partida.get('equipe_b_operacional')
    atletas = listar_atletas_aprovados_da_equipe(equipe_nome, competicao) or []
    numeros_elenco = {str(a.get('numero') or '').strip() for a in atletas}
    if numero_entra not in numeros_elenco:
        return False, 'O atleta que entra não pertence ao elenco aprovado da equipe.'

    rotacao_nova = [numero_entra if str(n).strip() == numero_sai else n for n in rotacao_atual]
    status_jogadores[numero_entra] = {'tipo': 'substituto', 'vinculo': numero_sai, 'excepcional': True}
    status_jogadores[numero_sai] = {'tipo': 'bloqueado_excepcional', 'motivo': motivo or 'excepcional'}

    
    detalhe = f"#{numero_sai} → #{numero_entra}"
    if motivo:
        detalhe += f" | motivo: {motivo}"
    if observacao:
        detalhe += f" | obs: {observacao}"

    registrar_evento_partida(
        partida_id,
        competicao,
        set_atual,
        equipe,
        "substituicao_excepcional",
        fundamento="excepcional",
        detalhe=detalhe,
        numero=numero_entra
    )

    snapshot = {
        'saque_atual': estado.get('saque_atual') or '',
        'status_jogo': estado.get('status_jogo') or 'pre_jogo',
        'rotacao_a': rotacao_nova if equipe == 'A' else list(estado.get('rotacao_a') or ["", "", "", "", "", ""]),
        'rotacao_b': rotacao_nova if equipe == 'B' else list(estado.get('rotacao_b') or ["", "", "", "", "", ""]),
        'status_jogadores_a': status_jogadores if equipe == 'A' else dict(estado.get('status_jogadores_a') or {}),
        'status_jogadores_b': status_jogadores if equipe == 'B' else dict(estado.get('status_jogadores_b') or {}),
        'subs_a': int(estado.get('subs_a') or 0),
        'subs_b': int(estado.get('subs_b') or 0),
        'titulares_iniciais_a': estado.get('titulares_iniciais_a', []),
        'titulares_iniciais_b': estado.get('titulares_iniciais_b', []),
        'vinculos_titular_reserva_a': dict(estado.get('vinculos_titular_reserva_a') or {}),
        'vinculos_titular_reserva_b': dict(estado.get('vinculos_titular_reserva_b') or {}),
        'vinculos_reserva_titular_a': dict(estado.get('vinculos_reserva_titular_a') or {}),
        'vinculos_reserva_titular_b': dict(estado.get('vinculos_reserva_titular_b') or {}),
        'sancoes_a': estado.get('sancoes_a', []),
        'sancoes_b': estado.get('sancoes_b', []),
        'cartoes_verdes_a': estado.get('cartoes_verdes_a', []),
        'cartoes_verdes_b': estado.get('cartoes_verdes_b', []),
        'bloqueios': dict(estado.get('bloqueios') or {}),
        'substituicao_forcada': dict(estado.get('substituicao_forcada') or {}),
        'retardamentos_a': list(estado.get('retardamentos_a') or []),
        'retardamentos_b': list(estado.get('retardamentos_b') or []),
        'subs_excepcionais': list(estado.get('subs_excepcionais') or []) + [{
            'equipe': equipe, 'numero_sai': numero_sai, 'numero_entra': numero_entra, 'motivo': motivo, 'observacao': observacao, 'set_numero': set_atual
        }],
    }
    snapshot['bloqueios'][numero_sai] = {'tipo': 'substituicao_excepcional', 'escopo': 'partida', 'set_numero': set_atual}
    _salvar_snapshot_estado_jogo(partida_id, competicao, snapshot)

    estado_atualizado = buscar_estado_jogo_partida(partida_id, competicao)
    tempos = buscar_tempos_restantes_partida(partida_id, competicao)

    return True, {
        'mensagem': 'Substituição excepcional registrada.',
        'pontos_a': int(estado_atualizado.get('pontos_a') or 0),
        'pontos_b': int(estado_atualizado.get('pontos_b') or 0),
        'sets_a': int(estado_atualizado.get('sets_a') or 0),
        'sets_b': int(estado_atualizado.get('sets_b') or 0),
        'set_atual': int(estado_atualizado.get('set_atual') or 1),
        'saque_atual': estado_atualizado.get('saque_atual') or '',
        'status_jogo': estado_atualizado.get('status_jogo') or 'pre_jogo',
        'partida_finalizada': (estado_atualizado.get('status_jogo') or '').lower() == 'finalizada',
        'rotacao_a': estado_atualizado.get('rotacao_a', ['', '', '', '', '', '']),
        'rotacao_b': estado_atualizado.get('rotacao_b', ['', '', '', '', '', '']),
        'tempos_a': tempos.get('tempos_a'),
        'tempos_b': tempos.get('tempos_b'),
        'subs_a': int(estado_atualizado.get('subs_a') or 0),
        'subs_b': int(estado_atualizado.get('subs_b') or 0),
        'limite_substituicoes': int(estado_atualizado.get('limite_substituicoes') or 6),
        'status_jogadores_a': estado_atualizado.get('status_jogadores_a', {}),
        'status_jogadores_b': estado_atualizado.get('status_jogadores_b', {}),
        'sancoes_a': estado_atualizado.get('sancoes_a', []),
        'sancoes_b': estado_atualizado.get('sancoes_b', []),
        'cartoes_verdes_a': estado_atualizado.get('cartoes_verdes_a', []),
        'cartoes_verdes_b': estado_atualizado.get('cartoes_verdes_b', []),
        'bloqueios': estado_atualizado.get('bloqueios', {}),
        'substituicao_forcada': estado_atualizado.get('substituicao_forcada', {}),
        'retardamentos_a': estado_atualizado.get('retardamentos_a', []),
        'retardamentos_b': estado_atualizado.get('retardamentos_b', []),
        'subs_excepcionais': estado_atualizado.get('subs_excepcionais', []),
    }


def registrar_retardamento_partida(partida_id, competicao, equipe, observacao=''):
    criar_tabela_eventos()
    criar_campos_jogo_partida()
    criar_campos_sets_partida()

    equipe = (equipe or '').strip().upper()
    observacao = (observacao or '').strip()
    if equipe not in {'A', 'B'}:
        return False, 'Equipe inválida.'

    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return False, 'Partida não encontrada.'

    estado = buscar_estado_jogo_partida(partida_id, competicao)
    if not estado:
        return False, 'Estado da partida não encontrado.'

    chave = 'retardamentos_a' if equipe == 'A' else 'retardamentos_b'
    quantidade = len(list(estado.get(chave) or []))
    tipo_retardamento = 'advertencia' if quantidade == 0 else 'penalidade'
    set_atual = int(partida.get('set_atual') or 1)
    detalhes = {'tipo_retardamento': tipo_retardamento, 'observacao': observacao}
    registrar_evento_partida(
        partida_id,
        competicao,
        set_atual,
        equipe,
        "retardamento",
        detalhe=tipo_retardamento
    )

    estado = _reconstruir_e_salvar_snapshot(partida_id, competicao, buscar_partida_operacional(partida_id, competicao))

    if tipo_retardamento == 'advertencia':
        tempos = buscar_tempos_restantes_partida(partida_id, competicao)
        estado['tempos_a'] = tempos.get('tempos_a')
        estado['tempos_b'] = tempos.get('tempos_b')
        estado['mensagem'] = 'Retardamento (advertência) registrado.'
        estado['ultima_acao'] = _montar_ultima_acao_partida(partida, 'retardamento', equipe=equipe, detalhes=detalhes)
        estado['partida_finalizada'] = (estado.get('status_jogo') or '').lower() == 'finalizada'
        return True, estado

    adversario = 'B' if equipe == 'A' else 'A'
    ok, resultado = registrar_ponto_partida(partida_id, competicao, adversario, tipo='retardamento_penalidade', detalhes={
        'origem_retardamento': True,
        'tipo_lance': 'falta',
        'detalhe_lance': 'retardamento',
        'fundamento': 'retardamento',
        'resultado': 'erro',
        'responsavel_lado': equipe,
        'observacao': observacao,
    })
    if not ok:
        return False, resultado
    resultado['mensagem'] = 'Retardamento (penalidade) registrado.'
    resultado['ultima_acao'] = _montar_ultima_acao_partida(partida, 'retardamento', equipe=equipe, detalhes={'tipo_retardamento': 'penalidade', 'observacao': observacao})
    return True, resultado


def desfazer_ultima_acao_partida(partida_id, competicao):
    criar_tabela_eventos()
    criar_campos_jogo_partida()
    criar_campos_sets_partida()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))
            partida = cur.fetchone()

            if not partida:
                return False, "Partida não encontrada."

            cur.execute("""
                SELECT id, tipo
                FROM eventos
                WHERE partida_id = %s
                  AND competicao = %s
                ORDER BY id DESC
                LIMIT 3
            """, (partida_id, competicao))
            recentes = cur.fetchall()

            if not recentes:
                return False, "Nenhuma ação para desfazer."

            ids_para_remover = []
            for evento in recentes:
                tipo = (evento.get("tipo") or "").strip().lower()
                if tipo in {"fim_partida", "fim_set"}:
                    ids_para_remover.append(evento["id"])
                    continue
                if tipo == "retardamento_penalidade":
                    ids_para_remover.append(evento["id"])
                    continue
                if tipo in {"ponto", "tempo", "substituicao", "substituicao_excepcional", "retardamento"}:
                    ids_para_remover.append(evento["id"])
                break

            if not ids_para_remover:
                ids_para_remover.append(recentes[0]["id"])

            cur.execute(
                f"DELETE FROM eventos WHERE id IN ({', '.join(['%s'] * len(ids_para_remover))})",
                tuple(ids_para_remover)
            )
        conn.commit()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE partidas
                SET pontos_a = 0,
                    pontos_b = 0,
                    sets_a = 0,
                    sets_b = 0,
                    set_atual = 1,
                    saque_atual = NULL,
                    status_jogo = 'pre_jogo',
                    status = CASE WHEN status = 'finalizada' THEN 'em_andamento' ELSE status END,
                    status_operacao = CASE WHEN status_operacao = 'finalizada' THEN 'em_andamento' ELSE status_operacao END
                WHERE id = %s
                  AND competicao = %s
            """, (partida_id, competicao))
        conn.commit()

    partida_reconstruida = buscar_partida_operacional(partida_id, competicao)
    estado = _reconstruir_e_salvar_snapshot(partida_id, competicao, partida_reconstruida)
    tempos = buscar_tempos_restantes_partida(partida_id, competicao)

    return True, {
        "mensagem": "Última ação desfeita.",
        "pontos_a": int(estado.get("pontos_a") or 0),
        "pontos_b": int(estado.get("pontos_b") or 0),
        "sets_a": int(estado.get("sets_a") or 0),
        "sets_b": int(estado.get("sets_b") or 0),
        "set_atual": int(estado.get("set_atual") or 1),
        "saque_atual": estado.get("saque_atual") or "",
        "status_jogo": estado.get("status_jogo") or "pre_jogo",
        "partida_finalizada": (estado.get("status_jogo") or "").lower() == "finalizada",
        "rotacao_a": estado.get("rotacao_a", ["", "", "", "", "", ""]),
        "rotacao_b": estado.get("rotacao_b", ["", "", "", "", "", ""]),
        "tempos_a": tempos.get("tempos_a"),
        "tempos_b": tempos.get("tempos_b"),
        "subs_a": int(estado.get("subs_a") or 0),
        "subs_b": int(estado.get("subs_b") or 0),
        "limite_substituicoes": int(estado.get("limite_substituicoes") or 6),
        "status_jogadores_a": estado.get("status_jogadores_a", {}),
        "status_jogadores_b": estado.get("status_jogadores_b", {}),
        "sancoes_a": estado.get("sancoes_a", []),
        "sancoes_b": estado.get("sancoes_b", []),
        "cartoes_verdes_a": estado.get("cartoes_verdes_a", []),
        "cartoes_verdes_b": estado.get("cartoes_verdes_b", []),
        "bloqueios": estado.get("bloqueios", {}),
        "substituicao_forcada": estado.get("substituicao_forcada", {}),
        "retardamentos_a": estado.get("retardamentos_a", []),
        "retardamentos_b": estado.get("retardamentos_b", []),
        "subs_excepcionais": estado.get("subs_excepcionais", []),
    }

def registrar_tempo_partida(partida_id, competicao, equipe):
    criar_tabela_eventos()

    equipe = (equipe or "").strip().upper()
    if equipe not in {"A", "B"}:
        return False, "Equipe inválida."

    with conectar() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT set_atual
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
            """, (partida_id, competicao))

            partida = cur.fetchone()
            if not partida:
                return False, "Partida não encontrada."

            set_atual = int(partida.get("set_atual") or 1)

            cur.execute("""
                SELECT tempos_por_set
                FROM competicoes
                WHERE nome = %s
                LIMIT 1
            """, (competicao,))

            regra = cur.fetchone()
            limite = int((regra or {}).get("tempos_por_set") or 2)

            cur.execute("""
                SELECT COUNT(*) AS total
                FROM eventos
                WHERE partida_id = %s
                AND competicao = %s
                AND set_numero = %s
                AND equipe = %s
                AND tipo = 'tempo'
            """, (partida_id, competicao, set_atual, equipe))

            usados = int(cur.fetchone()["total"] or 0)

            if usados >= limite:
                return False, "Limite de tempos atingido."

            cur.execute("""
                INSERT INTO eventos (
                    partida_id, competicao, set_numero, equipe, tipo, detalhes
                )
                VALUES (%s, %s, %s, %s, 'tempo', 'pedido_tempo')
            """, (partida_id, competicao, set_atual, equipe))

        conn.commit()

    estado = buscar_estado_jogo_partida(partida_id, competicao)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    SUM(CASE WHEN equipe = 'A' THEN 1 ELSE 0 END) as tempos_a,
                    SUM(CASE WHEN equipe = 'B' THEN 1 ELSE 0 END) as tempos_b
                FROM eventos
                WHERE partida_id = %s
                  AND competicao = %s
                  AND set_numero = %s
                  AND tipo = 'tempo'
            """, (partida_id, competicao, set_atual))
            tempos = cur.fetchone()

    usados_a = int(tempos["tempos_a"] or 0)
    usados_b = int(tempos["tempos_b"] or 0)

    return True, {
        "mensagem": "Tempo solicitado.",
        "pontos_a": estado["pontos_a"],
        "pontos_b": estado["pontos_b"],
        "sets_a": estado["sets_a"],
        "sets_b": estado["sets_b"],
        "set_atual": estado["set_atual"],
        "saque_atual": estado["saque_atual"],
        "status_jogo": estado["status_jogo"],
        "tempos_a": limite - usados_a,
        "tempos_b": limite - usados_b,
        "partida_finalizada": (estado["status_jogo"] or "").lower() == "finalizada",
        "rotacao_a": estado.get("rotacao_a", ["", "", "", "", "", ""]),
        "rotacao_b": estado.get("rotacao_b", ["", "", "", "", "", ""]),
        "ultima_acao": "Ponto registrado",
        "historico": _montar_historico_resumido_partida(partida_id, competicao, limite=5),
        "subs_a": int(estado.get("subs_a") or 0),
        "subs_b": int(estado.get("subs_b") or 0),
        "limite_substituicoes": int(estado.get("limite_substituicoes") or 6),
        "status_jogadores_a": estado.get("status_jogadores_a", {}),
        "status_jogadores_b": estado.get("status_jogadores_b", {}),
        "sancoes_a": estado.get("sancoes_a", []),
        "sancoes_b": estado.get("sancoes_b", []),
        "cartoes_verdes_a": estado.get("cartoes_verdes_a", []),
        "cartoes_verdes_b": estado.get("cartoes_verdes_b", []),
        "bloqueios": estado.get("bloqueios", {}),
        "substituicao_forcada": estado.get("substituicao_forcada", {}),
        "retardamentos_a": estado.get("retardamentos_a", []),
        "retardamentos_b": estado.get("retardamentos_b", []),
        "subs_excepcionais": estado.get("subs_excepcionais", []),
    }

def buscar_tempos_restantes_partida(partida_id, competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT set_atual
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))

            partida = cur.fetchone()
            if not partida:
                return {"tempos_a": 2, "tempos_b": 2}

            set_atual = int(partida.get("set_atual") or 1)

            cur.execute("""
                SELECT tempos_por_set
                FROM competicoes
                WHERE nome = %s
                LIMIT 1
            """, (competicao,))

            regra = cur.fetchone()
            limite = int((regra or {}).get("tempos_por_set") or 2)

            cur.execute("""
                SELECT
                    SUM(CASE WHEN equipe = 'A' THEN 1 ELSE 0 END) AS tempos_a,
                    SUM(CASE WHEN equipe = 'B' THEN 1 ELSE 0 END) AS tempos_b
                FROM eventos
                WHERE partida_id = %s
                  AND competicao = %s
                  AND set_numero = %s
                  AND tipo = 'tempo'
            """, (partida_id, competicao, set_atual))

            tempos = cur.fetchone()

    usados_a = int(tempos["tempos_a"] or 0)
    usados_b = int(tempos["tempos_b"] or 0)

    return {
        "tempos_a": max(limite - usados_a, 0),
        "tempos_b": max(limite - usados_b, 0),
    }

# =========================================================
# EVENTOS DE PARTIDA (SCOUT REAL)
# =========================================================

def criar_tabela_eventos(force=False):
    if _schema_ja_pronto("tabela_eventos", force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS eventos (
                    id SERIAL PRIMARY KEY,
                    partida_id INTEGER,
                    competicao TEXT,
                    set_numero INTEGER,
                    equipe TEXT,
                    tipo TEXT,
                    tipo_evento TEXT,
                    fundamento TEXT,
                    resultado TEXT,
                    detalhe TEXT,
                    atleta_id INTEGER,
                    atleta_nome TEXT,
                    numero INTEGER,
                    detalhes TEXT,
                    criado_em TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("ALTER TABLE eventos ADD COLUMN IF NOT EXISTS tipo_evento TEXT")
            cur.execute("ALTER TABLE eventos ADD COLUMN IF NOT EXISTS detalhes TEXT")
            cur.execute("ALTER TABLE eventos ADD COLUMN IF NOT EXISTS atleta_id INTEGER")
            cur.execute("ALTER TABLE eventos ADD COLUMN IF NOT EXISTS atleta_nome TEXT")
            cur.execute("ALTER TABLE eventos ADD COLUMN IF NOT EXISTS numero INTEGER")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_eventos_partida_competicao ON eventos (partida_id, competicao, id DESC)")
        conn.commit()

    _marcar_schema_pronto("tabela_eventos")


def listar_eventos_partida(partida_id, competicao, limite=1000):
    criar_tabela_eventos()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    partida_id,
                    competicao,
                    set_numero,
                    equipe,
                    tipo,
                    tipo_evento,
                    fundamento,
                    resultado,
                    detalhe,
                    detalhes,
                    atleta_id,
                    atleta_nome,
                    numero,
                    criado_em,
                    CONCAT(
                        COALESCE(equipe, '-'),
                        ' • ',
                        COALESCE(tipo, '-'),
                        CASE WHEN COALESCE(fundamento, '') <> '' THEN ' • ' || fundamento ELSE '' END,
                        CASE WHEN COALESCE(resultado, '') <> '' THEN ' • ' || resultado ELSE '' END,
                        CASE WHEN COALESCE(detalhe, '') <> '' THEN ' • ' || detalhe ELSE '' END,
                        CASE WHEN COALESCE(numero::text, '') <> '' THEN ' • #' || numero::text ELSE '' END,
                        CASE WHEN COALESCE(atleta_nome, '') <> '' THEN ' - ' || atleta_nome ELSE '' END
                    ) AS descricao
                FROM eventos
                WHERE partida_id = %s
                  AND competicao = %s
                ORDER BY id DESC
                LIMIT %s
            """, (partida_id, competicao, int(limite or 1000)))

            return cur.fetchall()


# ================= ETAPA 2 SET FLOW =================
def verificar_fim_de_set(partida_id, competicao):
    estado = buscar_estado_jogo_partida(partida_id, competicao)
    if not estado:
        return False

    comp = buscar_competicao_por_nome(competicao) or {}
    formato = _normalizar_formato_sets(comp.get("sets_tipo"))
    diferenca_minima = int(comp.get("diferenca_minima") or 2)
    pontos_set = int(comp.get("pontos_set") or 25)
    pontos_tiebreak = int(comp.get("pontos_tiebreak") or 15)
    tem_tiebreak = bool(comp.get("tem_tiebreak", True))

    set_atual = int(estado.get("set_atual") or 1)
    pontos_a = int(estado.get("pontos_a") or 0)
    pontos_b = int(estado.get("pontos_b") or 0)

    alvo = pontos_set
    if formato == "melhor_de_3" and tem_tiebreak and set_atual == 3:
        alvo = pontos_tiebreak
    elif formato == "melhor_de_5" and tem_tiebreak and set_atual == 5:
        alvo = pontos_tiebreak

    return (pontos_a >= alvo or pontos_b >= alvo) and abs(pontos_a - pontos_b) >= diferenca_minima


def finalizar_set_e_avancar(partida_id, competicao):
    estado_antes = buscar_estado_jogo_partida(partida_id, competicao)
    if not estado_antes:
        return False, "Estado da partida não encontrado."

    pontos_a = int(estado_antes.get("pontos_a") or 0)
    pontos_b = int(estado_antes.get("pontos_b") or 0)

    if pontos_a == pontos_b:
        return False, "Não é possível finalizar set empatado."

    vencedor = "A" if pontos_a > pontos_b else "B"

    ok, msg = registrar_resultado_set(partida_id, competicao, vencedor)
    if not ok:
        return False, msg

    estado = buscar_estado_jogo_partida(partida_id, competicao) or {}
    status_jogo = (estado.get("status_jogo") or "").lower()

    retorno = {
        "set_finalizado": True,
        "partida_finalizada": status_jogo == "finalizada",
        "redirecionar_papeleta": status_jogo == "entre_sets",
        "redirecionar_tiebreak": status_jogo == "tiebreak_sorteio",
        "set_atual": int(estado.get("set_atual") or 1),
        "sets_a": int(estado.get("sets_a") or 0),
        "sets_b": int(estado.get("sets_b") or 0),
        "pontos_a": int(estado.get("pontos_a") or 0),
        "pontos_b": int(estado.get("pontos_b") or 0),
        "status_jogo": estado.get("status_jogo") or "pre_jogo",
        "ultima_acao": estado.get("ultima_acao"),
        "historico": estado.get("historico") or [],
        "saque_atual": estado.get("saque_atual"),
        "rotacao_a": estado.get("rotacao_a") or [],
        "rotacao_b": estado.get("rotacao_b") or [],
        "banco_a": estado.get("banco_a") or [],
        "banco_b": estado.get("banco_b") or [],
        "tempos_a": int(estado.get("tempos_a") or 0),
        "tempos_b": int(estado.get("tempos_b") or 0),
        "subs_a": int(estado.get("subs_a") or 0),
        "subs_b": int(estado.get("subs_b") or 0),
        "limite_substituicoes": int(estado.get("limite_substituicoes") or 6),
        "status_jogadores_a": estado.get("status_jogadores_a") or {},
        "status_jogadores_b": estado.get("status_jogadores_b") or {},
        "sancoes_a": estado.get("sancoes_a") or [],
        "sancoes_b": estado.get("sancoes_b") or [],
        "cartoes_verdes_a": estado.get("cartoes_verdes_a") or [],
        "cartoes_verdes_b": estado.get("cartoes_verdes_b") or [],
    }

    return True, retorno


# ================= TRAVAS GLOBAIS =================

def partida_encerrada(partida):
    return (partida.get("status_jogo") or "").lower() == "encerrado"


def pode_editar_pre_jogo(partida):
    return (partida.get("fase_partida") or "") == "pre_jogo"


def pode_editar_papeleta(estado):
    if not estado:
        return True
    status_jogo = str(estado.get('status_jogo') or '').strip().lower()
    if status_jogo in {'em_andamento', 'finalizada', 'encerrado'}:
        return False
    return not (estado.get("pontos_a", 0) > 0 or estado.get("pontos_b", 0) > 0)


def competicao_bloqueada(competicao):
    if competicao_esta_travada(competicao):
        return True

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM partidas
                WHERE competicao = %s
                AND status_jogo != 'nao_iniciado'
                LIMIT 1
            """, (competicao,))
            return cur.fetchone() is not None


# ================= TIEBREAK =================

def precisa_tiebreak(partida, estado):
    sets_a = estado.get("sets_a", 0)
    sets_b = estado.get("sets_b", 0)
    sets_para_vencer = partida.get("sets_para_vencer", 1)

    return sets_a == sets_b and sets_a == sets_para_vencer - 1


def salvar_sorteio_tiebreak(partida_id, competicao, lado, saque):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE estado_jogo
                SET tiebreak_lado = %s,
                    tiebreak_saque = %s,
                    tiebreak_realizado = true
                WHERE partida_id = %s AND competicao = %s
            """, (lado, saque, partida_id, competicao))
        conn.commit()


# ================= FIM DE PARTIDA =================

def verificar_fim_partida(partida, estado):
    sets_a = estado.get("sets_a", 0)
    sets_b = estado.get("sets_b", 0)
    sets_para_vencer = partida.get("sets_para_vencer", 1)

    return sets_a == sets_para_vencer or sets_b == sets_para_vencer


def encerrar_partida(partida_id, competicao, observacoes):
    from datetime import datetime

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE partidas
                SET status_jogo = 'encerrado',
                    observacoes = %s,
                    data_fim = %s
                WHERE id = %s AND competicao = %s
            """, (observacoes, datetime.now(), partida_id, competicao))
        conn.commit()


# ================= GARANTIR ESTADO =================

def garantir_estado_partida(partida_id, competicao):
    criar_campos_jogo_partida()
    criar_campos_sets_partida()
    criar_tabela_eventos()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))
            partida = cur.fetchone()

    if not partida:
        return False

    estado = _snapshot_estado_partida(partida, competicao)
    rot_a = estado.get("rotacao_a") or []
    rot_b = estado.get("rotacao_b") or []

    precisa_reconstruir = not (
        len(rot_a) == 6
        and len(rot_b) == 6
        and (
            any(str(x).strip() for x in rot_a)
            or any(str(x).strip() for x in rot_b)
            or estado.get("status_jogo") == "pre_jogo"
        )
    )

    if precisa_reconstruir:
        _reconstruir_e_salvar_snapshot(partida_id, competicao, partida)

    return True


# =========================================================
# MODO TREINADOR
# =========================================================

def criar_tabela_solicitacoes_treinador():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS solicitacoes_treinador (
                    id SERIAL PRIMARY KEY,
                    partida_id INTEGER NOT NULL,
                    competicao TEXT NOT NULL,
                    equipe TEXT NOT NULL,
                    tipo TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pendente',
                    detalhes_json TEXT NOT NULL DEFAULT '{}',
                    criado_em TIMESTAMP DEFAULT NOW()
                )
            """)
        conn.commit()


def buscar_partida_treinador_por_equipe(competicao, equipe_nome):
    """
    Encontra a partida correta para o modo treinador.
    Regra importante:
    - nunca retorna partida finalizada/encerrada;
    - prioriza a partida que o apontador colocou em operação agora;
    - se existir mais de uma partida da mesma equipe, pega a mais recente em operação
      em vez de pegar a primeira da tabela por ordem.
    """
    if not competicao or not equipe_nome:
        return None

    equipe_nome = str(equipe_nome or "").strip()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM partidas
                WHERE competicao = %s
                  AND (
                        LOWER(COALESCE(equipe_a_operacional, equipe_a, '')) = LOWER(%s)
                     OR LOWER(COALESCE(equipe_b_operacional, equipe_b, '')) = LOWER(%s)
                     OR LOWER(COALESCE(equipe_a, '')) = LOWER(%s)
                     OR LOWER(COALESCE(equipe_b, '')) = LOWER(%s)
                  )
                  AND LOWER(COALESCE(status_jogo, 'pre_jogo')) NOT IN ('finalizada', 'finalizado', 'encerrado', 'encerrada')
                  AND LOWER(COALESCE(status, '')) NOT IN ('finalizada', 'finalizado', 'encerrado', 'encerrada')
                  AND LOWER(COALESCE(fase_partida, 'pre_jogo')) NOT IN ('finalizada', 'finalizado', 'encerrado', 'encerrada')
                  AND LOWER(COALESCE(status_operacao, 'livre')) NOT IN ('finalizada', 'finalizado', 'encerrado', 'encerrada')
                ORDER BY
                    CASE
                        WHEN LOWER(COALESCE(status_jogo, '')) = 'em_andamento' THEN 1
                        WHEN LOWER(COALESCE(status_operacao, '')) = 'em_andamento' THEN 1
                        WHEN COALESCE(pontos_a, 0) > 0 OR COALESCE(pontos_b, 0) > 0 THEN 2
                        WHEN LOWER(COALESCE(status_jogo, '')) IN ('entre_sets', 'tiebreak_sorteio') THEN 3
                        WHEN LOWER(COALESCE(status_operacao, '')) IN ('pre_jogo', 'em_papeleta', 'papeleta', 'reservado') THEN 4
                        WHEN LOWER(COALESCE(fase_partida, '')) IN ('papeleta', 'papeleta_pronta', 'intervalo_set', 'jogo') THEN 5
                        ELSE 9
                    END ASC,
                    CASE WHEN COALESCE(operador_login, '') <> '' THEN 0 ELSE 1 END ASC,
                    COALESCE(pre_jogo_iniciado_em, reservado_em, TIMESTAMP '1970-01-01') DESC,
                    id DESC
                LIMIT 1
            """, (competicao, equipe_nome, equipe_nome, equipe_nome, equipe_nome))
            return cur.fetchone()

def _lado_treinador_da_partida(partida, equipe_nome):
    equipe_a = partida.get('equipe_a_operacional') or partida.get('equipe_a')
    equipe_b = partida.get('equipe_b_operacional') or partida.get('equipe_b')
    if equipe_nome == equipe_a:
        return 'A'
    if equipe_nome == equipe_b:
        return 'B'
    return ''


def papeleta_liberada_para_treinador(partida):
    fase = (partida.get('fase_partida') or '').strip().lower()
    status_jogo = (partida.get('status_jogo') or '').strip().lower()
    status_operacao = (partida.get('status_operacao') or '').strip().lower()

    pontos_a = int(partida.get("pontos_a") or 0)
    pontos_b = int(partida.get("pontos_b") or 0)

    if status_jogo in {'finalizada', 'encerrado'}:
        return False

    if fase in {'papeleta', 'papeleta_pronta', 'intervalo_set'}:
        return True

    if status_jogo == 'entre_sets':
        return True

    if status_operacao in {'papeleta', 'pre_jogo', 'em_papeleta'} and pontos_a == 0 and pontos_b == 0:
        return True

    return False


def papeleta_editavel_para_treinador(partida):
    fase = (partida.get('fase_partida') or '').strip().lower()
    status_jogo = (partida.get('status_jogo') or '').strip().lower()
    status_operacao = (partida.get('status_operacao') or '').strip().lower()

    pontos_a = int(partida.get("pontos_a") or 0)
    pontos_b = int(partida.get("pontos_b") or 0)

    if status_jogo in {'finalizada', 'encerrado'}:
        return False

    if pontos_a > 0 or pontos_b > 0:
        return False

    if fase in {'papeleta', 'papeleta_pronta', 'intervalo_set'}:
        return True

    if status_jogo == 'entre_sets':
        return True

    if status_operacao in {'papeleta', 'pre_jogo', 'em_papeleta'}:
        return True

    return False


def registrar_solicitacao_treinador(partida_id, competicao, equipe, tipo, detalhes=None):
    criar_tabela_solicitacoes_treinador()
    detalhes = detalhes or {}

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO solicitacoes_treinador (
                    partida_id, competicao, equipe, tipo, status, detalhes_json
                ) VALUES (%s, %s, %s, %s, 'pendente', %s)
            """, (partida_id, competicao, equipe, tipo, json.dumps(detalhes, ensure_ascii=False)))
        conn.commit()


def listar_solicitacoes_treinador(partida_id, competicao, equipe=None, status=None, limite=30):
    criar_tabela_solicitacoes_treinador()

    clausulas = ["partida_id = %s", "competicao = %s"]
    params = [partida_id, competicao]

    if equipe:
        clausulas.append("equipe = %s")
        params.append(equipe)

    if status:
        clausulas.append("status = %s")
        params.append(status)

    params.append(int(limite or 30))

    sql = f"""
        SELECT *
        FROM solicitacoes_treinador
        WHERE {' AND '.join(clausulas)}
        ORDER BY id DESC
        LIMIT %s
    """

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()

    for row in rows:
        try:
            row["detalhes"] = json.loads(row.get("detalhes_json") or "{}")
        except Exception:
            row["detalhes"] = {}

    return rows


def _detalhes_evento_dict(valor):
    if isinstance(valor, dict):
        return valor

    if isinstance(valor, str) and valor.strip():
        try:
            dados = json.loads(valor)
            if isinstance(dados, dict):
                return dados
        except Exception:
            return {}

    return {}


def _normalizar_scout(valor):
    valor = str(valor or "").strip().lower()
    valor = valor.replace("ç", "c").replace("ã", "a").replace("á", "a").replace("à", "a")
    valor = valor.replace("é", "e").replace("ê", "e").replace("í", "i")
    valor = valor.replace("ó", "o").replace("ô", "o").replace("ú", "u")
    valor = valor.replace("-", "_").replace(" ", "_")
    return valor


def resumir_scout_equipe_partida(partida_id, competicao, lado):
    """
    Monta o scout da equipe no modo treinador.

    IMPORTANTE SOBRE A LÓGICA DO SISTEMA:
    - Em lances positivos (ataque, bloqueio, ace), o campo eventos.equipe é o lado que pontuou.
    - Em erro/falta, o campo eventos.equipe normalmente é o lado que GANHOU o ponto.
      Portanto o erro/falta deve ser atribuído ao adversário desse lado.
    """
    lado = (lado or "").strip().upper()

    resumo = {
        "equipe": {
            "pontos": 0,
            "ataques": 0,
            "aces": 0,
            "bloqueios": 0,
            "erros_saque": 0,
            "erros_rotacao": 0,
            "faltas": 0,
            "erros_gerais": 0,
        },
        "atletas": {},
        "eventos": [],
        "atletas_lista": [],
    }

    if lado not in {"A", "B"}:
        return resumo

    def _oposto(l):
        return "B" if l == "A" else "A"

    def _num_txt(valor):
        return str(valor or "").strip()

    def atleta_bucket(numero, nome):
        numero_txt = _num_txt(numero)
        nome_txt = str(nome or "").strip()
        chave = numero_txt or nome_txt.lower().replace(" ", "_") or "sem_identificacao"

        if chave not in resumo["atletas"]:
            resumo["atletas"][chave] = {
                "numero": numero_txt,
                "nome": nome_txt or "Sem identificação",
                "pontos": 0,
                "ataques": 0,
                "aces": 0,
                "bloqueios": 0,
            }

        return resumo["atletas"][chave]

    try:
        eventos = listar_eventos_partida(partida_id, competicao, limite=2000) or []
    except TypeError:
        eventos = listar_eventos_partida(partida_id, competicao) or []
    except Exception as e:
        print("ERRO resumir_scout_equipe_partida/listar_eventos:", repr(e), flush=True)
        return resumo

    for ev in eventos:
        lado_evento = str(ev.get("equipe") or "").strip().upper()
        if lado_evento not in {"A", "B"}:
            continue

        detalhes_json = _detalhes_evento_dict(ev.get("detalhes"))

        fundamento = _normalizar_scout(
            detalhes_json.get("fundamento")
            or detalhes_json.get("detalhe_lance")
            or detalhes_json.get("tipo_erro")
            or ev.get("fundamento")
            or ev.get("detalhe")
        )

        resultado = _normalizar_scout(
            detalhes_json.get("resultado")
            or detalhes_json.get("tipo_lance")
            or ev.get("resultado")
            or ev.get("tipo_evento")
            or ev.get("tipo")
        )

        detalhe = _normalizar_scout(
            detalhes_json.get("detalhe_lance")
            or detalhes_json.get("tipo_erro")
            or detalhes_json.get("detalhe")
            or ev.get("detalhe")
            or ev.get("detalhes")
        )

        tipo = _normalizar_scout(ev.get("tipo"))
        tipo_evento = _normalizar_scout(ev.get("tipo_evento"))
        texto = f"{tipo} {tipo_evento} {fundamento} {resultado} {detalhe}"

        numero = (
            detalhes_json.get("atleta_numero")
            or detalhes_json.get("numero")
            or ev.get("numero")
            or ""
        )
        nome = (
            detalhes_json.get("atleta_nome")
            or detalhes_json.get("atleta_label")
            or ev.get("atleta_nome")
            or ""
        )

        eh_erro_saque = (
            "erro_saque" in texto
            or "erro_de_saque" in texto
            or detalhe == "erro_saque"
            or fundamento == "erro_saque"
            or (fundamento == "saque" and resultado == "erro")
        )

        eh_rotacao = (
            "rotacao" in texto
            or detalhe == "rotacao"
            or fundamento == "rotacao"
        )

        eh_falta = (
            resultado == "falta"
            or tipo == "falta"
            or tipo_evento == "falta"
            or detalhe in {"rede", "invasao", "rotacao", "conducao", "dois_toques"}
            or fundamento in {"rede", "invasao", "rotacao", "conducao", "dois_toques"}
        )

        eh_erro_geral = (
            resultado == "erro"
            or tipo == "erro"
            or tipo_evento == "erro"
            or detalhe in {"erro", "erro_geral"}
            or fundamento == "erro_geral"
        ) and not eh_erro_saque and not eh_falta

        eh_ataque = (
            fundamento == "ataque"
            or detalhe == "ataque"
            or " ataque" in f" {texto}"
        ) and not (eh_erro_saque or eh_falta or eh_erro_geral)

        eh_ace = (
            fundamento == "ace"
            or detalhe == "ace"
            or " ace" in f" {texto}"
        ) and not (eh_erro_saque or eh_falta or eh_erro_geral)

        eh_bloqueio = (
            fundamento == "bloqueio"
            or detalhe == "bloqueio"
            or " bloqueio" in f" {texto}"
        ) and not (eh_erro_saque or eh_falta or eh_erro_geral)

        # Regra oficial do scout:
        # - lances positivos contam para quem pontuou;
        # - erro/falta contam para quem cometeu o erro/falta.
        # Eventos novos salvam equipe_scout/responsavel_lado nos detalhes.
        # Eventos antigos podem ter salvo eventos.equipe como quem ganhou o ponto; nesse caso,
        # mantemos fallback para o oposto apenas em erro/falta.
        equipe_scout_detalhe = str(
            detalhes_json.get("equipe_scout")
            or detalhes_json.get("responsavel_lado")
            or ""
        ).strip().upper()

        equipe_pontuadora_detalhe = str(
            detalhes_json.get("equipe_pontuadora")
            or ""
        ).strip().upper()

        if equipe_scout_detalhe in {"A", "B"}:
            lado_responsavel = equipe_scout_detalhe
        elif eh_erro_saque or eh_falta or eh_erro_geral:
            lado_responsavel = _oposto(lado_evento)
        else:
            lado_responsavel = equipe_pontuadora_detalhe if equipe_pontuadora_detalhe in {"A", "B"} else lado_evento

        if lado_responsavel != lado:
            continue

        resumo["eventos"].append(ev)

        tem_atleta = _num_txt(numero) not in {"", "0", "None", "none"} or bool(str(nome or "").strip())
        bucket = atleta_bucket(numero, nome) if tem_atleta else None

        if eh_erro_saque:
            resumo["equipe"]["erros_saque"] += 1
            resumo["equipe"]["erros_gerais"] += 1
            continue

        if eh_falta:
            resumo["equipe"]["faltas"] += 1
            resumo["equipe"]["erros_gerais"] += 1
            if eh_rotacao:
                resumo["equipe"]["erros_rotacao"] += 1
            continue

        if eh_erro_geral:
            resumo["equipe"]["erros_gerais"] += 1
            continue

        # Ponto positivo da própria equipe.
        if eh_ataque:
            resumo["equipe"]["ataques"] += 1
            resumo["equipe"]["pontos"] += 1
            if bucket:
                bucket["ataques"] += 1
                bucket["pontos"] += 1
            continue

        if eh_ace:
            resumo["equipe"]["aces"] += 1
            resumo["equipe"]["pontos"] += 1
            if bucket:
                bucket["aces"] += 1
                bucket["pontos"] += 1
            continue

        if eh_bloqueio:
            resumo["equipe"]["bloqueios"] += 1
            resumo["equipe"]["pontos"] += 1
            if bucket:
                bucket["bloqueios"] += 1
                bucket["pontos"] += 1
            continue

        # Fallback: se o evento é ponto da equipe mas veio sem detalhe,
        # pelo menos o total de pontos não fica zerado.
        if resultado == "ponto" or tipo == "ponto" or tipo_evento == "ponto":
            resumo["equipe"]["pontos"] += 1
            if bucket:
                bucket["pontos"] += 1

    resumo["eventos"] = resumo["eventos"][:30]
    resumo["atletas_lista"] = sorted(
        resumo["atletas"].values(),
        key=lambda x: (
            -int(x.get("pontos") or 0),
            str(x.get("numero") or ""),
            str(x.get("nome") or ""),
        ),
    )

    return resumo

def montar_contexto_treinador(partida_id, competicao, equipe_nome=None, lado=None, modo_rapido=False, incluir_scout=True, incluir_solicitacoes=True, incluir_banco=True):
    def _int(v, padrao=0):
        try:
            return int(v or padrao)
        except Exception:
            return padrao

    def _txt(v):
        return str(v or "").strip()

    def _norm(v):
        return _txt(v).lower()

    def _normalizar_rotacao(rotacao):
        if not isinstance(rotacao, list):
            rotacao = []
        rotacao = [_txt(x) for x in rotacao]
        while len(rotacao) < 6:
            rotacao.append("")
        return rotacao[:6]

    def _tem_rotacao(rotacao):
        return any(_txt(x) for x in (rotacao or []))

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))
            partida = cur.fetchone()

    if not partida:
        return None

    status_partida = _txt(partida.get("status")).lower()
    status_jogo = _txt(partida.get("status_jogo")).lower()
    status_operacao = _txt(partida.get("status_operacao")).lower()
    fase_partida_status = _txt(partida.get("fase_partida")).lower()

    if (
        status_partida in {"finalizada", "finalizado", "encerrado", "encerrada"}
        or status_jogo in {"finalizada", "finalizado", "encerrado", "encerrada"}
        or status_operacao in {"finalizada", "finalizado", "encerrado", "encerrada"}
        or fase_partida_status in {"finalizada", "finalizado", "encerrado", "encerrada"}
    ):
        return None

    equipe_a = _txt(partida.get("equipe_a_operacional") or partida.get("equipe_a"))
    equipe_b = _txt(partida.get("equipe_b_operacional") or partida.get("equipe_b"))
    equipe_nome_limpa = _txt(equipe_nome)

    lado_final = _txt(lado).upper()

    if equipe_nome_limpa and _norm(equipe_nome_limpa) == _norm(equipe_a):
        lado_final = "A"
    elif equipe_nome_limpa and _norm(equipe_nome_limpa) == _norm(equipe_b):
        lado_final = "B"

    if lado_final not in {"A", "B"}:
        return None

    estado = buscar_estado_jogo_partida(partida_id, competicao) or {}

    pontos_a = _int(estado.get("pontos_a") or estado.get("placar_a") or partida.get("pontos_a"))
    pontos_b = _int(estado.get("pontos_b") or estado.get("placar_b") or partida.get("pontos_b"))
    sets_a = _int(estado.get("sets_a") or partida.get("sets_a"))
    sets_b = _int(estado.get("sets_b") or partida.get("sets_b"))
    set_atual = _int(estado.get("set_atual") or partida.get("set_atual"), 1)

    rotacao_a = _normalizar_rotacao(estado.get("rotacao_a") or [])
    rotacao_b = _normalizar_rotacao(estado.get("rotacao_b") or [])

    # No carregamento inicial do treinador, não recalcula rotação por eventos,
    # porque isso pode varrer histórico e deixar a tela demorando muito para abrir.
    # O estado salvo/socket atualiza depois. Nas chamadas completas, mantém a lógica antiga.
    if not modo_rapido:
        try:
            rotacoes_calc = _calcular_rotacoes_partida(partida_id, competicao, partida) or {}

            rotacao_a_calc = _normalizar_rotacao(rotacoes_calc.get("rotacao_a") or [])
            rotacao_b_calc = _normalizar_rotacao(rotacoes_calc.get("rotacao_b") or [])

            if _tem_rotacao(rotacao_a_calc):
                rotacao_a = rotacao_a_calc

            if _tem_rotacao(rotacao_b_calc):
                rotacao_b = rotacao_b_calc

            if rotacoes_calc.get("saque_calculado") in ("A", "B"):
                estado["saque_atual"] = rotacoes_calc.get("saque_calculado")

        except Exception as e:
            print("ERRO recalcular rotacao:", repr(e), flush=True)

    if lado_final == "A":
        lado_adversario = "B"
        equipe_atual = equipe_a
        equipe_adversaria = equipe_b
        rotacao_propria = rotacao_a
    else:
        lado_adversario = "A"
        equipe_atual = equipe_b
        equipe_adversaria = equipe_a
        rotacao_propria = rotacao_b

    comp = buscar_competicao_por_nome(competicao) or {}

    tempos_limite = _int(comp.get("tempos_por_set") or partida.get("tempos_por_set"), 2)
    subs_limite = _int(comp.get("substituicoes_por_set") or partida.get("substituicoes_por_set"), 6)

    # Tempos no apontador são exibidos como RESTANTES (ex.: 2 x 2).
    # Por isso o treinador não pode tratar tempos_a/tempos_b como usados,
    # senão 2 por set vira 0 restantes.
    try:
        tempos_restantes_db = buscar_tempos_restantes_partida(partida_id, competicao) or {}
    except Exception:
        tempos_restantes_db = {}

    tempos_a = _int(tempos_restantes_db.get("tempos_a"), tempos_limite)
    tempos_b = _int(tempos_restantes_db.get("tempos_b"), tempos_limite)

    # Substituições salvas em subs_a/subs_b são USADAS.
    # Se algum estado antigo vier com valor maior/igual ao limite, não deixa virar negativo.
    subs_a = _int(estado.get("subs_a") if estado.get("subs_a") is not None else partida.get("subs_a"))
    subs_b = _int(estado.get("subs_b") if estado.get("subs_b") is not None else partida.get("subs_b"))

    tempos_restantes = tempos_a if lado_final == "A" else tempos_b
    subs_usadas = subs_a if lado_final == "A" else subs_b
    subs_restantes = max(0, subs_limite - subs_usadas)

    saque_inicial = _txt(partida.get("saque_inicial") or estado.get("saque_inicial"))
    saque_atual = _txt(estado.get("saque_atual") or partida.get("saque_atual") or saque_inicial)

    saque_inicial_nome = equipe_a if saque_inicial == "A" else equipe_b if saque_inicial == "B" else "-"
    saque_atual_nome = equipe_a if saque_atual == "A" else equipe_b if saque_atual == "B" else "-"

    atletas = []
    banco = []

    if incluir_banco:
        atletas = listar_atletas_aprovados_da_equipe(equipe_atual, competicao) or []
        atletas = [a for a in atletas if a.get("numero") not in (None, "")]
        atletas.sort(key=lambda a: _int(a.get("numero")))

        numeros_quadra = {
            str(n or "").strip()
            for n in rotacao_propria
            if str(n or "").strip()
        }

        banco = [
            a for a in atletas
            if str(a.get("numero") or "").strip() not in numeros_quadra
        ]

    papeleta_rows = listar_papeleta(partida_id, competicao, equipe_atual, set_atual) or []

    papeleta = {
        _int(row.get("posicao")): str(row.get("numero") or "")
        for row in papeleta_rows
    }

    for i in range(1, 7):
        papeleta.setdefault(i, "")

    if incluir_scout:
        scout = resumir_scout_equipe_partida(partida_id, competicao, lado_final) or {}
        scout.setdefault("equipe", {})
        scout.setdefault("atletas_lista", [])
    else:
        scout = {"equipe": {}, "atletas_lista": [], "eventos": []}

    if incluir_solicitacoes:
        try:
            solicitacoes = listar_solicitacoes_treinador(
                partida_id,
                competicao,
                equipe=lado_final,
                limite=20
            ) or []
        except Exception:
            solicitacoes = []
    else:
        solicitacoes = []

    fase = _txt(partida.get("fase_partida") or estado.get("fase_partida")).lower()
    status_jogo = _txt(partida.get("status_jogo") or estado.get("status_jogo")).lower()
    status_operacao = _txt(partida.get("status_operacao")).lower()

    papeleta_liberada = False
    papeleta_editavel = False

    if status_jogo not in {"finalizada", "encerrado"}:
        if pontos_a == 0 and pontos_b == 0:
            papeleta_liberada = True
            papeleta_editavel = True

        if fase in {"papeleta", "papeleta_pronta", "intervalo_set"}:
            papeleta_liberada = True

        if status_jogo == "entre_sets":
            papeleta_liberada = True
            papeleta_editavel = True

        if status_operacao in {"pre_jogo", "papeleta", "em_papeleta", "reservado"}:
            papeleta_liberada = True
            if pontos_a == 0 and pontos_b == 0:
                papeleta_editavel = True

    placar_proprio = pontos_a if lado_final == "A" else pontos_b
    placar_adversario = pontos_b if lado_final == "A" else pontos_a
    sets_proprios = sets_a if lado_final == "A" else sets_b
    sets_adversario = sets_b if lado_final == "A" else sets_a

    descricao_set = "SET ÚNICO" if set_atual <= 0 else f"{set_atual}º SET"

    return {
        "partida": partida,
        "estado": estado,

        "lado": lado_final,
        "lado_adversario": lado_adversario,
        "lado_quadra": "Esquerda" if lado_final == "A" else "Direita",

        "equipe_nome": equipe_atual,
        "equipe_adversaria": equipe_adversaria,
        "equipe_a": equipe_a,
        "equipe_b": equipe_b,

        "set_atual": set_atual,
        "descricao_set": descricao_set,

        "pontos_a": pontos_a,
        "pontos_b": pontos_b,
        "placar_a": pontos_a,
        "placar_b": pontos_b,
        "sets_a": sets_a,
        "sets_b": sets_b,

        "placar_proprio": placar_proprio,
        "placar_adversario": placar_adversario,
        "sets_proprios": sets_proprios,
        "sets_adversario": sets_adversario,

        "saque_inicial": saque_inicial,
        "saque_inicial_nome": saque_inicial_nome,
        "saque_atual": saque_atual,
        "saque_atual_nome": saque_atual_nome,

        "tempos_limite": tempos_limite,
        "subs_limite": subs_limite,
        "tempos_a": tempos_a,
        "tempos_b": tempos_b,
        "subs_a": subs_a,
        "subs_b": subs_b,
        "tempos_restantes": tempos_restantes,
        "subs_restantes": subs_restantes,

        "rotacao": rotacao_propria,
        "rotacao_a": rotacao_a,
        "rotacao_b": rotacao_b,

        "atletas": atletas,
        "jogadores": [a.get("numero") for a in atletas],
        "banco": banco,

        "papeleta": papeleta,
        "papeleta_liberada": papeleta_liberada,
        "papeleta_editavel": papeleta_editavel,
        "papeleta_completa": len(papeleta_rows) == 6,

        "scout": scout,
        "eventos": scout.get("eventos") or [],
        "atletas_lista": scout.get("atletas_lista") or [],

        "solicitacoes": solicitacoes,
    }

def buscar_config_conferencia_atletas(nome_competicao):
    criar_campos_conferencia_atletas()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    nome,
                    conferencia_liberada,
                    conferencia_encerrada,
                    conferencia_prazo,
                    conferencia_link
                FROM competicoes
                WHERE nome = %s
                LIMIT 1
            """, (nome_competicao,))
            return cur.fetchone()


def listar_atletas_para_conferencia(nome_competicao):
    criar_campos_conferencia_atletas()
    
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    equipe,
                    nome,
                    cpf,
                    data_nascimento
                FROM atletas
                WHERE competicao = %s
                ORDER BY equipe, nome
            """, (nome_competicao,))
            return cur.fetchall()