import os
import random
import re
import string
import json
import base64
from io import BytesIO
from datetime import datetime
from threading import Lock, BoundedSemaphore
from contextlib import contextmanager

from dotenv import load_dotenv
load_dotenv()

from psycopg import connect
from psycopg.rows import dict_row
try:
    from psycopg_pool import ConnectionPool
except Exception:
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
    "tabela_competicao_quadras": False,
    "tabela_competicao_agenda_config": False,
    "campos_trava_operacional_partida": False,
}
_SCHEMA_LOCK = Lock()
_POOL_LOCK = Lock()
_DB_POOL = None
_DIRECT_FALLBACK_SEMAPHORE = None
_PINS_OPERACIONAIS_SCHEMA_OK = False


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


def _env_int(nome, padrao, minimo=None, maximo=None):
    try:
        valor = int(os.environ.get(nome, padrao))
    except Exception:
        valor = int(padrao)
    if minimo is not None:
        valor = max(minimo, valor)
    if maximo is not None:
        valor = min(maximo, valor)
    return valor


def _env_float(nome, padrao, minimo=None, maximo=None):
    try:
        valor = float(os.environ.get(nome, padrao))
    except Exception:
        valor = float(padrao)
    if minimo is not None:
        valor = max(minimo, valor)
    if maximo is not None:
        valor = min(maximo, valor)
    return valor


def _pool_habilitado():
    """Define se o pool local do psycopg deve ser usado.

    Correção para Render/Neon:
    - o pool fica LIGADO por padrão;
    - só desliga se DB_POOL_ENABLED=0/false/no/off;
    - não desliga automaticamente por causa de URL contendo "pooler".

    O problema do 502 vinha de várias requisições caindo direto no fallback
    fora do pool, abrindo conexões novas em sequência.
    """
    valor_env = os.environ.get("DB_POOL_ENABLED")

    if valor_env is None:
        return True

    valor = str(valor_env).strip().lower()
    return valor not in {"0", "false", "no", "off", "nao", "não"}


def _conexao_direta():
    return connect(
        _obter_database_url(),
        row_factory=dict_row,
        sslmode="require",
        connect_timeout=_env_int("DB_CONNECT_TIMEOUT", 8, minimo=3, maximo=30),
        prepare_threshold=None,
    )


def _obter_pool():
    global _DB_POOL

    if ConnectionPool is None or not _pool_habilitado():
        return None

    if _DB_POOL is not None:
        return _DB_POOL

    with _POOL_LOCK:
        if _DB_POOL is not None:
            return _DB_POOL

        # O sistema tem telas que disparam várias leituras seguidas (apontador,
        # relatórios e painel da equipe). Com max_size=5 o Render/Neon entra em
        # fila muito rápido e cada request pode esperar vários segundos.
        # Pode ajustar no Render por ENV, mas o padrão novo já é mais adequado.
        min_size = _env_int("DB_POOL_MIN_SIZE", 1, minimo=0, maximo=10)
        max_size = _env_int("DB_POOL_MAX_SIZE", 8, minimo=2, maximo=20)
        if max_size < min_size:
            max_size = min_size or 1

        _DB_POOL = ConnectionPool(
            conninfo=_obter_database_url(),
            kwargs={
                "row_factory": dict_row,
                "sslmode": "require",
                "connect_timeout": _env_int("DB_CONNECT_TIMEOUT", 8, minimo=3, maximo=30),
                "prepare_threshold": None,
            },
            min_size=min_size,
            max_size=max_size,
            timeout=_env_float("DB_POOL_TIMEOUT", 10, minimo=2, maximo=60),
            max_idle=_env_float("DB_POOL_MAX_IDLE", 120, minimo=20, maximo=600),
            max_lifetime=_env_float("DB_POOL_MAX_LIFETIME", 600, minimo=60, maximo=1800),
            reconnect_timeout=_env_float("DB_POOL_RECONNECT_TIMEOUT", 15, minimo=3, maximo=60),
            open=True,
        )

        return _DB_POOL


def _erro_conexao_quebrada(exc):
    """Identifica erros típicos de conexão SSL/Neon/psycopg quebrada.

    Quando isso acontece, não adianta devolver a conexão para o pool: ela deve
    ser descartada e o pool recriado. Isso evita reutilizar conexão BAD em
    rotas como login e painel da equipe.
    """
    mensagem = repr(exc).lower()
    termos = (
        "ssl syscall error",
        "ssl error",
        "eof detected",
        "bad record mac",
        "consuming input failed",
        "connection bad",
        "connection is closed",
        "closed connection",
        "server closed the connection",
        "terminating connection",
        "the connection is lost",
        "couldn't get a connection",
        "pooltimeout",
        "pool closed",
    )
    return any(t in mensagem for t in termos)


def _conexao_fechada_ou_ruim(conn):
    if conn is None:
        return True
    try:
        if bool(getattr(conn, "closed", False)):
            return True
    except Exception:
        return True
    try:
        if bool(getattr(conn, "broken", False)):
            return True
    except Exception:
        pass
    return False


def _validar_conexao_pool(conn):
    """Faz um ping curto na conexão recebida do pool.

    O Neon pode encerrar conexões SSL antigas. Sem esse teste, o pool entrega a
    conexão aparentemente livre, mas o primeiro SELECT real explode com
    "SSL SYSCALL error: EOF detected" ou "bad record mac".
    """
    if _conexao_fechada_ou_ruim(conn):
        return False

    testar = str(os.environ.get("DB_POOL_PING", "1")).strip().lower()
    if testar in {"0", "false", "no", "off", "nao", "não"}:
        return True

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except Exception as e:
        print("AVISO: conexão do pool falhou no ping:", repr(e), flush=True)
        return False


def _fechar_pool_quebrado():
    global _DB_POOL

    pool = _DB_POOL
    _DB_POOL = None

    try:
        if pool is not None:
            pool.close(timeout=1)
    except Exception:
        pass


@contextmanager
def conectar():
    """Abre conexão com o banco usando pool com fallback seguro.

    Ajuste importante para Render/Neon:
    - valida a conexão antes de entregar para a rota;
    - se detectar SSL EOF/BAD/bad record mac, fecha o pool inteiro;
    - cai para conexão direta controlada;
    - evita reutilizar conexão quebrada em login, painel da equipe e apontador.
    """
    global _DIRECT_FALLBACK_SEMAPHORE

    pool = _obter_pool()
    timeout_pool = _env_float("DB_POOL_TIMEOUT", 10, minimo=1, maximo=30)

    # =====================================================
    # 1) TENTA PEGAR UMA CONEXÃO DO POOL ANTES DO YIELD
    # =====================================================
    pool_cm = None
    conn_pool = None

    if pool is not None:
        try:
            pool_cm = pool.connection(timeout=timeout_pool)
            conn_pool = pool_cm.__enter__()

            if not _validar_conexao_pool(conn_pool):
                erro_ping = RuntimeError("Conexão inválida recebida do pool.")
                try:
                    pool_cm.__exit__(RuntimeError, erro_ping, erro_ping.__traceback__)
                except Exception:
                    pass
                _fechar_pool_quebrado()
                pool_cm = None
                conn_pool = None
                raise erro_ping

        except Exception as e:
            print("AVISO: pool do banco indisponível:", repr(e), flush=True)

            if _erro_conexao_quebrada(e):
                _fechar_pool_quebrado()

            fallback_ligado = str(
                os.environ.get("DB_DIRECT_FALLBACK_ENABLED", "1")
            ).strip().lower()

            if fallback_ligado in {"0", "false", "no", "off", "nao", "não"}:
                raise
        else:
            erro_do_bloco = None
            try:
                yield conn_pool
            except BaseException as exc:
                erro_do_bloco = exc
                if _erro_conexao_quebrada(exc):
                    try:
                        if conn_pool is not None:
                            conn_pool.close()
                    except Exception:
                        pass
                    _fechar_pool_quebrado()
                raise
            finally:
                try:
                    if erro_do_bloco is None:
                        pool_cm.__exit__(None, None, None)
                    else:
                        pool_cm.__exit__(
                            type(erro_do_bloco),
                            erro_do_bloco,
                            erro_do_bloco.__traceback__,
                        )
                except Exception as e:
                    print("AVISO: erro ao devolver conexão ao pool:", repr(e), flush=True)
                    if _erro_conexao_quebrada(e):
                        _fechar_pool_quebrado()
            return

    # =====================================================
    # 2) FALLBACK DIRETO CONTROLADO
    # =====================================================
    limite_fallback = _env_int(
        "DB_DIRECT_FALLBACK_MAX",
        2,
        minimo=0,
        maximo=6,
    )

    if limite_fallback <= 0:
        raise RuntimeError(
            "Pool do banco indisponível e fallback direto desativado."
        )

    if _DIRECT_FALLBACK_SEMAPHORE is None:
        with _POOL_LOCK:
            if _DIRECT_FALLBACK_SEMAPHORE is None:
                _DIRECT_FALLBACK_SEMAPHORE = BoundedSemaphore(limite_fallback)

    adquiriu = _DIRECT_FALLBACK_SEMAPHORE.acquire(
        timeout=_env_float(
            "DB_DIRECT_FALLBACK_TIMEOUT",
            3,
            minimo=0.2,
            maximo=10,
        )
    )

    if not adquiriu:
        raise RuntimeError(
            "Banco ocupado: pool indisponível e limite de conexões diretas atingido."
        )

    conn = None

    try:
        print(
            "AVISO: usando conexão direta controlada fora do pool",
            flush=True,
        )

        conn = _conexao_direta()
        yield conn

    except BaseException as exc:
        if _erro_conexao_quebrada(exc):
            _fechar_pool_quebrado()
        raise

    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

        try:
            _DIRECT_FALLBACK_SEMAPHORE.release()
        except Exception:
            pass

# =========================================================
# CACHE DE CLASSIFICAÇÃO
# =========================================================
def criar_tabela_cache_classificacao():
    """Tabela pequena para guardar a classificação pronta por competição.

    A assinatura muda quando partidas/grupos/equipes do grupo mudam. Assim a
    tela usa cache quando nada mudou e recalcula automaticamente quando houve
    alteração relevante.
    """
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS classificacao_cache (
                        competicao TEXT PRIMARY KEY,
                        assinatura TEXT NOT NULL,
                        payload_json JSONB NOT NULL,
                        atualizado_em TIMESTAMP DEFAULT NOW()
                    )
                """)
            conn.commit()
    except Exception as e:
        print("AVISO criar_tabela_cache_classificacao:", repr(e))


def assinatura_classificacao_competicao(competicao):
    """Assinatura leve dos dados que afetam a classificação.

    Evita transferir todas as partidas só para saber se o cache continua válido.
    O PostgreSQL calcula um MD5 dos campos relevantes.
    """
    try:
        criar_tabela_cache_classificacao()
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    WITH partidas_sig AS (
                        SELECT COALESCE(
                            md5(string_agg(
                                CONCAT_WS('|',
                                    id,
                                    COALESCE(grupo, ''),
                                    COALESCE(fase, ''),
                                    COALESCE(equipe_a, ''),
                                    COALESCE(equipe_b, ''),
                                    COALESCE(status, ''),
                                    COALESCE(status_jogo, ''),
                                    COALESCE(fase_partida, ''),
                                    COALESCE(vencedor, ''),
                                    COALESCE(sets_a::TEXT, ''),
                                    COALESCE(sets_b::TEXT, ''),
                                    COALESCE(set1_a::TEXT, ''),
                                    COALESCE(set1_b::TEXT, ''),
                                    COALESCE(set2_a::TEXT, ''),
                                    COALESCE(set2_b::TEXT, ''),
                                    COALESCE(set3_a::TEXT, ''),
                                    COALESCE(set3_b::TEXT, ''),
                                    COALESCE(set4_a::TEXT, ''),
                                    COALESCE(set4_b::TEXT, ''),
                                    COALESCE(set5_a::TEXT, ''),
                                    COALESCE(set5_b::TEXT, ''),
                                    COALESCE(pontos_a::TEXT, ''),
                                    COALESCE(pontos_b::TEXT, ''),
                                    COALESCE(origem_resultado, ''),
                                    COALESCE(tipo_encerramento, '')
                                ), '§' ORDER BY id
                            )), 'sem_partidas') AS sig
                        FROM partidas
                        WHERE competicao = %s
                    ), grupos_sig AS (
                        SELECT COALESCE(
                            md5(string_agg(
                                CONCAT_WS('|',
                                    COALESCE(g.id::TEXT, ''),
                                    COALESCE(g.nome, ''),
                                    COALESCE(ge.equipe, '')
                                ), '§' ORDER BY g.id, ge.equipe
                            )), 'sem_grupos') AS sig
                        FROM grupos g
                        LEFT JOIN grupos_equipes ge
                               ON ge.grupo_id = g.id
                              AND ge.competicao = g.competicao
                        WHERE g.competicao = %s
                    )
                    SELECT md5((SELECT sig FROM partidas_sig) || '::' || (SELECT sig FROM grupos_sig)) AS assinatura
                """, (competicao, competicao))
                row = cur.fetchone() or {}
                return row.get("assinatura") or "sem_assinatura"
    except Exception as e:
        print("AVISO assinatura_classificacao_competicao:", repr(e))
        return None


def obter_cache_classificacao(competicao, assinatura):
    if not assinatura:
        return None
    try:
        criar_tabela_cache_classificacao()
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT payload_json
                    FROM classificacao_cache
                    WHERE competicao = %s
                      AND assinatura = %s
                    LIMIT 1
                """, (competicao, assinatura))
                row = cur.fetchone()
                if not row:
                    return None
                payload = row.get("payload_json")
                if isinstance(payload, str):
                    return json.loads(payload)
                return payload
    except Exception as e:
        print("AVISO obter_cache_classificacao:", repr(e))
        return None


def salvar_cache_classificacao(competicao, assinatura, payload):
    if not assinatura:
        return False
    try:
        criar_tabela_cache_classificacao()
        payload_json = json.dumps(payload, ensure_ascii=False, default=str)
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO classificacao_cache (competicao, assinatura, payload_json, atualizado_em)
                    VALUES (%s, %s, %s::jsonb, NOW())
                    ON CONFLICT (competicao)
                    DO UPDATE SET
                        assinatura = EXCLUDED.assinatura,
                        payload_json = EXCLUDED.payload_json,
                        atualizado_em = NOW()
                """, (competicao, assinatura, payload_json))
            conn.commit()
        return True
    except Exception as e:
        print("AVISO salvar_cache_classificacao:", repr(e))
        return False


def invalidar_cache_classificacao(competicao):
    if not competicao:
        return False
    try:
        criar_tabela_cache_classificacao()
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM classificacao_cache WHERE competicao = %s", (competicao,))
            conn.commit()
        return True
    except Exception as e:
        print("AVISO invalidar_cache_classificacao:", repr(e))
        return False


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
    return f"arb_{_normalizar_texto_base(nome_mesario)}"


def somente_digitos(valor):
    return re.sub(r"\D", "", str(valor or ""))


def formatar_cpf(cpf):
    cpf = somente_digitos(cpf)
    if len(cpf) != 11:
        return cpf
    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"


def cpf_valido(cpf):
    cpf = somente_digitos(cpf)

    if len(cpf) != 11:
        return False

    if cpf == cpf[0] * 11:
        return False

    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    digito_1 = 11 - (soma % 11)
    digito_1 = 0 if digito_1 >= 10 else digito_1

    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    digito_2 = 11 - (soma % 11)
    digito_2 = 0 if digito_2 >= 10 else digito_2

    return cpf[-2:] == f"{digito_1}{digito_2}"


def _cpf_sql_limpo(campo="cpf"):
    return f"REGEXP_REPLACE(COALESCE({campo}, ''), '\\D', '', 'g')"


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
        _campo_ou_alias(colunas, "exigir_foto_atleta", "FALSE AS exigir_foto_atleta") if not prefixo else (
            f"{p}exigir_foto_atleta" if "exigir_foto_atleta" in colunas else "FALSE AS exigir_foto_atleta"
        ),
        _campo_ou_alias(colunas, "exigir_instagram_atleta", "FALSE AS exigir_instagram_atleta") if not prefixo else (
            f"{p}exigir_instagram_atleta" if "exigir_instagram_atleta" in colunas else "FALSE AS exigir_instagram_atleta"
        ),
        _campo_ou_alias(colunas, "aprovacao_automatica_atletas", "FALSE AS aprovacao_automatica_atletas") if not prefixo else (
            f"{p}aprovacao_automatica_atletas" if "aprovacao_automatica_atletas" in colunas else "FALSE AS aprovacao_automatica_atletas"
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

            _atualizar_vinculos_login_equipe(cur, login_atual, novo_login)

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
# MINHA CONTA - DADOS DO USUÁRIO
# =========================================================
def _normalizar_login_conta(login):
    login = (login or "").strip()
    login = re.sub(r"\s+", "_", login)
    login = re.sub(r"[^A-Za-z0-9_.@-]", "", login)
    return login[:80]


def _atualizar_vinculos_login_equipe(cur, login_atual, novo_login):
    """
    Mantém íntegros os vínculos da equipe quando o login da conta muda.

    O sistema ainda usa o login como chave de compatibilidade em algumas telas
    (principalmente equipes_competicoes). Se o login muda só em usuarios/equipes,
    a equipe some do organizador e não carrega a competição selecionada.
    """
    login_atual = (login_atual or "").strip()
    novo_login = (novo_login or "").strip()

    if not login_atual or not novo_login or login_atual == novo_login:
        return

    tabelas_campos = [
        ("equipes_competicoes", "equipe_login"),
    ]

    for tabela, campo in tabelas_campos:
        try:
            colunas = _buscar_colunas_tabela(tabela)
            if campo not in colunas:
                continue

            cur.execute(
                f"""
                UPDATE {tabela}
                SET {campo} = %s
                WHERE {campo} = %s
                """,
                (novo_login, login_atual),
            )
        except Exception as e:
            print(f"AVISO _atualizar_vinculos_login_equipe/{tabela}.{campo}:", repr(e))


def _atualizar_vinculos_login_organizador(cur, login_atual, novo_login):
    login_atual = (login_atual or "").strip()
    novo_login = (novo_login or "").strip()

    if not login_atual or not novo_login or login_atual == novo_login:
        return

    try:
        cur.execute("""
            UPDATE competicoes
            SET organizador_login = %s
            WHERE organizador_login = %s
        """, (novo_login, login_atual))
    except Exception as e:
        print("AVISO _atualizar_vinculos_login_organizador/competicoes:", repr(e))


def atualizar_dados_conta_usuario(login_atual, novo_login, nome):
    """
    Atualiza dados básicos da conta sem permitir alteração de perfil.

    Também mantém os vínculos principais quando o login muda:
    - usuarios.login
    - equipes.login
    - competicoes.organizador_login
    """
    login_atual = (login_atual or "").strip()
    novo_login = _normalizar_login_conta(novo_login)
    nome = (nome or "").strip()

    if not login_atual or not novo_login or not nome:
        return {"ok": False, "erro": "Preencha nome e login."}

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login, perfil
                FROM usuarios
                WHERE login = %s
                LIMIT 1
            """, (login_atual,))
            usuario = cur.fetchone()

            if not usuario:
                return {"ok": False, "erro": "Usuário não encontrado."}

            if novo_login.lower() != login_atual.lower():
                cur.execute("""
                    SELECT login
                    FROM usuarios
                    WHERE LOWER(login) = LOWER(%s)
                      AND login <> %s
                    LIMIT 1
                """, (novo_login, login_atual))
                if cur.fetchone():
                    return {"ok": False, "erro": "Este login já está em uso."}

            cur.execute("""
                UPDATE usuarios
                SET login = %s,
                    nome = %s
                WHERE login = %s
            """, (novo_login, nome, login_atual))

            # Se for equipe, o login da tabela equipes e dos vínculos também precisa acompanhar.
            if usuario.get("perfil") == "equipe":
                try:
                    cur.execute("""
                        UPDATE equipes
                        SET login = %s
                        WHERE login = %s
                    """, (novo_login, login_atual))
                    _atualizar_vinculos_login_equipe(cur, login_atual, novo_login)
                except Exception as e:
                    print("AVISO atualizar_dados_conta_usuario/equipes:", repr(e))

            # Se for organizador, as competições precisam continuar vinculadas.
            if usuario.get("perfil") == "organizador":
                _atualizar_vinculos_login_organizador(cur, login_atual, novo_login)

        conn.commit()

    return {"ok": True, "login": novo_login, "nome": nome}


def atualizar_dados_conta_apontador(cpf_atual, novo_login, nome):
    """
    Atualiza a conta do apontador sem alterar perfil/função.

    No cadastro atual do sistema, o login do apontador é o CPF/login salvo
    em apontadores_acesso.cpf. Por isso, ao trocar o login, atualizamos também
    os vínculos com oficiais/competicao_oficiais quando existirem.
    """
    cpf_atual = (cpf_atual or "").strip()
    novo_login = _normalizar_login_conta(novo_login)
    nome = (nome or "").strip()

    if not cpf_atual or not novo_login or not nome:
        return {"ok": False, "erro": "Preencha nome e login."}

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT cpf
                FROM apontadores_acesso
                WHERE cpf = %s
                LIMIT 1
            """, (cpf_atual,))
            apontador = cur.fetchone()

            if not apontador:
                return {"ok": False, "erro": "Apontador não encontrado."}

            if novo_login.lower() != cpf_atual.lower():
                cur.execute("""
                    SELECT cpf
                    FROM apontadores_acesso
                    WHERE LOWER(cpf) = LOWER(%s)
                      AND cpf <> %s
                    LIMIT 1
                """, (novo_login, cpf_atual))
                if cur.fetchone():
                    return {"ok": False, "erro": "Este login já está em uso para outro apontador."}

                cur.execute("""
                    SELECT login
                    FROM usuarios
                    WHERE LOWER(login) = LOWER(%s)
                    LIMIT 1
                """, (novo_login,))
                if cur.fetchone():
                    return {"ok": False, "erro": "Este login já está em uso por outro usuário."}

            cur.execute("""
                UPDATE apontadores_acesso
                SET cpf = %s
                WHERE cpf = %s
            """, (novo_login, cpf_atual))

            try:
                cur.execute("""
                    UPDATE oficiais
                    SET cpf = %s,
                        nome = %s
                    WHERE cpf = %s
                """, (novo_login, nome, cpf_atual))
            except Exception as e:
                print("AVISO atualizar_dados_conta_apontador/oficiais:", repr(e))

            try:
                cur.execute("""
                    UPDATE competicao_oficiais
                    SET cpf = %s
                    WHERE cpf = %s
                """, (novo_login, cpf_atual))
            except Exception as e:
                print("AVISO atualizar_dados_conta_apontador/competicao_oficiais:", repr(e))

        conn.commit()

    return {"ok": True, "login": novo_login, "nome": nome}


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
                ADD COLUMN IF NOT EXISTS conferencia_link TEXT,
                ADD COLUMN IF NOT EXISTS aprovacao_automatica_atletas BOOLEAN DEFAULT FALSE;
            """)
        conn.commit()

    # Depois de alterar a tabela, limpa o cache local de colunas para que
    # _campos_competicao() e outras consultas enxerguem o campo novo.
    try:
        _CACHE_COLUNAS.pop("competicoes", None)
    except Exception:
        pass


def sincronizar_status_competicoes(nome_competicao=None):
    """
    Atualiza automaticamente o status da competição olhando o estado das partidas.

    Regra:
    - Sem partida iniciada/finalizada: Em preparação
    - Alguma partida iniciada e ainda não finalizada: Em andamento
    - Todas as partidas existentes finalizadas: Finalizada
    """
    filtro = ""
    params = []

    if nome_competicao:
        filtro = "WHERE c.nome = %s"
        params.append(nome_competicao)

    sql = f"""
        WITH resumo AS (
            SELECT
                c.nome,
                COUNT(p.id) AS total_partidas,
                SUM(
                    CASE
                        WHEN LOWER(COALESCE(p.status_jogo, p.status_operacao, p.status, ''))
                             IN ('finalizada', 'finalizado', 'encerrada', 'encerrado')
                        THEN 1 ELSE 0
                    END
                ) AS finalizadas,
                SUM(
                    CASE
                        WHEN LOWER(COALESCE(p.status_jogo, p.status_operacao, p.status, p.fase_partida, ''))
                             IN (
                                'pre_jogo', 'sorteio', 'papeleta', 'papeleta_pronta',
                                'ao_vivo', 'em_andamento', 'andamento', 'jogo',
                                'entre_sets', 'intervalo_set', 'tiebreak_sorteio',
                                'pausada', 'pausado'
                             )
                        THEN 1 ELSE 0
                    END
                ) AS iniciadas
            FROM competicoes c
            LEFT JOIN partidas p
                ON p.competicao = c.nome
            {filtro}
            GROUP BY c.nome
        ), calculado AS (
            SELECT
                nome,
                CASE
                    WHEN total_partidas > 0 AND finalizadas = total_partidas THEN 'Finalizada'
                    WHEN iniciadas > 0 THEN 'Em andamento'
                    ELSE 'Em preparação'
                END AS novo_status
            FROM resumo
        )
        UPDATE competicoes c
        SET status = calculado.novo_status
        FROM calculado
        WHERE c.nome = calculado.nome
          AND COALESCE(c.status, '') <> calculado.novo_status
    """

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
            conn.commit()
    except Exception as e:
        print("AVISO sincronizar_status_competicoes:", repr(e), flush=True)


def sincronizar_status_competicao(nome_competicao):
    return sincronizar_status_competicoes(nome_competicao)


def listar_competicoes():
    sincronizar_status_competicoes()

    campos = _campos_competicao(prefixo="c", incluir_senha_organizador=True)

    sql = f"""
        SELECT {", ".join(campos)}
        FROM competicoes c
        LEFT JOIN usuarios u
            ON u.login = c.organizador_login
        ORDER BY
            CASE
                WHEN LOWER(COALESCE(c.status, '')) IN ('em andamento', 'em_andamento', 'andamento') THEN 1
                WHEN LOWER(COALESCE(c.status, '')) IN ('em preparação', 'em preparacao', 'preparação', 'preparacao') THEN 2
                WHEN LOWER(COALESCE(c.status, '')) IN ('finalizada', 'finalizado', 'encerrada', 'encerrado') THEN 3
                ELSE 4
            END,
            c.nome
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
                "aprovacao_automatica_atletas": False,
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

    try:
        garantir_quadras_competicao(nome, 1)
    except Exception as e:
        print("AVISO: não foi possível criar quadra padrão da competição:", e)

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
    if "exigir_foto_atleta" in colunas:
        sets.append("exigir_foto_atleta = %s")
        valores.append(dados.get("exigir_foto_atleta", False))
    if "exigir_instagram_atleta" in colunas:
        sets.append("exigir_instagram_atleta = %s")
        valores.append(dados.get("exigir_instagram_atleta", False))

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

                # Atletas ficam cadastrados. Apenas remove vínculo com competição/equipe.
                _update_vinculo_null(
                    cur,
                    tabelas_existentes,
                    colunas_por_tabela,
                    "atletas",
                    "competicao",
                    nome,
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


# =========================================================
# DEMONSTRAÇÃO TEMPORÁRIA - VOLLEYTABLE PRO
# =========================================================
DEMO_PREFIXO = "DEMO-VTP-"


def criar_tabela_demos():
    """
    Cria/atualiza a tabela que controla as demonstrações temporárias.

    Regras:
    - A demo dura 4 horas por padrão.
    - A competição gerada sempre começa com DEMO-VTP-.
    - O histórico de CPF/WhatsApp permanece salvo para evitar abuso.
    - Os dados operacionais da demo podem ser apagados sem apagar o histórico.
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS demos_temporarias (
                    id SERIAL PRIMARY KEY,
                    codigo TEXT UNIQUE NOT NULL,
                    nome TEXT DEFAULT '',
                    cpf TEXT DEFAULT '',
                    whatsapp TEXT DEFAULT '',
                    competicao TEXT UNIQUE NOT NULL,
                    login TEXT UNIQUE NOT NULL,
                    senha TEXT NOT NULL,
                    criado_em TIMESTAMP DEFAULT NOW(),
                    expira_em TIMESTAMP NOT NULL,
                    encerrada BOOLEAN DEFAULT FALSE,
                    motivo_encerramento TEXT DEFAULT '',
                    whatsapp_enviado BOOLEAN DEFAULT FALSE,
                    liberado_novo_teste BOOLEAN DEFAULT FALSE
                )
            """)

            cur.execute("""
                ALTER TABLE demos_temporarias
                ADD COLUMN IF NOT EXISTS nome TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS cpf TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS whatsapp TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS motivo_encerramento TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS whatsapp_enviado BOOLEAN DEFAULT FALSE,
                ADD COLUMN IF NOT EXISTS liberado_novo_teste BOOLEAN DEFAULT FALSE
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_demos_temporarias_cpf_limpo
                ON demos_temporarias (
                    REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g')
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_demos_temporarias_whatsapp_limpo
                ON demos_temporarias (
                    REGEXP_REPLACE(COALESCE(whatsapp, ''), '\\D', '', 'g')
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_demos_temporarias_login
                ON demos_temporarias (login)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_demos_temporarias_competicao
                ON demos_temporarias (competicao)
            """)

        conn.commit()

    return True


def _buscar_tabelas_publicas(cur):
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    return {row["table_name"] for row in cur.fetchall()}


def _buscar_colunas_cur(cur, tabela):
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
    """, (tabela,))
    return {row["column_name"] for row in cur.fetchall()}


def _gerar_codigo_demo_unico(cur):
    criar_tabela_demos()

    for _ in range(50):
        numero = "".join(random.choice(string.digits) for _ in range(6))
        codigo = f"{DEMO_PREFIXO}{numero}"

        cur.execute("""
            SELECT 1
            FROM demos_temporarias
            WHERE codigo = %s
               OR competicao = %s
            LIMIT 1
        """, (codigo, codigo))

        if not cur.fetchone():
            return codigo

    raise RuntimeError("Não foi possível gerar um código único para a demonstração.")


def _gerar_senha_demo():
    numero = "".join(random.choice(string.digits) for _ in range(4))
    return f"VTPro-{numero}"


def _gerar_login_demo(codigo):
    numero = (codigo or "").replace(DEMO_PREFIXO, "").strip().lower()
    return f"demo_{numero}"


def demo_ja_usada_por_cpf_ou_whatsapp(cpf, whatsapp):
    """
    Verifica se CPF ou WhatsApp já usaram uma demo.

    Observação:
    - Se o superadmin clicou em 'Liberar novo teste', o registro antigo não bloqueia.
    """
    criar_tabela_demos()

    cpf_limpo = somente_digitos(cpf)
    whatsapp_limpo = somente_digitos(whatsapp)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM demos_temporarias
                WHERE (
                    REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                    OR REGEXP_REPLACE(COALESCE(whatsapp, ''), '\\D', '', 'g') = %s
                )
                AND COALESCE(liberado_novo_teste, FALSE) = FALSE
                LIMIT 1
            """, (cpf_limpo, whatsapp_limpo))
            return cur.fetchone()


def _delete_por_competicao_se_existir(cur, tabelas, tabela, competicao):
    if tabela not in tabelas:
        return

    colunas = _buscar_colunas_cur(cur, tabela)
    if "competicao" not in colunas:
        return

    cur.execute(f"DELETE FROM {tabela} WHERE competicao = %s", (competicao,))


def limpar_demo_por_competicao(competicao, motivo="expirada"):
    """
    Apaga os dados operacionais de uma demo com segurança.

    Segurança:
    - Só executa se a competição começar com DEMO-VTP-.
    - Mantém o registro em demos_temporarias para histórico/bloqueio de CPF/WhatsApp.
    """
    criar_tabela_demos()

    competicao = (competicao or "").strip()

    if not competicao.startswith(DEMO_PREFIXO):
        print("⚠️ Limpeza de demo bloqueada. Prefixo inválido:", competicao)
        return False

    tabelas_por_competicao = [
        "competicao_quadras",
        "competicao_oficiais",
        "equipe_conferencia",
        "eventos",
        "eventos_partida",
        "grupo_equipes",
        "grupos_equipes",
        "historico_rotacao",
        "papeletas",
        "sancoes_partida",
        "solicitacoes_treinador",
        "atletas",
        "equipes_competicoes",
        "grupos",
        "partidas",
    ]

    with conectar() as conn:
        with conn.cursor() as cur:
            tabelas = _buscar_tabelas_publicas(cur)

            cpfs_demo = []
            if "competicao_oficiais" in tabelas:
                colunas_oficiais = _buscar_colunas_cur(cur, "competicao_oficiais")
                if "competicao" in colunas_oficiais and "cpf" in colunas_oficiais:
                    cur.execute("""
                        SELECT DISTINCT cpf
                        FROM competicao_oficiais
                        WHERE competicao = %s
                    """, (competicao,))
                    cpfs_demo = [
                        row["cpf"]
                        for row in cur.fetchall()
                        if row.get("cpf")
                    ]

            # Apaga usuários criados/vinculados na demo:
            # organizador demo, equipes, treinadores, árbitros/mesários etc.
            if "usuarios" in tabelas:
                colunas_usuarios = _buscar_colunas_cur(cur, "usuarios")
                if "competicao_vinculada" in colunas_usuarios:
                    cur.execute("""
                        DELETE FROM usuarios
                        WHERE competicao_vinculada = %s
                          AND COALESCE(perfil, '') <> 'superadmin'
                    """, (competicao,))

            # Remove dados principais vinculados à competição demo.
            for tabela in tabelas_por_competicao:
                _delete_por_competicao_se_existir(cur, tabelas, tabela, competicao)

            # Remove apontadores criados somente para essa demo, se não houver vínculo real.
            if cpfs_demo and "apontadores" in tabelas:
                colunas_apontadores = _buscar_colunas_cur(cur, "apontadores")
                if "cpf" in colunas_apontadores:
                    for cpf in cpfs_demo:
                        tem_vinculo_real = False

                        if "competicao_oficiais" in tabelas:
                            colunas_oficiais = _buscar_colunas_cur(cur, "competicao_oficiais")
                            if "cpf" in colunas_oficiais and "competicao" in colunas_oficiais:
                                cur.execute("""
                                    SELECT 1
                                    FROM competicao_oficiais
                                    WHERE cpf = %s
                                      AND competicao <> %s
                                    LIMIT 1
                                """, (cpf, competicao))
                                tem_vinculo_real = cur.fetchone() is not None

                        if not tem_vinculo_real:
                            cur.execute("""
                                DELETE FROM apontadores
                                WHERE cpf = %s
                            """, (cpf,))

            if "competicoes" in tabelas:
                cur.execute("""
                    DELETE FROM competicoes
                    WHERE nome = %s
                """, (competicao,))

            cur.execute("""
                UPDATE demos_temporarias
                SET encerrada = TRUE,
                    motivo_encerramento = CASE
                        WHEN COALESCE(motivo_encerramento, '') = ''
                        THEN %s
                        ELSE motivo_encerramento
                    END
                WHERE competicao = %s
            """, (motivo or "expirada", competicao))

        conn.commit()

    print("✅ Demo limpa com segurança:", competicao)
    return True


def limpar_demos_expiradas():
    """
    Limpa todas as demos vencidas.
    Pode ser chamada ao acessar /demo, /login ou /demos.
    """
    criar_tabela_demos()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT competicao
                FROM demos_temporarias
                WHERE encerrada = FALSE
                  AND expira_em <= NOW()
            """)
            demos = cur.fetchall()

    total = 0
    for demo in demos:
        if limpar_demo_por_competicao(demo["competicao"], motivo="expirada"):
            total += 1

    return total


def criar_demo_temporaria(nome="", cpf="", whatsapp=""):
    """
    Cria uma competição demo, um organizador demo e validade de 4 horas.

    Tudo fica isolado pela competição DEMO-VTP-XXXXXX.
    O CPF/WhatsApp ficam salvos para histórico e bloqueio de novo teste.
    """
    criar_tabela_demos()
    limpar_demos_expiradas()

    nome = (nome or "").strip() or "Solicitante Demo"
    cpf_limpo = somente_digitos(cpf)
    whatsapp_limpo = somente_digitos(whatsapp)

    if cpf_limpo and not cpf_valido(cpf_limpo):
        raise ValueError("CPF inválido.")

    if cpf_limpo or whatsapp_limpo:
        ja_usou = demo_ja_usada_por_cpf_ou_whatsapp(cpf_limpo, whatsapp_limpo)
        if ja_usou:
            raise ValueError("Este CPF ou WhatsApp já utilizou a demonstração gratuita.")

    with conectar() as conn:
        with conn.cursor() as cur:
            tabelas = _buscar_tabelas_publicas(cur)

            if "usuarios" not in tabelas or "competicoes" not in tabelas:
                raise RuntimeError("Tabelas usuarios/competicoes não encontradas.")

            codigo = _gerar_codigo_demo_unico(cur)
            competicao = codigo
            login = _gerar_login_demo(codigo)
            senha = _gerar_senha_demo()

            cur.execute("""
                INSERT INTO usuarios (
                    login, nome, senha, perfil, ativo, equipe, competicao_vinculada
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                login,
                f"Organizador Demo - {nome}",
                senha,
                "organizador",
                True,
                None,
                competicao,
            ))

            colunas = _buscar_colunas_cur(cur, "competicoes")

            campos = ["nome", "data", "status", "organizador_login"]
            valores = [
                competicao,
                datetime.now().strftime("%Y-%m-%d"),
                "Demo ativa",
                login,
            ]

            defaults = {
                "cidade": "Demonstração",
                "ginasio": "Ginásio Demo VolleyTable Pro",
                "categoria": "Demo",
                "sexo": "Livre",
                "divisao": "Demonstração",
                "qtd_equipes": 0,
                "formato": "grupos",
                "tem_grupos": False,
                "qtd_grupos": 0,
                "qtd_quadras": 1,
                "modo_operacao": "simples",
                "tempos_por_set": 2,
                "substituicoes_por_set": 6,
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
                "bloquear_apos_inicio": False,
                "limite_atletas": 12,
                "permitir_edicao_pos_prazo": True,
                "travada": False,
                "motivo_travamento": "",
                "travada_em": None,
            }

            for campo, valor in defaults.items():
                if campo in colunas:
                    campos.append(campo)
                    valores.append(valor)

            placeholders = ", ".join(["%s"] * len(valores))

            cur.execute(
                f"""
                INSERT INTO competicoes ({", ".join(campos)})
                VALUES ({placeholders})
                """,
                tuple(valores),
            )

            cur.execute("""
                INSERT INTO demos_temporarias (
                    codigo,
                    nome,
                    cpf,
                    whatsapp,
                    competicao,
                    login,
                    senha,
                    expira_em,
                    encerrada
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    NOW() + INTERVAL '4 hours',
                    FALSE
                )
                RETURNING *
            """, (
                codigo,
                nome,
                formatar_cpf(cpf_limpo) if cpf_limpo else "",
                whatsapp_limpo,
                competicao,
                login,
                senha,
            ))

            demo = cur.fetchone()

        conn.commit()

    try:
        garantir_quadras_competicao(competicao, 1)
    except Exception as e:
        print("AVISO: não foi possível criar quadra padrão da demo:", e)

    return demo


def buscar_demo_ativa_por_codigo(codigo):
    criar_tabela_demos()

    codigo = (codigo or "").strip()
    if not codigo:
        return None

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM demos_temporarias
                WHERE codigo = %s
                  AND encerrada = FALSE
                LIMIT 1
            """, (codigo,))
            demo = cur.fetchone()

            if not demo:
                return None

            cur.execute("SELECT NOW() AS agora")
            agora = cur.fetchone()["agora"]

            if demo.get("expira_em") and demo["expira_em"] <= agora:
                competicao = demo["competicao"]
            else:
                return demo

    limpar_demo_por_competicao(competicao, motivo="expirada")
    return None


def demo_expirada(login):
    """
    Retorna True se o login pertence a uma demo vencida.
    Se estiver vencida, limpa automaticamente a demo.
    """
    criar_tabela_demos()

    login = (login or "").strip()
    if not login:
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT competicao, expira_em
                FROM demos_temporarias
                WHERE login = %s
                  AND encerrada = FALSE
                LIMIT 1
            """, (login,))
            demo = cur.fetchone()

            if not demo:
                return False

            cur.execute("SELECT NOW() AS agora")
            agora = cur.fetchone()["agora"]

            if demo["expira_em"] <= agora:
                competicao = demo["competicao"]
            else:
                return False

    limpar_demo_por_competicao(competicao, motivo="expirada")
    return True


def usuario_eh_demo(login):
    criar_tabela_demos()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1
                FROM demos_temporarias
                WHERE login = %s
                  AND encerrada = FALSE
                LIMIT 1
            """, ((login or "").strip(),))
            return cur.fetchone() is not None


def listar_demos_admin():
    """
    Lista todas as demonstrações para o painel do superadmin.
    """
    criar_tabela_demos()
    limpar_demos_expiradas()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    *,
                    CASE
                        WHEN encerrada = TRUE THEN 'encerrada'
                        WHEN expira_em <= NOW() THEN 'expirada'
                        ELSE 'ativa'
                    END AS status_demo
                FROM demos_temporarias
                ORDER BY criado_em DESC
            """)
            return cur.fetchall()


def estender_demo(demo_id, horas):
    """
    Aumenta o tempo de uma demonstração.
    Se ela já estiver vencida, soma o tempo a partir de agora.
    """
    criar_tabela_demos()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE demos_temporarias
                SET expira_em = GREATEST(expira_em, NOW()) + (%s || ' hours')::interval,
                    encerrada = FALSE,
                    motivo_encerramento = ''
                WHERE id = %s
                RETURNING *
            """, (int(horas), int(demo_id)))
            demo = cur.fetchone()

        conn.commit()

    return demo


def encerrar_demo(demo_id):
    """
    Encerra uma demonstração manualmente pelo superadmin.
    """
    criar_tabela_demos()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT competicao
                FROM demos_temporarias
                WHERE id = %s
                LIMIT 1
            """, (int(demo_id),))
            demo = cur.fetchone()

    if not demo:
        return False

    return limpar_demo_por_competicao(
        demo["competicao"],
        motivo="encerrada_manual"
    )


def liberar_novo_teste(demo_id):
    """
    Libera o mesmo CPF/WhatsApp para pedir outra demo.
    """
    criar_tabela_demos()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE demos_temporarias
                SET liberado_novo_teste = TRUE
                WHERE id = %s
            """, (int(demo_id),))

        conn.commit()

    return True


def marcar_whatsapp_demo_enviado(demo_id):
    criar_tabela_demos()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE demos_temporarias
                SET whatsapp_enviado = TRUE
                WHERE id = %s
            """, (int(demo_id),))

        conn.commit()

    return True


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
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS exigir_foto_atleta BOOLEAN DEFAULT FALSE
            """)
            cur.execute("""
                ALTER TABLE competicoes
                ADD COLUMN IF NOT EXISTS exigir_instagram_atleta BOOLEAN DEFAULT FALSE
            """)
        conn.commit()

    _CACHE_COLUNAS.pop("competicoes", None)
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
                    permitir_edicao_pos_prazo,
                    COALESCE(exigir_foto_atleta, FALSE) AS exigir_foto_atleta,
                    COALESCE(exigir_instagram_atleta, FALSE) AS exigir_instagram_atleta
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
    permitir_edicao_pos_prazo=False,
    exigir_foto_atleta=False,
    exigir_instagram_atleta=False
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
                    permitir_edicao_pos_prazo = %s,
                    exigir_foto_atleta = %s,
                    exigir_instagram_atleta = %s
                WHERE nome = %s
            """, (
                data_limite_inscricao or None,
                hora_limite_inscricao or None,
                bloquear_apos_inicio,
                limite_atletas,
                permitir_edicao_pos_prazo,
                exigir_foto_atleta,
                exigir_instagram_atleta,
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


def criar_tabela_equipes_competicoes(force=False):
    """
    Cria a tabela de vínculo entre equipe global e competição.

    Mantém compatibilidade com o modelo antigo, onde equipes.competicao
    guardava uma única competição. Depois da criação, migra os vínculos
    antigos para equipes_competicoes sem apagar login/senha da equipe.
    """
    chave = "tabela_equipes_competicoes"
    if _schema_ja_pronto(chave, force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS equipes_competicoes (
                    id SERIAL PRIMARY KEY,
                    equipe_id INTEGER,
                    equipe_login TEXT,
                    equipe_nome TEXT NOT NULL,
                    competicao TEXT NOT NULL,
                    status TEXT DEFAULT 'ativa',
                    grupo TEXT,
                    criado_em TIMESTAMP DEFAULT NOW(),
                    UNIQUE (equipe_nome, competicao)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_equipes_competicoes_competicao
                ON equipes_competicoes (competicao)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_equipes_competicoes_login
                ON equipes_competicoes (equipe_login)
            """)

            colunas_equipes = _buscar_colunas_tabela("equipes")
            if "competicao" in colunas_equipes:
                cur.execute("""
                    INSERT INTO equipes_competicoes (equipe_id, equipe_login, equipe_nome, competicao, status)
                    SELECT
                        NULL,
                        e.login,
                        e.nome,
                        e.competicao,
                        'ativa'
                    FROM equipes e
                    WHERE COALESCE(e.competicao, '') <> ''
                    ON CONFLICT (equipe_nome, competicao) DO UPDATE
                    SET equipe_id = EXCLUDED.equipe_id,
                        equipe_login = EXCLUDED.equipe_login,
                        status = 'ativa'
                """)
        conn.commit()

    _CACHE_COLUNAS.pop("equipes_competicoes", None)
    _marcar_schema_pronto(chave)


def buscar_equipe_global_por_nome(nome_equipe, conn=None):
    criar_campos_quadro_tecnico_equipes()
    criar_campos_liberacao_extra_equipes()
    criar_campos_perfil_equipe()
    criar_campo_escudo_equipes()

    sql = """
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
            liberacao_extra_hora,
            cidade,
            responsavel,
            telefone,
            email,
            instagram,
            COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo,
            escudo_blob,
            COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo_exibicao,
            COALESCE(perfil_completo, FALSE) AS perfil_completo
        FROM equipes
        WHERE LOWER(TRIM(nome)) = LOWER(TRIM(%s))
        ORDER BY nome ASC
        LIMIT 1
    """

    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(sql, (nome_equipe,))
            return cur.fetchone()

    with conectar() as conn2:
        return buscar_equipe_global_por_nome(nome_equipe, conn2)


def buscar_equipes_globais_por_nome(termo, limite=20):
    """
    Busca no cadastro GLOBAL de equipes, sem limitar pela competição atual.
    Usado pelo organizador ao adicionar equipe: digita um nome e o sistema
    mostra possíveis equipes já cadastradas, com cidade/responsável/telefone
    para conferência antes de vincular.
    """
    termo = (termo or "").strip()
    if not termo:
        return []

    criar_campos_quadro_tecnico_equipes()
    criar_campos_liberacao_extra_equipes()
    criar_campos_perfil_equipe()
    criar_campo_escudo_equipes()

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
                    liberacao_extra_hora,
                    cidade,
                    responsavel,
                    telefone,
                    email,
                    instagram,
                    COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo,
                    escudo_blob,
                    COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo_exibicao,
                    COALESCE(perfil_completo, FALSE) AS perfil_completo
                FROM equipes
                WHERE LOWER(TRIM(nome)) LIKE LOWER(TRIM(%s))
                ORDER BY
                    CASE WHEN LOWER(TRIM(nome)) = LOWER(TRIM(%s)) THEN 0 ELSE 1 END,
                    nome ASC,
                    login ASC
                LIMIT %s
            """, (f"%{termo}%", termo, limite))
            return cur.fetchall()

def vincular_equipe_a_competicao(nome_equipe, nome_competicao, conn=None):
    criar_tabela_equipes_competicoes()

    def _executar(cnx):
        equipe = buscar_equipe_global_por_nome(nome_equipe, cnx)
        if not equipe:
            return None

        with cnx.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM equipes_competicoes
                WHERE equipe_login = %s
                  AND competicao = %s
                LIMIT 1
            """, (equipe["login"], nome_competicao))
            ja_vinculada = cur.fetchone() is not None

            cur.execute("""
                INSERT INTO equipes_competicoes (equipe_login, equipe_nome, competicao, status)
                VALUES (%s, %s, %s, 'ativa')
                ON CONFLICT (equipe_nome, competicao) DO UPDATE
                SET equipe_login = EXCLUDED.equipe_login,
                    status = 'ativa'
            """, (equipe["login"], equipe["nome"], nome_competicao))

            colunas_equipes = _buscar_colunas_tabela("equipes")
            if "competicao" in colunas_equipes and not (equipe.get("competicao") if isinstance(equipe, dict) else None):
                cur.execute("""
                    UPDATE equipes
                    SET competicao = COALESCE(NULLIF(competicao, ''), %s)
                    WHERE login = %s
                """, (nome_competicao, equipe["login"]))

            cur.execute("""
                UPDATE usuarios
                SET competicao_vinculada = COALESCE(NULLIF(competicao_vinculada, ''), %s),
                    equipe = %s
                WHERE login = %s
                  AND perfil = 'equipe'
            """, (nome_competicao, equipe["nome"], equipe["login"]))

        return {
            "login": equipe["login"],
            "senha": equipe["senha"],
            "nome": equipe["nome"],
            "escudo": equipe.get("escudo") if isinstance(equipe, dict) else None,
            "escudo_blob": equipe.get("escudo_blob") if isinstance(equipe, dict) else None,
            "escudo_exibicao": equipe.get("escudo_exibicao") if isinstance(equipe, dict) else None,
            "ja_vinculada": ja_vinculada,
            "vinculada": True,
        }

    if conn is not None:
        return _executar(conn)

    with conectar() as conn2:
        resultado = _executar(conn2)
        conn2.commit()
        return resultado


def vincular_equipe_existente_competicao(login_equipe, nome_competicao, conn=None):
    """
    Vincula uma equipe global existente à competição atual pelo LOGIN.
    Isso evita erro quando existem equipes com nomes parecidos ou duplicados.
    """
    login_equipe = (login_equipe or "").strip()
    nome_competicao = (nome_competicao or "").strip()

    if not login_equipe or not nome_competicao:
        return None

    criar_campos_quadro_tecnico_equipes()
    criar_campos_liberacao_extra_equipes()
    criar_campos_perfil_equipe()
    criar_campo_escudo_equipes()
    criar_tabela_equipes_competicoes()

    def _executar(cnx):
        with cnx.cursor() as cur:
            cur.execute("""
                SELECT
                    nome,
                    login,
                    senha,
                    competicao,
                    cidade,
                    responsavel,
                    telefone,
                    email,
                    instagram,
                    COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo,
                    escudo_blob,
                    COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo_exibicao,
                    COALESCE(perfil_completo, FALSE) AS perfil_completo
                FROM equipes
                WHERE login = %s
                LIMIT 1
            """, (login_equipe,))
            equipe = cur.fetchone()

            if not equipe:
                return None

            cur.execute("""
                SELECT id
                FROM equipes_competicoes
                WHERE equipe_login = %s
                  AND competicao = %s
                LIMIT 1
            """, (equipe["login"], nome_competicao))
            ja_vinculada = cur.fetchone() is not None

            cur.execute("""
                INSERT INTO equipes_competicoes (equipe_login, equipe_nome, competicao, status)
                VALUES (%s, %s, %s, 'ativa')
                ON CONFLICT (equipe_nome, competicao) DO UPDATE
                SET equipe_login = EXCLUDED.equipe_login,
                    equipe_nome = EXCLUDED.equipe_nome,
                    status = 'ativa'
            """, (equipe["login"], equipe["nome"], nome_competicao))

            colunas_equipes = _buscar_colunas_tabela("equipes")
            if "competicao" in colunas_equipes:
                cur.execute("""
                    UPDATE equipes
                    SET competicao = COALESCE(NULLIF(competicao, ''), %s)
                    WHERE login = %s
                """, (nome_competicao, equipe["login"]))

            cur.execute("""
                UPDATE usuarios
                SET equipe = %s,
                    competicao_vinculada = COALESCE(NULLIF(competicao_vinculada, ''), %s)
                WHERE login = %s
                  AND perfil = 'equipe'
            """, (equipe["nome"], nome_competicao, equipe["login"]))

            return {
                "login": equipe["login"],
                "senha": equipe["senha"],
                "nome": equipe["nome"],
                "escudo": equipe.get("escudo") if isinstance(equipe, dict) else None,
                "escudo_exibicao": equipe.get("escudo_exibicao") if isinstance(equipe, dict) else None,
                "ja_existia": True,
                "ja_vinculada": ja_vinculada,
                "vinculada": True,
            }

    if conn is not None:
        return _executar(conn)

    with conectar() as conn2:
        resultado = _executar(conn2)
        conn2.commit()
        return resultado

def listar_competicoes_da_equipe_por_login(login):
    criar_tabela_equipes_competicoes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    ec.competicao AS nome,
                    COALESCE(c.status, '') AS status,
                    COALESCE(c.data, '') AS data
                FROM equipes_competicoes ec
                LEFT JOIN competicoes c
                    ON c.nome = ec.competicao
                WHERE (
                    ec.equipe_login = %s
                    OR LOWER(ec.equipe_nome) = LOWER(
                        COALESCE(
                            (SELECT equipe
                             FROM usuarios
                             WHERE login = %s
                             LIMIT 1),
                            ''
                        )
                    )
                )
                AND COALESCE(ec.status, 'ativa') = 'ativa'
                ORDER BY
                    c.data DESC NULLS LAST,
                    ec.competicao
            """, (login, login))

            return cur.fetchall()

def listar_equipes_da_competicao(nome_competicao):
    """Lista as equipes vinculadas à competição já com escudo pronto para exibição.

    Correção importante:
    - a tela do organizador lista equipes a partir de equipes_competicoes;
    - o escudo real fica na tabela global equipes;
    - registros antigos podem ter equipe_login vazio/desatualizado e só bater pelo nome;
    - quando o JOIN por nome encontra mais de uma linha, priorizamos a equipe cujo login
      bate com o vínculo e depois a que possui escudo salvo.

    Assim /equipes, nova vinculação e telas do organizador recebem sempre:
    escudo, escudo_blob e escudo_exibicao.
    """
    criar_campos_quadro_tecnico_equipes()
    criar_campos_liberacao_extra_equipes()
    criar_campos_perfil_equipe()
    criar_campo_escudo_equipes()
    criar_tabela_equipes_competicoes()

    nome_competicao = (nome_competicao or "").strip()
    if not nome_competicao:
        return []

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                WITH vinculos AS (
                    SELECT
                        ec.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY
                                LOWER(TRIM(COALESCE(ec.equipe_login, ''))),
                                LOWER(TRIM(COALESCE(ec.equipe_nome, ''))),
                                ec.competicao
                            ORDER BY ec.id NULLS LAST
                        ) AS rn
                    FROM equipes_competicoes ec
                    WHERE ec.competicao = %s
                      AND COALESCE(ec.status, 'ativa') = 'ativa'
                )
                SELECT
                    COALESCE(NULLIF(e.nome, ''), v.equipe_nome) AS nome,
                    COALESCE(NULLIF(e.login, ''), v.equipe_login) AS login,
                    e.senha,
                    v.competicao,
                    v.equipe_nome AS nome_vinculo,
                    v.equipe_login AS login_vinculo,
                    v.grupo,

                    e.treinador,
                    e.auxiliar_tecnico,
                    e.preparador_fisico,
                    e.medico,
                    e.liberacao_extra_inscricao,
                    e.liberacao_extra_data,
                    e.liberacao_extra_hora,
                    e.cidade,
                    e.responsavel,
                    e.telefone,
                    e.email,
                    e.instagram,

                    COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), '/static/img/escudo_padrao.svg') AS escudo,
                    e.escudo_blob,
                    COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), '/static/img/escudo_padrao.svg') AS escudo_exibicao,
                    COALESCE(e.perfil_completo, FALSE) AS perfil_completo,
                    v.status AS status_vinculo
                FROM vinculos v
                LEFT JOIN LATERAL (
                    SELECT e2.*
                    FROM equipes e2
                    WHERE
                        (
                            COALESCE(v.equipe_login, '') <> ''
                            AND LOWER(TRIM(e2.login)) = LOWER(TRIM(v.equipe_login))
                        )
                        OR (
                            COALESCE(v.equipe_nome, '') <> ''
                            AND LOWER(TRIM(e2.nome)) = LOWER(TRIM(v.equipe_nome))
                        )
                    ORDER BY
                        CASE
                            WHEN COALESCE(v.equipe_login, '') <> ''
                             AND LOWER(TRIM(e2.login)) = LOWER(TRIM(v.equipe_login)) THEN 0
                            ELSE 1
                        END,
                        CASE
                            WHEN COALESCE(NULLIF(e2.escudo_blob, ''), NULLIF(e2.escudo, '')) IS NOT NULL THEN 0
                            ELSE 1
                        END,
                        e2.nome ASC,
                        e2.login ASC
                    LIMIT 1
                ) e ON TRUE
                WHERE v.rn = 1
                ORDER BY COALESCE(NULLIF(e.nome, ''), v.equipe_nome) ASC
            """, (nome_competicao,))
            return cur.fetchall()

def buscar_equipe_por_nome_e_competicao(nome_equipe, nome_competicao):
    criar_campos_quadro_tecnico_equipes()
    criar_campos_liberacao_extra_equipes()
    criar_campos_perfil_equipe()
    criar_campo_escudo_equipes()
    criar_tabela_equipes_competicoes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    e.nome,
                    e.login,
                    e.senha,
                    ec.competicao,
                    e.treinador,
                    e.auxiliar_tecnico,
                    e.preparador_fisico,
                    e.medico,
                    e.liberacao_extra_inscricao,
                    e.liberacao_extra_data,
                    e.liberacao_extra_hora,
                    e.cidade,
                    e.responsavel,
                    e.telefone,
                    e.email,
                    e.instagram,
                    COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), '/static/img/escudo_padrao.svg') AS escudo,
                    e.escudo_blob,
                    COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), '/static/img/escudo_padrao.svg') AS escudo_exibicao,
                    COALESCE(e.perfil_completo, FALSE) AS perfil_completo,
                    ec.status AS status_vinculo
                FROM equipes_competicoes ec
                JOIN equipes e
                  ON e.login = ec.equipe_login
                WHERE LOWER(ec.equipe_nome) = LOWER(%s)
                  AND ec.competicao = %s
                LIMIT 1
            """, (nome_equipe, nome_competicao))
            equipe = cur.fetchone()

            if equipe:
                return equipe

            # Compatibilidade com registros antigos ainda não migrados.
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
                    liberacao_extra_hora,
                    cidade,
                    responsavel,
                    telefone,
                    email,
                    instagram,
                    COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo,
                    escudo_blob,
                    COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo_exibicao,
                    COALESCE(perfil_completo, FALSE) AS perfil_completo,
                    'ativa' AS status_vinculo
                FROM equipes
                WHERE LOWER(nome) = LOWER(%s)
                  AND competicao = %s
                LIMIT 1
            """, (nome_equipe, nome_competicao))
            return cur.fetchone()


def buscar_equipe_por_login(login, competicao_atual=None):
    criar_campos_quadro_tecnico_equipes()
    criar_campos_liberacao_extra_equipes()
    criar_campos_perfil_equipe()
    criar_campo_escudo_equipes()
    criar_tabela_equipes_competicoes()

    login = (login or "").strip()
    competicao_atual = (competicao_atual or "").strip()

    if not login:
        return None

    with conectar() as conn:
        with conn.cursor() as cur:
            if competicao_atual:
                cur.execute("""
                    SELECT
                        e.nome,
                        e.login,
                        e.senha,

                        e.cidade,
                        e.responsavel,
                        e.telefone,
                        e.email,
                        e.instagram,
                        COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), '/static/img/escudo_padrao.svg') AS escudo,
                        e.escudo_blob,
                        COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), '/static/img/escudo_padrao.svg') AS escudo_exibicao,
                        COALESCE(e.perfil_completo, FALSE) AS perfil_completo,

                        ec.competicao,
                        ec.grupo,
                        ec.status AS status_vinculo,

                        e.treinador,
                        e.auxiliar_tecnico,
                        e.preparador_fisico,
                        e.medico,
                        e.liberacao_extra_inscricao,
                        e.liberacao_extra_data,
                        e.liberacao_extra_hora
                    FROM equipes e
                    JOIN equipes_competicoes ec
                      ON ec.equipe_login = e.login
                      OR LOWER(TRIM(e.nome)) = LOWER(TRIM(ec.equipe_nome))
                    WHERE (
                            e.login = %s
                         OR ec.equipe_login = %s
                         OR LOWER(TRIM(ec.equipe_nome)) = LOWER(TRIM(COALESCE((
                                SELECT u.equipe
                                FROM usuarios u
                                WHERE u.login = %s
                                LIMIT 1
                            ), '')))
                    )
                      AND ec.competicao = %s
                      AND COALESCE(ec.status, 'ativa') = 'ativa'
                    LIMIT 1
                """, (login, login, login, competicao_atual))

                equipe = cur.fetchone()

                if equipe:
                    return equipe

                return None

            cur.execute("""
                SELECT
                    e.nome,
                    e.login,
                    e.senha,

                    e.cidade,
                    e.responsavel,
                    e.telefone,
                    e.email,
                    e.instagram,
                    COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), '/static/img/escudo_padrao.svg') AS escudo,
                    e.escudo_blob,
                    COALESCE(NULLIF(e.escudo_blob, ''), NULLIF(e.escudo, ''), '/static/img/escudo_padrao.svg') AS escudo_exibicao,
                    COALESCE(e.perfil_completo, FALSE) AS perfil_completo,

                    NULL::text AS competicao,
                    NULL::text AS grupo,
                    NULL::text AS status_vinculo,

                    e.treinador,
                    e.auxiliar_tecnico,
                    e.preparador_fisico,
                    e.medico,
                    e.liberacao_extra_inscricao,
                    e.liberacao_extra_data,
                    e.liberacao_extra_hora
                FROM equipes e
                WHERE e.login = %s
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
    criar_tabela_equipes_competicoes()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM equipes_competicoes
                WHERE LOWER(equipe_nome) = LOWER(%s)
                  AND competicao = %s
                LIMIT 1
            """, (nome_equipe, nome_competicao))
            if cur.fetchone() is not None:
                return True

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
    criar_tabela_equipes_competicoes()

    with conectar() as conn:
        with conn.cursor() as cur:
            equipe_existente = buscar_equipe_global_por_nome(nome_equipe, conn)

            if equipe_existente:
                resultado = vincular_equipe_a_competicao(equipe_existente["nome"], nome_competicao, conn)
                conn.commit()
                return {
                    "login": resultado["login"],
                    "senha": resultado["senha"],
                    "nome": resultado["nome"],
                    "vinculada": True,
                    "ja_existia": True,
                    "ja_vinculada": resultado.get("ja_vinculada", False),
                }

            login_equipe = _gerar_login_unico(_normalizar_login_equipe(nome_equipe))
            senha_equipe = _gerar_senha_aleatoria(8)

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
                INSERT INTO equipes_competicoes (equipe_login, equipe_nome, competicao, status)
                VALUES (%s, %s, %s, 'ativa')
                ON CONFLICT (equipe_nome, competicao) DO UPDATE
                SET equipe_login = EXCLUDED.equipe_login,
                    status = 'ativa'
            """, (login_equipe, nome_equipe, nome_competicao))

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
        "senha": senha_equipe,
        "nome": nome_equipe,
        "vinculada": True,
        "ja_existia": False,
        "ja_vinculada": False,
    }



def criar_nova_equipe_com_credenciais(nome_equipe, nome_competicao):
    """
    Cria uma NOVA equipe global mesmo que já exista outra com nome parecido.
    Usado quando o organizador conferiu os resultados e escolheu criar uma nova.
    """
    criar_campos_quadro_tecnico_equipes()
    criar_campos_liberacao_extra_equipes()
    criar_campos_perfil_equipe()
    criar_tabela_equipes_competicoes()

    nome_equipe = (nome_equipe or "").strip()
    nome_competicao = (nome_competicao or "").strip()

    if not nome_equipe or not nome_competicao:
        return None

    with conectar() as conn:
        with conn.cursor() as cur:
            login_equipe = _gerar_login_unico(_normalizar_login_equipe(nome_equipe))
            senha_equipe = _gerar_senha_aleatoria(8)

            cur.execute("""
                INSERT INTO equipes (
                    nome, login, senha, competicao,
                    treinador, auxiliar_tecnico, preparador_fisico, medico,
                    liberacao_extra_inscricao, liberacao_extra_data, liberacao_extra_hora,
                    cidade, responsavel, telefone, email, instagram, escudo, perfil_completo
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE)
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
                None,
                "",
                "",
                "",
                "",
                "",
                "",
            ))

            cur.execute("""
                INSERT INTO equipes_competicoes (equipe_login, equipe_nome, competicao, status)
                VALUES (%s, %s, %s, 'ativa')
                ON CONFLICT (equipe_nome, competicao) DO UPDATE
                SET equipe_login = EXCLUDED.equipe_login,
                    status = 'ativa'
            """, (login_equipe, nome_equipe, nome_competicao))

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
                nome_competicao,
            ))

        conn.commit()

    return {
        "login": login_equipe,
        "senha": senha_equipe,
        "nome": nome_equipe,
        "vinculada": True,
        "ja_existia": False,
        "ja_vinculada": False,
    }

def atualizar_nome_equipe(nome_atual, nome_competicao, novo_nome):
    """
    Atualiza o nome de exibição da equipe mantendo todos os vínculos antigos.

    Ponto importante: várias partes antigas do sistema ainda guardam o nome da
    equipe como texto (partidas.equipe_a/equipe_b, grupos_equipes.equipe,
    atletas.equipe, vencedor etc.). Quando o organizador renomeava a equipe,
    somente equipes/equipes_competicoes/usuarios eram atualizadas, e a tabela
    continuava mostrando o nome antigo. Aqui atualizamos todos os campos legados
    que dependem do nome para que tabela, jogos, visualizador público e escudos
    passem a apontar para o cadastro atual.
    """
    ok_edicao, _ = validar_competicao_editavel(nome_competicao, "alteração estrutural")
    if not ok_edicao:
        return False

    nome_atual = (nome_atual or "").strip()
    novo_nome = (novo_nome or "").strip()
    nome_competicao = (nome_competicao or "").strip()

    if not nome_atual or not novo_nome or not nome_competicao:
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    e.login,
                    e.nome AS nome_global,
                    ec.equipe_nome AS nome_vinculo
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
            nome_global_antigo = (equipe.get("nome_global") or "").strip()
            nome_vinculo_antigo = (equipe.get("nome_vinculo") or "").strip()

            nomes_antigos = []
            for valor in (nome_atual, nome_global_antigo, nome_vinculo_antigo):
                valor = (valor or "").strip()
                if valor and valor.lower() not in {v.lower() for v in nomes_antigos}:
                    nomes_antigos.append(valor)

            cur.execute("""
                UPDATE equipes
                SET nome = %s
                WHERE login = %s
            """, (novo_nome, login_equipe))

            cur.execute("""
                UPDATE equipes_competicoes
                SET equipe_nome = %s,
                    equipe_login = %s
                WHERE competicao = %s
                  AND (
                        equipe_login = %s
                     OR LOWER(TRIM(equipe_nome)) = ANY(%s)
                  )
            """, (novo_nome, login_equipe, nome_competicao, login_equipe, [n.lower() for n in nomes_antigos]))

            cur.execute("""
                UPDATE usuarios
                SET nome = %s,
                    equipe = %s
                WHERE login = %s
                  AND perfil = 'equipe'
            """, (novo_nome, novo_nome, login_equipe))

            # Campos legados que guardam o NOME da equipe como texto.
            # Mantém a tabela/jogos/público coerentes logo após renomear.
            for nome_antigo in nomes_antigos:
                cur.execute("""
                    UPDATE grupos_equipes
                    SET equipe = %s
                    WHERE competicao = %s
                      AND LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
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
                    WHERE competicao = %s
                      AND (
                            LOWER(TRIM(COALESCE(equipe_a, ''))) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(COALESCE(equipe_b, ''))) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(COALESCE(equipe_a_operacional, ''))) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(COALESCE(equipe_b_operacional, ''))) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(COALESCE(lado_esquerdo, ''))) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(COALESCE(saque_inicial, ''))) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(COALESCE(sorteio_vencedor, ''))) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(COALESCE(vencedor, ''))) = LOWER(TRIM(%s))
                      )
                """, (
                    nome_antigo, novo_nome,
                    nome_antigo, novo_nome,
                    nome_antigo, novo_nome,
                    nome_antigo, novo_nome,
                    nome_antigo, novo_nome,
                    nome_antigo, novo_nome,
                    nome_antigo, novo_nome,
                    nome_antigo, novo_nome,
                    nome_competicao,
                    nome_antigo, nome_antigo, nome_antigo, nome_antigo,
                    nome_antigo, nome_antigo, nome_antigo, nome_antigo,
                ))

                cur.execute("""
                    UPDATE atletas
                    SET equipe = %s
                    WHERE competicao = %s
                      AND LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                """, (novo_nome, nome_competicao, nome_antigo))

                # Papeletas/ações antigas podem existir em bancos que já têm essas tabelas.
                # Usa try para não quebrar bancos sem as tabelas/colunas.
                for tabela, campo in (
                    ("papeletas", "equipe"),
                    ("papeleta", "equipe"),
                    ("eventos_partida", "equipe"),
                    ("eventos", "equipe"),
                    ("historico_rotacao", "equipe"),
                ):
                    try:
                        colunas = _buscar_colunas_tabela(tabela)
                        if campo in colunas and "competicao" in colunas:
                            cur.execute(
                                f"""
                                UPDATE {tabela}
                                SET {campo} = %s
                                WHERE competicao = %s
                                  AND LOWER(TRIM({campo})) = LOWER(TRIM(%s))
                                """,
                                (novo_nome, nome_competicao, nome_antigo),
                            )
                    except Exception as e:
                        print(f"AVISO atualizar_nome_equipe/{tabela}.{campo}:", repr(e))

        conn.commit()

    return True, "Atualizado com sucesso!"

def redefinir_senha_da_equipe(nome_equipe, nome_competicao):
    nova_senha = _gerar_senha_aleatoria(8)

    with conectar() as conn:
        with conn.cursor() as cur:
            equipe = buscar_equipe_por_nome_e_competicao(nome_equipe, nome_competicao)

            if not equipe:
                return None

            login_equipe = equipe["login"]

            cur.execute("""
                UPDATE equipes
                SET senha = %s
                WHERE login = %s
            """, (nova_senha, login_equipe))

            cur.execute("""
                UPDATE usuarios
                SET senha = %s
                WHERE login = %s
                  AND perfil = 'equipe'
            """, (nova_senha, login_equipe))

        conn.commit()

    return {
        "login": login_equipe,
        "senha": nova_senha
    }


def excluir_equipe(nome_equipe, nome_competicao):
    """
    Remove a equipe somente da competição atual.

    IMPORTANTE:
    - Não apaga o cadastro global da equipe.
    - Não apaga login/senha da equipe.
    - Remove o vínculo correto mesmo quando o nome exibido vem de equipes.nome
      e o vínculo antigo em equipes_competicoes.equipe_nome está diferente.
    """
    nome_equipe = (nome_equipe or "").strip()
    nome_competicao = (nome_competicao or "").strip()

    if not nome_equipe or not nome_competicao:
        return False

    ok_edicao, _ = validar_competicao_editavel(nome_competicao, "alteração estrutural")
    if not ok_edicao:
        return False

    criar_tabela_equipes_competicoes()

    with conectar() as conn:
        with conn.cursor() as cur:
            # Localiza o vínculo real da equipe nesta competição.
            # Usa ec.equipe_nome E e.nome porque o nome pode ter sido alterado
            # depois do vínculo ter sido criado.
            cur.execute("""
                SELECT
                    ec.id,
                    ec.equipe_login,
                    ec.equipe_nome,
                    e.login AS login_global,
                    e.nome AS nome_global
                FROM equipes_competicoes ec
                LEFT JOIN equipes e
                  ON e.login = ec.equipe_login
                  OR LOWER(TRIM(e.nome)) = LOWER(TRIM(ec.equipe_nome))
                WHERE ec.competicao = %s
                  AND (
                        LOWER(TRIM(ec.equipe_nome)) = LOWER(TRIM(%s))
                     OR LOWER(TRIM(e.nome)) = LOWER(TRIM(%s))
                  )
                ORDER BY ec.id
                LIMIT 1
            """, (nome_competicao, nome_equipe, nome_equipe))
            vinculo = cur.fetchone()

            if not vinculo:
                return False

            vinculo_id = vinculo.get("id")
            login_equipe = (vinculo.get("equipe_login") or vinculo.get("login_global") or "").strip()
            nome_vinculo = (vinculo.get("equipe_nome") or "").strip()
            nome_global = (vinculo.get("nome_global") or nome_equipe).strip()

            # Remove exatamente o vínculo encontrado.
            cur.execute("""
                DELETE FROM equipes_competicoes
                WHERE id = %s
                  AND competicao = %s
            """, (vinculo_id, nome_competicao))
            removidas = cur.rowcount

            if removidas <= 0:
                conn.rollback()
                return False

            # Remove atletas somente dessa competição, tentando os nomes antigo/atual.
            cur.execute("""
                DELETE FROM atletas
                WHERE competicao = %s
                  AND (
                        LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                     OR LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                     OR LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                  )
            """, (nome_competicao, nome_equipe, nome_vinculo, nome_global))

            # Limpa vínculo antigo do usuário apenas se ele apontava para essa competição.
            if login_equipe:
                cur.execute("""
                    UPDATE usuarios
                    SET competicao_vinculada = NULL
                    WHERE perfil = 'equipe'
                      AND login = %s
                      AND competicao_vinculada = %s
                """, (login_equipe, nome_competicao))
            else:
                cur.execute("""
                    UPDATE usuarios
                    SET competicao_vinculada = NULL
                    WHERE perfil = 'equipe'
                      AND competicao_vinculada = %s
                      AND (
                            LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(equipe)) = LOWER(TRIM(%s))
                      )
                """, (nome_competicao, nome_equipe, nome_vinculo, nome_global))

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
                WHERE perfil IN ('mesario', 'arbitro')
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
                WHERE perfil IN ('mesario', 'arbitro')
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
                "arbitro",
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
                WHERE perfil IN ('mesario', 'arbitro')
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
                  AND perfil IN ('mesario', 'arbitro')
                  AND competicao_vinculada = %s
            """, (nova_senha, login_mesario, nome_competicao))

        conn.commit()

    return {"login": login_mesario, "senha": nova_senha}


def excluir_mesario(nome_mesario, nome_competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM usuarios
                WHERE perfil IN ('mesario', 'arbitro')
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
                    cpf TEXT NOT NULL,
                    data_nascimento TEXT,
                    numero INTEGER,
                    equipe TEXT,
                    competicao TEXT,
                    status TEXT DEFAULT 'pendente'
                )
            """)

            # Vínculo estável para não depender do nome da equipe.
            # Nome e login podem mudar; por isso mantemos compatibilidade com
            # dados antigos, mas passamos a salvar também o login/id quando possível.
            cur.execute("ALTER TABLE atletas ADD COLUMN IF NOT EXISTS equipe_login TEXT")
            cur.execute("ALTER TABLE atletas ADD COLUMN IF NOT EXISTS equipe_id INTEGER")
            cur.execute("ALTER TABLE atletas ADD COLUMN IF NOT EXISTS foto_atleta TEXT")
            cur.execute("ALTER TABLE atletas ADD COLUMN IF NOT EXISTS instagram TEXT")

            # Cadastro global persistente por CPF.
            # A tabela atletas é o vínculo do atleta com equipe/competição.
            # Se a equipe excluir o vínculo, os dados globais não podem sumir.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS atletas_globais (
                    cpf_limpo TEXT PRIMARY KEY,
                    cpf TEXT,
                    nome TEXT,
                    data_nascimento TEXT,
                    foto_atleta TEXT,
                    instagram TEXT,
                    atualizado_em TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("ALTER TABLE atletas_globais ADD COLUMN IF NOT EXISTS cpf TEXT")
            cur.execute("ALTER TABLE atletas_globais ADD COLUMN IF NOT EXISTS nome TEXT")
            cur.execute("ALTER TABLE atletas_globais ADD COLUMN IF NOT EXISTS data_nascimento TEXT")
            cur.execute("ALTER TABLE atletas_globais ADD COLUMN IF NOT EXISTS foto_atleta TEXT")
            cur.execute("ALTER TABLE atletas_globais ADD COLUMN IF NOT EXISTS instagram TEXT")
            cur.execute("ALTER TABLE atletas_globais ADD COLUMN IF NOT EXISTS atualizado_em TIMESTAMP DEFAULT NOW()")

            # Migração leve: copia dados bons já existentes para o cadastro global.
            cur.execute(f"""
                INSERT INTO atletas_globais (cpf_limpo, cpf, nome, data_nascimento, foto_atleta, instagram, atualizado_em)
                SELECT DISTINCT ON ({_cpf_sql_limpo('cpf')})
                    {_cpf_sql_limpo('cpf')} AS cpf_limpo,
                    cpf,
                    nome,
                    data_nascimento,
                    foto_atleta,
                    instagram,
                    NOW()
                FROM atletas
                WHERE {_cpf_sql_limpo('cpf')} <> ''
                ORDER BY
                    {_cpf_sql_limpo('cpf')},
                    CASE WHEN COALESCE(foto_atleta, '') <> '' THEN 0 ELSE 1 END,
                    CASE WHEN COALESCE(instagram, '') <> '' THEN 0 ELSE 1 END,
                    id DESC
                ON CONFLICT (cpf_limpo) DO UPDATE SET
                    cpf = COALESCE(NULLIF(EXCLUDED.cpf, ''), atletas_globais.cpf),
                    nome = COALESCE(NULLIF(EXCLUDED.nome, ''), atletas_globais.nome),
                    data_nascimento = COALESCE(NULLIF(EXCLUDED.data_nascimento, ''), atletas_globais.data_nascimento),
                    foto_atleta = COALESCE(NULLIF(EXCLUDED.foto_atleta, ''), atletas_globais.foto_atleta),
                    instagram = COALESCE(NULLIF(EXCLUDED.instagram, ''), atletas_globais.instagram),
                    atualizado_em = NOW()
            """)

            # Compatibilidade com bancos antigos: remove trava global de CPF.
            cur.execute("ALTER TABLE atletas DROP CONSTRAINT IF EXISTS atletas_cpf_key")

            # O mesmo CPF pode estar em competições diferentes, mas não pode duplicar dentro da mesma competição.
            #
            # IMPORTANTE:
            # Quando uma competição é excluída, os atletas permanentes ficam no banco e
            # perdem o vínculo da competição. Como vários atletas podem ficar com
            # competicao vazia, o índice precisa ignorar registros sem competição.
            # Sem isso, a exclusão falha com:
            # duplicate key value violates unique constraint "uq_atletas_cpf_competicao"
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
            cur.execute("ALTER TABLE atletas ADD COLUMN IF NOT EXISTS capitao_padrao BOOLEAN DEFAULT FALSE")
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_atletas_competicao_equipe
                ON atletas (competicao, equipe)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_atletas_competicao_equipe_login
                ON atletas (competicao, equipe_login)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_atletas_competicao_equipe_id
                ON atletas (competicao, equipe_id)
            """)
        conn.commit()

    _CACHE_COLUNAS.pop("atletas", None)
    _marcar_schema_pronto(chave)
    # criar_indices_desempenho()

def atleta_existe_por_cpf(cpf):
    cpf_limpo = somente_digitos(cpf)
    if not cpf_limpo:
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT id
                FROM atletas
                WHERE {_cpf_sql_limpo('cpf')} = %s
                LIMIT 1
            """, (cpf_limpo,))
            return cur.fetchone() is not None



def buscar_atleta_global_por_cpf(cpf):
    """
    Busca um atleta global pelo CPF.
    Primeiro usa atletas_globais, que não é apagada quando o vínculo com a
    competição é removido. Se ainda não existir, usa a tabela atletas como fallback.
    """
    cpf_limpo = somente_digitos(cpf)
    if not cpf_limpo:
        return None

    criar_tabela_atletas()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    nome,
                    cpf,
                    data_nascimento,
                    foto_atleta,
                    instagram
                FROM atletas_globais
                WHERE cpf_limpo = %s
                LIMIT 1
            """, (cpf_limpo,))
            row = cur.fetchone()
            if row:
                return row

            cur.execute(f"""
                SELECT
                    nome,
                    cpf,
                    data_nascimento,
                    foto_atleta,
                    instagram
                FROM atletas
                WHERE {_cpf_sql_limpo('cpf')} = %s
                ORDER BY
                    CASE WHEN COALESCE(foto_atleta, '') <> '' THEN 0 ELSE 1 END,
                    CASE WHEN COALESCE(instagram, '') <> '' THEN 0 ELSE 1 END,
                    id DESC
                LIMIT 1
            """, (cpf_limpo,))
            return cur.fetchone()


def _salvar_atleta_global(cur, nome, cpf, data_nascimento, foto_atleta=None, instagram=None):
    """Salva/atualiza os dados globais do atleta por CPF.

    Usa COALESCE/NULLIF para não apagar foto/Instagram existentes quando o
    formulário vier vazio. Também replica para vínculos antigos do mesmo CPF,
    assim na próxima busca os dados aparecem completos em qualquer competição.
    """
    cpf_limpo = somente_digitos(cpf)
    if not cpf_limpo:
        return

    cpf_formatado = formatar_cpf(cpf_limpo)
    nome = (nome or '').strip()
    data_nascimento = (data_nascimento or '').strip()
    foto_atleta = (foto_atleta or '').strip()
    instagram = (instagram or '').strip()
    if instagram and not instagram.startswith('@'):
        instagram = '@' + instagram.lstrip('@')

    cur.execute("""
        INSERT INTO atletas_globais (cpf_limpo, cpf, nome, data_nascimento, foto_atleta, instagram, atualizado_em)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (cpf_limpo) DO UPDATE SET
            cpf = COALESCE(NULLIF(EXCLUDED.cpf, ''), atletas_globais.cpf),
            nome = COALESCE(NULLIF(EXCLUDED.nome, ''), atletas_globais.nome),
            data_nascimento = COALESCE(NULLIF(EXCLUDED.data_nascimento, ''), atletas_globais.data_nascimento),
            foto_atleta = COALESCE(NULLIF(EXCLUDED.foto_atleta, ''), atletas_globais.foto_atleta),
            instagram = COALESCE(NULLIF(EXCLUDED.instagram, ''), atletas_globais.instagram),
            atualizado_em = NOW()
    """, (cpf_limpo, cpf_formatado, nome, data_nascimento, foto_atleta or None, instagram or None))

    cur.execute(f"""
        UPDATE atletas
        SET
            nome = COALESCE(NULLIF(%s, ''), nome),
            cpf = COALESCE(NULLIF(%s, ''), cpf),
            data_nascimento = CASE
                WHEN NULLIF(%s, '') IS NULL THEN data_nascimento
                ELSE %s::date
            END,
            foto_atleta = COALESCE(NULLIF(%s, ''), foto_atleta),
            instagram = COALESCE(NULLIF(%s, ''), instagram)
        WHERE {_cpf_sql_limpo('cpf')} = %s
    """, (nome, cpf_formatado, data_nascimento, data_nascimento, foto_atleta, instagram, cpf_limpo))

def atleta_existe_na_competicao_por_cpf(cpf, competicao):
    cpf_limpo = somente_digitos(cpf)
    competicao = (competicao or "").strip()
    if not cpf_limpo or not competicao:
        return False

    criar_tabela_atletas()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT id
                FROM atletas
                WHERE {_cpf_sql_limpo('cpf')} = %s
                  AND competicao = %s
                LIMIT 1
            """, (cpf_limpo, competicao))
            return cur.fetchone() is not None

def cadastrar_atleta(nome, cpf, data_nascimento, numero, equipe, competicao, foto_atleta=None, instagram=None):
    nome = (nome or "").strip()
    cpf_limpo = somente_digitos(cpf)
    cpf = formatar_cpf(cpf_limpo)
    data_nascimento = (data_nascimento or "").strip()
    equipe = (equipe or "").strip()
    competicao = (competicao or "").strip()
    foto_atleta = (foto_atleta or "").strip()
    instagram = (instagram or "").strip()
    if instagram and not instagram.startswith("@"):
        instagram = "@" + instagram.lstrip("@")

    if not nome or not cpf_limpo:
        return False, "Informe nome e CPF do atleta."

    if not data_nascimento:
        return False, "Informe a data de nascimento do atleta."

    if not cpf_valido(cpf_limpo):
        return False, "CPF inválido. Informe um CPF real no formato 000.000.000-00."

    numero_final = None
    if numero not in (None, ""):
        try:
            numero_final = int(numero)
        except (TypeError, ValueError):
            return False, "Número inválido."

    criar_tabela_atletas()
    criar_campos_controle_inscricao_competicoes()
    criar_campos_liberacao_extra_equipes()
    criar_campos_conferencia_atletas()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT id
                FROM atletas
                WHERE {_cpf_sql_limpo('cpf')} = %s
                  AND competicao = %s
                LIMIT 1
            """, (cpf_limpo, competicao))
            if cur.fetchone() is not None:
                return False, "Este atleta já está cadastrado nesta competição."

            cur.execute("""
                SELECT
                    c.nome,
                    c.data_limite_inscricao,
                    c.hora_limite_inscricao,
                    COALESCE(c.bloquear_apos_inicio, TRUE) AS bloquear_apos_inicio,
                    COALESCE(c.limite_atletas, 0) AS limite_atletas,
                    COALESCE(c.exigir_foto_atleta, FALSE) AS exigir_foto_atleta,
                    COALESCE(c.exigir_instagram_atleta, FALSE) AS exigir_instagram_atleta,
                    COALESCE(c.aprovacao_automatica_atletas, FALSE) AS aprovacao_automatica_atletas,
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

            # Se o atleta já existe em outra competição, reaproveita foto/Instagram globais.
            cur.execute(f"""
                SELECT foto_atleta, instagram
                FROM atletas
                WHERE {_cpf_sql_limpo('cpf')} = %s
                  AND (COALESCE(foto_atleta, '') <> '' OR COALESCE(instagram, '') <> '')
                ORDER BY
                    CASE WHEN COALESCE(foto_atleta, '') <> '' THEN 0 ELSE 1 END,
                    CASE WHEN COALESCE(instagram, '') <> '' THEN 0 ELSE 1 END,
                    id DESC
                LIMIT 1
            """, (cpf_limpo,))
            atleta_global_dados = cur.fetchone() or {}
            if not foto_atleta:
                foto_atleta = (atleta_global_dados.get("foto_atleta") or "").strip()
            if not instagram:
                instagram = (atleta_global_dados.get("instagram") or "").strip()

            # Salva as alterações do cadastro global ANTES de vincular.
            # Assim, mesmo que a equipe exclua o atleta desta competição depois,
            # foto/Instagram/nome/data continuam guardados para a próxima busca por CPF.
            _salvar_atleta_global(cur, nome, cpf, data_nascimento, foto_atleta, instagram)

            pendencias_obrigatorias = []
            if bool(controle.get("exigir_foto_atleta")) and not foto_atleta:
                pendencias_obrigatorias.append("foto")
            if bool(controle.get("exigir_instagram_atleta")) and not instagram:
                pendencias_obrigatorias.append("Instagram")
            if pendencias_obrigatorias:
                return False, "Esta competição exige " + " e ".join(pendencias_obrigatorias) + " dos atletas. Preencha antes de concluir a inscrição."

            status_inicial = "aprovado" if bool(controle.get("aprovacao_automatica_atletas")) else "pendente"

            equipe_login_vinculo = None
            equipe_id_vinculo = None
            try:
                cur.execute("""
                    SELECT ec.equipe_login, ec.equipe_id
                    FROM equipes_competicoes ec
                    LEFT JOIN equipes e
                      ON e.login = ec.equipe_login
                      OR LOWER(TRIM(e.nome)) = LOWER(TRIM(ec.equipe_nome))
                    WHERE ec.competicao = %s
                      AND (
                            LOWER(TRIM(ec.equipe_nome)) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(COALESCE(e.nome, ''))) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(COALESCE(ec.equipe_login, ''))) = LOWER(TRIM(%s))
                         OR LOWER(TRIM(COALESCE(e.login, ''))) = LOWER(TRIM(%s))
                      )
                    ORDER BY ec.id DESC
                    LIMIT 1
                """, (competicao, equipe, equipe, equipe, equipe))
                vinculo_equipe = cur.fetchone() or {}
                equipe_login_vinculo = vinculo_equipe.get("equipe_login")
                equipe_id_vinculo = vinculo_equipe.get("equipe_id")
            except Exception as e:
                print("AVISO cadastrar_atleta/vinculo_equipe:", repr(e), flush=True)

            cur.execute("""
                INSERT INTO atletas (
                    nome, cpf, data_nascimento, numero, equipe, competicao, status, equipe_login, equipe_id, foto_atleta, instagram
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (nome, cpf, data_nascimento, numero_final, equipe, competicao, status_inicial, equipe_login_vinculo, equipe_id_vinculo, foto_atleta or None, instagram or None))
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


def atualizar_atleta_equipe(id_atleta, equipe, competicao, nome, cpf, data_nascimento, foto_atleta=None, instagram=None):
    """
    Atualiza dados básicos do atleta pela própria equipe.
    Regras:
    - Só permite editar atleta da própria equipe/competição.
    - Atleta reprovado não pode ser editado pela equipe; só excluído.
    - CPF não pode duplicar dentro da mesma competição em outro atleta.
    - Respeita o travamento da competição quando a equipe já iniciou jogos.
    """
    nome = (nome or "").strip()
    cpf = (cpf or "").strip()
    data_nascimento = (data_nascimento or "").strip()
    foto_atleta = (foto_atleta or "").strip()
    instagram = (instagram or "").strip()
    if instagram and not instagram.startswith("@"):
        instagram = "@" + instagram.lstrip("@")
    cpf_limpo = somente_digitos(cpf)

    if not nome or not cpf or not data_nascimento:
        return False, "Preencha nome, CPF e data de nascimento."

    if not cpf_valido(cpf):
        return False, "CPF inválido. Informe um CPF real."

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, equipe, competicao, status, foto_atleta, instagram
                    FROM atletas
                    WHERE id = %s
                    LIMIT 1
                """, (id_atleta,))
                atleta = cur.fetchone()

                if not atleta:
                    return False, "Atleta não encontrado."

                if atleta.get("equipe") != equipe or atleta.get("competicao") != competicao:
                    return False, "Este atleta não pertence a esta equipe."

                status = (atleta.get("status") or "").strip().lower()
                if status == "reprovado":
                    return False, "Atleta reprovado não pode ser editado. Só é possível excluir."

                cur.execute("""
                    SELECT
                        COALESCE(travada, FALSE) AS travada,
                        COALESCE(exigir_foto_atleta, FALSE) AS exigir_foto_atleta,
                        COALESCE(exigir_instagram_atleta, FALSE) AS exigir_instagram_atleta
                    FROM competicoes
                    WHERE nome = %s
                    LIMIT 1
                """, (competicao,))
                comp = cur.fetchone()

                if comp and comp.get("travada"):
                    cur.execute("""
                        SELECT id
                        FROM partidas
                        WHERE competicao = %s
                          AND (equipe_a = %s OR equipe_b = %s OR equipe_a_operacional = %s OR equipe_b_operacional = %s)
                          AND (
                              COALESCE(pontos_a, 0) > 0 OR COALESCE(pontos_b, 0) > 0
                              OR LOWER(COALESCE(status_jogo, '')) IN ('em_andamento', 'entre_sets', 'tiebreak_sorteio', 'finalizada', 'encerrado')
                              OR LOWER(COALESCE(status, '')) IN ('em_andamento', 'andamento', 'iniciada', 'iniciado', 'finalizada')
                          )
                        LIMIT 1
                    """, (competicao, equipe, equipe, equipe, equipe))

                    if cur.fetchone():
                        return False, "Competição travada: esta equipe já iniciou jogos. Edição bloqueada."

                cur.execute(f"""
                    SELECT id
                    FROM atletas
                    WHERE {_cpf_sql_limpo('cpf')} = %s
                      AND COALESCE(competicao, '') = COALESCE(%s, '')
                      AND id <> %s
                    LIMIT 1
                """, (cpf_limpo, competicao, id_atleta))
                if cur.fetchone():
                    return False, "Já existe outro atleta com este CPF nesta competição."

                foto_final = foto_atleta or (atleta.get("foto_atleta") or "")
                instagram_final = instagram or (atleta.get("instagram") or "")

                pendencias = []
                if comp and bool(comp.get("exigir_foto_atleta")) and not foto_final:
                    pendencias.append("foto")
                if comp and bool(comp.get("exigir_instagram_atleta")) and not instagram_final:
                    pendencias.append("Instagram")
                if pendencias:
                    return False, "Esta competição exige " + " e ".join(pendencias) + " dos atletas. Preencha antes de salvar."

                cur.execute("""
                    UPDATE atletas
                    SET nome = %s,
                        cpf = %s,
                        data_nascimento = %s,
                        foto_atleta = COALESCE(NULLIF(%s, ''), foto_atleta),
                        instagram = COALESCE(NULLIF(%s, ''), instagram)
                    WHERE id = %s
                """, (nome, cpf, data_nascimento, foto_atleta, instagram, id_atleta))

                _salvar_atleta_global(cur, nome, cpf, data_nascimento, foto_final, instagram_final)

            conn.commit()

        return True, "Atleta atualizado com sucesso."

    except Exception as e:
        return False, f"Erro ao atualizar atleta: {str(e)}"


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
                SELECT id, nome, cpf, data_nascimento, numero, equipe, status, foto_atleta, instagram
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


def aprovar_todos_atletas_pendentes(nome_competicao):
    nome_competicao = (nome_competicao or "").strip()

    if not nome_competicao:
        return False, "Competição inválida."

    criar_tabela_atletas()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE atletas
                SET status = 'aprovado'
                WHERE competicao = %s
                  AND LOWER(COALESCE(status, '')) = 'pendente'
            """, (nome_competicao,))
            total = cur.rowcount or 0

        conn.commit()

    if total == 0:
        return True, "Não havia atletas pendentes para aprovar."

    return True, f"{total} atleta(s) pendente(s) aprovado(s) com sucesso."


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

            cur.execute("ALTER TABLE grupos ADD COLUMN IF NOT EXISTS quadra_id INTEGER")
            cur.execute("ALTER TABLE grupos ADD COLUMN IF NOT EXISTS quadra_nome TEXT DEFAULT ''")

        conn.commit()

    _CACHE_COLUNAS.pop("grupos", None)


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


def listar_equipes_por_grupos_competicao(competicao):
    """
    Retorna todas as equipes de todos os grupos da competição em uma única consulta.

    Antes, várias telas chamavam listar_equipes_por_grupo() dentro de loop,
    gerando uma ida ao Neon para cada grupo. Em competições com muitos grupos,
    isso deixava tabela, visualizador público e painel da equipe bem mais lentos.

    Formato de retorno:
        {grupo_id: [linhas_de_grupos_equipes]}
    """
    resultado = {}
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ge.*
                FROM grupos_equipes ge
                JOIN grupos g ON g.id = ge.grupo_id
                WHERE ge.competicao = %s
                  AND g.competicao = %s
                ORDER BY g.nome, ge.equipe
            """, (competicao, competicao))
            for row in cur.fetchall() or []:
                gid = row.get("grupo_id")
                resultado.setdefault(gid, []).append(row)
    return resultado


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
                    status TEXT DEFAULT 'aguardando'
                )
            """)

            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS rodada INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS quadra TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS quadra_id INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS quadra_nome TEXT DEFAULT ''")
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
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set4_a INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set4_b INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set5_a INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set5_b INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS origem_resultado TEXT DEFAULT 'apontada'")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS scout_preenchido BOOLEAN DEFAULT FALSE")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS vencedor TEXT")

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
    # Performance: listar partidas precisa ser somente leitura.
    # Criação de campos/tabelas e normalização de quadras devem rodar no boot/migração
    # ou em ações administrativas, nunca em toda abertura de painel.
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    p.*,
                    COALESCE(cq.nome, '') AS quadra_nome_cadastro,
                    COALESCE(cq.local, '') AS quadra_local_cadastro,
                    COALESCE(ea.escudo, '') AS escudo_a,
                    COALESCE(eb.escudo, '') AS escudo_b,
                    COALESCE(ev.eventos_total, 0) AS eventos_total
                FROM partidas p
                LEFT JOIN (
                    SELECT partida_id, COUNT(*) AS eventos_total
                    FROM eventos
                    WHERE competicao = %s
                    GROUP BY partida_id
                ) ev ON ev.partida_id = p.id
                LEFT JOIN competicao_quadras cq
                  ON cq.competicao = p.competicao
                 AND cq.id = p.quadra_id
                LEFT JOIN equipes_competicoes eca
                  ON eca.competicao = p.competicao
                 AND LOWER(TRIM(eca.equipe_nome)) = LOWER(TRIM(p.equipe_a))
                LEFT JOIN equipes ea
                  ON ea.login = eca.equipe_login
                LEFT JOIN equipes_competicoes ecb
                  ON ecb.competicao = p.competicao
                 AND LOWER(TRIM(ecb.equipe_nome)) = LOWER(TRIM(p.equipe_b))
                LEFT JOIN equipes eb
                  ON eb.login = ecb.equipe_login
                WHERE p.competicao = %s
                ORDER BY COALESCE(p.rodada, 999999), p.ordem, p.id
            """, (competicao, competicao))
            linhas = cur.fetchall() or []
            for linha in linhas:
                try:
                    if linha.get("quadra_id") and linha.get("quadra_nome_cadastro"):
                        linha["quadra_nome"] = formatar_quadra_exibicao({
                            "nome": linha.get("quadra_nome_cadastro"),
                            "local": linha.get("quadra_local_cadastro"),
                            "ordem": linha.get("quadra_id"),
                        })
                        linha["quadra_label"] = linha["quadra_nome"]
                except Exception:
                    pass
            return linhas


def listar_partidas_da_equipe(competicao, equipe, limite=50):
    """Lista somente as partidas de uma equipe em uma competição.

    Versão leve para o painel inicial da equipe.
    Diferente de listar_partidas(), esta função NÃO conta eventos ponto a ponto
    e NÃO carrega todos os jogos da competição para depois filtrar em Python.
    """
    competicao = (competicao or "").strip()
    equipe = (equipe or "").strip()

    if not competicao or not equipe:
        return []

    try:
        limite_int = int(limite or 50)
    except Exception:
        limite_int = 50
    limite_int = max(1, min(limite_int, 200))

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    p.*,
                    COALESCE(cq.nome, '') AS quadra_nome_cadastro,
                    COALESCE(cq.local, '') AS quadra_local_cadastro,
                    COALESCE(ea.escudo, '') AS escudo_a,
                    COALESCE(eb.escudo, '') AS escudo_b
                FROM partidas p
                LEFT JOIN competicao_quadras cq
                  ON cq.competicao = p.competicao
                 AND cq.id = p.quadra_id
                LEFT JOIN equipes_competicoes eca
                  ON eca.competicao = p.competicao
                 AND LOWER(TRIM(eca.equipe_nome)) = LOWER(TRIM(p.equipe_a))
                LEFT JOIN equipes ea
                  ON ea.login = eca.equipe_login
                LEFT JOIN equipes_competicoes ecb
                  ON ecb.competicao = p.competicao
                 AND LOWER(TRIM(ecb.equipe_nome)) = LOWER(TRIM(p.equipe_b))
                LEFT JOIN equipes eb
                  ON eb.login = ecb.equipe_login
                WHERE p.competicao = %s
                  AND (
                        LOWER(TRIM(p.equipe_a)) = LOWER(TRIM(%s))
                     OR LOWER(TRIM(p.equipe_b)) = LOWER(TRIM(%s))
                  )
                ORDER BY
                    CASE
                        WHEN LOWER(COALESCE(p.status_jogo, p.status_operacao, p.status, '')) IN ('ao_vivo','em_andamento','andamento','jogo') THEN 1
                        WHEN LOWER(COALESCE(p.status_jogo, p.status_operacao, p.status, '')) IN ('pre_jogo','papeleta','papeleta_pronta') THEN 2
                        WHEN LOWER(COALESCE(p.status_jogo, p.status_operacao, p.status, '')) IN ('finalizada','finalizado','encerrada','encerrado') THEN 4
                        ELSE 3
                    END,
                    COALESCE(p.rodada, 999999),
                    COALESCE(p.ordem, 999999),
                    p.id
                LIMIT %s
            """, (competicao, equipe, equipe, limite_int))

            linhas = cur.fetchall() or []
            for linha in linhas:
                try:
                    if linha.get("quadra_id") and linha.get("quadra_nome_cadastro"):
                        linha["quadra_nome"] = formatar_quadra_exibicao({
                            "nome": linha.get("quadra_nome_cadastro"),
                            "local": linha.get("quadra_local_cadastro"),
                            "ordem": linha.get("quadra_id"),
                        })
                        linha["quadra_label"] = linha["quadra_nome"]
                except Exception:
                    pass
            return linhas


def buscar_partida_por_id(partida_id, competicao):
    # Performance: esta função é chamada por relatórios, apontador e telas de consulta.
    # Não rode criação/verificação de schema aqui; isso deve acontecer no boot/migração.
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    p.*,
                    COALESCE(cq.nome, '') AS quadra_nome_cadastro,
                    COALESCE(cq.local, '') AS quadra_local_cadastro,
                    COALESCE(ea.escudo, '') AS escudo_a,
                    COALESCE(eb.escudo, '') AS escudo_b
                FROM partidas p
                LEFT JOIN competicao_quadras cq
                  ON cq.competicao = p.competicao
                 AND cq.id = p.quadra_id
                LEFT JOIN equipes_competicoes eca
                  ON eca.competicao = p.competicao
                 AND LOWER(TRIM(eca.equipe_nome)) = LOWER(TRIM(p.equipe_a))
                LEFT JOIN equipes ea
                  ON ea.login = eca.equipe_login
                LEFT JOIN equipes_competicoes ecb
                  ON ecb.competicao = p.competicao
                 AND LOWER(TRIM(ecb.equipe_nome)) = LOWER(TRIM(p.equipe_b))
                LEFT JOIN equipes eb
                  ON eb.login = ecb.equipe_login
                WHERE p.id = %s
                  AND p.competicao = %s
                LIMIT 1
            """, (partida_id, competicao))
            linha = cur.fetchone()
            if linha and linha.get("quadra_id") and linha.get("quadra_nome_cadastro"):
                linha["quadra_nome"] = formatar_quadra_exibicao({
                    "nome": linha.get("quadra_nome_cadastro"),
                    "local": linha.get("quadra_local_cadastro"),
                    "ordem": linha.get("quadra_id"),
                })
                linha["quadra_label"] = linha["quadra_nome"]
            return linha

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
    """Retorna True somente quando a partida realmente saiu do estado inicial.

    IMPORTANTE:
    "pre_jogo" sozinho NÃO pode bloquear a tabela. Em bases antigas, partidas
    recém-criadas podem nascer com status_jogo='pre_jogo' por DEFAULT do banco,
    mesmo sem apontador ter aberto a conferência. Isso fazia a geração
    automática criar só o primeiro jogo e travar a fase inteira.
    """
    status = (status or "").strip().lower().replace("-", "_")
    status_jogo = (status_jogo or "").strip().lower().replace("-", "_")

    bloqueados = {
        "em_andamento", "em andamento", "andamento",
        "entre_sets", "tiebreak_sorteio", "finalizada", "finalizado",
        "encerrada", "encerrado", "iniciada", "iniciado",
        "ao_vivo", "ao vivo",
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
                         OR LOWER(REPLACE(COALESCE(status_jogo, ''), '-', '_')) IN ('em_andamento', 'em andamento', 'andamento', 'entre_sets', 'tiebreak_sorteio', 'finalizada', 'finalizado', 'encerrada', 'encerrado', 'ao_vivo', 'ao vivo')
                         OR LOWER(REPLACE(COALESCE(status, ''), '-', '_')) IN ('em_andamento', 'em andamento', 'andamento', 'iniciada', 'iniciado', 'finalizada', 'finalizado', 'encerrada', 'encerrado', 'ao_vivo', 'ao vivo')
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

def criar_partida(competicao, grupo, equipe_a, equipe_b, ordem, quadra=None, fase='grupos', data_hora=None, rodada=None, origem='manual', quadra_id=None, quadra_nome=None):
    fase = _normalizar_fase_partida(fase)
    grupo = grupo if fase == "grupos" else None

    if quadra_id:
        q = buscar_quadra_competicao_por_id(competicao, quadra_id)
        if q:
            quadra_id = int(q["id"])
            quadra_nome = formatar_quadra_exibicao(q)
            quadra = str(quadra_id)
    elif quadra_nome or quadra:
        q = buscar_quadra_competicao_por_texto(competicao, quadra_nome or quadra)
        if q:
            quadra_id = int(q["id"])
            quadra_nome = formatar_quadra_exibicao(q)
            quadra = str(quadra_id)

    if not fase_partidas_pode_ser_alterada(competicao, fase):
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            colunas_partidas = _buscar_colunas_tabela("partidas")

            campos = [
                "competicao", "grupo", "equipe_a", "equipe_b", "fase", "ordem",
                "quadra", "quadra_id", "quadra_nome", "data_hora", "rodada",
                "origem", "status",
            ]
            valores = [
                competicao, grupo, equipe_a, equipe_b, fase, ordem,
                quadra, quadra_id, quadra_nome or quadra or '', data_hora, rodada,
                origem, "aguardando",
            ]

            # Evita que DEFAULT antigo do banco salve partida nova como PRÉ-JOGO.
            # Jogo gerado/manual deve nascer como AGUARDANDO.
            if "status_jogo" in colunas_partidas:
                campos.append("status_jogo")
                valores.append("aguardando")

            if "fase_partida" in colunas_partidas:
                campos.append("fase_partida")
                valores.append("aguardando")

            for campo in ("sets_a", "sets_b"):
                if campo in colunas_partidas:
                    campos.append(campo)
                    valores.append(0)

            for campo in ("set1_a", "set1_b", "set2_a", "set2_b", "set3_a", "set3_b"):
                if campo in colunas_partidas:
                    campos.append(campo)
                    valores.append(None)

            placeholders = ", ".join(["%s"] * len(valores))
            cur.execute(
                f"""
                INSERT INTO partidas ({", ".join(campos)})
                VALUES ({placeholders})
                """,
                tuple(valores)
            )
        conn.commit()


def atualizar_partida(partida_id, competicao, grupo, fase, equipe_a, equipe_b, quadra=None, data_hora=None, status='aguardando', rodada=None, quadra_id=None, quadra_nome=None):
    fase = _normalizar_fase_partida(fase)
    grupo = grupo if fase == "grupos" else None

    if quadra_id:
        q = buscar_quadra_competicao_por_id(competicao, quadra_id)
        if q:
            quadra_id = int(q["id"])
            quadra_nome = formatar_quadra_exibicao(q)
            quadra = str(quadra_id)
    elif quadra_nome or quadra:
        q = buscar_quadra_competicao_por_texto(competicao, quadra_nome or quadra)
        if q:
            quadra_id = int(q["id"])
            quadra_nome = formatar_quadra_exibicao(q)
            quadra = str(quadra_id)

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
                    quadra_id = %s,
                    quadra_nome = %s,
                    data_hora = %s,
                    status = %s,
                    status_jogo = %s,
                    fase_partida = %s,
                    rodada = %s
                WHERE id = %s
                  AND competicao = %s
            """, (grupo, fase, equipe_a, equipe_b, quadra, quadra_id, quadra_nome or quadra or '', data_hora, status, status, status, rodada, partida_id, competicao))
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

    # IMPORTANTE:
    # Não podemos deixar o app travar na inicialização esperando ALTER TABLE.
    # Em banco remoto (Neon/Postgres), ALTER TABLE pode ficar preso se existir
    # alguma transação aberta/idle usando a tabela partidas. Por isso primeiro
    # conferimos as colunas existentes e, se faltar algo, usamos lock_timeout
    # curto. Se o banco estiver bloqueado, o app sobe normalmente e tenta de novo
    # em outra inicialização, sem deixar o terminal parado por vários minutos.
    try:
        colunas = _buscar_colunas_tabela("partidas")
    except Exception as e:
        print("AVISO criar_campos_rotacao_partidas/colunas:", repr(e))
        return

    campos_necessarios = {
        "rotacao_a": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS rotacao_a TEXT[]",
        "rotacao_b": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS rotacao_b TEXT[]",
        "saque_atual": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS saque_atual TEXT",
        "saque_inicial": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS saque_inicial TEXT",
        "rotacao_validacao_ativa": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS rotacao_validacao_ativa BOOLEAN DEFAULT TRUE",
    }

    faltantes = [nome for nome in campos_necessarios if nome not in colunas]

    if not faltantes:
        _marcar_schema_pronto(chave)
        return

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = '1500ms'")
                cur.execute("SET LOCAL statement_timeout = '5000ms'")

                for nome in faltantes:
                    cur.execute(campos_necessarios[nome])

            conn.commit()

        _marcar_schema_pronto(chave)

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print("AVISO criar_campos_rotacao_partidas: banco ocupado/bloqueado, app continuará sem travar:", repr(e))
        return


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
    """
    Normaliza a rotação na ordem visual/oficial do sistema:
    [IV, III, II, V, VI, I].

    Aceita lista de números, lista de dicts, tuplas ou JSON em texto.
    Isso evita que um estado parcial/socket/JSON antigo quebre um lado da rotação.
    """
    if isinstance(rotacao, str):
        try:
            rotacao = json.loads(rotacao or "[]")
        except Exception:
            rotacao = []

    if isinstance(rotacao, tuple):
        rotacao = list(rotacao)

    if not isinstance(rotacao, list):
        rotacao = []

    normalizada = []

    for item in rotacao[:6]:
        if isinstance(item, dict):
            numero = (
                item.get("numero")
                or item.get("camisa")
                or item.get("numero_camisa")
                or item.get("n")
                or ""
            )
        else:
            numero = item

        normalizada.append(str(numero or "").strip())

    while len(normalizada) < 6:
        normalizada.append("")

    return normalizada[:6]


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
    cpf_limpo = somente_digitos(cpf)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM oficiais
                WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                LIMIT 1
            """, (cpf_limpo,))
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
    cpf_limpo = somente_digitos(cpf)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM apontadores_acesso
                WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                LIMIT 1
            """, (cpf_limpo,))
            return cur.fetchone() is not None


def criar_apontador(cpf):
    cpf_limpo = somente_digitos(cpf)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM apontadores_acesso
                WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                LIMIT 1
            """, (cpf_limpo,))
            existente = cur.fetchone()

            if not existente:
                cur.execute("""
                    INSERT INTO apontadores_acesso (cpf, senha, ativo, primeiro_acesso)
                    VALUES (%s, NULL, TRUE, TRUE)
                    ON CONFLICT (cpf) DO NOTHING
                """, (cpf_limpo,))
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
# CONFIGURAÇÃO DE AGENDA / GERAÇÃO INTELIGENTE DE PARTIDAS
# =========================================================
def criar_tabela_competicao_agenda_config(force=False):
    """Cria tabela isolada para configurações do motor de agenda.

    Mantida fora da tabela competicoes para não quebrar bancos antigos e para
    permitir evoluir a geração automática sem alterar a estrutura principal da competição.
    """
    try:
        if _schema_ja_pronto("tabela_competicao_agenda_config", force=force):
            return
    except Exception:
        pass

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS competicao_agenda_config (
                    competicao TEXT PRIMARY KEY,
                    modo_distribuicao TEXT DEFAULT 'automatico_inteligente',
                    descanso_minimo_jogos INTEGER DEFAULT 1,
                    rodizio_grupos TEXT DEFAULT 'por_rodada',
                    permitir_relaxar_descanso BOOLEAN DEFAULT TRUE,
                    grupos_compartilhados_json JSONB DEFAULT '{}'::jsonb,
                    quadras_compartilhadas_json JSONB DEFAULT '[]'::jsonb,
                    atualizado_em TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                ALTER TABLE competicao_agenda_config
                ADD COLUMN IF NOT EXISTS competicao TEXT PRIMARY KEY,
                ADD COLUMN IF NOT EXISTS modo_distribuicao TEXT DEFAULT 'automatico_inteligente',
                ADD COLUMN IF NOT EXISTS descanso_minimo_jogos INTEGER DEFAULT 1,
                ADD COLUMN IF NOT EXISTS rodizio_grupos TEXT DEFAULT 'por_rodada',
                ADD COLUMN IF NOT EXISTS permitir_relaxar_descanso BOOLEAN DEFAULT TRUE,
                ADD COLUMN IF NOT EXISTS grupos_compartilhados_json JSONB DEFAULT '{}'::jsonb,
                ADD COLUMN IF NOT EXISTS quadras_compartilhadas_json JSONB DEFAULT '[]'::jsonb,
                ADD COLUMN IF NOT EXISTS atualizado_em TIMESTAMP DEFAULT NOW()
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_competicao_agenda_config_competicao
                ON competicao_agenda_config (competicao)
            """)
        conn.commit()

    _CACHE_COLUNAS.pop("competicao_agenda_config", None)
    try:
        _marcar_schema_pronto("tabela_competicao_agenda_config")
    except Exception:
        pass


def _agenda_config_padrao():
    return {
        "modo_distribuicao": "automatico_inteligente",
        "descanso_minimo_jogos": 1,
        "rodizio_grupos": "por_rodada",
        "permitir_relaxar_descanso": True,
        "grupos_compartilhados": {},
        "quadras_compartilhadas": [],
    }


def _normalizar_json_config_agenda(valor, padrao):
    if valor in (None, ""):
        return padrao
    if isinstance(valor, (dict, list)):
        return valor
    try:
        return json.loads(valor)
    except Exception:
        return padrao


def buscar_configuracao_agenda_competicao(nome_competicao):
    criar_tabela_competicao_agenda_config()
    nome_competicao = (nome_competicao or "").strip()
    if not nome_competicao:
        return _agenda_config_padrao()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    modo_distribuicao,
                    descanso_minimo_jogos,
                    rodizio_grupos,
                    permitir_relaxar_descanso,
                    grupos_compartilhados_json,
                    quadras_compartilhadas_json
                FROM competicao_agenda_config
                WHERE competicao = %s
                LIMIT 1
            """, (nome_competicao,))
            row = cur.fetchone()

    config = _agenda_config_padrao()
    if not row:
        return config

    modo = str(row.get("modo_distribuicao") or config["modo_distribuicao"]).strip().lower()
    if modo not in {"grupo_fixo", "quadras_compartilhadas", "automatico_inteligente"}:
        modo = "automatico_inteligente"

    rodizio = str(row.get("rodizio_grupos") or config["rodizio_grupos"]).strip().lower()
    if rodizio not in {"por_rodada", "alternado_inteligente", "por_grupo_inteiro"}:
        rodizio = "por_rodada"

    try:
        descanso = int(row.get("descanso_minimo_jogos") or 1)
    except (TypeError, ValueError):
        descanso = 1
    descanso = max(0, min(descanso, 5))

    config.update({
        "modo_distribuicao": modo,
        "descanso_minimo_jogos": descanso,
        "rodizio_grupos": rodizio,
        "permitir_relaxar_descanso": bool(row.get("permitir_relaxar_descanso")),
        "grupos_compartilhados": _normalizar_json_config_agenda(row.get("grupos_compartilhados_json"), {}),
        "quadras_compartilhadas": _normalizar_json_config_agenda(row.get("quadras_compartilhadas_json"), []),
    })
    return config


def atualizar_configuracao_agenda_competicao(
    nome_competicao,
    modo_distribuicao="automatico_inteligente",
    descanso_minimo_jogos=1,
    rodizio_grupos="por_rodada",
    permitir_relaxar_descanso=True,
    grupos_compartilhados=None,
    quadras_compartilhadas=None,
):
    ok_edicao, _ = validar_competicao_editavel(nome_competicao, "alteração da agenda automática")
    if not ok_edicao:
        return False

    criar_tabela_competicao_agenda_config()

    modo = str(modo_distribuicao or "automatico_inteligente").strip().lower()
    if modo not in {"grupo_fixo", "quadras_compartilhadas", "automatico_inteligente"}:
        modo = "automatico_inteligente"

    rodizio = str(rodizio_grupos or "por_rodada").strip().lower()
    if rodizio not in {"por_rodada", "alternado_inteligente", "por_grupo_inteiro"}:
        rodizio = "por_rodada"

    try:
        descanso = int(descanso_minimo_jogos or 1)
    except (TypeError, ValueError):
        descanso = 1
    descanso = max(0, min(descanso, 5))

    grupos_json = json.dumps(grupos_compartilhados or {}, ensure_ascii=False)
    quadras_json = json.dumps(quadras_compartilhadas or [], ensure_ascii=False)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO competicao_agenda_config (
                    competicao,
                    modo_distribuicao,
                    descanso_minimo_jogos,
                    rodizio_grupos,
                    permitir_relaxar_descanso,
                    grupos_compartilhados_json,
                    quadras_compartilhadas_json,
                    atualizado_em
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
                ON CONFLICT (competicao)
                DO UPDATE SET
                    modo_distribuicao = EXCLUDED.modo_distribuicao,
                    descanso_minimo_jogos = EXCLUDED.descanso_minimo_jogos,
                    rodizio_grupos = EXCLUDED.rodizio_grupos,
                    permitir_relaxar_descanso = EXCLUDED.permitir_relaxar_descanso,
                    grupos_compartilhados_json = EXCLUDED.grupos_compartilhados_json,
                    quadras_compartilhadas_json = EXCLUDED.quadras_compartilhadas_json,
                    atualizado_em = NOW()
            """, (
                nome_competicao,
                modo,
                descanso,
                rodizio,
                bool(permitir_relaxar_descanso),
                grupos_json,
                quadras_json,
            ))
        conn.commit()

    return True


def inicializar_configuracao_agenda_competicao(nome_competicao):
    criar_tabela_competicao_agenda_config()
    if buscar_configuracao_agenda_competicao(nome_competicao):
        # buscar_configuracao já retorna padrão quando não existe; garante UPSERT real.
        cfg = buscar_configuracao_agenda_competicao(nome_competicao)
        return atualizar_configuracao_agenda_competicao(nome_competicao, **cfg)
    return False


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
        garantir_campos_libero_atletas(conn)
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

            # A numeração da equipe NÃO pode ser bloqueada pelo prazo de inscrição.
            # O prazo continua bloqueando cadastro/edição/exclusão de atletas,
            # mas a camisa/número precisa poder ser ajustada a qualquer momento do torneio.
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



# =========================================================
# TRAVA OPERACIONAL DO APONTADOR
# =========================================================
TRAVA_OPERACIONAL_TIMEOUT_SEGUNDOS = 75


def garantir_campos_trava_operacional_partida(force=False):
    """Garante os campos usados para impedir dois apontadores na mesma partida.

    IMPORTANTE:
    Esta função é chamada na inicialização do app. Por isso ela NÃO pode ficar
    presa em ALTER TABLE quando o Neon/Postgres estiver com alguma transação
    antiga segurando lock na tabela partidas.

    Estratégia:
    - primeiro consulta as colunas existentes;
    - só tenta criar o que realmente estiver faltando;
    - usa lock_timeout/statement_timeout curtos;
    - se o banco estiver ocupado, pula sem derrubar o app;
    - marca como pronto quando as colunas já existem ou quando conseguiu criar.
    """
    chave = "campos_trava_operacional_partida"

    if _schema_ja_pronto(chave, force=force):
        return

    try:
        colunas = _buscar_colunas_tabela("partidas")
    except Exception as e:
        print("AVISO garantir_campos_trava_operacional_partida/colunas:", repr(e))
        return

    campos_necessarios = {
        "operador_login": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS operador_login TEXT",
        "operador_nome": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS operador_nome TEXT",
        "apontador_login": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS apontador_login TEXT",
        "apontador_nome": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS apontador_nome TEXT",
        "status_operacao": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS status_operacao TEXT DEFAULT 'livre'",
        "reservado_em": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS reservado_em TIMESTAMP",
        "operador_heartbeat": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS operador_heartbeat TIMESTAMP",
        "operador_socket_id": "ALTER TABLE partidas ADD COLUMN IF NOT EXISTS operador_socket_id TEXT",
    }

    ddls = [
        ddl
        for campo, ddl in campos_necessarios.items()
        if force or campo not in colunas
    ]

    # Se as colunas já existem, não faz ALTER TABLE no startup.
    if not ddls:
        _marcar_schema_pronto(chave)
        return

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                # Evita travar o app esperando lock no banco.
                cur.execute("SET LOCAL lock_timeout = '1500ms'")
                cur.execute("SET LOCAL statement_timeout = '4000ms'")

                for ddl in ddls:
                    cur.execute(ddl)

                # Só altera defaults quando conseguimos obter lock rápido.
                try:
                    cur.execute("ALTER TABLE partidas ALTER COLUMN status_jogo SET DEFAULT 'aguardando'")
                except Exception as e:
                    print("AVISO default status_jogo ignorado:", repr(e))
                    conn.rollback()
                    return

                try:
                    cur.execute("SET LOCAL lock_timeout = '1500ms'")
                    cur.execute("SET LOCAL statement_timeout = '4000ms'")
                    cur.execute("ALTER TABLE partidas ALTER COLUMN fase_partida SET DEFAULT 'aguardando'")
                except Exception as e:
                    print("AVISO default fase_partida ignorado:", repr(e))
                    conn.rollback()
                    return

            conn.commit()

        try:
            _CACHE_COLUNAS.pop("partidas", None)
        except Exception:
            pass

        _marcar_schema_pronto(chave)

    except Exception as e:
        # Não derruba o servidor se o banco estiver segurando lock.
        # Em outra inicialização ou em manutenção SQL as colunas podem ser criadas.
        print("AVISO garantir_campos_trava_operacional_partida:", repr(e))
        try:
            _fechar_pool_quebrado()
        except Exception:
            pass
        return


def _lock_expirado(row):
    if not row:
        return True
    status = (row.get("status_operacao") or "livre").strip().lower()
    if status in {"", "livre", "finalizada", "finalizado", "encerrada", "encerrado"}:
        return True
    if not row.get("operador_login"):
        return True

    referencia = row.get("operador_heartbeat") or row.get("reservado_em")
    if not referencia:
        return False

    try:
        return (datetime.now() - referencia).total_seconds() > TRAVA_OPERACIONAL_TIMEOUT_SEGUNDOS
    except Exception:
        return False


def validar_operador_partida(partida_id, competicao, operador_login, renovar=True):
    """Valida se o usuário atual ainda é o dono da operação da partida."""
    garantir_campos_trava_operacional_partida()
    operador_login = (operador_login or "").strip()
    if not operador_login:
        return False, "Sessão do apontador não identificada.", None

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
                return False, "Partida não encontrada.", None

            dono = (partida.get("operador_login") or "").strip()
            status_jogo = (partida.get("status_jogo") or "").strip().lower()
            status_operacao = (partida.get("status_operacao") or "livre").strip().lower()

            if status_jogo in {"finalizada", "finalizado", "encerrada", "encerrado"}:
                return False, "A partida já está finalizada.", partida

            if dono != operador_login:
                if dono and not _lock_expirado(partida):
                    nome = partida.get("operador_nome") or dono
                    return False, f"Esta partida já está em operação por {nome}.", partida

                return False, "Esta partida não está sob sua operação. Assuma a partida antes de operar.", partida

            if renovar and status_operacao not in {"finalizada", "finalizado", "encerrada", "encerrado"}:
                cur.execute("""
                    UPDATE partidas
                    SET operador_heartbeat = NOW(),
                        status_operacao = CASE
                            WHEN COALESCE(status_operacao, 'livre') IN ('livre', 'reservado', 'pre_jogo') THEN status_operacao
                            ELSE status_operacao
                        END
                    WHERE id = %s
                      AND competicao = %s
                      AND operador_login = %s
                """, (partida_id, competicao, operador_login))
                conn.commit()

    return True, "Operação liberada.", partida


def heartbeat_partida_operacional(partida_id, competicao, operador_login, socket_id=None):
    garantir_campos_trava_operacional_partida()
    operador_login = (operador_login or "").strip()
    if not operador_login:
        return False, "Sessão do apontador não identificada."

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT operador_login, operador_nome, status_operacao,
                       reservado_em, operador_heartbeat, status_jogo
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))
            partida = cur.fetchone()

            if not partida:
                return False, "Partida não encontrada."

            if (partida.get("status_jogo") or "").strip().lower() in {"finalizada", "finalizado", "encerrada", "encerrado"}:
                return False, "Partida finalizada."

            dono = (partida.get("operador_login") or "").strip()
            if dono and dono != operador_login and not _lock_expirado(partida):
                nome = partida.get("operador_nome") or dono
                return False, f"Esta partida já está em operação por {nome}."

            if not dono or dono == operador_login or _lock_expirado(partida):
                cur.execute("""
                    UPDATE partidas
                    SET operador_login = %s,
                        apontador_login = %s,
                        operador_heartbeat = NOW(),
                        operador_socket_id = COALESCE(%s, operador_socket_id),
                        reservado_em = COALESCE(reservado_em, NOW()),
                        status_operacao = CASE
                            WHEN COALESCE(status_operacao, 'livre') IN ('livre', '') THEN 'reservado'
                            ELSE status_operacao
                        END
                    WHERE id = %s
                      AND competicao = %s
                """, (operador_login, operador_login, socket_id, partida_id, competicao))
                conn.commit()

    return True, "Heartbeat atualizado."


def liberar_trava_partida_operacional(partida_id, competicao, operador_login):
    garantir_campos_trava_operacional_partida()
    operador_login = (operador_login or "").strip()
    if not operador_login:
        return False, "Sessão do apontador não identificada."

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE partidas
                SET operador_login = NULL,
                    operador_nome = NULL,
                    apontador_login = NULL,
                    apontador_nome = NULL,
                    operador_heartbeat = NULL,
                    operador_socket_id = NULL,
                    reservado_em = NULL,
                    status_operacao = 'livre'
                WHERE id = %s
                  AND competicao = %s
                  AND operador_login = %s
                  AND COALESCE(status_jogo, '') NOT IN ('em_andamento', 'entre_sets', 'finalizada')
            """, (partida_id, competicao, operador_login))
        conn.commit()

    return True, "Partida liberada."

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
    garantir_campos_trava_operacional_partida()
    operador_login = (operador_login or "").strip()
    operador_nome = (operador_nome or operador_login or "Apontador").strip()

    if not operador_login:
        return False, "Sessão do apontador não identificada."

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT operador_login, operador_nome, status_operacao,
                       reservado_em, operador_heartbeat, status_jogo
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                FOR UPDATE
            """, (partida_id, competicao))
            atual = cur.fetchone()

            if not atual:
                return False, "Partida não encontrada."

            status_jogo = (atual.get("status_jogo") or "").strip().lower()
            if status_jogo in {"finalizada", "finalizado", "encerrada", "encerrado"}:
                return False, "Esta partida já está finalizada."

            dono = (atual.get("operador_login") or "").strip()
            status_operacao = (atual.get("status_operacao") or "livre").strip().lower()

            if dono and dono != operador_login and status_operacao not in {"livre", "finalizada", "finalizado"} and not _lock_expirado(atual):
                nome = atual.get("operador_nome") or dono
                return False, f"Esta partida já está em operação por {nome}."

            cur.execute("""
                UPDATE partidas
                SET operador_login = %s,
                    operador_nome = %s,
                    apontador_login = %s,
                    apontador_nome = %s,
                    status_operacao = CASE
                        WHEN COALESCE(status_operacao, 'livre') IN ('livre', '', 'reservado') THEN 'reservado'
                        ELSE status_operacao
                    END,
                    reservado_em = COALESCE(reservado_em, NOW()),
                    operador_heartbeat = NOW()
                WHERE id = %s
                  AND competicao = %s
            """, (operador_login, operador_nome, operador_login, operador_nome, partida_id, competicao))
        conn.commit()

    return True, "Partida assumida com sucesso."

def abandonar_partida_operacional(partida_id, competicao, operador_login):
    garantir_campos_trava_operacional_partida()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status_operacao, operador_login, status_jogo
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

            status_operacao = (atual.get("status_operacao") or "livre").strip().lower()
            status_jogo = (atual.get("status_jogo") or "").strip().lower()
            if status_operacao != "reservado" and status_jogo not in {"", "pre_jogo", "aguardando", "agendada"}:
                return False, "A partida já iniciou e não pode mais ser abandonada dessa forma. Use Salvar e sair."

            cur.execute("""
                UPDATE partidas
                SET operador_login = NULL,
                    operador_nome = NULL,
                    status_operacao = 'livre',
                    reservado_em = NULL,
                    operador_heartbeat = NULL,
                    operador_socket_id = NULL,
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
    if fase_atual not in {"pre_jogo", "", "reservado", "aguardando", "agendada"}:
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
                    operador_heartbeat = NOW(),
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
                _lado_operacional_por_nome(
                    equipe_a_operacional,
                    equipe_b_operacional,
                    _resolver_equipe_por_lado_set1({
                        "equipe_a": equipe_a_cadastro,
                        "equipe_b": equipe_b_cadastro,
                        "lado_esquerdo": lado_esquerdo,
                    }, saque_inicial),
                ) or saque_inicial,
                lado_esquerdo,
                equipe_a_operacional,
                equipe_b_operacional,
                partida_id,
                competicao,
            ))

        conn.commit()

    sincronizar_status_competicao(competicao)
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
            config_tb = _configuracao_operacional_set(
                dict(
                    partida,
                    tiebreak_definido=True,
                    saque_tiebreak=saque_tiebreak,
                    lado_esquerdo_tiebreak=lado_esquerdo_tiebreak,
                ),
                formato=(partida.get("sets_tipo") or (buscar_competicao_por_nome(competicao) or {}).get("sets_tipo")),
                set_numero=partida.get("set_atual") or 1,
            )

            cur.execute("""
                UPDATE partidas
                SET sorteio_tiebreak_vencedor = %s,
                    sorteio_tiebreak_escolha = %s,
                    saque_tiebreak = %s,
                    lado_esquerdo_tiebreak = %s,
                    equipe_a_operacional = %s,
                    equipe_b_operacional = %s,
                    saque_atual = %s,
                    rotacao_a = NULL,
                    rotacao_b = NULL,
                    rotacao_a_json = NULL,
                    rotacao_b_json = NULL,
                    tiebreak_pendente = FALSE,
                    tiebreak_definido = TRUE,
                    fase_partida = 'papeleta',
                    status_jogo = 'entre_sets',
                    status_operacao = 'papeleta'
                WHERE id = %s
                  AND competicao = %s
            """, (
                sorteio_vencedor,
                sorteio_escolha,
                saque_tiebreak,
                lado_esquerdo_tiebreak,
                config_tb.get("equipe_a_operacional"),
                config_tb.get("equipe_b_operacional"),
                config_tb.get("saque_atual"),
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
# CONFERÊNCIA DO APONTADOR - ATLETA / LÍBERO
# =========================================================
def garantir_campos_libero_atletas(conn=None):
    def _executar(c):
        with c.cursor() as cur:
            cur.execute("ALTER TABLE atletas ADD COLUMN IF NOT EXISTS libero BOOLEAN DEFAULT FALSE")

    if conn is not None:
        _executar(conn)
        return

    with conectar() as conn:
        _executar(conn)
        conn.commit()


def contar_liberos_equipe(equipe, competicao, ignorar_atleta_id=None, conn=None):
    def _executar(c):
        garantir_campos_libero_atletas(c)
        with c.cursor() as cur:
            if ignorar_atleta_id is not None:
                cur.execute("""
                    SELECT COUNT(*) AS total
                    FROM atletas
                    WHERE equipe = %s
                      AND competicao = %s
                      AND status = 'aprovado'
                      AND COALESCE(libero, FALSE) = TRUE
                      AND id <> %s
                """, (equipe, competicao, ignorar_atleta_id))
            else:
                cur.execute("""
                    SELECT COUNT(*) AS total
                    FROM atletas
                    WHERE equipe = %s
                      AND competicao = %s
                      AND status = 'aprovado'
                      AND COALESCE(libero, FALSE) = TRUE
                """, (equipe, competicao))
            row = cur.fetchone() or {}
            return int(row.get('total') or 0)

    if conn is not None:
        return _executar(conn)

    with conectar() as conn:
        return _executar(conn)


def salvar_liberos_equipe(equipe, competicao, libero_ids):
    equipe = (equipe or '').strip()
    competicao = (competicao or '').strip()
    ids = {str(i).strip() for i in (libero_ids or []) if str(i).strip()}

    if len(ids) > 2:
        return False, 'Cada equipe pode ter no máximo 2 líberos.'

    with conectar() as conn:
        garantir_campos_libero_atletas(conn)
        with conn.cursor() as cur:
            if ids:
                cur.execute("""
                    SELECT id
                    FROM atletas
                    WHERE equipe = %s
                      AND competicao = %s
                      AND status = 'aprovado'
                      AND id = ANY(%s::int[])
                """, (equipe, competicao, [int(i) for i in ids]))
                encontrados = {str(r.get('id')) for r in (cur.fetchall() or [])}
                if encontrados != ids:
                    return False, 'Um dos líberos selecionados não pertence a esta equipe.'

            cur.execute("""
                UPDATE atletas
                SET libero = FALSE
                WHERE equipe = %s
                  AND competicao = %s
                  AND status = 'aprovado'
            """, (equipe, competicao))

            if ids:
                cur.execute("""
                    UPDATE atletas
                    SET libero = TRUE
                    WHERE equipe = %s
                      AND competicao = %s
                      AND status = 'aprovado'
                      AND id = ANY(%s::int[])
                """, (equipe, competicao, [int(i) for i in ids]))
        conn.commit()

    return True, 'Líberos atualizados com sucesso.'


def atualizar_atleta_conferencia_apontador(id_atleta, equipe, competicao, nome, cpf, data_nascimento, numero=None, libero=False):
    nome = (nome or '').strip()
    cpf = (cpf or '').strip()
    data_nascimento = (data_nascimento or '').strip()

    if not nome or not cpf or not data_nascimento:
        return False, 'Preencha nome, CPF e data de nascimento.'

    if not cpf_valido(cpf):
        return False, 'CPF inválido. Informe um CPF real.'

    numero_final = None
    if numero not in (None, ''):
        try:
            numero_final = int(numero)
        except (TypeError, ValueError):
            return False, 'Número inválido.'
        if numero_final < 1 or numero_final > 99:
            return False, 'O número precisa ser entre 1 e 99.'

    with conectar() as conn:
        garantir_campos_libero_atletas(conn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, equipe, competicao, status
                FROM atletas
                WHERE id = %s
                LIMIT 1
            """, (id_atleta,))
            atleta = cur.fetchone()

            if not atleta:
                return False, 'Atleta não encontrado.'
            if atleta.get('equipe') != equipe or atleta.get('competicao') != competicao:
                return False, 'Este atleta não pertence a esta equipe.'
            if atleta.get('status') != 'aprovado':
                return False, 'Somente atletas aprovados podem ser editados na conferência.'

            cpf_limpo = somente_digitos(cpf)
            cur.execute(f"""
                SELECT id
                FROM atletas
                WHERE competicao = %s
                  AND {_cpf_sql_limpo('cpf')} = %s
                  AND id <> %s
                LIMIT 1
            """, (competicao, cpf_limpo, id_atleta))
            if cur.fetchone():
                return False, 'Já existe outro atleta com este CPF nesta competição.'

            if numero_final is not None:
                cur.execute("""
                    SELECT id
                    FROM atletas
                    WHERE equipe = %s
                      AND competicao = %s
                      AND numero = %s
                      AND id <> %s
                    LIMIT 1
                """, (equipe, competicao, numero_final, id_atleta))
                if cur.fetchone():
                    return False, 'Já existe outro atleta com essa numeração nesta equipe.'

            if bool(libero):
                total_liberos = contar_liberos_equipe(equipe, competicao, ignorar_atleta_id=id_atleta, conn=conn)
                if total_liberos >= 2:
                    return False, 'Cada equipe pode ter no máximo 2 líberos.'

            cur.execute("""
                UPDATE atletas
                SET nome = %s,
                    cpf = %s,
                    data_nascimento = %s,
                    numero = %s,
                    libero = %s
                WHERE id = %s
            """, (nome, cpf, data_nascimento, numero_final, bool(libero), id_atleta))
        conn.commit()

    return True, 'Atleta atualizado com sucesso.'


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


def _normalizar_dados_papeleta(dados):
    """
    Recebe a papeleta em qualquer formato usado pelo apontador/treinador e
    devolve:
      - dados normalizados por posição 1..6;
      - rotação visual/oficial [IV, III, II, V, VI, I].

    Aceita:
      {1: atleta, 2: atleta...}
      {"1": atleta, "2": atleta...}
      ["12", "11", ...]
      [{"numero": 12, ...}, ...]
    """
    ordem_visual = [4, 3, 2, 5, 6, 1]
    normalizado = {}

    if isinstance(dados, (list, tuple)):
        for idx, item in enumerate(list(dados)[:6]):
            posicao = ordem_visual[idx] if idx < len(ordem_visual) else idx + 1
            if isinstance(item, dict):
                atleta = dict(item)
            else:
                atleta = {"numero": item, "nome": "", "id": None}
            normalizado[posicao] = atleta
    elif isinstance(dados, dict):
        for posicao, atleta in dados.items():
            try:
                pos_int = int(posicao)
            except Exception:
                continue

            if isinstance(atleta, dict):
                item = dict(atleta)
            else:
                item = {"numero": atleta, "nome": "", "id": None}
            normalizado[pos_int] = item

    # Garante as 6 posições e normaliza número/nome/id.
    saida = {}
    for pos in range(1, 7):
        atleta = normalizado.get(pos) or {}
        numero = ""
        if isinstance(atleta, dict):
            numero = (
                atleta.get("numero")
                or atleta.get("camisa")
                or atleta.get("numero_camisa")
                or atleta.get("n")
                or ""
            )
            nome = atleta.get("nome") or atleta.get("atleta_nome") or ""
            atleta_id = atleta.get("id") or atleta.get("atleta_id")
        else:
            numero = atleta
            nome = ""
            atleta_id = None

        numero = str(numero or "").strip()
        nome = str(nome or "").strip()

        numero_int = None
        if numero:
            try:
                numero_int = int(numero)
            except Exception:
                numero_int = None

        atleta_id_int = None
        if atleta_id not in (None, ""):
            try:
                atleta_id_int = int(atleta_id)
            except Exception:
                atleta_id_int = None

        saida[pos] = {
            "id": atleta_id_int,
            "numero": numero_int if numero_int is not None else numero,
            "nome": nome,
        }

    rotacao = []
    for pos in ordem_visual:
        numero = saida.get(pos, {}).get("numero")
        rotacao.append(str(numero or "").strip())

    while len(rotacao) < 6:
        rotacao.append("")

    return saida, rotacao[:6]


def _atualizar_rotacao_partida_por_papeleta(cur, partida_id, competicao, equipe, rotacao):
    """
    Quando a papeleta é enviada pelo treinador ou pelo apontador, atualiza também
    a rotação da partida. Sem isso, a tabela papeletas fica certa, mas o jogo ao
    vivo continua lendo rotacao_a_json/rotacao_b_json vazio e a quadra abre sem atletas.
    """
    equipe = str(equipe or "").strip()
    if not equipe:
        return

    try:
        criar_campos_jogo_partida()
    except Exception:
        pass

    cur.execute("""
        SELECT equipe_a, equipe_b, equipe_a_operacional, equipe_b_operacional
        FROM partidas
        WHERE id = %s
          AND competicao = %s
        LIMIT 1
    """, (partida_id, competicao))
    partida = cur.fetchone() or {}

    def mesmo_nome(a, b):
        return str(a or "").strip().lower() == str(b or "").strip().lower()

    lado = ""
    if mesmo_nome(equipe, partida.get("equipe_a_operacional")) or mesmo_nome(equipe, partida.get("equipe_a")):
        lado = "A"
    elif mesmo_nome(equipe, partida.get("equipe_b_operacional")) or mesmo_nome(equipe, partida.get("equipe_b")):
        lado = "B"

    if lado not in {"A", "B"}:
        return

    campo_rot = "rotacao_a" if lado == "A" else "rotacao_b"
    campo_json = "rotacao_a_json" if lado == "A" else "rotacao_b_json"
    campo_titulares = "titulares_iniciais_a_json" if lado == "A" else "titulares_iniciais_b_json"

    rotacao = _normalizar_rotacao_oficial(rotacao)

    cur.execute(f"""
        UPDATE partidas
        SET {campo_rot} = %s,
            {campo_json} = %s,
            {campo_titulares} = %s
        WHERE id = %s
          AND competicao = %s
    """, (
        rotacao,
        json.dumps(rotacao, ensure_ascii=False),
        json.dumps(rotacao, ensure_ascii=False),
        partida_id,
        competicao,
    ))


def salvar_papeleta(partida_id, competicao, equipe, set_numero, dados):
    criar_tabela_papeleta()
    dados_normalizados, rotacao = _normalizar_dados_papeleta(dados)

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

            for posicao in [1, 2, 3, 4, 5, 6]:
                atleta = dados_normalizados.get(posicao) or {}
                registros.append((
                    partida_id,
                    competicao,
                    equipe,
                    set_numero,
                    posicao,
                    atleta.get("id"),
                    atleta.get("numero"),
                    atleta.get("nome"),
                ))

            if registros:
                cur.executemany("""
                    INSERT INTO papeletas (
                        partida_id, competicao, equipe, set_numero,
                        posicao, atleta_id, numero, nome
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, registros)

            _atualizar_rotacao_partida_por_papeleta(
                cur,
                partida_id,
                competicao,
                equipe,
                rotacao,
            )

        conn.commit()

    return True

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
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS fase_partida TEXT DEFAULT 'aguardando'")

            # 🔥 GARANTE CONSISTÊNCIA NAS ANTIGAS
            cur.execute("""
                UPDATE partidas
                SET fase_partida = 'aguardando'
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
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set1_a INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set1_b INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set2_a INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set2_b INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set3_a INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set3_b INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set4_a INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set4_b INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set5_a INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS set5_b INTEGER")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS origem_resultado TEXT DEFAULT 'apontada'")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS scout_preenchido BOOLEAN DEFAULT FALSE")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS vencedor TEXT")

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


def _nome_equipe_cadastro_oposta(partida, equipe):
    equipe = str(equipe or "").strip()
    equipe_a = str((partida or {}).get("equipe_a") or "").strip()
    equipe_b = str((partida or {}).get("equipe_b") or "").strip()
    if equipe and equipe == equipe_a:
        return equipe_b
    if equipe and equipe == equipe_b:
        return equipe_a
    return equipe_b if equipe_a else ""


def _resolver_equipe_por_lado_set1(partida, valor):
    partida = partida or {}
    valor_txt = str(valor or "").strip()
    if not valor_txt:
        return ""

    equipe_a_cadastro = str(partida.get("equipe_a") or "").strip()
    equipe_b_cadastro = str(partida.get("equipe_b") or "").strip()
    lado_esquerdo_set1 = str(partida.get("lado_esquerdo") or partida.get("equipe_a_operacional") or equipe_a_cadastro or "").strip()

    if lado_esquerdo_set1 == equipe_b_cadastro:
        equipe_a_set1 = equipe_b_cadastro
        equipe_b_set1 = equipe_a_cadastro
    else:
        equipe_a_set1 = equipe_a_cadastro
        equipe_b_set1 = equipe_b_cadastro

    if valor_txt.upper() == "A":
        return equipe_a_set1
    if valor_txt.upper() == "B":
        return equipe_b_set1
    if valor_txt in {equipe_a_cadastro, equipe_b_cadastro, equipe_a_set1, equipe_b_set1}:
        return valor_txt
    return ""


def _lado_operacional_por_nome(equipe_a_operacional, equipe_b_operacional, equipe_nome):
    equipe_nome = str(equipe_nome or "").strip()
    if equipe_nome and equipe_nome == str(equipe_a_operacional or "").strip():
        return "A"
    if equipe_nome and equipe_nome == str(equipe_b_operacional or "").strip():
        return "B"
    return ""


def _configuracao_operacional_set(partida, formato=None, set_numero=None):
    """Calcula lados e saque inicial do set atual sem destruir o sorteio original."""
    partida = partida or {}
    formato = _normalizar_formato_sets(formato or partida.get("sets_tipo") or "melhor_de_3")
    try:
        set_numero = int(set_numero or partida.get("set_atual") or 1)
    except Exception:
        set_numero = 1

    equipe_a_cadastro = str(partida.get("equipe_a") or "").strip()
    equipe_b_cadastro = str(partida.get("equipe_b") or "").strip()

    if not equipe_a_cadastro or not equipe_b_cadastro:
        return {
            "equipe_a_operacional": partida.get("equipe_a_operacional") or equipe_a_cadastro,
            "equipe_b_operacional": partida.get("equipe_b_operacional") or equipe_b_cadastro,
            "saque_atual": partida.get("saque_atual") or partida.get("saque_inicial") or "",
            "saque_nome": "",
            "lado_esquerdo": partida.get("lado_esquerdo") or "",
        }

    if set_eh_tiebreak(formato, set_numero) and bool(partida.get("tiebreak_definido")):
        lado_esquerdo = str(partida.get("lado_esquerdo_tiebreak") or partida.get("lado_esquerdo") or equipe_a_cadastro).strip()
        if lado_esquerdo == equipe_b_cadastro:
            equipe_a_operacional = equipe_b_cadastro
            equipe_b_operacional = equipe_a_cadastro
        else:
            equipe_a_operacional = equipe_a_cadastro
            equipe_b_operacional = equipe_b_cadastro

        saque_raw = str(partida.get("saque_tiebreak") or "").strip()
        if saque_raw.upper() == "A":
            saque_nome = equipe_a_operacional
        elif saque_raw.upper() == "B":
            saque_nome = equipe_b_operacional
        else:
            saque_nome = saque_raw

        saque_lado = _lado_operacional_por_nome(equipe_a_operacional, equipe_b_operacional, saque_nome)
        return {
            "equipe_a_operacional": equipe_a_operacional,
            "equipe_b_operacional": equipe_b_operacional,
            "saque_atual": saque_lado or saque_nome,
            "saque_nome": saque_nome,
            "lado_esquerdo": lado_esquerdo,
        }

    lado_esquerdo_set1 = str(partida.get("lado_esquerdo") or partida.get("equipe_a_operacional") or equipe_a_cadastro).strip()
    if lado_esquerdo_set1 not in {equipe_a_cadastro, equipe_b_cadastro}:
        lado_esquerdo_set1 = equipe_a_cadastro

    lado_esquerdo = _nome_equipe_cadastro_oposta(partida, lado_esquerdo_set1) if set_deve_inverter_lados(formato, set_numero) else lado_esquerdo_set1

    if lado_esquerdo == equipe_b_cadastro:
        equipe_a_operacional = equipe_b_cadastro
        equipe_b_operacional = equipe_a_cadastro
    else:
        equipe_a_operacional = equipe_a_cadastro
        equipe_b_operacional = equipe_b_cadastro

    saque_set1_nome = _resolver_equipe_por_lado_set1(partida, partida.get("saque_inicial"))
    if saque_set1_nome not in {equipe_a_cadastro, equipe_b_cadastro}:
        saque_set1_nome = equipe_a_cadastro

    saque_nome = _nome_equipe_cadastro_oposta(partida, saque_set1_nome) if set_deve_inverter_lados(formato, set_numero) else saque_set1_nome
    saque_lado = _lado_operacional_por_nome(equipe_a_operacional, equipe_b_operacional, saque_nome)

    return {
        "equipe_a_operacional": equipe_a_operacional,
        "equipe_b_operacional": equipe_b_operacional,
        "saque_atual": saque_lado or saque_nome,
        "saque_nome": saque_nome,
        "lado_esquerdo": lado_esquerdo,
    }


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
                    fase_partida = COALESCE(fase_partida, 'pre_jogo'),
                    status_operacao = CASE WHEN COALESCE(status_operacao, 'livre') IN ('reservado', 'pre_jogo') THEN 'em_andamento' ELSE status_operacao END,
                    operador_heartbeat = NOW()
                WHERE id = %s
                  AND competicao = %s
            """, (sets_max, sets_para_vencer, partida_id, competicao))
        conn.commit()

    sincronizar_status_competicao(competicao)
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



def _sets_config_partida_seguro(partida=None, competicao=None):
    """Resolve a regra real de sets da partida sem cair para set único por engano.

    Prioridade:
    1) campos gravados na própria partida (sets_max/sets_para_vencer), quando válidos;
    2) configuração da competição;
    3) padrão seguro melhor_de_3.
    """
    partida = partida or {}

    def _int_pos(valor, padrao=0):
        try:
            valor = int(valor or 0)
        except Exception:
            valor = padrao
        return valor if valor > 0 else padrao

    sets_max_partida = _int_pos(partida.get("sets_max"), 0)
    sets_para_partida = _int_pos(partida.get("sets_para_vencer"), 0)

    sets_tipo = str(partida.get("sets_tipo") or "").strip().lower()

    comp = {}
    if competicao:
        try:
            comp = buscar_competicao_por_nome(competicao) or {}
        except Exception:
            comp = {}

    if not sets_tipo:
        sets_tipo = str(comp.get("sets_tipo") or "melhor_de_3").strip().lower()

    sets_tipo = _normalizar_formato_sets(sets_tipo)
    sets_max_cfg = calcular_sets_max(sets_tipo)
    sets_para_cfg = calcular_sets_para_vencer(sets_tipo)

    sets_max = sets_max_partida or sets_max_cfg
    sets_para_vencer = sets_para_partida or sets_para_cfg

    # Corrige combinações inválidas que causavam melhor_de_3 finalizar com 1 set.
    if sets_max >= 5:
        sets_tipo = "melhor_de_5"
        sets_max = 5
        sets_para_vencer = 3
    elif sets_max == 3:
        sets_tipo = "melhor_de_3"
        sets_para_vencer = 2
    else:
        sets_tipo = "set_unico"
        sets_max = 1
        sets_para_vencer = 1

    return sets_tipo, sets_max, sets_para_vencer


def _partida_tem_sets_para_finalizar(partida=None, estado=None, competicao=None):
    base = {}
    if isinstance(partida, dict):
        base.update(partida)
    if isinstance(estado, dict):
        base.update(estado)

    _sets_tipo, _sets_max, sets_para_vencer = _sets_config_partida_seguro(base, competicao)
    try:
        sets_a = int(base.get("sets_a") or 0)
        sets_b = int(base.get("sets_b") or 0)
    except Exception:
        sets_a = sets_b = 0

    return max(sets_a, sets_b) >= sets_para_vencer, sets_para_vencer



def _lado_cadastro_por_lado_operacional_partida(partida, lado_operacional):
    """Converte lado operacional (A/B da quadra atual) para lado do cadastro da partida.

    Importante: entre sets nós invertemos equipe_a_operacional/equipe_b_operacional.
    Os campos sets_a/sets_b e set1_a/set1_b pertencem ao confronto original
    (partidas.equipe_a / partidas.equipe_b), não ao lado visual do set.
    """
    lado_operacional = str(lado_operacional or "").strip().upper()
    equipe_a_cadastro = str((partida or {}).get("equipe_a") or "").strip()
    equipe_b_cadastro = str((partida or {}).get("equipe_b") or "").strip()
    equipe_a_operacional = str((partida or {}).get("equipe_a_operacional") or equipe_a_cadastro).strip()
    equipe_b_operacional = str((partida or {}).get("equipe_b_operacional") or equipe_b_cadastro).strip()

    nome_operacional = equipe_a_operacional if lado_operacional == "A" else equipe_b_operacional if lado_operacional == "B" else ""

    if nome_operacional and nome_operacional == equipe_a_cadastro:
        return "A"
    if nome_operacional and nome_operacional == equipe_b_cadastro:
        return "B"

    # Fallback seguro: se por algum motivo não encontrou nome, mantém o lado recebido.
    return lado_operacional if lado_operacional in {"A", "B"} else ""


def _placar_operacional_para_cadastro_partida(partida, pontos_a_operacional, pontos_b_operacional):
    """Converte placar A/B operacional para placar A/B original da partida."""
    lado_cadastro_do_operacional_a = _lado_cadastro_por_lado_operacional_partida(partida, "A")
    if lado_cadastro_do_operacional_a == "A":
        return int(pontos_a_operacional or 0), int(pontos_b_operacional or 0)
    if lado_cadastro_do_operacional_a == "B":
        return int(pontos_b_operacional or 0), int(pontos_a_operacional or 0)
    return int(pontos_a_operacional or 0), int(pontos_b_operacional or 0)


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

            formato, sets_max, sets_para_vencer = _sets_config_partida_seguro(partida, competicao)

            sets_a = int(partida.get("sets_a") or 0)
            sets_b = int(partida.get("sets_b") or 0)
            set_atual = int(partida.get("set_atual") or 1)

            vencedor_operacional = str(vencedor or "").strip().upper()
            vencedor_cadastro = _lado_cadastro_por_lado_operacional_partida(partida, vencedor_operacional)

            if vencedor_cadastro == "A":
                sets_a += 1
            elif vencedor_cadastro == "B":
                sets_b += 1
            else:
                return False, "Vencedor inválido."

            try:
                vencedor_nome = (partida.get("equipe_a") if vencedor_cadastro == "A" else partida.get("equipe_b")) or ""
                detalhes_fim_set = {
                    "tipo": "fim_set",
                    "set_numero": set_atual,
                    "vencedor_operacional": vencedor_operacional,
                    "vencedor_cadastro": vencedor_cadastro,
                    "vencedor_nome": vencedor_nome,
                    "sets_a": int(sets_a or 0),
                    "sets_b": int(sets_b or 0),
                    "equipe_a_cadastro": partida.get("equipe_a") or "",
                    "equipe_b_cadastro": partida.get("equipe_b") or "",
                    "equipe_a_operacional": partida.get("equipe_a_operacional") or partida.get("equipe_a") or "",
                    "equipe_b_operacional": partida.get("equipe_b_operacional") or partida.get("equipe_b") or "",
                }
                cur.execute("""
                    INSERT INTO eventos (
                        partida_id, competicao, set_numero, equipe,
                        tipo, tipo_evento, fundamento, resultado, detalhe,
                        atleta_id, atleta_nome, numero, detalhes
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    partida_id, competicao, set_atual, vencedor_cadastro,
                    "fim_set", "fim_set", "fim_set", "fim_set",
                    f"Fim do set {set_atual}: {vencedor_nome}",
                    None, str(vencedor_nome or ""), None,
                    json.dumps(detalhes_fim_set, ensure_ascii=False),
                ))
            except Exception as e:
                print("AVISO nao gravou evento fim_set manual:", repr(e), flush=True)

            # Partida só acaba quando alguém atinge a quantidade de sets necessária.
            # Não finalize apenas porque terminou um set ou porque set_atual chegou ao limite.
            acabou = max(sets_a, sets_b) >= sets_para_vencer

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

                config_proximo_set = _configuracao_operacional_set(partida, formato=formato, set_numero=proximo_set)

                if precisa_tiebreak:
                    cur.execute("""
                        UPDATE partidas
                        SET sets_a = %s,
                            sets_b = %s,
                            set_atual = %s,
                            pontos_a = 0,
                            pontos_b = 0,
                            saque_atual = NULL,
                            rotacao_a = NULL,
                            rotacao_b = NULL,
                            rotacao_a_json = NULL,
                            rotacao_b_json = NULL,
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
                            saque_atual = %s,
                            equipe_a_operacional = %s,
                            equipe_b_operacional = %s,
                            rotacao_a = NULL,
                            rotacao_b = NULL,
                            rotacao_a_json = NULL,
                            rotacao_b_json = NULL,
                            sets_max = %s,
                            sets_para_vencer = %s,
                            fase_partida = 'intervalo_set',
                            status_jogo = 'entre_sets',
                            status_operacao = 'papeleta',
                            tiebreak_pendente = FALSE
                        WHERE id = %s
                          AND competicao = %s
                    """, (
                        sets_a,
                        sets_b,
                        proximo_set,
                        config_proximo_set.get("saque_atual"),
                        config_proximo_set.get("equipe_a_operacional"),
                        config_proximo_set.get("equipe_b_operacional"),
                        sets_max,
                        sets_para_vencer,
                        partida_id,
                        competicao,
                    ))

        conn.commit()

    sincronizar_status_competicao(competicao)
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

    sincronizar_status_competicao(competicao)

    if fluxo and fluxo["fase_partida"] == "encerrado":
        try:
            sincronizar_avanco_automatico_competicao(competicao)
        except Exception as e:
            print("AVISO registrar_resultado_set/sincronizar_avanco:", repr(e), flush=True)
        return True, "Partida finalizada com sucesso."
    return True, "Set atualizado com sucesso."



def salvar_resultado_manual_partida(partida_id, competicao, sets, operador_login=None, origem="manual"):
    """Salva/edita o resultado de uma partida sem exigir scout.

    Correção importante:
    a validação do lançamento manual precisa obedecer à REGRA DA PARTIDA
    (sets_tipo/sets_max/sets_para_vencer salvos em partidas), não apenas à
    regra global da competição. Isso permite finais M5 dentro de competição
    originalmente configurada como set único.
    """
    criar_campos_sets_partida()
    criar_campos_jogo_partida()

    sets_validos = []
    for item in sets or []:
        if not isinstance(item, dict):
            continue
        a = item.get("a")
        b = item.get("b")
        if a in (None, "") and b in (None, ""):
            continue
        try:
            a = int(a)
            b = int(b)
        except Exception:
            return False, "Informe apenas números nos placares dos sets."
        if a < 0 or b < 0:
            return False, "Placares não podem ser negativos."
        if a == b:
            return False, "Um set não pode terminar empatado."
        sets_validos.append({"a": a, "b": b})

    if not sets_validos:
        return False, "Preencha pelo menos um set."

    comp = buscar_competicao_por_nome(competicao) or {}
    formato_competicao = _normalizar_formato_sets(comp.get("sets_tipo"))
    sets_max = calcular_sets_max(formato_competicao)
    sets_para_vencer = calcular_sets_para_vencer(formato_competicao)
    formato_partida = formato_competicao

    with conectar() as conn:
        with conn.cursor() as cur:
            colunas_partidas = _buscar_colunas_tabela("partidas")

            campos_select = ["equipe_a", "equipe_b"]
            for campo in ("sets_tipo", "sets_max", "sets_para_vencer", "pontos_set", "pontos_tiebreak"):
                if campo in colunas_partidas:
                    campos_select.append(campo)

            cur.execute(f"""
                SELECT {", ".join(campos_select)}
                FROM partidas
                WHERE id = %s AND competicao = %s
                LIMIT 1
            """, (partida_id, competicao))
            partida = cur.fetchone()
            if not partida:
                return False, "Partida não encontrada."

            # 1) Prioridade absoluta: regra salva na própria partida.
            sets_max_partida = None
            try:
                sets_max_partida = int(partida.get("sets_max") or 0)
            except Exception:
                sets_max_partida = None

            if sets_max_partida in (1, 3, 5):
                sets_max = sets_max_partida
                if sets_max == 1:
                    formato_partida = "set_unico"
                elif sets_max == 5:
                    formato_partida = "melhor_de_5"
                else:
                    formato_partida = "melhor_de_3"
            elif partida.get("sets_tipo"):
                formato_partida = _normalizar_formato_sets(partida.get("sets_tipo"))
                sets_max = calcular_sets_max(formato_partida)

            try:
                spv_partida = int(partida.get("sets_para_vencer") or 0)
            except Exception:
                spv_partida = 0

            if spv_partida in (1, 2, 3):
                sets_para_vencer = spv_partida
            else:
                sets_para_vencer = calcular_sets_para_vencer(formato_partida)

            if len(sets_validos) > sets_max:
                return False, f"Esta partida permite no máximo {sets_max} set(s)."

            sets_a = sum(1 for st in sets_validos if st["a"] > st["b"])
            sets_b = sum(1 for st in sets_validos if st["b"] > st["a"])

            if sets_a == sets_b:
                return False, "O resultado precisa ter um vencedor."
            if max(sets_a, sets_b) < sets_para_vencer:
                return False, f"O vencedor precisa vencer {sets_para_vencer} set(s)."
            if max(sets_a, sets_b) > sets_para_vencer:
                return False, "Quantidade de sets vencidos inválida para a regra da partida."

            # Evita lançar sets extras depois que o vencedor já foi definido.
            vencedor_parcial_a = 0
            vencedor_parcial_b = 0
            for idx, st in enumerate(sets_validos, start=1):
                if st["a"] > st["b"]:
                    vencedor_parcial_a += 1
                else:
                    vencedor_parcial_b += 1

                if idx < len(sets_validos) and max(vencedor_parcial_a, vencedor_parcial_b) >= sets_para_vencer:
                    return False, "Há set(s) extra(s) depois que a partida já estava decidida."

            vencedor_lado = "A" if sets_a > sets_b else "B"
            vencedor_nome = partida.get("equipe_a") if vencedor_lado == "A" else partida.get("equipe_b")

            valores_sets = {}
            for i in range(1, 6):
                if i <= len(sets_validos):
                    valores_sets[f"set{i}_a"] = sets_validos[i - 1]["a"]
                    valores_sets[f"set{i}_b"] = sets_validos[i - 1]["b"]
                else:
                    valores_sets[f"set{i}_a"] = None
                    valores_sets[f"set{i}_b"] = None

            cur.execute("""
                UPDATE partidas
                SET sets_a = %s,
                    sets_b = %s,
                    set_atual = %s,
                    sets_max = %s,
                    sets_para_vencer = %s,
                    pontos_a = 0,
                    pontos_b = 0,
                    set1_a = %s, set1_b = %s,
                    set2_a = %s, set2_b = %s,
                    set3_a = %s, set3_b = %s,
                    set4_a = %s, set4_b = %s,
                    set5_a = %s, set5_b = %s,
                    status = 'finalizada',
                    status_jogo = 'finalizada',
                    status_operacao = 'finalizada',
                    fase_partida = 'encerrado',
                    pre_jogo_finalizado = TRUE,
                    tiebreak_pendente = FALSE,
                    tipo_encerramento = %s,
                    origem_resultado = %s,
                    vencedor = %s,
                    operador_login = COALESCE(operador_login, %s),
                    apontador_login = COALESCE(apontador_login, %s),
                    data_fim = NOW()
                WHERE id = %s AND competicao = %s
            """, (
                sets_a, sets_b, len(sets_validos), sets_max, sets_para_vencer,
                valores_sets["set1_a"], valores_sets["set1_b"],
                valores_sets["set2_a"], valores_sets["set2_b"],
                valores_sets["set3_a"], valores_sets["set3_b"],
                valores_sets["set4_a"], valores_sets["set4_b"],
                valores_sets["set5_a"], valores_sets["set5_b"],
                origem, origem, vencedor_nome, operador_login, operador_login,
                partida_id, competicao,
            ))
        conn.commit()

    sincronizar_status_competicao(competicao)

    try:
        sincronizar_avanco_automatico_competicao(competicao)
    except Exception as e:
        print("AVISO salvar_resultado_manual_partida/sincronizar_avanco:", repr(e), flush=True)

    return True, "Resultado salvo e partida finalizada com sucesso."


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
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS status_jogo TEXT DEFAULT 'aguardando'")
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
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS fase_partida TEXT DEFAULT 'aguardando'")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS pre_jogo_finalizado BOOLEAN DEFAULT FALSE")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS pre_jogo_finalizado_em TIMESTAMP")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS operador_login TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS operador_nome TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS apontador_login TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS apontador_nome TEXT")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS status_operacao TEXT DEFAULT 'livre'")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS reservado_em TIMESTAMP")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS operador_heartbeat TIMESTAMP")
            cur.execute("ALTER TABLE partidas ADD COLUMN IF NOT EXISTS operador_socket_id TEXT")
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

    if status_jogo in {"em_andamento", "entre_sets", "pausada", "pausado", "finalizada", "finalizado", "encerrada", "encerrado"}:
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
                    status_jogo = 'em_andamento',
                    fase_partida = 'jogo',
                    status_operacao = CASE
                        WHEN COALESCE(status_operacao, 'livre') IN ('livre', '', 'reservado', 'pre_jogo') THEN 'em_andamento'
                        ELSE status_operacao
                    END,
                    operador_heartbeat = COALESCE(operador_heartbeat, NOW())
                WHERE id = %s
                  AND competicao = %s
            """, (partida_id, competicao))
        conn.commit()

    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return None

    try:
        estado = _buscar_estado_jogo_partida_base(
            partida_id,
            competicao,
            garantir=False,
            permitir_reconstrucao=False,
        )
    except Exception:
        estado = None

    rot_a = (estado or {}).get("rotacao_a") or []
    rot_b = (estado or {}).get("rotacao_b") or []

    # Reconstrói somente quando abriu o jogo e a rotação veio realmente vazia.
    # Não roda em todo refresh/clique, então não deixa o sistema lento.
    if (
        not estado
        or not _rotacao_estado_tem_atletas(rot_a)
        or not _rotacao_estado_tem_atletas(rot_b)
    ):
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

        detalhes = evento.get('detalhes')
        if isinstance(detalhes, str):
            try:
                detalhes = json.loads(detalhes)
            except Exception:
                detalhes = {}
        if not isinstance(detalhes, dict):
            detalhes = {}

        # Para ponto por erro/falta, evento.equipe guarda quem cometeu/foi scoutado.
        # A equipe que realmente ganhou o ponto fica em detalhes.equipe_pontuadora.
        # A rotação precisa usar SEMPRE quem ganhou o ponto, senão um lado não gira.
        equipe_ponto = str(
            detalhes.get('equipe_pontuadora')
            or detalhes.get('equipe_ponto')
            or evento.get("equipe")
            or ""
        ).strip().upper()

        if tipo == 'ponto':
            if equipe_ponto not in {"A", "B"}:
                continue

            if saque_corrente in {"A", "B"} and equipe_ponto != saque_corrente:
                if equipe_ponto == "A":
                    posicoes_a = _girar_posicoes_horario(posicoes_a)
                else:
                    posicoes_b = _girar_posicoes_horario(posicoes_b)

            saque_corrente = equipe_ponto
            continue

        if equipe_evento not in {"A", "B"}:
            continue

        if tipo not in {'substituicao', 'substituicao_excepcional'}:
            continue

        numero_sai = str(detalhes.get('numero_sai') or detalhes.get('sai') or '').strip()
        numero_entra = str(detalhes.get('numero_entra') or detalhes.get('entra') or '').strip()

        # Eventos antigos de substituição podem ter sido gravados só no campo detalhe
        # no formato "6>2". Mantém compatibilidade para reconstruir rotação/relatório.
        if (not numero_sai or not numero_entra):
            detalhe_txt = str(evento.get('detalhe') or detalhes.get('detalhe') or '').strip()
            if '>' in detalhe_txt:
                partes = [p.strip() for p in detalhe_txt.split('>', 1)]
                if len(partes) == 2:
                    numero_sai = numero_sai or partes[0]
                    numero_entra = numero_entra or partes[1]

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




def _rotacao_snapshot_partida(partida, lado):
    """
    Recupera a rotação salva da partida sem fazer consulta pesada.

    Prioridade:
    1. *_json válido com 6 atletas;
    2. campo array rotacao_* válido;
    3. titulares_iniciais_*_json válido;
    4. melhor valor disponível normalizado.

    Isso evita que um JSON antigo/vazio sobrescreva a rotação correta salva
    pela papeleta e deixe substituição/sanção/cartão verde sem atletas.
    """
    lado = str(lado or "").strip().lower()
    campo_json = f"rotacao_{lado}_json"
    campo_array = f"rotacao_{lado}"
    campo_titulares = f"titulares_iniciais_{lado}_json"

    candidatos = [
        _json_load_text(partida.get(campo_json), []),
        partida.get(campo_array),
        _json_load_text(partida.get(campo_titulares), []),
    ]

    for candidato in candidatos:
        rotacao = _normalizar_rotacao_oficial(candidato)
        if _rotacao_tem_6_validos(rotacao):
            return rotacao

    for candidato in candidatos:
        rotacao = _normalizar_rotacao_oficial(candidato)
        if any(str(x).strip() for x in rotacao):
            return rotacao

    return ["", "", "", "", "", ""]


def _rotacao_estado_tem_atletas(rotacao):
    rotacao = _normalizar_rotacao_oficial(rotacao)
    return len(rotacao) == 6 and any(str(x).strip() for x in rotacao)

def _snapshot_estado_partida(partida, competicao):
    comp = buscar_competicao_por_nome(competicao) or {}
    return {
        "id": partida.get("id"),
        "competicao": partida.get("competicao"),
        "equipe_a": partida.get("equipe_a"),
        "equipe_b": partida.get("equipe_b"),
        "equipe_a_cadastro": partida.get("equipe_a"),
        "equipe_b_cadastro": partida.get("equipe_b"),
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
        "rotacao_a": _rotacao_snapshot_partida(partida, "a"),
        "rotacao_b": _rotacao_snapshot_partida(partida, "b"),
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
                    rotacao_a = %s,
                    rotacao_b = %s,
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
                _normalizar_rotacao_oficial(estado.get("rotacao_a", ["", "", "", "", "", ""])),
                _normalizar_rotacao_oficial(estado.get("rotacao_b", ["", "", "", "", "", ""])),
                json.dumps(_normalizar_rotacao_oficial(estado.get("rotacao_a", ["", "", "", "", "", ""])), ensure_ascii=False),
                json.dumps(_normalizar_rotacao_oficial(estado.get("rotacao_b", ["", "", "", "", "", ""])), ensure_ascii=False),
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
    estado = _buscar_estado_jogo_partida_base(
        partida_id,
        competicao,
        garantir=False,
        permitir_reconstrucao=False,
    )

    # Caminho rápido: se já tem rotação dos dois lados, não faz mais nada.
    # Caminho de correção: se o cache/snapshot nasceu sem atletas, reconstrói
    # uma única vez a partir da papeleta salva, sem varrer relatórios/tabelas pesadas.
    try:
        rot_a = (estado or {}).get("rotacao_a") or []
        rot_b = (estado or {}).get("rotacao_b") or []
        if _rotacao_estado_tem_atletas(rot_a) and _rotacao_estado_tem_atletas(rot_b):
            return estado

        partida = buscar_partida_operacional(partida_id, competicao)
        if not partida:
            return estado

        return _reconstruir_e_salvar_snapshot(partida_id, competicao, partida) or estado
    except Exception as e:
        print("AVISO buscar_estado_jogo_partida/fallback_rotacao:", repr(e), flush=True)
        return estado


def _montar_historico_resumido_partida(partida_id, competicao, limite=5):
    """Últimas ações do apontador: somente pontos/scout.

    Tempo, substituição, sanções e avisos continuam salvos no banco para relatório/súmula,
    mas não entram neste quadro porque o árbitro, ao pedir desfazer, volta somente o ponto.
    """
    eventos = listar_eventos_partida(partida_id, competicao, limite=max(50, int(limite or 5) * 8)) or []
    historico = []
    tipos_ponto = {"ponto", "retardamento_penalidade"}

    for ev in eventos:
        tipo = str(ev.get("tipo") or ev.get("tipo_evento") or "").strip().lower()
        if tipo not in tipos_ponto:
            continue

        descricao = str(ev.get("descricao") or "").strip() or "Ponto registrado"
        historico.append({"descricao": descricao})
        if len(historico) >= int(limite or 5):
            break

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
    import json
    from urllib.parse import quote

    global _ESTRUTURA_PONTO_GARANTIDA
    try:
        _ESTRUTURA_PONTO_GARANTIDA
    except NameError:
        _ESTRUTURA_PONTO_GARANTIDA = False

    if not _ESTRUTURA_PONTO_GARANTIDA:
        criar_estrutura_rotacao_profissional()
        criar_tabela_eventos()
        criar_campos_jogo_partida()
        criar_campos_sets_partida()
        _ESTRUTURA_PONTO_GARANTIDA = True

    def _carregar_rotacao_real(partida, lado):
        campo_array = f"rotacao_{lado}"
        campo_json = f"rotacao_{lado}_json"

        # Prioridade para o JSON, porque substituições/sanções salvam o snapshot nele.
        # Antes o array ganhava prioridade e podia estar atrasado, fazendo um lado não girar.
        rotacao_json = partida.get(campo_json)
        try:
            rotacao_json = json.loads(rotacao_json or "[]") if isinstance(rotacao_json, str) else rotacao_json
        except Exception:
            rotacao_json = []

        if _rotacao_tem_6_validos(rotacao_json):
            return _normalizar_rotacao_oficial(rotacao_json)

        rotacao = partida.get(campo_array)
        if _rotacao_tem_6_validos(rotacao):
            return _normalizar_rotacao_oficial(rotacao)

        return ["", "", "", "", "", ""]

    def _lado_saque(valor, partida):
        valor = str(valor or "").strip()
        if not valor:
            return ""

        valor_upper = valor.upper()
        if valor_upper in {"A", "B"}:
            return valor_upper

        equipe_a_nome = str(partida.get("equipe_a_operacional") or partida.get("equipe_a") or "").strip().lower()
        equipe_b_nome = str(partida.get("equipe_b_operacional") or partida.get("equipe_b") or "").strip().lower()

        if valor.lower() == equipe_a_nome:
            return "A"
        if valor.lower() == equipe_b_nome:
            return "B"

        return ""

    def _oposto(lado):
        return "B" if lado == "A" else "A"

    equipe = (equipe or "").strip().upper()
    if equipe not in {"A", "B"}:
        return False, "Equipe inválida."

    detalhes = detalhes if isinstance(detalhes, dict) else {}

    equipe_pontuadora = str(detalhes.get("equipe_pontuadora") or equipe).strip().upper()
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

    regras = _regras_jogo_competicao(competicao) or {}

    fim_set = False
    fim_jogo = False
    vencedor_set = None
    vencedor_partida = None
    proximo_eh_tiebreak_resultado = False

    girou = False
    equipe_girou = ""
    saque_antes = ""
    saque_depois = equipe_pontuadora

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM partidas
                WHERE id = %s
                  AND competicao = %s
                FOR UPDATE
            """, (partida_id, competicao))

            partida = cur.fetchone()

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

            # REGRA REAL DA PARTIDA (não só da competição).
            # Partidas do avanço/3º lugar/final podem ter sets_max/sets_para_vencer
            # salvos direto na própria partida. Antes, esta função lia apenas a
            # configuração da competição e podia cair em set único, finalizando
            # a partida logo no 1º set.
            sets_tipo_partida, sets_max_partida, sets_para_vencer = _sets_config_partida_seguro(partida, competicao)
            sets_tipo_regra = sets_tipo_partida

            def _int_regra(valor, padrao):
                try:
                    valor = int(valor or 0)
                except Exception:
                    valor = 0
                return valor if valor > 0 else padrao

            pontos_set_normal = _int_regra(partida.get("pontos_set"), _int_regra(regras.get("pontos_set"), 25))
            pontos_tiebreak = _int_regra(partida.get("pontos_tiebreak"), _int_regra(regras.get("pontos_tiebreak"), 15))
            diferenca_minima = _int_regra(partida.get("diferenca_minima"), _int_regra(regras.get("diferenca_minima"), 2))
            pontos_set = pontos_tiebreak if set_eh_tiebreak(sets_tipo_regra, set_atual) else pontos_set_normal

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

            set_coluna = max(1, min(int(set_atual or 1), 5))
            pontos_set_a_cadastro = partida.get(f"set{set_coluna}_a")
            pontos_set_b_cadastro = partida.get(f"set{set_coluna}_b")

            if (
                (pontos_a >= pontos_set or pontos_b >= pontos_set)
                and abs(pontos_a - pontos_b) >= diferenca_minima
            ):
                fim_set = True
                vencedor_set = "A" if pontos_a > pontos_b else "B"

                vencedor_set_cadastro = _lado_cadastro_por_lado_operacional_partida(partida, vencedor_set)
                if vencedor_set_cadastro == "A":
                    sets_a += 1
                else:
                    sets_b += 1

                fim_jogo = sets_a >= sets_para_vencer or sets_b >= sets_para_vencer

                if fim_jogo:
                    vencedor_partida_lado = "A" if sets_a > sets_b else "B"
                    vencedor_partida = (partida.get("equipe_a") if vencedor_partida_lado == "A" else partida.get("equipe_b")) or vencedor_partida_lado

                coluna_a = f"set{set_coluna}_a"
                coluna_b = f"set{set_coluna}_b"
                pontos_set_a_cadastro, pontos_set_b_cadastro = _placar_operacional_para_cadastro_partida(partida, pontos_a, pontos_b)

                # Auditoria oficial do fechamento do set.
                # O evento normal acima usa A/B operacional (lado atual da quadra).
                # Aqui gravamos também o lado de cadastro, porque sets_a/sets_b pertencem
                # sempre a partidas.equipe_a/equipe_b, mesmo quando os lados invertem no 2º set.
                try:
                    vencedor_nome = (partida.get("equipe_a") if vencedor_set_cadastro == "A" else partida.get("equipe_b")) or ""
                    detalhes_fim_set = {
                        "tipo": "fim_set",
                        "set_numero": set_atual,
                        "vencedor_operacional": vencedor_set,
                        "vencedor_cadastro": vencedor_set_cadastro,
                        "vencedor_nome": vencedor_nome,
                        "placar_operacional_a": int(pontos_a or 0),
                        "placar_operacional_b": int(pontos_b or 0),
                        "placar_cadastro_a": int(pontos_set_a_cadastro or 0),
                        "placar_cadastro_b": int(pontos_set_b_cadastro or 0),
                        "sets_a": int(sets_a or 0),
                        "sets_b": int(sets_b or 0),
                        "equipe_a_cadastro": partida.get("equipe_a") or "",
                        "equipe_b_cadastro": partida.get("equipe_b") or "",
                        "equipe_a_operacional": partida.get("equipe_a_operacional") or partida.get("equipe_a") or "",
                        "equipe_b_operacional": partida.get("equipe_b_operacional") or partida.get("equipe_b") or "",
                    }
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
                        vencedor_set_cadastro,
                        "fim_set",
                        "fim_set",
                        "fim_set",
                        "fim_set",
                        f"Fim do set {set_atual}: {vencedor_nome}",
                        None,
                        str(vencedor_nome or ""),
                        None,
                        json.dumps(detalhes_fim_set, ensure_ascii=False),
                    ))
                except Exception as e:
                    print("AVISO nao gravou evento fim_set:", repr(e), flush=True)

                if fim_jogo:
                    cur.execute(f"""
                        UPDATE partidas
                        SET pontos_a = %s,
                            pontos_b = %s,
                            sets_a = %s,
                            sets_b = %s,
                            sets_max = %s,
                            sets_para_vencer = %s,
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
                        sets_max_partida,
                        sets_para_vencer,
                        pontos_set_a_cadastro,
                        pontos_set_b_cadastro,
                        saque_depois,
                        rotacao_a,
                        rotacao_b,
                        json.dumps(rotacao_a, ensure_ascii=False),
                        json.dumps(rotacao_b, ensure_ascii=False),
                        vencedor_partida,
                        partida_id,
                        competicao,
                    ))
                else:
                    proximo_set = set_atual + 1

                    # Tie-break deve ser decidido pelo PLACAR DE SETS oficial
                    # (partidas.equipe_a/equipe_b), não apenas por lado visual.
                    # Isso evita perder o sorteio quando os lados são invertidos no 2º/4º set.
                    sets_tipo_norm = _normalizar_formato_sets(sets_tipo_regra)
                    proximo_eh_tiebreak = (
                        (sets_tipo_norm == "melhor_de_3" and int(sets_a or 0) == 1 and int(sets_b or 0) == 1 and int(proximo_set or 0) == 3)
                        or (sets_tipo_norm == "melhor_de_5" and int(sets_a or 0) == 2 and int(sets_b or 0) == 2 and int(proximo_set or 0) == 5)
                    )
                    # Fallback de segurança para configurações válidas antigas.
                    proximo_eh_tiebreak = bool(proximo_eh_tiebreak or set_eh_tiebreak(sets_tipo_regra, proximo_set))
                    proximo_eh_tiebreak_resultado = proximo_eh_tiebreak

                    config_proximo_set = _configuracao_operacional_set(partida, formato=sets_tipo_regra, set_numero=proximo_set)

                    if proximo_eh_tiebreak:
                        proxima_fase_partida = 'tiebreak_sorteio'
                        proximo_status_jogo = 'tiebreak_sorteio'
                        proximo_status_operacao = 'tiebreak_sorteio'
                        proximo_tiebreak_pendente = True
                        proximo_tiebreak_definido = False
                        proximo_saque_atual = None
                        proximo_equipe_a_operacional = partida.get("equipe_a_operacional") or partida.get("equipe_a")
                        proximo_equipe_b_operacional = partida.get("equipe_b_operacional") or partida.get("equipe_b")
                    else:
                        # Entre um set normal e outro, o apontador deve voltar para a papeleta
                        # já com lado/sacador calculados automaticamente.
                        proxima_fase_partida = 'intervalo_set'
                        proximo_status_jogo = 'entre_sets'
                        proximo_status_operacao = 'papeleta'
                        proximo_tiebreak_pendente = False
                        proximo_tiebreak_definido = False
                        proximo_saque_atual = config_proximo_set.get("saque_atual")
                        proximo_equipe_a_operacional = config_proximo_set.get("equipe_a_operacional")
                        proximo_equipe_b_operacional = config_proximo_set.get("equipe_b_operacional")

                    cur.execute(f"""
                        UPDATE partidas
                        SET pontos_a = 0,
                            pontos_b = 0,
                            sets_a = %s,
                            sets_b = %s,
                            sets_max = %s,
                            sets_para_vencer = %s,
                            set_atual = %s,
                            {coluna_a} = %s,
                            {coluna_b} = %s,
                            saque_atual = %s,
                            equipe_a_operacional = %s,
                            equipe_b_operacional = %s,
                            rotacao_a = NULL,
                            rotacao_b = NULL,
                            rotacao_a_json = NULL,
                            rotacao_b_json = NULL,
                            status_jogo = %s,
                            fase_partida = %s,
                            status_operacao = %s,
                            tiebreak_pendente = %s,
                            tiebreak_definido = %s,
                            sorteio_tiebreak_vencedor = CASE WHEN %s THEN NULL ELSE sorteio_tiebreak_vencedor END,
                            sorteio_tiebreak_escolha = CASE WHEN %s THEN NULL ELSE sorteio_tiebreak_escolha END,
                            saque_tiebreak = CASE WHEN %s THEN NULL ELSE saque_tiebreak END,
                            lado_esquerdo_tiebreak = CASE WHEN %s THEN NULL ELSE lado_esquerdo_tiebreak END
                        WHERE id = %s
                          AND competicao = %s
                    """, (
                        sets_a,
                        sets_b,
                        sets_max_partida,
                        sets_para_vencer,
                        proximo_set,
                        pontos_set_a_cadastro,
                        pontos_set_b_cadastro,
                        proximo_saque_atual,
                        proximo_equipe_a_operacional,
                        proximo_equipe_b_operacional,
                        proximo_status_jogo,
                        proxima_fase_partida,
                        proximo_status_operacao,
                        proximo_tiebreak_pendente,
                        proximo_tiebreak_definido,
                        proximo_eh_tiebreak,
                        proximo_eh_tiebreak,
                        proximo_eh_tiebreak,
                        proximo_eh_tiebreak,
                        partida_id,
                        competicao,
                    ))

                    pontos_a = 0
                    pontos_b = 0
                    set_atual = proximo_set
                    saque_depois = ""
            else:
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
            except Exception as e:
                print("⚠️ erro historico_rotacao:", repr(e), flush=True)

        conn.commit()

    sincronizar_status_competicao(competicao)

    try:
        historico = _montar_historico_resumido_partida(partida_id, competicao, limite=5)
    except Exception:
        historico = []

    return True, {
        "mensagem": "Jogo finalizado." if fim_jogo else ("Set finalizado." if fim_set else "Ponto registrado."),
        "competicao": competicao,
        "partida_id": partida_id,
        "equipe_a": partida.get("equipe_a") or partida.get("equipe_a_operacional") or "",
        "equipe_b": partida.get("equipe_b") or partida.get("equipe_b_operacional") or "",
        "equipe_a_cadastro": partida.get("equipe_a") or partida.get("equipe_a_operacional") or "",
        "equipe_b_cadastro": partida.get("equipe_b") or partida.get("equipe_b_operacional") or "",
        "equipe_a_operacional": partida.get("equipe_a_operacional") or partida.get("equipe_a") or "",
        "equipe_b_operacional": partida.get("equipe_b_operacional") or partida.get("equipe_b") or "",
        "pontos_a": pontos_a,
        "pontos_b": pontos_b,
        "placar_a": pontos_a,
        "placar_b": pontos_b,
        "sets_a": sets_a,
        "sets_b": sets_b,
        "set_atual": set_atual,
        "set1_a": pontos_set_a_cadastro if fim_set and set_coluna == 1 else partida.get("set1_a"),
        "set1_b": pontos_set_b_cadastro if fim_set and set_coluna == 1 else partida.get("set1_b"),
        "set2_a": pontos_set_a_cadastro if fim_set and set_coluna == 2 else partida.get("set2_a"),
        "set2_b": pontos_set_b_cadastro if fim_set and set_coluna == 2 else partida.get("set2_b"),
        "set3_a": pontos_set_a_cadastro if fim_set and set_coluna == 3 else partida.get("set3_a"),
        "set3_b": pontos_set_b_cadastro if fim_set and set_coluna == 3 else partida.get("set3_b"),
        "saque_atual": saque_depois,
        "status_jogo": "finalizada" if fim_jogo else (("tiebreak_sorteio" if fim_set and proximo_eh_tiebreak_resultado else "entre_sets") if fim_set else "em_andamento"),
        "fase_partida": "encerrado" if fim_jogo else (("tiebreak_sorteio" if fim_set and proximo_eh_tiebreak_resultado else "intervalo_set") if fim_set else "jogo"),
        "redirecionar_tiebreak": bool(fim_set and not fim_jogo and proximo_eh_tiebreak_resultado),
        "redirecionar_papeleta": bool(fim_set and not fim_jogo and not proximo_eh_tiebreak_resultado),
        "url_redirecionamento": (f"/apontador/tiebreak/{quote(str(competicao), safe='')}/{partida_id}" if (fim_set and not fim_jogo and proximo_eh_tiebreak_resultado) else (f"/apontador/papeleta/{quote(str(competicao), safe='')}/{partida_id}" if (fim_set and not fim_jogo) else None)),
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
        "tipo_evento": "ponto",
        "equipe_pontuadora": equipe_pontuadora,
        "saque_anterior": saque_antes,
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

    sincronizar_status_competicao(competicao)

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

    if bool(atletas_validos.get(numero_entra, {}).get('libero')) or bool(atletas_validos.get(numero_sai, {}).get('libero')):
        return False, 'Líbero não participa de substituição normal. Use a regra própria do líbero.'

    vinc_tit_res = dict(estado.get('vinculos_titular_reserva_a') or {}) if equipe == 'A' else dict(estado.get('vinculos_titular_reserva_b') or {})
    vinc_res_tit = dict(estado.get('vinculos_reserva_titular_a') or {}) if equipe == 'A' else dict(estado.get('vinculos_reserva_titular_b') or {})

    sai_titular = numero_sai in titulares_iniciais
    entra_titular = numero_entra in titulares_iniciais

    # Substituição normal tem vínculo fechado: titular ↔ reserva.
    # Reserva não pode entrar em outro titular e, quando o titular volta, esse vínculo encerra.
    if sai_titular and not entra_titular:
        if status_entra.get('tipo') in {'encerrado', 'vinculo_encerrado'} or status_entra.get('substituicao_encerrada'):
            return False, f'O atleta #{numero_entra} já teve o vínculo de substituição encerrado neste set.'
        reserva_vinculada = str(vinc_tit_res.get(numero_sai) or '').strip()
        titular_do_reserva = str(vinc_res_tit.get(numero_entra) or '').strip()
        if reserva_vinculada and reserva_vinculada != numero_entra:
            return False, f'O titular #{numero_sai} só pode retornar/relacionar com o reserva #{reserva_vinculada}.'
        if titular_do_reserva and titular_do_reserva != numero_sai:
            return False, f'O reserva #{numero_entra} já está vinculado ao titular #{titular_do_reserva}.'
        vinc_tit_res[numero_sai] = numero_entra
        vinc_res_tit[numero_entra] = numero_sai

        status_entra['em_quadra'] = True
        status_entra['tipo'] = 'substituto'
        status_entra['vinculo'] = numero_sai
        status_entra['substituicao_encerrada'] = False

        status_sai['em_quadra'] = False
        status_sai['tipo'] = 'titular_substituido'
        status_sai['vinculo'] = numero_entra

    elif (not sai_titular) and entra_titular:
        titular_esperado = str(vinc_res_tit.get(numero_sai) or status_sai.get('vinculo') or '').strip()
        reserva_esperado = str(vinc_tit_res.get(numero_entra) or status_entra.get('vinculo') or '').strip()
        if titular_esperado != numero_entra or (reserva_esperado and reserva_esperado != numero_sai):
            return False, f'O atleta #{numero_sai} só pode sair para o retorno do titular ao qual está vinculado.'

        vinc_res_tit.pop(numero_sai, None)
        vinc_tit_res.pop(numero_entra, None)

        status_sai['em_quadra'] = False
        status_sai['tipo'] = 'vinculo_encerrado'
        status_sai['vinculo'] = numero_entra
        status_sai['substituicao_encerrada'] = True

        status_entra['em_quadra'] = True
        status_entra['tipo'] = 'titular_retorno'
        status_entra['vinculo'] = numero_sai
    else:
        return False, 'Substituição normal deve ser titular por reserva ou retorno do titular. Para exceções, use substituição excepcional.'

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
        numero=numero_entra,
        detalhes={
            'numero_sai': numero_sai,
            'numero_entra': numero_entra,
            'placar_a': int(estado.get('pontos_a') or 0),
            'placar_b': int(estado.get('pontos_b') or 0),
            'set_atual': set_atual,
            'origem': 'apontador',
        }
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
        'vinculos_titular_reserva_a': vinc_tit_res if equipe == 'A' else estado.get('vinculos_titular_reserva_a', {}),
        'vinculos_titular_reserva_b': vinc_tit_res if equipe == 'B' else estado.get('vinculos_titular_reserva_b', {}),
        'vinculos_reserva_titular_a': vinc_res_tit if equipe == 'A' else estado.get('vinculos_reserva_titular_a', {}),
        'vinculos_reserva_titular_b': vinc_res_tit if equipe == 'B' else estado.get('vinculos_reserva_titular_b', {}),
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

    def _int_local(valor, padrao=0):
        try:
            if valor is None or valor == "":
                return padrao
            return int(valor)
        except Exception:
            return padrao

    def _detalhes_local(valor):
        if isinstance(valor, dict):
            return valor
        if isinstance(valor, str) and valor.strip():
            try:
                dados = json.loads(valor)
                return dados if isinstance(dados, dict) else {}
            except Exception:
                return {}
        return {}

    def _lado_pontuador_evento(evento):
        detalhes = _detalhes_local(evento.get("detalhes"))
        lado = str(
            detalhes.get("equipe_pontuadora")
            or detalhes.get("equipe_ponto")
            or evento.get("equipe")
            or ""
        ).strip().upper()
        return lado if lado in {"A", "B"} else ""

    def _limite_set(set_numero, sets_para_vencer, comp):
        try:
            tem_tiebreak = comp.get("tem_tiebreak")
            if isinstance(tem_tiebreak, str):
                tem_tiebreak = tem_tiebreak.strip().lower() not in {"0", "false", "nao", "não", "no", "off"}
            if tem_tiebreak is None:
                tem_tiebreak = True
            set_decisivo = (sets_para_vencer * 2) - 1
            if tem_tiebreak and int(set_numero or 1) == set_decisivo:
                return _int_local(comp.get("pontos_tiebreak"), 15)
            return _int_local(comp.get("pontos_set"), 25)
        except Exception:
            return 25

    def _set_fechado(a, b, set_numero, sets_para_vencer, comp):
        limite = _limite_set(set_numero, sets_para_vencer, comp)
        diferenca = _int_local(comp.get("diferenca_minima"), 2)
        return (a >= limite or b >= limite) and abs(a - b) >= diferenca

    def _recalcular_resumo_por_eventos(cur, partida):
        comp = buscar_competicao_por_nome(competicao) or {}
        sets_tipo = str(comp.get("sets_tipo") or partida.get("sets_tipo") or "melhor_de_3").strip().lower()
        try:
            sets_para_vencer = calcular_sets_para_vencer(sets_tipo)
        except Exception:
            sets_para_vencer = 1 if sets_tipo in {"set_unico", "unico", "único"} else (3 if sets_tipo == "melhor_de_5" else 2)

        cur.execute("""
            SELECT id, set_numero, equipe, tipo, detalhes
            FROM eventos
            WHERE partida_id = %s
              AND competicao = %s
              AND tipo IN ('ponto', 'retardamento_penalidade')
            ORDER BY id ASC
        """, (partida_id, competicao))
        eventos_ponto = cur.fetchall() or []

        placares_sets = {i: {"A": 0, "B": 0} for i in range(1, 6)}
        maior_set_com_evento = 1
        for evento in eventos_ponto:
            set_numero = max(1, min(_int_local(evento.get("set_numero"), 1), 5))
            maior_set_com_evento = max(maior_set_com_evento, set_numero)
            lado = _lado_pontuador_evento(evento)
            if lado in {"A", "B"}:
                placares_sets[set_numero][lado] += 1

        sets_a = 0
        sets_b = 0
        set_atual = 1
        pontos_a = 0
        pontos_b = 0

        for numero_set in range(1, 6):
            a = placares_sets[numero_set]["A"]
            b = placares_sets[numero_set]["B"]
            if (a or b) and _set_fechado(a, b, numero_set, sets_para_vencer, comp):
                if a > b:
                    sets_a += 1
                elif b > a:
                    sets_b += 1
                if sets_a >= sets_para_vencer or sets_b >= sets_para_vencer:
                    set_atual = numero_set
                    pontos_a, pontos_b = a, b
                    break
                set_atual = min(numero_set + 1, 5)
                pontos_a, pontos_b = 0, 0
                continue

            set_atual = max(numero_set, maior_set_com_evento if numero_set < maior_set_com_evento else numero_set)
            pontos_a, pontos_b = a, b
            break

        status_jogo = "em_andamento" if eventos_ponto else "pre_jogo"
        fase_partida = "jogo" if eventos_ponto else "pre_jogo"

        return {
            "pontos_a": pontos_a,
            "pontos_b": pontos_b,
            "sets_a": sets_a,
            "sets_b": sets_b,
            "set_atual": set_atual,
            "status_jogo": status_jogo,
            "fase_partida": fase_partida,
            "placares_sets": placares_sets,
        }

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
                LIMIT 5
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
                if tipo in {"ponto", "tempo", "substituicao", "substituicao_excepcional", "retardamento", "sancao", "cartao_verde"}:
                    ids_para_remover.append(evento["id"])
                break

            if not ids_para_remover:
                ids_para_remover.append(recentes[0]["id"])

            cur.execute(
                f"DELETE FROM eventos WHERE id IN ({', '.join(['%s'] * len(ids_para_remover))})",
                tuple(ids_para_remover)
            )

            resumo = _recalcular_resumo_por_eventos(cur, partida)
            placares_sets = resumo["placares_sets"]

            cur.execute("""
                UPDATE partidas
                SET pontos_a = %s,
                    pontos_b = %s,
                    sets_a = %s,
                    sets_b = %s,
                    set_atual = %s,
                    set1_a = %s,
                    set1_b = %s,
                    set2_a = %s,
                    set2_b = %s,
                    set3_a = %s,
                    set3_b = %s,
                    set4_a = %s,
                    set4_b = %s,
                    set5_a = %s,
                    set5_b = %s,
                    saque_atual = NULL,
                    status_jogo = %s,
                    fase_partida = %s,
                    status = CASE WHEN status = 'finalizada' THEN 'em_andamento' ELSE status END,
                    status_operacao = CASE WHEN status_operacao = 'finalizada' THEN 'em_andamento' ELSE status_operacao END,
                    vencedor = CASE WHEN status = 'finalizada' THEN NULL ELSE vencedor END,
                    data_fim = CASE WHEN status = 'finalizada' THEN NULL ELSE data_fim END,
                    tipo_encerramento = CASE WHEN status = 'finalizada' THEN NULL ELSE tipo_encerramento END
                WHERE id = %s
                  AND competicao = %s
            """, (
                resumo["pontos_a"],
                resumo["pontos_b"],
                resumo["sets_a"],
                resumo["sets_b"],
                resumo["set_atual"],
                placares_sets[1]["A"] or None, placares_sets[1]["B"] or None,
                placares_sets[2]["A"] or None, placares_sets[2]["B"] or None,
                placares_sets[3]["A"] or None, placares_sets[3]["B"] or None,
                placares_sets[4]["A"] or None, placares_sets[4]["B"] or None,
                placares_sets[5]["A"] or None, placares_sets[5]["B"] or None,
                resumo["status_jogo"],
                resumo["fase_partida"],
                partida_id,
                competicao,
            ))
        conn.commit()

    partida_reconstruida = buscar_partida_operacional(partida_id, competicao)
    estado = _reconstruir_e_salvar_snapshot(partida_id, competicao, partida_reconstruida)
    tempos = buscar_tempos_restantes_partida(partida_id, competicao)
    historico = _montar_historico_resumido_partida(partida_id, competicao, limite=5)

    return True, {
        "mensagem": "Última ação desfeita.",
        "desfazer": True,
        "origem": "DESFAZER",
        "fonte": "desfazer_fetch",
        "pontos_a": int(estado.get("pontos_a") or 0),
        "pontos_b": int(estado.get("pontos_b") or 0),
        "placar_a": int(estado.get("pontos_a") or 0),
        "placar_b": int(estado.get("pontos_b") or 0),
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
        "historico": historico,
        "ultima_acao": "Última ação desfeita.",
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
    """Finaliza a partida somente se a regra de sets permitir.

    Proteção importante: se a tela/JS chamar encerramento logo ao terminar o
    1º set de um melhor_de_3, o banco NÃO marca como finalizada. Ele mantém a
    partida entre sets e devolve False para a rota avisar o apontador.
    """
    from datetime import datetime

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
                return False, "Partida não encontrada."

            pode_finalizar, sets_para_vencer = _partida_tem_sets_para_finalizar(
                partida=partida,
                competicao=competicao,
            )

            sets_a = int(partida.get("sets_a") or 0)
            sets_b = int(partida.get("sets_b") or 0)
            set_atual = int(partida.get("set_atual") or 1)
            _sets_tipo, sets_max, _sets_para = _sets_config_partida_seguro(partida, competicao)

            if not pode_finalizar:
                proximo_set = min(set_atual + 1, sets_max) if max(sets_a, sets_b) > 0 else set_atual
                cur.execute("""
                    UPDATE partidas
                    SET status = CASE WHEN status = 'finalizada' THEN 'em_andamento' ELSE COALESCE(status, 'em_andamento') END,
                        status_jogo = 'entre_sets',
                        status_operacao = 'papeleta',
                        fase_partida = 'intervalo_set',
                        set_atual = %s,
                        pontos_a = 0,
                        pontos_b = 0,
                        vencedor = NULL,
                        tipo_encerramento = NULL,
                        data_fim = NULL,
                        observacoes = COALESCE(observacoes, '')
                    WHERE id = %s AND competicao = %s
                """, (proximo_set, partida_id, competicao))
                conn.commit()
                return False, f"Partida ainda não pode ser finalizada: precisa vencer {sets_para_vencer} set(s)."

            vencedor = None
            if sets_a > sets_b:
                vencedor = partida.get("equipe_a") or partida.get("equipe_a_operacional") or "A"
            elif sets_b > sets_a:
                vencedor = partida.get("equipe_b") or partida.get("equipe_b_operacional") or "B"

            cur.execute("""
                UPDATE partidas
                SET status = 'finalizada',
                    status_jogo = 'finalizada',
                    status_operacao = 'finalizada',
                    fase_partida = 'encerrado',
                    observacoes = %s,
                    vencedor = COALESCE(%s, vencedor),
                    tipo_encerramento = COALESCE(tipo_encerramento, 'normal'),
                    data_fim = %s
                WHERE id = %s AND competicao = %s
            """, (observacoes, vencedor, datetime.now(), partida_id, competicao))
        conn.commit()

    return True, "Partida finalizada com sucesso."





# =========================================================
# FINALIZAÇÃO / DESTAQUE DA PARTIDA
# =========================================================
def criar_tabela_destaques_partida():
    """Guarda o atleta eleito destaque e bloqueia nova eleição na mesma competição."""
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS destaques_partida (
                    id SERIAL PRIMARY KEY,
                    competicao TEXT NOT NULL,
                    partida_id INTEGER NOT NULL,
                    equipe TEXT,
                    lado TEXT,
                    atleta_id INTEGER,
                    numero INTEGER,
                    nome TEXT NOT NULL,
                    observacao TEXT,
                    criado_em TIMESTAMP DEFAULT NOW(),
                    UNIQUE (competicao, atleta_id)
                )
            """)
            cur.execute("ALTER TABLE destaques_partida ADD COLUMN IF NOT EXISTS lado TEXT")
            cur.execute("ALTER TABLE destaques_partida ADD COLUMN IF NOT EXISTS observacao TEXT")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_destaques_competicao ON destaques_partida (competicao)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_destaques_partida ON destaques_partida (partida_id, competicao)")
        conn.commit()


def _texto_igual_finalizacao(a, b):
    return str(a or '').strip().lower() == str(b or '').strip().lower()


def _resolver_lados_vencedor_finalizacao(partida):
    """Resolve vencedor/perdedor SEM usar lado operacional.

    Na finalização/destaques, A e B sempre significam a ordem original da
    partida (partidas.equipe_a / partidas.equipe_b). Os campos operacionais
    mudam entre sets/tie-break para papeleta, rotação e placar em quadra, mas
    não podem definir quem aparece como equipe vencedora nas observações.
    """
    partida = partida or {}
    equipe_a_original = partida.get('equipe_a') or 'Equipe A'
    equipe_b_original = partida.get('equipe_b') or 'Equipe B'
    vencedor = str(partida.get('vencedor') or '').strip()

    vencedor_lado = ''
    if vencedor:
        if vencedor.upper() in {'A', 'B'}:
            vencedor_lado = vencedor.upper()
        elif _texto_igual_finalizacao(vencedor, equipe_a_original):
            vencedor_lado = 'A'
        elif _texto_igual_finalizacao(vencedor, equipe_b_original):
            vencedor_lado = 'B'

    if vencedor_lado not in {'A', 'B'}:
        sets_a = int(partida.get('sets_a') or 0)
        sets_b = int(partida.get('sets_b') or 0)
        if sets_a != sets_b:
            vencedor_lado = 'A' if sets_a > sets_b else 'B'

    perdedor_lado = 'B' if vencedor_lado == 'A' else ('A' if vencedor_lado == 'B' else '')
    return vencedor_lado, perdedor_lado


def _atleta_chave_finalizacao(atleta):
    atleta_id = atleta.get('id') or atleta.get('atleta_id')
    if atleta_id not in (None, ''):
        return f"id:{atleta_id}"
    return f"num:{str(atleta.get('numero') or '').strip()}|nome:{str(atleta.get('nome') or '').strip().lower()}"


def _montar_atletas_lado_finalizacao(partida_id, competicao, equipe_nome, lado, eleitos_por_id, eleitos_por_chave):
    """
    Lista quem deve aparecer na finalização:
    - atletas da papeleta de qualquer set;
    - atletas que entraram por substituição;
    - líberos marcados na equipe, mesmo sem entrar.
    """
    equipe_nome = (equipe_nome or '').strip()
    lado = (lado or '').strip().upper()
    mapa_elenco = {}
    atletas = {}

    if equipe_nome:
        try:
            for row in listar_atletas_aprovados_da_equipe(equipe_nome, competicao) or []:
                numero = str(row.get('numero') or '').strip()
                if numero:
                    mapa_elenco[numero] = row
        except Exception as e:
            print('AVISO finalização/elenco:', repr(e))

    def adicionar(numero='', nome='', atleta_id=None, origem=''):
        numero_txt = str(numero or '').strip()
        nome_txt = str(nome or '').strip()
        atleta_id_final = atleta_id

        if numero_txt and numero_txt in mapa_elenco:
            base = mapa_elenco[numero_txt]
            nome_txt = nome_txt or str(base.get('nome') or '').strip()
            atleta_id_final = atleta_id_final or base.get('id')

        if not nome_txt and not numero_txt:
            return

        item = {
            'id': atleta_id_final,
            'atleta_id': atleta_id_final,
            'numero': numero_txt,
            'nome': nome_txt or f'Atleta #{numero_txt}',
            'equipe': equipe_nome,
            'lado': lado,
            'origens': set([origem]) if origem else set(),
        }
        chave = _atleta_chave_finalizacao(item)
        if chave in atletas:
            atletas[chave]['origens'].update(item['origens'])
            if not atletas[chave].get('id') and atleta_id_final:
                atletas[chave]['id'] = atletas[chave]['atleta_id'] = atleta_id_final
            if not atletas[chave].get('nome') and item.get('nome'):
                atletas[chave]['nome'] = item['nome']
            return
        atletas[chave] = item

    # Papeletas de todos os sets salvos.
    try:
        criar_tabela_papeleta()
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT atleta_id, numero, nome
                    FROM papeletas
                    WHERE partida_id = %s
                      AND competicao = %s
                      AND equipe = %s
                    ORDER BY set_numero, posicao
                """, (partida_id, competicao, equipe_nome))
                for row in cur.fetchall() or []:
                    adicionar(row.get('numero'), row.get('nome'), row.get('atleta_id'), 'papeleta')
    except Exception as e:
        print('AVISO finalização/papeleta:', repr(e))

    # Entradas por substituição normal/excepcional registradas nos eventos.
    try:
        eventos = listar_eventos_partida(partida_id, competicao, limite=3000) or []
        for ev in eventos:
            tipo = str(ev.get('tipo') or ev.get('tipo_evento') or '').strip().lower()
            if tipo not in {'substituicao', 'substituicao_excepcional'}:
                continue
            if str(ev.get('equipe') or '').strip().upper() != lado:
                continue

            numero_entra = ev.get('numero')
            detalhe = str(ev.get('detalhe') or ev.get('detalhes') or '').strip()
            if not numero_entra and '>' in detalhe:
                numero_entra = detalhe.split('>')[-1].strip()
            adicionar(numero_entra, ev.get('atleta_nome'), ev.get('atleta_id'), 'substituicao')
    except Exception as e:
        print('AVISO finalização/substituições:', repr(e))

    # Líberos aparecem mesmo que não tenham entrado.
    for numero, row in mapa_elenco.items():
        try:
            if bool(row.get('libero')):
                adicionar(numero, row.get('nome'), row.get('id'), 'libero')
        except Exception:
            pass

    saida = []
    for item in atletas.values():
        item['origens'] = sorted(list(item.get('origens') or []))
        atleta_id = item.get('id') or item.get('atleta_id')
        chave_eleito = f"{item.get('equipe')}|{item.get('numero')}|{str(item.get('nome') or '').strip().lower()}"
        item['ja_eleito'] = bool((atleta_id and int(atleta_id) in eleitos_por_id) or chave_eleito in eleitos_por_chave)
        item['status_destaque'] = 'Já eleito' if item['ja_eleito'] else 'Disponível'
        saida.append(item)

    def ordem(item):
        try:
            n = int(item.get('numero') or 9999)
        except Exception:
            n = 9999
        return (n, str(item.get('nome') or '').lower())

    return sorted(saida, key=ordem)


def listar_dados_finalizacao_partida(partida_id, competicao):
    criar_tabela_destaques_partida()
    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return {}

    vencedor_lado, perdedor_lado = _resolver_lados_vencedor_finalizacao(partida)
    # Finalização/destaques devem exibir as equipes originais da partida.
    # A ordem operacional pode estar invertida no 2º set ou tie-break.
    equipe_a = partida.get('equipe_a') or 'Equipe A'
    equipe_b = partida.get('equipe_b') or 'Equipe B'

    eleitos_por_id = set()
    eleitos_por_chave = set()
    destaques_partida = []
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM destaques_partida
                WHERE competicao = %s
                ORDER BY criado_em DESC, id DESC
            """, (competicao,))
            for row in cur.fetchall() or []:
                if row.get('atleta_id') not in (None, ''):
                    try:
                        eleitos_por_id.add(int(row.get('atleta_id')))
                    except Exception:
                        pass
                eleitos_por_chave.add(f"{row.get('equipe')}|{str(row.get('numero') or '').strip()}|{str(row.get('nome') or '').strip().lower()}")
                if int(row.get('partida_id') or 0) == int(partida_id):
                    destaques_partida.append(row)

    equipes = [
        {
            'lado': 'A',
            'nome': equipe_a,
            'resultado': 'vencedora' if vencedor_lado == 'A' else ('perdedora' if perdedor_lado == 'A' else ''),
            'pode_selecionar': vencedor_lado == 'A',
            'atletas': _montar_atletas_lado_finalizacao(partida_id, competicao, equipe_a, 'A', eleitos_por_id, eleitos_por_chave),
        },
        {
            'lado': 'B',
            'nome': equipe_b,
            'resultado': 'vencedora' if vencedor_lado == 'B' else ('perdedora' if perdedor_lado == 'B' else ''),
            'pode_selecionar': vencedor_lado == 'B',
            'atletas': _montar_atletas_lado_finalizacao(partida_id, competicao, equipe_b, 'B', eleitos_por_id, eleitos_por_chave),
        },
    ]

    return {
        'partida': partida,
        'vencedor_lado': vencedor_lado,
        'perdedor_lado': perdedor_lado,
        'equipes': equipes,
        'destaques_partida': destaques_partida,
    }


def salvar_destaque_partida(partida_id, competicao, lado, atleta_id=None, numero=None, nome='', observacao=''):
    criar_tabela_destaques_partida()
    partida = buscar_partida_operacional(partida_id, competicao)
    if not partida:
        return False, 'Partida não encontrada.'

    lado = str(lado or '').strip().upper()
    vencedor_lado, _perdedor_lado = _resolver_lados_vencedor_finalizacao(partida)
    if lado != vencedor_lado:
        return False, 'Só é possível eleger destaque da equipe vencedora.'

    # O destaque é salvo pela equipe original vencedora, não pelo lado operacional
    # atual da quadra. Isso evita marcar Elite como vencedora quando Conexão é
    # equipe_a original mas ficou como equipe_b_operacional no tie-break.
    equipe_nome = (partida.get('equipe_a') or 'Equipe A') if lado == 'A' else (partida.get('equipe_b') or 'Equipe B')
    numero_txt = str(numero or '').strip()
    nome = str(nome or '').strip()
    atleta_id_final = None
    if atleta_id not in (None, ''):
        try:
            atleta_id_final = int(atleta_id)
        except Exception:
            atleta_id_final = None

    # Confere se o atleta está realmente na lista da finalização.
    dados = listar_dados_finalizacao_partida(partida_id, competicao)
    permitidos = []
    for equipe in dados.get('equipes') or []:
        if equipe.get('lado') == lado:
            permitidos = equipe.get('atletas') or []
            break

    escolhido = None
    for atleta in permitidos:
        mesmo_id = atleta_id_final and atleta.get('id') and int(atleta.get('id')) == atleta_id_final
        mesmo_numero_nome = str(atleta.get('numero') or '').strip() == numero_txt and str(atleta.get('nome') or '').strip().lower() == nome.lower()
        if mesmo_id or mesmo_numero_nome:
            escolhido = atleta
            break

    if not escolhido:
        return False, 'Atleta não disponível na relação desta partida.'

    if escolhido.get('ja_eleito'):
        # Permite reabrir a própria finalização para salvar observação/encerramento sem erro.
        with conectar() as conn:
            with conn.cursor() as cur:
                if atleta_id_final:
                    cur.execute("""
                        SELECT id
                        FROM destaques_partida
                        WHERE competicao = %s
                          AND partida_id = %s
                          AND atleta_id = %s
                        LIMIT 1
                    """, (competicao, partida_id, atleta_id_final))
                else:
                    cur.execute("""
                        SELECT id
                        FROM destaques_partida
                        WHERE competicao = %s
                          AND partida_id = %s
                          AND equipe = %s
                          AND COALESCE(numero::text, '') = %s
                          AND LOWER(COALESCE(nome, '')) = LOWER(%s)
                        LIMIT 1
                    """, (competicao, partida_id, equipe_nome, numero_txt, nome))
                if not cur.fetchone():
                    return False, 'Este atleta já foi eleito destaque nesta competição.'

    atleta_id_final = escolhido.get('id') or escolhido.get('atleta_id') or atleta_id_final
    numero_final = None
    if escolhido.get('numero') not in (None, ''):
        try:
            numero_final = int(str(escolhido.get('numero')).strip())
        except Exception:
            numero_final = None
    nome_final = str(escolhido.get('nome') or nome).strip()

    with conectar() as conn:
        with conn.cursor() as cur:
            # Evita dois destaques para a mesma partida; se quiser vários no futuro, remove esta limpeza.
            cur.execute("""
                DELETE FROM destaques_partida
                WHERE partida_id = %s
                  AND competicao = %s
            """, (partida_id, competicao))

            if atleta_id_final:
                cur.execute("""
                    SELECT id
                    FROM destaques_partida
                    WHERE competicao = %s
                      AND atleta_id = %s
                    LIMIT 1
                """, (competicao, atleta_id_final))
                if cur.fetchone():
                    conn.rollback()
                    return False, 'Este atleta já foi eleito destaque nesta competição.'

            cur.execute("""
                INSERT INTO destaques_partida (
                    competicao, partida_id, equipe, lado, atleta_id, numero, nome, observacao
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                competicao,
                partida_id,
                equipe_nome,
                lado,
                atleta_id_final,
                numero_final,
                nome_final,
                str(observacao or '').strip(),
            ))
        conn.commit()

    return True, 'Destaque salvo com sucesso.'


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


def criar_tabela_atalhos_apontador(force=False):
    chave = "atalhos_apontador"
    if _schema_ja_pronto(chave, force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS atalhos_apontador (
                    id SERIAL PRIMARY KEY,
                    apontador_login TEXT NOT NULL,
                    acao TEXT NOT NULL,
                    tecla TEXT NOT NULL,
                    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    atualizado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (apontador_login, acao)
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_atalhos_apontador_login
                ON atalhos_apontador (apontador_login)
            """)
        conn.commit()


def listar_atalhos_apontador(apontador_login):
    criar_tabela_atalhos_apontador()

    login = str(apontador_login or "").strip()
    if not login:
        return {}

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT acao, tecla
                FROM atalhos_apontador
                WHERE apontador_login = %s
            """, (login,))
            rows = cur.fetchall()

    atalhos = {}
    for row in rows or []:
        acao = row.get("acao") if isinstance(row, dict) else row[0]
        tecla = row.get("tecla") if isinstance(row, dict) else row[1]
        atalhos[str(acao or "").strip()] = str(tecla or "").strip().upper()

    return atalhos


def salvar_atalhos_apontador(apontador_login, atalhos):
    criar_tabela_atalhos_apontador()

    login = str(apontador_login or "").strip()
    if not login:
        return False

    if not isinstance(atalhos, dict):
        atalhos = {}

    acoes_permitidas = {
        "ponto_a",
        "ponto_b",
        "desfazer",
        "tempo_a",
        "tempo_b",
        "substituicao_a",
        "substituicao_b",
        "sancao",
        "cartao_verde",
        "retardamento",
        "sub_excepcional",
        "wo_a",
        "wo_b",
        "fullscreen",
        "placar_ao_vivo",
        "inverter_lados",
    }

    teclas_usadas = set()
    atalhos_limpos = {}

    for acao, tecla in atalhos.items():
        acao = str(acao or "").strip()
        if acao not in acoes_permitidas:
            continue

        tecla = str(tecla or "").strip().upper()
        if tecla:
            if tecla in teclas_usadas:
                continue
            teclas_usadas.add(tecla)

        atalhos_limpos[acao] = tecla

    with conectar() as conn:
        with conn.cursor() as cur:
            for acao in acoes_permitidas:
                tecla = atalhos_limpos.get(acao, "")

                if not tecla:
                    cur.execute("""
                        DELETE FROM atalhos_apontador
                        WHERE apontador_login = %s AND acao = %s
                    """, (login, acao))
                    continue

                cur.execute("""
                    INSERT INTO atalhos_apontador
                        (apontador_login, acao, tecla, atualizado_em)
                    VALUES
                        (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (apontador_login, acao)
                    DO UPDATE SET
                        tecla = EXCLUDED.tecla,
                        atualizado_em = CURRENT_TIMESTAMP
                """, (login, acao, tecla))

        conn.commit()

    return True

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
    Corrigido para nunca gerar WHERE AND e para priorizar a partida ativa/mais recente.
    """
    competicao = str(competicao or "").strip()
    equipe_nome = str(equipe_nome or "").strip()

    if not competicao or not equipe_nome:
        return None

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
                        WHEN LOWER(COALESCE(status_jogo, '')) IN ('em_andamento','andamento','ao_vivo','jogo') THEN 1
                        WHEN LOWER(COALESCE(status_operacao, '')) IN ('em_andamento','andamento','ao_vivo','jogo') THEN 2
                        WHEN COALESCE(pontos_a, 0) > 0 OR COALESCE(pontos_b, 0) > 0 THEN 3
                        WHEN LOWER(COALESCE(status_jogo, '')) IN ('entre_sets', 'tiebreak_sorteio') THEN 4
                        WHEN LOWER(COALESCE(status_operacao, '')) IN ('pre_jogo', 'em_papeleta', 'papeleta', 'reservado') THEN 5
                        WHEN LOWER(COALESCE(fase_partida, '')) IN ('papeleta', 'papeleta_pronta', 'intervalo_set', 'jogo') THEN 6
                        ELSE 9
                    END ASC,
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
                    conferencia_link,
                    COALESCE(aprovacao_automatica_atletas, FALSE) AS aprovacao_automatica_atletas
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

    return {
        "login": login_organizador,
        "senha": nova_senha
    }
# =========================================================
# JOGO AVULSO / JOGO RÁPIDO - PERMISSÃO DO APONTADOR + TRAVA MASTER
# =========================================================
def garantir_tabela_configuracoes_sistema():
    """Cria a tabela simples de configurações globais do sistema.

    Usada por travas do Master, como o Jogo Rápido global.
    Mantém JSON em texto para ser compatível com bancos antigos e simples.
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS configuracoes_sistema (
                    chave TEXT PRIMARY KEY,
                    valor TEXT NOT NULL DEFAULT '',
                    atualizado_por TEXT,
                    atualizado_em TIMESTAMP DEFAULT NOW()
                )
            """)
        conn.commit()


def obter_configuracao_sistema(chave, padrao=None):
    chave = str(chave or "").strip()
    if not chave:
        return padrao

    try:
        garantir_tabela_configuracoes_sistema()
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT valor
                    FROM configuracoes_sistema
                    WHERE chave = %s
                    LIMIT 1
                """, (chave,))
                row = cur.fetchone()
                if not row:
                    return padrao
                try:
                    return row.get("valor")
                except Exception:
                    return row[0]
    except Exception as e:
        print("AVISO obter_configuracao_sistema:", repr(e), flush=True)
        return padrao


def salvar_configuracao_sistema(chave, valor, atualizado_por=None):
    chave = str(chave or "").strip()
    if not chave:
        return False

    garantir_tabela_configuracoes_sistema()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO configuracoes_sistema (chave, valor, atualizado_por, atualizado_em)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (chave)
                DO UPDATE SET
                    valor = EXCLUDED.valor,
                    atualizado_por = EXCLUDED.atualizado_por,
                    atualizado_em = NOW()
            """, (chave, str(valor), atualizado_por))
        conn.commit()
    return True


def _config_bool(valor, padrao=False):
    if valor is None:
        return bool(padrao)
    if isinstance(valor, bool):
        return valor
    txt = str(valor).strip().lower()
    if txt in {"1", "true", "sim", "s", "yes", "on", "liberado", "liberada"}:
        return True
    if txt in {"0", "false", "nao", "não", "n", "no", "off", "bloqueado", "bloqueada"}:
        return False
    return bool(padrao)


def jogo_rapido_global_habilitado():
    """Trava global do Master para liberar/bloquear o Jogo Rápido.

    Importante:
    - se a configuração global ainda não existir no banco, considera LIBERADO;
    - quem decide se o apontador pode usar continua sendo a permissão individual
      em apontadores_acesso.pode_criar_jogo_avulso;
    - se o Master desligar a trava global no painel, aí bloqueia para todos.

    Isso evita o erro onde o apontador aparece como "Liberado" na tela de
    apontadores, mas o jogo rápido fica bloqueado porque a config global ainda
    não tinha sido criada.
    """
    return _config_bool(obter_configuracao_sistema("jogo_rapido_global_habilitado", "1"), True)


def definir_jogo_rapido_global_habilitado(liberado, atualizado_por=None):
    return salvar_configuracao_sistema("jogo_rapido_global_habilitado", "1" if liberado else "0", atualizado_por)


def garantir_coluna_jogo_avulso_apontador():
    """Cria a coluna de permissão do jogo rápido no cadastro de apontadores."""
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE apontadores_acesso
                ADD COLUMN IF NOT EXISTS pode_criar_jogo_avulso BOOLEAN DEFAULT FALSE
            """)
        conn.commit()


def apontador_tem_permissao_jogo_avulso_individual(cpf):
    """Retorna somente a permissão individual salva no cadastro do apontador."""
    if not cpf:
        return False

    cpf_limpo = somente_digitos(cpf)

    try:
        garantir_coluna_jogo_avulso_apontador()
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(pode_criar_jogo_avulso, FALSE) AS pode
                    FROM apontadores_acesso
                    WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                    LIMIT 1
                """, (cpf_limpo,))
                row = cur.fetchone()
                if not row:
                    return False
                try:
                    return bool(row.get("pode"))
                except Exception:
                    return bool(row[0])
    except Exception as e:
        print("ERRO apontador_tem_permissao_jogo_avulso_individual:", e, flush=True)
        return False


def apontador_pode_criar_jogo_avulso(cpf):
    """Permissão efetiva do Jogo Rápido.

    Só retorna True quando:
    1) o Master liberou globalmente;
    2) o apontador também possui permissão individual.
    """
    try:
        if not jogo_rapido_global_habilitado():
            return False
        return apontador_tem_permissao_jogo_avulso_individual(cpf)
    except Exception as e:
        print("ERRO apontador_pode_criar_jogo_avulso:", e, flush=True)
        return False


def definir_permissao_jogo_avulso_apontador(cpf, liberado):
    """Libera ou bloqueia o Jogo Rápido para um apontador."""
    if not cpf:
        return False

    cpf_limpo = somente_digitos(cpf)

    garantir_coluna_jogo_avulso_apontador()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE apontadores_acesso
                   SET pode_criar_jogo_avulso = %s
                 WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
            """, (bool(liberado), cpf_limpo))
            atualizado = cur.rowcount
        conn.commit()
    return atualizado > 0


# =========================================================
# QUADRAS DA COMPETIÇÃO
# =========================================================
def _tabela_existe_cur(cur, nome_tabela):
    cur.execute("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = %s
        LIMIT 1
    """, (nome_tabela,))
    return cur.fetchone() is not None


def criar_tabela_competicao_quadras(force=False):
    """
    Cria a estrutura real de quadras da competição.

    Essa tabela substitui a lógica antiga de usar apenas qtd_quadras como número solto.
    A competição continua mantendo qtd_quadras para compatibilidade, mas cada quadra
    passa a ter nome, local, ordem e status ativo/inativo.
    """
    try:
        if _schema_ja_pronto("tabela_competicao_quadras", force=force):
            return
    except Exception:
        pass

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS competicao_quadras (
                    id SERIAL PRIMARY KEY,
                    competicao TEXT NOT NULL,
                    nome TEXT NOT NULL,
                    local TEXT DEFAULT '',
                    ordem INTEGER DEFAULT 1,
                    ativa BOOLEAN DEFAULT TRUE,
                    criado_em TIMESTAMP DEFAULT NOW(),
                    atualizado_em TIMESTAMP DEFAULT NOW(),
                    pin_arbitragem VARCHAR(4),
                    pin_arbitragem_criado_em TIMESTAMP
                )
            """)

            cur.execute("""
                ALTER TABLE competicao_quadras
                ADD COLUMN IF NOT EXISTS competicao TEXT NOT NULL,
                ADD COLUMN IF NOT EXISTS nome TEXT NOT NULL DEFAULT 'Quadra',
                ADD COLUMN IF NOT EXISTS local TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS ordem INTEGER DEFAULT 1,
                ADD COLUMN IF NOT EXISTS ativa BOOLEAN DEFAULT TRUE,
                ADD COLUMN IF NOT EXISTS criado_em TIMESTAMP DEFAULT NOW(),
                ADD COLUMN IF NOT EXISTS atualizado_em TIMESTAMP DEFAULT NOW(),
                ADD COLUMN IF NOT EXISTS pin_arbitragem VARCHAR(4),
                ADD COLUMN IF NOT EXISTS pin_arbitragem_criado_em TIMESTAMP
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_competicao_quadras_competicao
                ON competicao_quadras (competicao)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_competicao_quadras_ordem
                ON competicao_quadras (competicao, ordem)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_competicao_quadras_pin_arbitragem
                ON competicao_quadras (pin_arbitragem)
            """)

            if _tabela_existe_cur(cur, "partidas"):
                cur.execute("""
                    ALTER TABLE partidas
                    ADD COLUMN IF NOT EXISTS quadra_id INTEGER
                """)
                cur.execute("""
                    ALTER TABLE partidas
                    ADD COLUMN IF NOT EXISTS quadra_nome TEXT DEFAULT ''
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_partidas_competicao_quadra
                    ON partidas (competicao, quadra_id)
                """)

            if _tabela_existe_cur(cur, "grupos"):
                cur.execute("""
                    ALTER TABLE grupos
                    ADD COLUMN IF NOT EXISTS quadra_id INTEGER
                """)
                cur.execute("""
                    ALTER TABLE grupos
                    ADD COLUMN IF NOT EXISTS quadra_nome TEXT DEFAULT ''
                """)

        conn.commit()

    _CACHE_COLUNAS.pop("competicao_quadras", None)
    _CACHE_COLUNAS.pop("partidas", None)
    _CACHE_COLUNAS.pop("grupos", None)

    try:
        _marcar_schema_pronto("tabela_competicao_quadras")
    except Exception:
        pass



def _normalizar_pin_arbitragem(pin):
    pin = re.sub(r"\D", "", str(pin or ""))
    if len(pin) != 4:
        return ""
    return pin


def _gerar_pin_arbitragem_unico_cur(cur):
    for _ in range(60):
        pin = str(random.randint(1000, 9999))
        cur.execute("""
            SELECT id
            FROM competicao_quadras
            WHERE pin_arbitragem = %s
            LIMIT 1
        """, (pin,))
        if not cur.fetchone():
            return pin
    return str(random.randint(1000, 9999))


def garantir_pins_arbitragem_quadras(nome_competicao):
    """
    Gera PIN de 4 números para cada quadra ativa da competição.
    O PIN fica salvo na quadra e vale enquanto a quadra/competição existir.
    Se a competição ainda não tiver quadras cadastradas, cria a lista a partir das partidas.
    """
    criar_tabela_competicao_quadras()

    nome_competicao = (nome_competicao or "").strip()
    if not nome_competicao:
        return []

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id
                FROM competicao_quadras
                WHERE competicao = %s
                  AND COALESCE(ativa, TRUE) = TRUE
                LIMIT 1
            """, (nome_competicao,))
            tem_quadras = cur.fetchone() is not None

            if not tem_quadras and _tabela_existe_cur(cur, "partidas"):
                cur.execute("""
                    SELECT
                        COALESCE(NULLIF(TRIM(quadra), ''), '1') AS quadra,
                        COALESCE(NULLIF(TRIM(quadra_nome), ''), NULLIF(TRIM(quadra), ''), 'Quadra 1') AS quadra_nome
                    FROM partidas
                    WHERE competicao = %s
                    GROUP BY COALESCE(NULLIF(TRIM(quadra), ''), '1'), COALESCE(NULLIF(TRIM(quadra_nome), ''), NULLIF(TRIM(quadra), ''), 'Quadra 1')
                    ORDER BY COALESCE(NULLIF(TRIM(quadra), ''), '1')
                """, (nome_competicao,))
                linhas = cur.fetchall() or []
                if not linhas:
                    linhas = [{"quadra": "1", "quadra_nome": "Quadra 1"}]

                for idx, linha in enumerate(linhas, start=1):
                    numero = (linha.get("quadra") or str(idx)).strip()
                    nome = (linha.get("quadra_nome") or f"Quadra {numero}").strip()
                    cur.execute("""
                        INSERT INTO competicao_quadras (competicao, nome, local, ordem, ativa)
                        VALUES (%s, %s, %s, %s, TRUE)
                    """, (nome_competicao, nome, "", idx))

            cur.execute("""
                SELECT id, pin_arbitragem
                FROM competicao_quadras
                WHERE competicao = %s
                  AND COALESCE(ativa, TRUE) = TRUE
                ORDER BY COALESCE(ordem, 9999), id
            """, (nome_competicao,))
            quadras = cur.fetchall() or []

            for quadra in quadras:
                pin_atual = _normalizar_pin_arbitragem(quadra.get("pin_arbitragem"))
                if pin_atual:
                    continue
                novo_pin = _gerar_pin_arbitragem_unico_cur(cur)
                cur.execute("""
                    UPDATE competicao_quadras
                    SET pin_arbitragem = %s,
                        pin_arbitragem_criado_em = COALESCE(pin_arbitragem_criado_em, NOW()),
                        atualizado_em = NOW()
                    WHERE id = %s
                """, (novo_pin, quadra["id"]))

        conn.commit()

    _CACHE_COLUNAS.pop("competicao_quadras", None)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, competicao, nome, local, ordem, ativa, pin_arbitragem
                FROM competicao_quadras
                WHERE competicao = %s
                  AND COALESCE(ativa, TRUE) = TRUE
                ORDER BY COALESCE(ordem, 9999), id
            """, (nome_competicao,))
            return cur.fetchall()


def buscar_vinculo_arbitragem_por_pin(pin):
    """Retorna a competição/quadra vinculada ao PIN informado pelo árbitro."""
    criar_tabela_competicao_quadras()
    pin = _normalizar_pin_arbitragem(pin)
    if not pin:
        return None

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, competicao, nome, local, ordem, ativa, pin_arbitragem
                FROM competicao_quadras
                WHERE pin_arbitragem = %s
                  AND COALESCE(ativa, TRUE) = TRUE
                LIMIT 1
            """, (pin,))
            return cur.fetchone()


def formatar_quadra_exibicao(quadra):
    """Retorna o texto visual padronizado da quadra.

    Regra do sistema:
    - o banco e as relações usam sempre quadra_id;
    - este texto é apenas para tela/relatório/socket.
    """
    if not quadra:
        return ""

    nome = str((quadra or {}).get("nome") or "").strip()
    local = str((quadra or {}).get("local") or "").strip()

    if not nome:
        ordem = (quadra or {}).get("ordem") or ""
        nome = f"Quadra {ordem}".strip()

    if local and local.lower() not in nome.lower():
        return f"{nome} — {local}"

    return nome


def _normalizar_texto_quadra(valor):
    texto = str(valor or "").strip().lower()
    texto = texto.replace("—", "-").replace("–", "-")
    texto = re.sub(r"\s+", " ", texto)
    texto = re.sub(r"[^a-z0-9áàâãéèêíïóôõöúçñ _.-]", "", texto)
    return texto.strip()


def _quadra_matches_texto(quadra, texto):
    texto = _normalizar_texto_quadra(texto)
    if not texto:
        return False

    nome = _normalizar_texto_quadra(quadra.get("nome"))
    local = _normalizar_texto_quadra(quadra.get("local"))
    exibicao = _normalizar_texto_quadra(formatar_quadra_exibicao(quadra))
    ordem = str(quadra.get("ordem") or "").strip()
    qid = str(quadra.get("id") or "").strip()

    candidatos = {nome, local, exibicao, qid}
    if ordem:
        candidatos.update({ordem, f"quadra {ordem}", f"q{ordem}"})

    # Também aceita o começo antes/depois do travessão: "Quadra 1 — Apollo".
    if "-" in texto:
        partes = [p.strip() for p in texto.split("-") if p.strip()]
        candidatos.update(partes)

    return texto in {c for c in candidatos if c}


def buscar_quadra_competicao_por_texto(nome_competicao, texto):
    """Compatibilidade para registros antigos que guardavam quadra como texto."""
    texto = str(texto or "").strip()
    if not nome_competicao or not texto:
        return None

    quadras = listar_quadras_competicao(nome_competicao)
    for quadra in quadras:
        if _quadra_matches_texto(quadra, texto):
            return quadra
    return None


def normalizar_vinculos_quadras_competicao(nome_competicao):
    """Preenche quadra_id/quadra_nome de grupos e partidas antigas.

    Não força vínculo quando o texto antigo é apenas o nome do grupo (A, B, C),
    evitando que Grupo A vire Quadra A por acidente. Apenas normaliza quando
    houver quadra_id existente ou quando o texto bate com uma quadra real.
    """
    criar_tabela_competicao_quadras()
    nome_competicao = str(nome_competicao or "").strip()
    if not nome_competicao:
        return False

    quadras = listar_quadras_competicao(nome_competicao)
    if not quadras:
        return False

    mapa_id = {}
    for q in quadras:
        try:
            mapa_id[int(q["id"])] = q
        except Exception:
            pass

    with conectar() as conn:
        with conn.cursor() as cur:
            if _tabela_existe_cur(cur, "grupos"):
                cur.execute("""
                    SELECT id, nome, quadra_id, quadra_nome
                    FROM grupos
                    WHERE competicao = %s
                """, (nome_competicao,))
                for g in cur.fetchall() or []:
                    quadra = None
                    try:
                        qid = int(g.get("quadra_id") or 0)
                        quadra = mapa_id.get(qid)
                    except Exception:
                        quadra = None
                    if not quadra:
                        quadra = buscar_quadra_competicao_por_texto(nome_competicao, g.get("quadra_nome"))
                    if quadra:
                        cur.execute("""
                            UPDATE grupos
                            SET quadra_id = %s,
                                quadra_nome = %s
                            WHERE id = %s
                        """, (quadra["id"], formatar_quadra_exibicao(quadra), g["id"]))

            if _tabela_existe_cur(cur, "partidas"):
                cur.execute("""
                    SELECT id, quadra, quadra_id, quadra_nome
                    FROM partidas
                    WHERE competicao = %s
                """, (nome_competicao,))
                for p in cur.fetchall() or []:
                    quadra = None
                    try:
                        qid = int(p.get("quadra_id") or 0)
                        quadra = mapa_id.get(qid)
                    except Exception:
                        quadra = None
                    if not quadra:
                        quadra = buscar_quadra_competicao_por_texto(nome_competicao, p.get("quadra_nome") or p.get("quadra"))
                    if quadra:
                        cur.execute("""
                            UPDATE partidas
                            SET quadra_id = %s,
                                quadra_nome = %s,
                                quadra = %s
                            WHERE id = %s
                        """, (quadra["id"], formatar_quadra_exibicao(quadra), str(quadra["id"]), p["id"]))
        conn.commit()
    return True

def listar_quadras_competicao(nome_competicao, somente_ativas=False):
    criar_tabela_competicao_quadras()

    sql = """
        SELECT id, competicao, nome, local, ordem, ativa
        FROM competicao_quadras
        WHERE competicao = %s
    """
    params = [nome_competicao]

    if somente_ativas:
        sql += " AND COALESCE(ativa, TRUE) = TRUE"

    sql += " ORDER BY COALESCE(ordem, 9999), id"

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            linhas = cur.fetchall() or []
            for linha in linhas:
                try:
                    linha["nome_exibicao"] = formatar_quadra_exibicao(linha)
                    linha["quadra_label"] = linha["nome_exibicao"]
                except Exception:
                    pass
            return linhas


def garantir_quadras_competicao(nome_competicao, qtd_quadras=1):
    """
    Garante que a competição tenha pelo menos qtd_quadras cadastradas.
    Não apaga quadras existentes; apenas completa as que faltarem.
    """
    criar_tabela_competicao_quadras()

    nome_competicao = (nome_competicao or "").strip()
    if not nome_competicao:
        return []

    try:
        qtd_quadras = int(qtd_quadras or 1)
    except (TypeError, ValueError):
        qtd_quadras = 1
    qtd_quadras = max(1, qtd_quadras)

    existentes = listar_quadras_competicao(nome_competicao)
    if len(existentes) >= qtd_quadras:
        return existentes

    with conectar() as conn:
        with conn.cursor() as cur:
            for ordem in range(len(existentes) + 1, qtd_quadras + 1):
                cur.execute("""
                    INSERT INTO competicao_quadras (competicao, nome, local, ordem, ativa)
                    VALUES (%s, %s, %s, %s, TRUE)
                """, (nome_competicao, f"Quadra {ordem}", "", ordem))
        conn.commit()

    return listar_quadras_competicao(nome_competicao)


def salvar_quadras_competicao(nome_competicao, quadras):
    """
    Salva quadras mantendo IDs existentes e criando novas quando id vier vazio.
    Não exclui fisicamente para não quebrar partidas antigas; quadras removidas do formulário ficam inativas.
    """
    criar_tabela_competicao_quadras()

    nome_competicao = (nome_competicao or "").strip()
    if not nome_competicao:
        return []

    quadras_normalizadas = []
    for idx, q in enumerate(quadras or [], start=1):
        q = q or {}
        nome = (q.get("nome") or f"Quadra {idx}").strip()
        local = (q.get("local") or "").strip()

        try:
            ordem = int(q.get("ordem") or idx)
        except (TypeError, ValueError):
            ordem = idx

        quadras_normalizadas.append({
            "id": q.get("id") or None,
            "nome": nome,
            "local": local,
            "ordem": max(1, ordem),
            "ativa": bool(q.get("ativa", True)),
        })

    if not quadras_normalizadas:
        quadras_normalizadas = [{"id": None, "nome": "Quadra 1", "local": "", "ordem": 1, "ativa": True}]

    ids_recebidos = []

    with conectar() as conn:
        with conn.cursor() as cur:
            for idx, q in enumerate(quadras_normalizadas, start=1):
                quadra_id = q.get("id")
                nome = q.get("nome") or f"Quadra {idx}"
                local = q.get("local") or ""
                ordem = q.get("ordem") or idx
                ativa = bool(q.get("ativa", True))

                if quadra_id:
                    cur.execute("""
                        UPDATE competicao_quadras
                        SET nome = %s,
                            local = %s,
                            ordem = %s,
                            ativa = %s,
                            atualizado_em = NOW()
                        WHERE id = %s
                          AND competicao = %s
                        RETURNING id
                    """, (nome, local, ordem, ativa, int(quadra_id), nome_competicao))
                    atualizada = cur.fetchone()
                    if atualizada:
                        ids_recebidos.append(int(atualizada["id"]))
                    else:
                        cur.execute("""
                            INSERT INTO competicao_quadras (competicao, nome, local, ordem, ativa)
                            VALUES (%s, %s, %s, %s, %s)
                            RETURNING id
                        """, (nome_competicao, nome, local, ordem, ativa))
                        nova = cur.fetchone()
                        if nova:
                            ids_recebidos.append(int(nova["id"]))
                else:
                    cur.execute("""
                        INSERT INTO competicao_quadras (competicao, nome, local, ordem, ativa)
                        VALUES (%s, %s, %s, %s, %s)
                        RETURNING id
                    """, (nome_competicao, nome, local, ordem, ativa))
                    nova = cur.fetchone()
                    if nova:
                        ids_recebidos.append(int(nova["id"]))

            if ids_recebidos:
                cur.execute("""
                    UPDATE competicao_quadras
                    SET ativa = FALSE,
                        atualizado_em = NOW()
                    WHERE competicao = %s
                      AND NOT (id = ANY(%s))
                """, (nome_competicao, ids_recebidos))

            colunas_comp = _buscar_colunas_tabela("competicoes")
            if "qtd_quadras" in colunas_comp:
                cur.execute("""
                    UPDATE competicoes
                    SET qtd_quadras = %s
                    WHERE nome = %s
                """, (len(quadras_normalizadas), nome_competicao))

        conn.commit()

    try:
        normalizar_vinculos_quadras_competicao(nome_competicao)
    except Exception as e:
        print("AVISO normalizar_vinculos_quadras_competicao:", repr(e))

    return listar_quadras_competicao(nome_competicao)


def buscar_quadra_competicao_por_id(nome_competicao, quadra_id):
    criar_tabela_competicao_quadras()

    if not quadra_id:
        return None

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, competicao, nome, local, ordem, ativa
                FROM competicao_quadras
                WHERE competicao = %s
                  AND id = %s
                LIMIT 1
            """, (nome_competicao, int(quadra_id)))
            quadra = cur.fetchone()
            if quadra:
                quadra["nome_exibicao"] = formatar_quadra_exibicao(quadra)
                quadra["quadra_label"] = quadra["nome_exibicao"]
            return quadra


def vincular_grupo_a_quadra(nome_competicao, grupo_nome, quadra_id):
    """
    Preparação para a próxima etapa: permite gravar a quadra padrão do grupo/chave
    quando existir tabela grupos com coluna quadra_id.
    """
    criar_tabela_competicao_quadras()

    quadra = buscar_quadra_competicao_por_id(nome_competicao, quadra_id)
    if not quadra:
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            if not _tabela_existe_cur(cur, "grupos"):
                return False

            colunas = _buscar_colunas_cur(cur, "grupos")
            if "quadra_id" not in colunas:
                return False

            campo_nome = "nome" if "nome" in colunas else ("grupo" if "grupo" in colunas else None)
            if not campo_nome:
                return False

            sets = ["quadra_id = %s"]
            valores = [quadra["id"]]

            if "quadra_nome" in colunas:
                sets.append("quadra_nome = %s")
                valores.append(formatar_quadra_exibicao(quadra))

            valores.extend([nome_competicao, grupo_nome])
            cur.execute(f"""
                UPDATE grupos
                SET {', '.join(sets)}
                WHERE competicao = %s
                  AND {campo_nome} = %s
            """, tuple(valores))

        conn.commit()

    return True


def aplicar_quadra_em_partida(nome_competicao, partida_id, quadra_id):
    """
    Permite sobrescrever a quadra de uma partida específica sem mudar a quadra padrão do grupo.
    """
    criar_tabela_competicao_quadras()

    quadra = buscar_quadra_competicao_por_id(nome_competicao, quadra_id)
    if not quadra:
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            if not _tabela_existe_cur(cur, "partidas"):
                return False

            colunas = _buscar_colunas_cur(cur, "partidas")
            if "quadra_id" not in colunas:
                return False

            sets = ["quadra_id = %s"]
            valores = [quadra["id"]]

            if "quadra_nome" in colunas:
                sets.append("quadra_nome = %s")
                valores.append(formatar_quadra_exibicao(quadra))

            if "quadra" in colunas:
                sets.append("quadra = %s")
                valores.append(str(quadra["id"]))

            valores.extend([nome_competicao, int(partida_id)])
            cur.execute(f"""
                UPDATE partidas
                SET {', '.join(sets)}
                WHERE competicao = %s
                  AND id = %s
            """, tuple(valores))

        conn.commit()

    return True

# =========================================================
# PERFIL GLOBAL DA EQUIPE
# =========================================================
def criar_campos_perfil_equipe(force=False):
    """
    Garante os campos de perfil global da equipe.

    Esses campos são opcionais para manter compatibilidade com equipes antigas.
    A própria equipe pode completar depois pelo painel/perfil.
    """
    chave = "campos_perfil_equipe"
    if _schema_ja_pronto(chave, force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE equipes
                ADD COLUMN IF NOT EXISTS cidade TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS responsavel TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS telefone TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS email TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS instagram TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS escudo TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS escudo_blob TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS perfil_completo BOOLEAN DEFAULT FALSE
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_equipes_nome_lower
                ON equipes (LOWER(TRIM(nome)))
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_equipes_login
                ON equipes (login)
            """)

        conn.commit()

    _CACHE_COLUNAS.pop("equipes", None)
    _marcar_schema_pronto(chave)


def criar_campo_escudo_equipes(force=False):
    """Garante os campos de escudo/logo da equipe.

    escudo: mantido por compatibilidade com telas antigas.
    escudo_blob: imagem definitiva em data URL/base64 salva no Neon.
    """
    chave = "campo_escudo_equipes"
    if _schema_ja_pronto(chave, force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE equipes
                ADD COLUMN IF NOT EXISTS escudo TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS escudo_blob TEXT DEFAULT ''
            """)
        conn.commit()

    _CACHE_COLUNAS.pop("equipes", None)
    _marcar_schema_pronto(chave)


def _escudo_exibicao_sql(prefixo=""):
    """SQL compatível para retornar sempre o melhor escudo disponível."""
    p = f"{prefixo}." if prefixo else ""
    try:
        colunas = _buscar_colunas_tabela("equipes")
    except Exception:
        colunas = set()

    partes = []
    if "escudo_blob" in colunas:
        partes.append(f"NULLIF({p}escudo_blob, '')")
    if "escudo" in colunas:
        partes.append(f"NULLIF({p}escudo, '')")

    if not partes:
        return "''"

    return "COALESCE(" + ", ".join(partes) + ", '')"


def _aplicar_escudo_exibicao_obj(equipe):
    if not equipe:
        return equipe

    try:
        escudo_blob = equipe.get("escudo_blob") or ""
        escudo = equipe.get("escudo") or ""
        exibicao = escudo_blob or escudo or escudo_padrao_equipe()
        equipe["escudo_exibicao"] = exibicao
        if not equipe.get("escudo") and exibicao:
            equipe["escudo"] = exibicao
    except Exception:
        pass

    return equipe


def aplicar_escudo_exibicao_lista(equipes):
    return [_aplicar_escudo_exibicao_obj(dict(e or {})) for e in (equipes or [])]


def atualizar_escudo_equipe_por_login(login, escudo, escudo_blob=None):
    """Atualiza o escudo global da equipe de forma robusta.

    Em alguns bancos antigos, o usuário da equipe pode ter o login atualizado em
    `usuarios`, mas a linha global de `equipes` ainda permanecer com outro login
    ou apenas com o mesmo nome da equipe. Antes essa situação fazia o upload
    processar a imagem corretamente, mas o UPDATE não alterava nenhuma linha e a
    tela mostrava "Não foi possível salvar o escudo".

    Agora tenta, nesta ordem:
    1) atualizar pela coluna equipes.login;
    2) localizar o nome da equipe em usuarios.equipe e atualizar por esse nome;
    3) se existir vínculo em equipes_competicoes, atualizar pelo vínculo.
    """
    login = (login or "").strip()
    escudo = (escudo or "").strip()
    if escudo_blob is None:
        escudo_blob = escudo
    escudo_blob = (escudo_blob or "").strip()

    if not login:
        return False

    try:
        criar_campo_escudo_equipes()
    except Exception as e:
        print("ERRO GARANTIR CAMPO ESCUDO:", repr(e))
        return False

    colunas = _buscar_colunas_tabela("equipes")

    sets = []
    valores_base = []

    if "escudo" in colunas:
        sets.append("escudo = %s")
        valores_base.append(escudo)
    if "escudo_blob" in colunas:
        sets.append("escudo_blob = %s")
        valores_base.append(escudo_blob)

    if not sets:
        return False

    set_sql = ", ".join(sets)

    with conectar() as conn:
        with conn.cursor() as cur:
            # 1) Caminho normal: login da equipe igual ao login da sessão.
            cur.execute(
                f"UPDATE equipes SET {set_sql} WHERE login = %s",
                tuple(valores_base + [login])
            )
            alteradas = cur.rowcount or 0

            # 2) Fallback: login da sessão está em usuarios, mas equipes.login ficou antigo.
            if alteradas <= 0:
                cur.execute("""
                    SELECT equipe
                    FROM usuarios
                    WHERE login = %s
                    LIMIT 1
                """, (login,))
                row_usuario = cur.fetchone()
                nome_equipe = ""
                try:
                    nome_equipe = (row_usuario.get("equipe") if hasattr(row_usuario, "get") else row_usuario[0]) or ""
                except Exception:
                    nome_equipe = ""
                nome_equipe = str(nome_equipe).strip()

                if nome_equipe:
                    cur.execute(
                        f"""
                        UPDATE equipes
                        SET {set_sql}
                        WHERE LOWER(TRIM(nome)) = LOWER(TRIM(%s))
                        """,
                        tuple(valores_base + [nome_equipe])
                    )
                    alteradas = cur.rowcount or 0

            # 3) Fallback extra: vínculo de competição guarda o login/nome antigo.
            if alteradas <= 0:
                try:
                    criar_tabela_equipes_competicoes()
                    cur.execute(
                        f"""
                        UPDATE equipes e
                        SET {set_sql}
                        FROM equipes_competicoes ec
                        WHERE (
                            ec.equipe_login = %s
                            OR LOWER(TRIM(ec.equipe_nome)) = LOWER(TRIM(e.nome))
                        )
                        AND (
                            e.login = ec.equipe_login
                            OR LOWER(TRIM(e.nome)) = LOWER(TRIM(ec.equipe_nome))
                        )
                        """,
                        tuple(valores_base + [login])
                    )
                    alteradas = cur.rowcount or 0
                except Exception as e:
                    print("ERRO FALLBACK ESCUDO VINCULO:", repr(e))

        conn.commit()

    return alteradas > 0


def escudo_padrao_equipe():
    return "/static/img/escudo_padrao.svg"


def escudo_equipe_url(equipe):
    if not equipe:
        return escudo_padrao_equipe()
    valor = ""
    try:
        valor = (
            equipe.get("escudo_exibicao")
            or equipe.get("escudo_blob")
            or equipe.get("escudo")
            or equipe.get("escudo_url")
            or ""
        )
    except Exception:
        valor = ""
    return valor or escudo_padrao_equipe()


def perfil_equipe_incompleto_por_login(login, conn=None):
    """
    Retorna True quando a equipe ainda não completou os dados mínimos do perfil.

    Campos mínimos:
    - cidade
    - responsavel
    - telefone

    Mantém compatibilidade: se a equipe não existir, não bloqueia o login.
    """
    login = (login or "").strip()
    if not login:
        return False

    criar_campos_perfil_equipe()

    sql = """
        SELECT
            nome,
            cidade,
            responsavel,
            telefone,
            email,
            instagram,
            COALESCE(perfil_completo, FALSE) AS perfil_completo
        FROM equipes
        WHERE login = %s
        LIMIT 1
    """

    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(sql, (login,))
            equipe = cur.fetchone()
    else:
        with conectar() as conn2:
            return perfil_equipe_incompleto_por_login(login, conn2)

    if not equipe:
        return False

    cidade = str(equipe.get("cidade") or "").strip()
    responsavel = str(equipe.get("responsavel") or "").strip()
    telefone = str(equipe.get("telefone") or "").strip()

    return not cidade or not responsavel or not telefone


def buscar_perfil_equipe_por_login(login, conn=None):
    """
    Busca o perfil global da equipe pelo login.
    Usado para preencher a tela /perfil-equipe.
    """
    login = (login or "").strip()
    if not login:
        return None

    criar_campos_perfil_equipe()

    colunas = _buscar_colunas_tabela("equipes")
    campos = [
        "nome",
        "login",
        "senha",
        "cidade",
        "responsavel",
        "telefone",
        "email",
        "instagram",
        "escudo",
        _campo_ou_alias(colunas, "escudo_blob", "'' AS escudo_blob"),
        f"{_escudo_exibicao_sql()} AS escudo_exibicao",
        "COALESCE(perfil_completo, FALSE) AS perfil_completo",
    ]

    if "competicao" in colunas:
        campos.insert(3, "competicao")
    else:
        campos.insert(3, "'' AS competicao")

    sql = f"""
        SELECT {", ".join(campos)}
        FROM equipes
        WHERE login = %s
        LIMIT 1
    """

    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(sql, (login,))
            return cur.fetchone()

    with conectar() as conn:
        return buscar_perfil_equipe_por_login(login, conn)


def salvar_perfil_equipe_por_login(
    login,
    cidade="",
    responsavel="",
    telefone="",
    email="",
    instagram="",
    escudo=None,
):
    """
    Atualiza o perfil global da equipe sem mexer no vínculo com competições.

    Versão robusta:
    - tenta atualizar por equipes.login;
    - se o login da sessão mudou e a linha de equipes ficou antiga, tenta
      localizar a equipe pelo nome salvo em usuarios.equipe;
    - como último fallback, usa o vínculo em equipes_competicoes.
    """
    login = (login or "").strip()
    if not login:
        return False

    criar_campos_perfil_equipe()

    cidade = (cidade or "").strip()
    responsavel = (responsavel or "").strip()
    telefone = (telefone or "").strip()
    email = (email or "").strip()
    instagram = (instagram or "").strip()

    perfil_completo = bool(cidade and responsavel and telefone)

    sets = [
        "cidade = %s",
        "responsavel = %s",
        "telefone = %s",
        "email = %s",
        "instagram = %s",
        "perfil_completo = %s",
    ]
    valores_base = [
        cidade,
        responsavel,
        telefone,
        email,
        instagram,
        perfil_completo,
    ]

    if escudo is not None:
        escudo_valor = (escudo or "").strip()
        sets.append("escudo = %s")
        valores_base.append(escudo_valor)
        try:
            colunas_equipes = _buscar_colunas_tabela("equipes")
            if "escudo_blob" in colunas_equipes:
                sets.append("escudo_blob = %s")
                valores_base.append(escudo_valor)
        except Exception:
            pass

    set_sql = ", ".join(sets)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE equipes
                SET {set_sql}
                WHERE login = %s
                """,
                tuple(valores_base + [login])
            )
            alteradas = cur.rowcount or 0

            if alteradas <= 0:
                cur.execute("""
                    SELECT equipe
                    FROM usuarios
                    WHERE login = %s
                    LIMIT 1
                """, (login,))
                row_usuario = cur.fetchone()
                nome_equipe = ""
                try:
                    nome_equipe = (row_usuario.get("equipe") if hasattr(row_usuario, "get") else row_usuario[0]) or ""
                except Exception:
                    nome_equipe = ""
                nome_equipe = str(nome_equipe).strip()

                if nome_equipe:
                    cur.execute(
                        f"""
                        UPDATE equipes
                        SET {set_sql}
                        WHERE LOWER(TRIM(nome)) = LOWER(TRIM(%s))
                        """,
                        tuple(valores_base + [nome_equipe])
                    )
                    alteradas = cur.rowcount or 0

            if alteradas <= 0:
                try:
                    criar_tabela_equipes_competicoes()
                    cur.execute(
                        f"""
                        UPDATE equipes e
                        SET {set_sql}
                        FROM equipes_competicoes ec
                        WHERE (
                            ec.equipe_login = %s
                            OR LOWER(TRIM(ec.equipe_nome)) = LOWER(TRIM(e.nome))
                        )
                        AND (
                            e.login = ec.equipe_login
                            OR LOWER(TRIM(e.nome)) = LOWER(TRIM(ec.equipe_nome))
                        )
                        """,
                        tuple(valores_base + [login])
                    )
                    alteradas = cur.rowcount or 0
                except Exception as e:
                    print("AVISO salvar_perfil_equipe_por_login/fallback_vinculo:", repr(e))

        conn.commit()

    return alteradas > 0



# =========================================================
# PIN OPERACIONAL POR APONTADOR (ÁRBITROS E TELÃO)
# =========================================================
def criar_tabela_pins_operacionais():
    """
    PIN operacional por competição + apontador.

    IMPORTANTE PERFORMANCE:
    antes esta função chamava criar_tabelas_oficiais() em toda entrada do
    apontador na competição. No Render/Neon isso segura conexão e deixa tablet
    travado. Agora a garantia é feita uma vez por processo e só para esta tabela.
    """
    global _PINS_OPERACIONAIS_SCHEMA_OK

    if _PINS_OPERACIONAIS_SCHEMA_OK:
        return True

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS competicao_pins_operacionais (
                    id SERIAL PRIMARY KEY,
                    competicao TEXT NOT NULL,
                    apontador_cpf TEXT NOT NULL,
                    pin VARCHAR(4) UNIQUE NOT NULL,
                    ativo BOOLEAN DEFAULT TRUE,
                    criado_em TIMESTAMP DEFAULT NOW(),
                    atualizado_em TIMESTAMP DEFAULT NOW(),
                    transferido_de_cpf TEXT DEFAULT '',
                    transferido_em TIMESTAMP
                )
            """)

            cur.execute("""
                ALTER TABLE competicao_pins_operacionais
                ADD COLUMN IF NOT EXISTS competicao TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS apontador_cpf TEXT NOT NULL DEFAULT '',
                ADD COLUMN IF NOT EXISTS pin VARCHAR(4),
                ADD COLUMN IF NOT EXISTS ativo BOOLEAN DEFAULT TRUE,
                ADD COLUMN IF NOT EXISTS criado_em TIMESTAMP DEFAULT NOW(),
                ADD COLUMN IF NOT EXISTS atualizado_em TIMESTAMP DEFAULT NOW(),
                ADD COLUMN IF NOT EXISTS transferido_de_cpf TEXT DEFAULT '',
                ADD COLUMN IF NOT EXISTS transferido_em TIMESTAMP
            """)

            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_competicao_pin_operacional_comp_apontador
                ON competicao_pins_operacionais (
                    competicao,
                    REGEXP_REPLACE(COALESCE(apontador_cpf, ''), '\\D', '', 'g')
                )
            """)

            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_competicao_pin_operacional_pin
                ON competicao_pins_operacionais (pin)
                WHERE pin IS NOT NULL AND pin <> ''
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_competicao_pin_operacional_competicao
                ON competicao_pins_operacionais (competicao)
            """)

        conn.commit()

        _CACHE_COLUNAS.pop("competicao_pins_operacionais", None)
    _PINS_OPERACIONAIS_SCHEMA_OK = True
    return True


def _normalizar_pin_operacional(pin):
    pin = re.sub(r"\D", "", str(pin or ""))
    if len(pin) != 4:
        return ""
    return pin


def _gerar_pin_operacional_unico_cur(cur):
    for _ in range(80):
        pin = str(random.randint(1000, 9999))

        cur.execute("""
            SELECT id
            FROM competicao_pins_operacionais
            WHERE pin = %s
            LIMIT 1
        """, (pin,))
        if cur.fetchone():
            continue

        try:
            cur.execute("""
                SELECT id
                FROM competicao_quadras
                WHERE pin_arbitragem = %s
                LIMIT 1
            """, (pin,))
            if cur.fetchone():
                continue
        except Exception:
            pass

        return pin

    return str(random.randint(1000, 9999))


def garantir_pin_operacional_apontador(competicao, apontador_cpf):
    criar_tabela_pins_operacionais()
    competicao = (competicao or "").strip()
    apontador_cpf = somente_digitos(apontador_cpf)

    if not competicao or not apontador_cpf:
        return None

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM competicao_pins_operacionais
                WHERE competicao = %s
                  AND REGEXP_REPLACE(COALESCE(apontador_cpf, ''), '\\D', '', 'g') = %s
                LIMIT 1
            """, (competicao, apontador_cpf))
            atual = cur.fetchone()

            if atual and _normalizar_pin_operacional(atual.get("pin")):
                return atual

            novo_pin = _gerar_pin_operacional_unico_cur(cur)

            if atual:
                cur.execute("""
                    UPDATE competicao_pins_operacionais
                    SET pin = %s,
                        ativo = TRUE,
                        atualizado_em = NOW()
                    WHERE id = %s
                """, (novo_pin, atual["id"]))
            else:
                cur.execute("""
                    INSERT INTO competicao_pins_operacionais (
                        competicao, apontador_cpf, pin, ativo
                    )
                    VALUES (%s, %s, %s, TRUE)
                """, (competicao, apontador_cpf, novo_pin))

        conn.commit()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM competicao_pins_operacionais
                WHERE competicao = %s
                  AND REGEXP_REPLACE(COALESCE(apontador_cpf, ''), '\\D', '', 'g') = %s
                LIMIT 1
            """, (competicao, apontador_cpf))
            return cur.fetchone()


def listar_pins_operacionais_competicao(competicao):
    """
    Lista todos os apontadores vinculados à competição e garante PIN para cada um.
    """
    criar_tabela_pins_operacionais()
    criar_tabelas_oficiais()
    competicao = (competicao or "").strip()

    if not competicao:
        return []

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT
                    o.nome,
                    o.cpf,
                    c.funcao
                FROM competicao_oficiais c
                JOIN oficiais o ON o.cpf = c.cpf
                WHERE c.competicao = %s
                  AND LOWER(COALESCE(c.funcao, '')) = 'apontador'
                ORDER BY o.nome
            """, (competicao,))
            apontadores = cur.fetchall() or []

    for apontador in apontadores:
        garantir_pin_operacional_apontador(competicao, apontador.get("cpf"))

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    p.id,
                    p.competicao,
                    p.apontador_cpf,
                    p.pin,
                    p.ativo,
                    p.criado_em,
                    p.atualizado_em,
                    o.nome AS apontador_nome,
                    a.ativo AS apontador_ativo
                FROM competicao_pins_operacionais p
                LEFT JOIN oficiais o
                    ON REGEXP_REPLACE(COALESCE(o.cpf, ''), '\\D', '', 'g') = REGEXP_REPLACE(COALESCE(p.apontador_cpf, ''), '\\D', '', 'g')
                LEFT JOIN apontadores_acesso a
                    ON REGEXP_REPLACE(COALESCE(a.cpf, ''), '\\D', '', 'g') = REGEXP_REPLACE(COALESCE(p.apontador_cpf, ''), '\\D', '', 'g')
                WHERE p.competicao = %s
                ORDER BY COALESCE(o.nome, p.apontador_cpf)
            """, (competicao,))
            return cur.fetchall() or []


def buscar_vinculo_operacional_por_pin(pin):
    criar_tabela_pins_operacionais()
    pin = _normalizar_pin_operacional(pin)
    if not pin:
        return None

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    p.id,
                    p.competicao,
                    p.apontador_cpf,
                    p.pin,
                    p.ativo,
                    o.nome AS apontador_nome
                FROM competicao_pins_operacionais p
                LEFT JOIN oficiais o
                    ON REGEXP_REPLACE(COALESCE(o.cpf, ''), '\\D', '', 'g') = REGEXP_REPLACE(COALESCE(p.apontador_cpf, ''), '\\D', '', 'g')
                WHERE p.pin = %s
                  AND COALESCE(p.ativo, TRUE) = TRUE
                LIMIT 1
            """, (pin,))
            return cur.fetchone()


def regenerar_pin_operacional_apontador(competicao, apontador_cpf):
    criar_tabela_pins_operacionais()
    competicao = (competicao or "").strip()
    apontador_cpf = somente_digitos(apontador_cpf)

    if not competicao or not apontador_cpf:
        return None

    with conectar() as conn:
        with conn.cursor() as cur:
            novo_pin = _gerar_pin_operacional_unico_cur(cur)
            cur.execute("""
                INSERT INTO competicao_pins_operacionais (
                    competicao, apontador_cpf, pin, ativo
                )
                VALUES (%s, %s, %s, TRUE)
                ON CONFLICT (competicao, REGEXP_REPLACE(COALESCE(apontador_cpf, ''), '\\D', '', 'g'))
                DO UPDATE SET
                    pin = EXCLUDED.pin,
                    ativo = TRUE,
                    atualizado_em = NOW()
            """, (competicao, apontador_cpf, novo_pin))
        conn.commit()

    return garantir_pin_operacional_apontador(competicao, apontador_cpf)


def transferir_pin_operacional(competicao, pin, novo_apontador_cpf):
    """
    Transfere o canal operacional para outro apontador sem trocar o PIN.
    Útil quando um apontador assume no meio do dia e árbitro/telão não podem parar.
    """
    criar_tabela_pins_operacionais()
    competicao = (competicao or "").strip()
    pin = _normalizar_pin_operacional(pin)
    novo_apontador_cpf = somente_digitos(novo_apontador_cpf)

    if not competicao or not pin or not novo_apontador_cpf:
        return False

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT apontador_cpf
                FROM competicao_pins_operacionais
                WHERE competicao = %s
                  AND pin = %s
                  AND COALESCE(ativo, TRUE) = TRUE
                LIMIT 1
            """, (competicao, pin))
            atual = cur.fetchone()
            if not atual:
                return False

            cur.execute("""
                UPDATE competicao_pins_operacionais
                SET apontador_cpf = %s,
                    transferido_de_cpf = %s,
                    transferido_em = NOW(),
                    atualizado_em = NOW()
                WHERE competicao = %s
                  AND pin = %s
            """, (novo_apontador_cpf, atual.get("apontador_cpf") or "", competicao, pin))
        conn.commit()

    return True



# =========================================================
# APONTADOR - STATUS INTELIGENTE / SALVAR E RETOMAR PARTIDA
# =========================================================
def _partida_tem_placar_ou_estado(partida):
    partida = partida or {}
    try:
        if int(partida.get("pontos_a") or 0) > 0:
            return True
        if int(partida.get("pontos_b") or 0) > 0:
            return True
        if int(partida.get("sets_a") or 0) > 0:
            return True
        if int(partida.get("sets_b") or 0) > 0:
            return True
        if int(partida.get("set_atual") or 1) > 1:
            return True
    except Exception:
        pass

    for campo in ("rotacao_a_json", "rotacao_b_json", "saque_atual"):
        valor = partida.get(campo)
        if valor not in (None, "", "[]", "{}"):
            return True

    return False


def normalizar_status_partidas_apontador(partidas, competicao):
    """
    Corrige a lista que aparece no painel do apontador.

    REGRAS:
    - Não consulta schema/tabelas durante request.
    - Não recalcula partidas finalizadas.
    - Reutiliza eventos_total quando já veio da listagem.
    - Evita funções pesadas por partida.
    - Não consulta snapshot/scout/JSON.
    """

    partidas = [dict(p or {}) for p in (partidas or [])]

    if not partidas:
        return []

    eventos_por_id = {}
    ids_consulta = []

    # =====================================================
    # DEFINE QUAIS PARTIDAS REALMENTE PRECISAM CONSULTAR
    # EVENTOS
    # =====================================================
    for p in partidas:
        status = str(p.get("status") or "").strip().lower()
        status_jogo = str(p.get("status_jogo") or "").strip().lower()
        status_op = str(p.get("status_operacao") or "").strip().lower()

        finalizada = (
            status in {"finalizada", "finalizado", "encerrada", "encerrado"}
            or status_jogo in {"finalizada", "finalizado", "encerrada", "encerrado"}
            or status_op in {"finalizada", "finalizado"}
        )

        # Não consulta partidas encerradas
        if finalizada:
            continue

        # Se já veio da query principal, reutiliza
        if p.get("eventos_total") is not None:
            continue

        pid = p.get("id")

        if pid is not None:
            ids_consulta.append(pid)

    # =====================================================
    # CONSULTA EVENTOS SOMENTE DO NECESSÁRIO
    # =====================================================
    if ids_consulta:
        try:
            with conectar() as conn:
                with conn.cursor() as cur:

                    cur.execute("""
                        SELECT
                            partida_id,
                            COUNT(*) AS total
                        FROM eventos
                        WHERE competicao = %s
                          AND partida_id = ANY(%s)
                        GROUP BY partida_id
                    """, (competicao, ids_consulta))

                    for row in cur.fetchall() or []:
                        eventos_por_id[
                            int(row["partida_id"])
                        ] = int(row["total"] or 0)

        except Exception as e:
            print(
                "ERRO normalizar_status_partidas_apontador:",
                repr(e),
                flush=True
            )

    # =====================================================
    # NORMALIZA STATUS
    # =====================================================
    for p in partidas:

        status = str(p.get("status") or "").strip().lower()
        status_jogo = str(p.get("status_jogo") or "").strip().lower()
        status_op = str(
            p.get("status_operacao") or "livre"
        ).strip().lower()

        total_eventos = int(
            p.get("eventos_total")
            or eventos_por_id.get(
                int(p.get("id") or 0),
                0
            )
            or 0
        )

        # =================================================
        # STATUS BASE
        # =================================================
        finalizada = (
            status in {
                "finalizada",
                "finalizado",
                "encerrada",
                "encerrado"
            }
            or status_jogo in {
                "finalizada",
                "finalizado",
                "encerrada",
                "encerrado"
            }
            or status_op in {
                "finalizada",
                "finalizado"
            }
        )

        pausada = (
            status_jogo in {
                "pausada",
                "pausado",
                "salva",
                "salvo"
            }
            or status_op in {
                "pausada",
                "pausado"
            }
        )

        # =================================================
        # DETECÇÃO LEVE DE PARTIDA INICIADA
        # =================================================
        possui_placar = any([
            int(p.get("sets_a") or 0) > 0,
            int(p.get("sets_b") or 0) > 0,
            int(p.get("pontos_a") or 0) > 0,
            int(p.get("pontos_b") or 0) > 0,

            bool(p.get("set1_a")),
            bool(p.get("set1_b")),

            bool(p.get("set2_a")),
            bool(p.get("set2_b")),

            bool(p.get("set3_a")),
            bool(p.get("set3_b")),

            bool(p.get("set4_a")),
            bool(p.get("set4_b")),

            bool(p.get("set5_a")),
            bool(p.get("set5_b")),
        ])

        iniciada = (
            status in {
                "em andamento",
                "em_andamento",
                "ao vivo",
                "ao_vivo",
                "iniciada",
                "iniciado"
            }

            or status_jogo in {
                "em_andamento",
                "em andamento",
                "entre_sets",
                "tiebreak_sorteio"
            }

            or status_op in {
                "em_andamento",
                "ao_vivo",
                "jogo",
                "iniciada",
                "iniciado"
            }

            or possui_placar
            or total_eventos > 0
        )

        # =================================================
        # FLAGS
        # =================================================
        p["eventos_total"] = total_eventos

        p["tem_jogo_iniciado"] = bool(
            iniciada
            and not finalizada
            and not pausada
        )

        p["tem_jogo_pausado"] = bool(
            pausada
            and not finalizada
        )

        p["tem_jogo_finalizado"] = bool(finalizada)

        # =================================================
        # STATUS FINAL
        # =================================================
        if finalizada:

            p["status_operacao"] = "finalizada"

        elif pausada:

            p["status_operacao"] = "pausada"

        elif iniciada:

            p["status_operacao"] = "em_andamento"

            if (
                not p.get("status_jogo")
                or str(
                    p.get("status_jogo")
                ).lower() == "pre_jogo"
            ):
                p["status_jogo"] = "em_andamento"

        elif status_op in {
            "pre_jogo",
            "reservado"
        }:

            p["status_operacao"] = status_op

        else:

            p["status_operacao"] = "livre"

    return partidas



def salvar_estado_manual_partida(partida_id, competicao, estado, operador=None, pausar=False):
    """Salva no banco o estado vivo do cache/JS para permitir troca de aparelho e retomada."""
    import json

    criar_campos_jogo_partida()
    criar_campos_sets_partida()

    estado = dict(estado or {})

    def _int(v, padrao=0):
        try:
            if v is None or v == "":
                return padrao
            return int(v)
        except Exception:
            return padrao

    def _lista6(v):
        return _normalizar_rotacao_oficial(v or ["", "", "", "", "", ""])

    pontos_a = _int(estado.get("pontos_a", estado.get("placar_a", 0)), 0)
    pontos_b = _int(estado.get("pontos_b", estado.get("placar_b", 0)), 0)
    sets_a = _int(estado.get("sets_a"), 0)
    sets_b = _int(estado.get("sets_b"), 0)
    set_atual = max(1, _int(estado.get("set_atual"), 1))
    rotacao_a = _lista6(estado.get("rotacao_a"))
    rotacao_b = _lista6(estado.get("rotacao_b"))

    status_jogo = "pausada" if pausar else str(estado.get("status_jogo") or "em_andamento").strip().lower()
    if status_jogo in {"pre_jogo", "", "livre"}:
        status_jogo = "em_andamento"

    colunas = _buscar_colunas_tabela("partidas")
    sets = [
        "pontos_a = %s", "pontos_b = %s", "sets_a = %s", "sets_b = %s", "set_atual = %s",
        "saque_atual = %s", "rotacao_a = %s", "rotacao_b = %s", "rotacao_a_json = %s", "rotacao_b_json = %s",
        "status_jogadores_a_json = %s", "status_jogadores_b_json = %s", "subs_a = %s", "subs_b = %s",
        "sancoes_a_json = %s", "sancoes_b_json = %s", "cartoes_verdes_a_json = %s", "cartoes_verdes_b_json = %s",
        "retardamentos_a_json = %s", "retardamentos_b_json = %s", "subs_excepcionais_json = %s",
        "status_jogo = %s", "fase_partida = %s"
    ]
    params = [
        pontos_a, pontos_b, sets_a, sets_b, set_atual,
        estado.get("saque_atual") or None, rotacao_a, rotacao_b, json.dumps(rotacao_a, ensure_ascii=False), json.dumps(rotacao_b, ensure_ascii=False),
        json.dumps(estado.get("status_jogadores_a") or {}, ensure_ascii=False), json.dumps(estado.get("status_jogadores_b") or {}, ensure_ascii=False),
        _int(estado.get("subs_a"), 0), _int(estado.get("subs_b"), 0),
        json.dumps(estado.get("sancoes_a") or [], ensure_ascii=False), json.dumps(estado.get("sancoes_b") or [], ensure_ascii=False),
        json.dumps(estado.get("cartoes_verdes_a") or [], ensure_ascii=False), json.dumps(estado.get("cartoes_verdes_b") or [], ensure_ascii=False),
        json.dumps(estado.get("retardamentos_a") or [], ensure_ascii=False), json.dumps(estado.get("retardamentos_b") or [], ensure_ascii=False),
        json.dumps(estado.get("subs_excepcionais") or [], ensure_ascii=False),
        status_jogo, "jogo",
    ]

    if "status_operacao" in colunas:
        sets.append("status_operacao = %s")
        params.append("pausada" if pausar else "em_andamento")
    if "status" in colunas:
        sets.append("status = %s")
        params.append("em andamento")
    if "operador_login" in colunas and operador:
        sets.append("operador_login = COALESCE(operador_login, %s)")
        params.append(operador)

    params.extend([partida_id, competicao])

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE partidas
                SET {', '.join(sets)}
                WHERE id = %s
                  AND competicao = %s
            """, tuple(params))
        conn.commit()

    return buscar_estado_jogo_partida(partida_id, competicao) or {}

# =========================================================
# RECUPERAÇÃO / CORREÇÃO DE ESCUDOS ANTIGOS
# =========================================================
from PIL import Image, ImageOps
import uuid


def corrigir_escudos_antigos(app_static_folder):
    """
    Reprocessa todos os escudos antigos:
    - corrige rotação EXIF;
    - converte para JPG;
    - padroniza 512x512;
    - remove transparência problemática;
    - substitui arquivos quebrados.

    Retorna estatísticas da operação.
    """

    pasta_escudos = os.path.join(
        app_static_folder,
        "uploads",
        "escudos"
    )

    os.makedirs(pasta_escudos, exist_ok=True)

    total = 0
    corrigidos = 0
    erros = 0

    with conectar() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT login, escudo
                FROM equipes
                WHERE escudo IS NOT NULL
                  AND escudo <> ''
            """)

            equipes = cur.fetchall()

            for equipe in equipes:
                total += 1

                login = (equipe.get("login") or "").strip()
                escudo = (equipe.get("escudo") or "").strip()

                try:
                    if not escudo.startswith("/static/"):
                        continue

                    caminho_antigo = escudo.replace("/static/", "")
                    caminho_antigo = os.path.join(
                        app_static_folder,
                        caminho_antigo
                    )

                    if not os.path.exists(caminho_antigo):
                        print("ESCUDO NÃO ENCONTRADO:", caminho_antigo)
                        erros += 1
                        continue

                    imagem = Image.open(caminho_antigo)

                    # Corrige rotação do celular
                    imagem = ImageOps.exif_transpose(imagem)

                    # Corrige transparência
                    if imagem.mode in ("RGBA", "LA", "P"):
                        fundo = Image.new(
                            "RGB",
                            imagem.size,
                            (255, 255, 255)
                        )

                        fundo.paste(
                            imagem,
                            mask=imagem.split()[-1]
                        )

                        imagem = fundo
                    else:
                        imagem = imagem.convert("RGB")

                    # Crop quadrado
                    tamanho = min(imagem.size)

                    esquerda = (imagem.width - tamanho) // 2
                    topo = (imagem.height - tamanho) // 2

                    imagem = imagem.crop((
                        esquerda,
                        topo,
                        esquerda + tamanho,
                        topo + tamanho
                    ))

                    # Resize padrão
                    imagem = imagem.resize((512, 512))

                    novo_nome = (
                        f"{login}_{uuid.uuid4().hex[:10]}.jpg"
                    )

                    novo_caminho = os.path.join(
                        pasta_escudos,
                        novo_nome
                    )

                    imagem.save(
                        novo_caminho,
                        format="JPEG",
                        quality=85,
                        optimize=True
                    )

                    novo_escudo = (
                        f"/static/uploads/escudos/{novo_nome}"
                    )

                    cur.execute("""
                        UPDATE equipes
                        SET escudo = %s
                        WHERE login = %s
                    """, (
                        novo_escudo,
                        login
                    ))

                    corrigidos += 1

                except Exception as e:
                    print(
                        "ERRO CORRIGIR ESCUDO:",
                        login,
                        e
                    )
                    erros += 1

        conn.commit()

    return {
        "total": total,
        "corrigidos": corrigidos,
        "erros": erros
    }

# =========================================================
# ESCUDOS DEFINITIVOS NO BANCO (BASE64 / NEON)
# =========================================================
def migrar_escudos_arquivos_para_banco(app_static_folder):
    """
    Migra para o Neon os escudos antigos que ainda existirem no disco.

    - Cria escudo_blob se faltar.
    - Se escudo já for data:image, só copia para escudo_blob.
    - Se escudo apontar para /static/uploads/..., tenta abrir o arquivo e salvar base64.
    - Não apaga nada do disco.
    - Não altera equipes cujo arquivo não existe/corrompeu, apenas registra erro.
    """
    criar_campo_escudo_equipes(force=True)

    try:
        from PIL import Image, ImageOps
    except Exception as e:
        return {
            "ok": False,
            "erro": f"Pillow não instalado: {e}",
            "total": 0,
            "migrados": 0,
            "ja_estavam_no_banco": 0,
            "sem_escudo": 0,
            "erros": [],
        }

    total = 0
    migrados = 0
    ja_estavam_no_banco = 0
    sem_escudo = 0
    erros = []

    def _imagem_para_data_url(caminho):
        img = Image.open(caminho)
        img = ImageOps.exif_transpose(img)
        img.load()

        if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
            img = img.convert("RGBA")
            fundo = Image.new("RGBA", img.size, (255, 255, 255, 255))
            fundo.alpha_composite(img)
            img = fundo.convert("RGB")
        else:
            img = img.convert("RGB")

        largura, altura = img.size
        lado = min(largura, altura)
        esquerda = max((largura - lado) // 2, 0)
        topo = max((altura - lado) // 2, 0)
        img = img.crop((esquerda, topo, esquerda + lado, topo + lado))
        filtro = getattr(Image, "Resampling", Image).LANCZOS
        img = img.resize((512, 512), filtro)

        buffer = BytesIO()
        img.save(buffer, format="JPEG", quality=82, optimize=True, progressive=True)
        encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
        return f"data:image/jpeg;base64,{encoded}"

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nome, login, COALESCE(escudo, '') AS escudo, COALESCE(escudo_blob, '') AS escudo_blob
                FROM equipes
                ORDER BY nome
            """)
            equipes = cur.fetchall()

            for equipe in equipes:
                total += 1
                nome = equipe.get("nome") or ""
                login = equipe.get("login") or ""
                escudo = (equipe.get("escudo") or "").strip()
                escudo_blob = (equipe.get("escudo_blob") or "").strip()

                if escudo_blob.startswith("data:image/"):
                    ja_estavam_no_banco += 1
                    continue

                if escudo.startswith("data:image/"):
                    cur.execute("""
                        UPDATE equipes
                        SET escudo_blob = %s, escudo = %s
                        WHERE login = %s
                    """, (escudo, escudo, login))
                    migrados += 1
                    continue

                if not escudo:
                    sem_escudo += 1
                    continue

                if not escudo.startswith("/static/"):
                    erros.append({"nome": nome, "login": login, "escudo": escudo, "motivo": "URL inválida"})
                    continue

                caminho_relativo = escudo.replace("/static/", "", 1)
                caminho = os.path.join(app_static_folder, caminho_relativo)

                if not os.path.exists(caminho):
                    erros.append({"nome": nome, "login": login, "escudo": escudo, "motivo": "Arquivo não existe no Render"})
                    continue

                try:
                    data_url = _imagem_para_data_url(caminho)
                    cur.execute("""
                        UPDATE equipes
                        SET escudo_blob = %s, escudo = %s
                        WHERE login = %s
                    """, (data_url, data_url, login))
                    migrados += 1
                except Exception as e:
                    erros.append({"nome": nome, "login": login, "escudo": escudo, "motivo": str(e)})

        conn.commit()

    try:
        # Marca como gerado quando a rotina automática/manual já materializou algo
        # ou quando a classificatória geral fechou. Não precisa mais depender só do botão.
        _salvar_flag_avanco_gerado_competicao(
            nome_competicao,
            bool(classificatoria_fechada or criadas > 0 or atualizadas > 0)
        )
    except Exception as e:
        print("AVISO gerar_avanco/salvar_flag:", repr(e))

    return {
        "ok": True,
        "total": total,
        "migrados": migrados,
        "ja_estavam_no_banco": ja_estavam_no_banco,
        "sem_escudo": sem_escudo,
        "erros_total": len(erros),
        "erros": erros,
    }


def listar_escudos_status():
    criar_campo_escudo_equipes(force=True)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    nome,
                    login,
                    CASE
                        WHEN COALESCE(escudo_blob, '') LIKE 'data:image/%' THEN 'banco'
                        WHEN COALESCE(escudo, '') LIKE 'data:image/%' THEN 'banco_compat'
                        WHEN COALESCE(escudo, '') LIKE '/static/%' THEN 'arquivo_render'
                        WHEN COALESCE(escudo, '') = '' THEN 'sem_escudo'
                        ELSE 'outro'
                    END AS status_escudo,
                    CASE
                        WHEN LENGTH(COALESCE(escudo_blob, '')) > 0 THEN LENGTH(escudo_blob)
                        ELSE LENGTH(COALESCE(escudo, ''))
                    END AS tamanho,
                    COALESCE(escudo, '') AS escudo
                FROM equipes
                ORDER BY nome
            """)
            return cur.fetchall()


# =========================================================
# AVANÇO / CHAVEAMENTO VISUAL (SÉRIES, ORIGENS E PREENCHIMENTO)
# =========================================================
def _avanco_regra_padrao():
    return {
        "usar_regra_propria": False,
        "sets_tipo": "padrao",
        "pontos_set": "",
        "tem_tiebreak": "padrao",
        "pontos_tiebreak": "",
        "modo_operacao": "padrao",
    }


def _avanco_config_padrao():
    regra = _avanco_regra_padrao()
    return {
        "series": [
            {"id": "ouro", "nome": "Série Ouro", "ativa": True, "fases": ["quartas", "semifinal", "final"], "regra": dict(regra)},
            {"id": "prata", "nome": "Série Prata", "ativa": False, "fases": ["quartas", "semifinal", "final"], "regra": dict(regra)},
            {"id": "bronze", "nome": "Série Bronze", "ativa": False, "fases": ["semifinal", "final"], "regra": dict(regra)},
        ],
        "jogos": [],
        "versao": 2,
    }


def _normalizar_regra_avanco(valor):
    regra = _avanco_regra_padrao()
    if isinstance(valor, dict):
        regra.update(valor)
    regra["usar_regra_propria"] = bool(regra.get("usar_regra_propria"))
    regra["sets_tipo"] = str(regra.get("sets_tipo") or "padrao").strip() or "padrao"
    regra["tem_tiebreak"] = str(regra.get("tem_tiebreak") or "padrao").strip() or "padrao"
    regra["modo_operacao"] = str(regra.get("modo_operacao") or "padrao").strip() or "padrao"
    for campo in ("pontos_set", "pontos_tiebreak"):
        valor_campo = regra.get(campo)
        regra[campo] = "" if valor_campo in (None, "0", 0) else str(valor_campo).strip()
    return regra


def _normalizar_json_avanco(valor):
    if isinstance(valor, dict):
        cfg = valor
    elif isinstance(valor, str) and valor.strip():
        try:
            cfg = json.loads(valor)
        except Exception:
            cfg = {}
    else:
        cfg = {}

    padrao = _avanco_config_padrao()
    cfg.setdefault("series", padrao["series"])
    cfg.setdefault("jogos", [])
    cfg.setdefault("versao", 2)

    if not isinstance(cfg.get("series"), list):
        cfg["series"] = padrao["series"]
    if not isinstance(cfg.get("jogos"), list):
        cfg["jogos"] = []

    series_norm = []
    for idx, serie in enumerate(cfg.get("series") or []):
        if not isinstance(serie, dict):
            continue
        sid = str(serie.get("id") or serie.get("nome") or f"serie_{idx + 1}").strip().lower()
        sid = sid.replace(" ", "_") or f"serie_{idx + 1}"
        fases = serie.get("fases") if isinstance(serie.get("fases"), list) else []
        if not fases:
            fases = ["semifinal", "final"]
        series_norm.append({
            "id": sid,
            "nome": str(serie.get("nome") or sid.title()).strip(),
            "ativa": bool(serie.get("ativa")),
            "fases": [str(f or "").strip() for f in fases if str(f or "").strip()],
            "ordem": int(serie.get("ordem") or idx + 1),
            "regra": _normalizar_regra_avanco(serie.get("regra")),
        })
    cfg["series"] = series_norm or padrao["series"]

    jogos_norm = []
    for idx, jogo in enumerate(cfg.get("jogos") or []):
        if not isinstance(jogo, dict):
            continue
        jogos_norm.append({
            "id": str(jogo.get("id") or f"J{idx + 1}").strip(),
            "serie": str(jogo.get("serie") or "ouro").strip().lower(),
            "fase": str(jogo.get("fase") or "quartas").strip(),
            "ordem": int(jogo.get("ordem") or idx + 1),
            "data_hora": str(jogo.get("data_hora") or "").strip(),
            "quadra_id": str(jogo.get("quadra_id") or "").strip(),
            "quadra_nome": str(jogo.get("quadra_nome") or "").strip(),
            "ginasio": str(jogo.get("ginasio") or jogo.get("local") or "").strip(),
            "origem_a": jogo.get("origem_a") if isinstance(jogo.get("origem_a"), dict) else {},
            "origem_b": jogo.get("origem_b") if isinstance(jogo.get("origem_b"), dict) else {},
            "proximo_vencedor": str(jogo.get("proximo_vencedor") or "").strip(),
            "proximo_perdedor": str(jogo.get("proximo_perdedor") or "").strip(),
            "regra": _normalizar_regra_avanco(jogo.get("regra")),
        })
    cfg["jogos"] = jogos_norm
    return cfg


def buscar_avanco_config_competicao(nome_competicao):
    """Busca a configuração visual do chaveamento salva dentro de fases_config.avanco."""
    config = buscar_configuracao_avancada_competicao(nome_competicao) or {}
    fases_config = config.get("fases_config") or {}
    return _normalizar_json_avanco(fases_config.get("avanco"))


def avanco_ja_gerado_competicao(nome_competicao):
    """Retorna True somente depois do clique manual que materializa o avanço."""
    config = buscar_configuracao_avancada_competicao(nome_competicao) or {}
    fases_config = config.get("fases_config") or {}
    return bool(fases_config.get("avanco_gerado"))


def _salvar_flag_avanco_gerado_competicao(nome_competicao, gerado):
    config = buscar_configuracao_avancada_competicao(nome_competicao) or {}
    fases_config = config.get("fases_config") or {}
    fases_config["avanco_gerado"] = bool(gerado)
    return atualizar_configuracao_avancada_competicao(
        nome_competicao=nome_competicao,
        tipo_classificacao=config.get("tipo_classificacao") or "grupo",
        qtd_classificados=config.get("qtd_classificados") or 0,
        formato_finais=config.get("formato_finais") or "mata_mata",
        possui_bye=config.get("possui_bye") or False,
        qtd_bye=config.get("qtd_bye") or 0,
        fases_config=fases_config,
        tipo_confronto=config.get("tipo_confronto") or "grupo_interno",
        cruzamentos_grupos=config.get("cruzamentos_grupos") or "",
        data_limite_inscricao=config.get("data_limite_inscricao"),
        hora_limite_inscricao=config.get("hora_limite_inscricao"),
        bloquear_apos_inicio=config.get("bloquear_apos_inicio") or False,
    )


def salvar_avanco_config_competicao(nome_competicao, avanco_config):
    """Salva o construtor de avanço sem criar coluna nova no banco."""
    config = buscar_configuracao_avancada_competicao(nome_competicao) or {}
    fases_config = config.get("fases_config") or {}
    avanco_normalizado = _normalizar_json_avanco(avanco_config)
    fases_config["avanco"] = avanco_normalizado
    # Sempre que o desenho do avanço muda, as partidas reais deixam de ser confiáveis.
    # O sistema volta a mostrar só os placeholders até o organizador clicar em gerar.
    fases_config["avanco_gerado"] = False
    fases_config = _aplicar_regras_avanco_em_fases_config(fases_config, avanco_normalizado)

    try:
        limpar_partidas_avanco_nao_iniciadas_competicao(nome_competicao)
    except Exception as e:
        print("AVISO salvar_avanco_config/limpar_avanco:", repr(e))

    return atualizar_configuracao_avancada_competicao(
        nome_competicao=nome_competicao,
        tipo_classificacao=config.get("tipo_classificacao") or "grupo",
        qtd_classificados=config.get("qtd_classificados") or 0,
        formato_finais=config.get("formato_finais") or "mata_mata",
        possui_bye=config.get("possui_bye") or False,
        qtd_bye=config.get("qtd_bye") or 0,
        fases_config=fases_config,
        tipo_confronto=config.get("tipo_confronto") or "grupo_interno",
        cruzamentos_grupos=config.get("cruzamentos_grupos") or "",
        data_limite_inscricao=config.get("data_limite_inscricao"),
        hora_limite_inscricao=config.get("hora_limite_inscricao"),
        bloquear_apos_inicio=config.get("bloquear_apos_inicio") or False,
    )


def _rotulo_posicao(pos):
    try:
        return f"{int(pos)}º"
    except Exception:
        return f"{pos}º"


def listar_origens_avanco_competicao(nome_competicao):
    """Opções do seletor de origem, filtradas pela estrutura real da competição.

    Se a classificação é por grupo, não mostra 8º de um grupo que só tem 4 times.
    Se a classificação é geral, mostra apenas posições gerais até a quantidade real de equipes.
    Em modo misto, mostra as duas famílias de origem.
    """
    config = buscar_configuracao_avancada_competicao(nome_competicao) or {}
    tipo_classificacao = str(config.get("tipo_classificacao") or "grupo").strip().lower()
    comp = buscar_competicao_por_nome(nome_competicao) or {}
    opcoes = []

    grupos = listar_grupos(nome_competicao) or []
    total_equipes = 0

    if tipo_classificacao in {"grupo", "misto", "por_grupo"}:
        for grupo in grupos:
            nome = str(grupo.get("nome") or "").strip().upper()
            if not nome:
                continue
            equipes_grupo = listar_equipes_por_grupo(grupo.get("id")) or []
            qtd = len([e for e in equipes_grupo if (e.get("equipe") or e.get("nome") or "").strip()])
            total_equipes += qtd
            for pos in range(1, qtd + 1):
                opcoes.append({
                    "tipo": "grupo_posicao",
                    "valor": f"{pos}{nome}",
                    "label": f"{_rotulo_posicao(pos)} Grupo {nome}",
                    "grupo": nome,
                    "posicao": pos,
                })

    if not total_equipes:
        try:
            total_equipes = len(listar_equipes_da_competicao(nome_competicao) or [])
        except Exception:
            total_equipes = int(comp.get("qtd_equipes") or 0)

    if tipo_classificacao in {"geral", "misto"}:
        limite = total_equipes or int(comp.get("qtd_equipes") or 0) or 32
        for pos in range(1, limite + 1):
            opcoes.append({"tipo": "geral_posicao", "valor": str(pos), "label": f"{_rotulo_posicao(pos)} Geral", "posicao": pos})

    # Opções especiais ficam no fim, sem poluir a lista principal.
    opcoes.extend([
        {"tipo": "melhor_terceiro", "valor": "1", "label": "Melhor 3º"},
        {"tipo": "melhor_quarto", "valor": "1", "label": "Melhor 4º"},
        {"tipo": "bye", "valor": "BYE", "label": "BYE / Sem adversário"},
        {"tipo": "manual", "valor": "", "label": "Equipe manual"},
    ])
    return opcoes


def _normalizar_fase_avanco_para_partida(fase):
    fase = str(fase or "").strip().lower().replace("í", "i").replace("á", "a").replace(" ", "_").replace("-", "_")
    mapa = {
        "oitavas": "oitavas",
        "quartas": "quartas",
        "semi": "semifinal",
        "semifinais": "semifinal",
        "semifinal": "semifinal",
        "final": "final",
        "terceiro_lugar": "terceiro_lugar",
        "3_lugar": "terceiro_lugar",
    }
    return mapa.get(fase, fase or "mata_mata")


def _calcular_classificacao_simples_avanco(nome_competicao):
    """Classificação leve para resolver 1º A/2º B sem depender do HTML da tabela."""
    grupos = listar_grupos(nome_competicao) or []
    partidas = listar_partidas(nome_competicao) or []
    tabela = {}

    for g in grupos:
        nome_g = str(g.get("nome") or "").strip().upper()
        tabela[nome_g] = {}
        for ge in listar_equipes_por_grupo(g.get("id")) or []:
            equipe = str(ge.get("equipe") or ge.get("nome") or "").strip()
            if equipe:
                tabela[nome_g][equipe] = {
                    "equipe": equipe, "grupo": nome_g, "pontos": 0, "vitorias": 0,
                    "saldo_sets": 0, "saldo_pontos": 0, "sets_pro": 0, "sets_contra": 0,
                    "pontos_pro": 0, "pontos_contra": 0,
                }

    for p in partidas:
        if str(p.get("fase") or "grupos").strip().lower() != "grupos":
            continue
        status = str(p.get("status") or p.get("status_jogo") or "").strip().lower()
        if status not in {"finalizada", "finalizado", "encerrada", "encerrado"} and not p.get("data_fim"):
            continue
        grupo = str(p.get("grupo") or "").strip().upper()
        if grupo not in tabela:
            continue
        a = str(p.get("equipe_a") or "").strip()
        b = str(p.get("equipe_b") or "").strip()
        if a not in tabela[grupo] or b not in tabela[grupo]:
            continue
        sets_a = int(p.get("sets_a") or 0)
        sets_b = int(p.get("sets_b") or 0)
        pts_a = sum(int(p.get(c) or 0) for c in ("set1_a", "set2_a", "set3_a", "set4_a", "set5_a", "pontos_a"))
        pts_b = sum(int(p.get(c) or 0) for c in ("set1_b", "set2_b", "set3_b", "set4_b", "set5_b", "pontos_b"))
        if sets_a == 0 and sets_b == 0 and (pts_a or pts_b):
            sets_a = 1 if pts_a > pts_b else 0
            sets_b = 1 if pts_b > pts_a else 0
        tabela[grupo][a]["sets_pro"] += sets_a
        tabela[grupo][a]["sets_contra"] += sets_b
        tabela[grupo][a]["saldo_sets"] += sets_a - sets_b
        tabela[grupo][a]["pontos_pro"] += pts_a
        tabela[grupo][a]["pontos_contra"] += pts_b
        tabela[grupo][a]["saldo_pontos"] += pts_a - pts_b
        tabela[grupo][b]["sets_pro"] += sets_b
        tabela[grupo][b]["sets_contra"] += sets_a
        tabela[grupo][b]["saldo_sets"] += sets_b - sets_a
        tabela[grupo][b]["pontos_pro"] += pts_b
        tabela[grupo][b]["pontos_contra"] += pts_a
        tabela[grupo][b]["saldo_pontos"] += pts_b - pts_a
        if sets_a > sets_b:
            tabela[grupo][a]["vitorias"] += 1
            tabela[grupo][a]["pontos"] += 3
        elif sets_b > sets_a:
            tabela[grupo][b]["vitorias"] += 1
            tabela[grupo][b]["pontos"] += 3

    por_grupo = {}
    geral = []
    for grupo, linhas in tabela.items():
        ordenadas = sorted(linhas.values(), key=lambda x: (x["pontos"], x["vitorias"], x["saldo_sets"], x["saldo_pontos"], x["pontos_pro"]), reverse=True)
        por_grupo[grupo] = ordenadas
        geral.extend(ordenadas)
    geral = sorted(geral, key=lambda x: (x["pontos"], x["vitorias"], x["saldo_sets"], x["saldo_pontos"], x["pontos_pro"]), reverse=True)
    return por_grupo, geral



def _fase_grupos_avanco(valor):
    texto = str(valor or "grupos").strip().lower().replace("á", "a").replace("í", "i").replace(" ", "_").replace("-", "_")
    return texto in {"", "grupo", "grupos", "classificatoria", "classificatorias", "classificatoria_grupos", "fase_de_grupos"}


def _status_finalizado_avanco(valor):
    texto = str(valor or "").strip().lower().replace("-", "_")
    return texto in {"finalizada", "finalizado", "encerrada", "encerrado", "partida_encerrada"}


def _mapa_fechamento_classificatoria_avanco(nome_competicao):
    """Indica se a classificatória inteira já pode alimentar o avanço.

    Regra de segurança do mata-mata:
    - enquanto existir qualquer jogo classificatório não finalizado, nenhuma
      origem real é resolvida, nem mesmo 1º Grupo A ou 2º Grupo B;
    - o chaveamento continua mostrando apenas os rótulos configurados
      (ex.: 1º Grupo A x 4º Grupo B);
    - as equipes reais só entram depois do fechamento completo e do clique
      manual em gerar partidas do avanço.
    """
    grupos = listar_grupos(nome_competicao) or []
    partidas = listar_partidas(nome_competicao) or []

    grupos_info = {}
    for g in grupos:
        nome_g = str(g.get("nome") or "").strip().upper()
        if not nome_g:
            continue
        try:
            qtd_equipes = len([
                e for e in (listar_equipes_por_grupo(g.get("id")) or [])
                if str(e.get("equipe") or e.get("nome") or "").strip()
            ])
        except Exception:
            qtd_equipes = 0
        grupos_info[nome_g] = {"qtd_equipes": qtd_equipes, "tem_partida": False, "pendentes": 0}

    classificatorias = []
    for p in partidas:
        if not _fase_grupos_avanco(p.get("fase")):
            continue
        origem = str(p.get("origem") or "").strip()
        if origem.startswith("avanco:"):
            continue
        classificatorias.append(p)
        grupo = str(p.get("grupo") or "").strip().upper()
        if grupo:
            grupos_info.setdefault(grupo, {"qtd_equipes": 0, "tem_partida": False, "pendentes": 0})
            grupos_info[grupo]["tem_partida"] = True
            status_ok = _status_finalizado_avanco(p.get("status")) or _status_finalizado_avanco(p.get("status_jogo")) or bool(p.get("data_fim"))
            if not status_ok:
                grupos_info[grupo]["pendentes"] += 1

    grupos_fechados = {}
    for grupo, info in grupos_info.items():
        # Só consideramos fechado se existe tabela/jogos daquele grupo e não há pendências.
        grupos_fechados[grupo] = bool(info.get("tem_partida") and info.get("pendentes", 0) == 0)

    if grupos_info:
        geral_fechado = bool(grupos_fechados) and all(grupos_fechados.values())
    else:
        # Competição sem grupos: exige que todos os jogos classificatórios existentes estejam finalizados.
        geral_fechado = bool(classificatorias) and all(
            _status_finalizado_avanco(p.get("status")) or _status_finalizado_avanco(p.get("status_jogo")) or bool(p.get("data_fim"))
            for p in classificatorias
        )

    pendentes_total = sum(int(info.get("pendentes") or 0) for info in grupos_info.values())
    total_classificatorias = len(classificatorias)

    return {
        "grupos": grupos_fechados,
        "geral": geral_fechado,
        "pendentes": pendentes_total,
        "total_classificatorias": total_classificatorias,
    }


def status_avanco_classificatorias_competicao(nome_competicao):
    """Resumo usado pelas telas para liberar/bloquear o botão do avanço."""
    fechamento = _mapa_fechamento_classificatoria_avanco(nome_competicao)
    return {
        "fechada": bool(fechamento.get("geral")),
        "pendentes": int(fechamento.get("pendentes") or 0),
        "total_classificatorias": int(fechamento.get("total_classificatorias") or 0),
        "mensagem": (
            "Classificatórias finalizadas. O avanço automático já pode materializar todos os jogos configurados."
            if fechamento.get("geral")
            else "O avanço automático libera jogos por origem: grupo fechado libera 1º/2º daquele grupo; geral/melhores terceiros aguardam o fechamento geral."
        ),
    }


def _rotulo_origem_avanco(origem):
    origem = origem if isinstance(origem, dict) else {}
    tipo = str(origem.get("tipo") or "").strip()
    valor = str(origem.get("valor") or "").strip()
    label = str(origem.get("label") or "").strip()

    if label:
        return label
    if tipo == "manual":
        return valor or "Equipe manual"
    if tipo == "bye":
        return "BYE"
    if tipo == "vencedor_jogo":
        return f"Vencedor {valor}".strip()
    if tipo == "perdedor_jogo":
        return f"Perdedor {valor}".strip()
    if tipo == "grupo_posicao":
        m = re.match(r"^(\d+)([A-Za-zÀ-ÿ0-9_-]+)$", valor.replace(" ", ""))
        if m:
            return f"{_rotulo_posicao(m.group(1))} Grupo {m.group(2).upper()}"
        return valor or "A definir"
    if tipo == "geral_posicao":
        return f"{_rotulo_posicao(valor)} Geral" if valor else "A definir"
    if tipo == "melhor_terceiro":
        return "Melhor 3º"
    if tipo == "melhor_quarto":
        return "Melhor 4º"
    return valor or "A definir"


def resolver_origem_avanco_competicao_se_fechada(nome_competicao, origem, serie=None):
    """Resolve origem somente quando ela pode virar partida real.

    Retorna (ok, equipe_ou_rotulo, motivo).

    Regra central do Avanço:
    - o quadro pode estar desenhado desde o começo;
    - partida real só é criada/liberada quando a origem já fechou;
    - antes disso, devolve apenas o rótulo configurado (ex.: 1º Grupo A).
    """
    origem = origem if isinstance(origem, dict) else {}
    tipo = str(origem.get("tipo") or "").strip()
    valor = str(origem.get("valor") or "").strip()
    label = _rotulo_origem_avanco(origem)

    if tipo == "manual":
        equipe = valor or str(origem.get("label") or "").strip()
        return (bool(equipe), equipe or label, "manual_sem_equipe" if not equipe else "")

    if tipo == "bye":
        return True, "BYE", "bye"

    if tipo in {"vencedor_jogo", "perdedor_jogo"}:
        partida = _buscar_partida_avanco_por_jogo(nome_competicao, valor, serie=serie)
        if not partida:
            return False, label, "jogo_origem_nao_criado"
        if not _partida_finalizada_avanco(partida):
            return False, label, "jogo_origem_nao_finalizado"
        vencedor, perdedor = _vencedor_perdedor_partida_avanco(partida)
        equipe = vencedor if tipo == "vencedor_jogo" else perdedor
        return (bool(equipe), equipe or label, "sem_vencedor" if not equipe else "")

    fechamento = _mapa_fechamento_classificatoria_avanco(nome_competicao)

    if tipo == "grupo_posicao":
        m = re.match(r"^(\d+)([A-Za-zÀ-ÿ0-9_-]+)$", valor.replace(" ", ""))
        if not m:
            return False, label, "origem_grupo_invalida"
        pos = int(m.group(1))
        grupo = m.group(2).upper()
        grupos_fechados = fechamento.get("grupos") or {}
        # Libera a origem assim que o grupo específico estiver fechado.
        # Antes o sistema esperava a classificatória inteira, atrasando Quartas/Prata/Ouro
        # mesmo quando aquele grupo já não podia mais mudar por jogos pendentes de outro grupo.
        if not grupos_fechados.get(grupo) and not fechamento.get("geral"):
            return False, label, "grupo_classificatoria_nao_finalizado"
        por_grupo, _geral = _calcular_classificacao_simples_avanco(nome_competicao)
        linhas = por_grupo.get(grupo) or []
        if len(linhas) >= pos:
            equipe = linhas[pos - 1].get("equipe") or ""
            return (bool(equipe), equipe or label, "equipe_nao_encontrada" if not equipe else "")
        return False, label, "posicao_indisponivel"

    if tipo == "geral_posicao":
        if not fechamento.get("geral"):
            return False, label, "classificatoria_nao_finalizada"
        try:
            pos = int(valor)
        except Exception:
            return False, label, "origem_geral_invalida"
        _por_grupo, geral = _calcular_classificacao_simples_avanco(nome_competicao)
        if len(geral) >= pos:
            equipe = geral[pos - 1].get("equipe") or ""
            return (bool(equipe), equipe or label, "equipe_nao_encontrada" if not equipe else "")
        return False, label, "posicao_indisponivel"

    if tipo == "melhor_terceiro":
        if not fechamento.get("geral"):
            return False, label, "classificatoria_nao_finalizada"
        por_grupo, _geral = _calcular_classificacao_simples_avanco(nome_competicao)
        terceiros = [linhas[2] for linhas in por_grupo.values() if len(linhas) >= 3]
        terceiros = sorted(terceiros, key=lambda x: (x["pontos"], x["vitorias"], x["saldo_sets"], x["saldo_pontos"], x["pontos_pro"]), reverse=True)
        equipe = terceiros[0].get("equipe") if terceiros else ""
        return (bool(equipe), equipe or label, "equipe_nao_encontrada" if not equipe else "")

    if tipo == "melhor_quarto":
        if not fechamento.get("geral"):
            return False, label, "classificatoria_nao_finalizada"
        por_grupo, _geral = _calcular_classificacao_simples_avanco(nome_competicao)
        quartos = [linhas[3] for linhas in por_grupo.values() if len(linhas) >= 4]
        quartos = sorted(quartos, key=lambda x: (x["pontos"], x["vitorias"], x["saldo_sets"], x["saldo_pontos"], x["pontos_pro"]), reverse=True)
        equipe = quartos[0].get("equipe") if quartos else ""
        return (bool(equipe), equipe or label, "equipe_nao_encontrada" if not equipe else "")

    return False, label, "tipo_origem_nao_resolvido"

def _partida_finalizada_avanco(partida):
    if not partida:
        return False
    status = str(partida.get("status") or partida.get("status_jogo") or "").strip().lower()
    return status in {"finalizada", "finalizado", "encerrada", "encerrado"} or bool(partida.get("data_fim"))


def _vencedor_perdedor_partida_avanco(partida):
    if not partida:
        return None, None
    a = str(partida.get("equipe_a") or "").strip()
    b = str(partida.get("equipe_b") or "").strip()
    sets_a = int(partida.get("sets_a") or 0)
    sets_b = int(partida.get("sets_b") or 0)
    if sets_a == sets_b:
        pts_a = sum(int(partida.get(c) or 0) for c in ("set1_a", "set2_a", "set3_a", "set4_a", "set5_a", "pontos_a"))
        pts_b = sum(int(partida.get(c) or 0) for c in ("set1_b", "set2_b", "set3_b", "set4_b", "set5_b", "pontos_b"))
        if pts_a == pts_b:
            return None, None
        return (a, b) if pts_a > pts_b else (b, a)
    return (a, b) if sets_a > sets_b else (b, a)


def _buscar_partida_avanco_por_jogo(nome_competicao, jogo_id, serie=None):
    jogo_id = str(jogo_id or "").strip()
    if not jogo_id:
        return None
    with conectar() as conn:
        with conn.cursor() as cur:
            if serie:
                cur.execute("""
                    SELECT * FROM partidas
                    WHERE competicao = %s AND origem = %s
                    LIMIT 1
                """, (nome_competicao, f"avanco:{serie}:{jogo_id}"))
            else:
                cur.execute("""
                    SELECT * FROM partidas
                    WHERE competicao = %s AND origem LIKE %s
                    ORDER BY id DESC
                    LIMIT 1
                """, (nome_competicao, f"avanco:%:{jogo_id}"))
            return cur.fetchone()


def resolver_origem_avanco_competicao(nome_competicao, origem, serie=None):
    """Retorna o que deve aparecer no espelho do avanço.

    Antes da origem fechar, mostra o rótulo configurado (1º Grupo A, Vencedor J1).
    Depois que fecha, mostra a equipe real.
    """
    ok, valor, _motivo = resolver_origem_avanco_competicao_se_fechada(nome_competicao, origem, serie=serie)
    return valor if valor else _rotulo_origem_avanco(origem)


def _regra_efetiva_jogo_avanco(avanco, jogo):
    jogo = jogo or {}
    regra_jogo = _normalizar_regra_avanco(jogo.get("regra"))
    if regra_jogo.get("usar_regra_propria"):
        return regra_jogo
    serie_id = str(jogo.get("serie") or "").strip().lower()
    for serie in avanco.get("series") or []:
        if str(serie.get("id") or "").strip().lower() == serie_id:
            return _normalizar_regra_avanco(serie.get("regra"))
    return _avanco_regra_padrao()


def _sets_avanco_por_regra(regra, comp=None):
    regra = _normalizar_regra_avanco(regra)
    sets_tipo = regra.get("sets_tipo") if regra.get("sets_tipo") != "padrao" else (comp or {}).get("sets_tipo")
    sets_tipo = str(sets_tipo or "melhor_de_3").strip().lower()
    if sets_tipo == "set_unico":
        return 1, 1
    if sets_tipo == "melhor_de_5":
        return 5, 3
    return 3, 2


def _aplicar_regras_avanco_em_fases_config(fases_config, avanco):
    """Guarda regras por série/jogo em fases_config para uso atual e futuro.

    A regra padrão da competição continua valendo. Apenas séries ou jogos marcados
    com regra própria entram aqui.
    """
    regras_avancadas = fases_config.get("regras_avancadas") or {}
    regras_avancadas.setdefault("series", {})
    regras_avancadas.setdefault("jogos", {})

    for serie in avanco.get("series") or []:
        regra = _normalizar_regra_avanco(serie.get("regra"))
        sid = str(serie.get("id") or "").strip().lower()
        if sid and regra.get("usar_regra_propria"):
            regras_avancadas["series"][sid] = regra

    for jogo in avanco.get("jogos") or []:
        regra = _normalizar_regra_avanco(jogo.get("regra"))
        if regra.get("usar_regra_propria"):
            chave = f"{jogo.get('serie') or 'ouro'}:{jogo.get('id') or ''}"
            regras_avancadas["jogos"][chave] = regra

    fases_config["regras_avancadas"] = regras_avancadas
    return fases_config


def _limpar_partidas_avanco_prematuras_cur(cur, nome_competicao, origem_tag=None):
    """Remove jogos do Avanço criados antes da origem fechar.

    Só apaga partidas que ainda não começaram. Isso corrige restos de versões
    anteriores e evita duplicação visual na tabela/apontador.
    """
    params = [nome_competicao]
    filtro_origem = "AND origem LIKE 'avanco:%%'"
    if origem_tag:
        filtro_origem = "AND origem = %s"
        params.append(origem_tag)

    cur.execute(f"""
        SELECT id, origem, status, status_jogo, pontos_a, pontos_b, sets_a, sets_b,
               pre_jogo_iniciado_em, pre_jogo_finalizado, data_fim
        FROM partidas
        WHERE competicao = %s
          {filtro_origem}
        ORDER BY origem, id
    """, tuple(params))

    removidas = 0
    for partida in cur.fetchall() or []:
        if not partida_ja_iniciou_ou_finalizou(partida):
            cur.execute("DELETE FROM partidas WHERE id = %s", (partida["id"],))
            removidas += 1
    return removidas


def limpar_partidas_avanco_nao_iniciadas_competicao(nome_competicao):
    """Remove partidas reais do avanço ainda não iniciadas.

    Usado quando o avanço ainda não foi gerado manualmente ou quando o desenho
    do chaveamento foi alterado. Não toca em jogo já iniciado/finalizado.
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            removidas = _limpar_partidas_avanco_prematuras_cur(cur, nome_competicao)
        conn.commit()
    return removidas


def _remover_duplicadas_avanco_cur(cur, nome_competicao):
    cur.execute("""
        SELECT id, origem, status, status_jogo, pontos_a, pontos_b, sets_a, sets_b,
               pre_jogo_iniciado_em, pre_jogo_finalizado, data_fim
        FROM partidas
        WHERE competicao = %s
          AND origem LIKE 'avanco:%%'
        ORDER BY origem, id
    """, (nome_competicao,))
    por_origem = {}
    for p in cur.fetchall() or []:
        por_origem.setdefault(p.get("origem"), []).append(p)

    removidas = 0
    for _origem, itens in por_origem.items():
        if len(itens) <= 1:
            continue
        principal = None
        for p in itens:
            if partida_ja_iniciou_ou_finalizou(p):
                principal = p
                break
        if principal is None:
            principal = itens[0]
        for p in itens:
            if p["id"] == principal["id"]:
                continue
            if not partida_ja_iniciou_ou_finalizou(p):
                cur.execute("DELETE FROM partidas WHERE id = %s", (p["id"],))
                removidas += 1
    return removidas


def _resolver_agenda_jogo_avanco(nome_competicao, jogo):
    """Resolve data/hora e quadra do quadro de avanço para a partida real.

    O avanço guarda agenda no desenho do jogo. Quando a origem fecha, a partida
    nasce já com data_hora, quadra_id e quadra_nome, sem precisar reagendar.
    """
    jogo = jogo if isinstance(jogo, dict) else {}
    data_hora = str(jogo.get("data_hora") or "").strip() or None

    quadra_id = None
    quadra_nome = str(jogo.get("quadra_nome") or "").strip()
    ginasio = str(jogo.get("ginasio") or jogo.get("local") or "").strip()

    try:
        if str(jogo.get("quadra_id") or "").strip():
            quadra_id = int(jogo.get("quadra_id"))
    except Exception:
        quadra_id = None

    try:
        q = None
        if quadra_id:
            q = buscar_quadra_competicao_por_id(nome_competicao, quadra_id)
        elif quadra_nome:
            q = buscar_quadra_competicao_por_texto(nome_competicao, quadra_nome)

        if q:
            quadra_id = int(q.get("id") or quadra_id or 0) or None
            quadra_nome = formatar_quadra_exibicao(q)
            if not ginasio:
                ginasio = str(q.get("local") or "").strip()
    except Exception as e:
        print("AVISO resolver_agenda_jogo_avanco:", repr(e))

    # A coluna quadra antiga recebe o id quando houver, senão o texto da quadra.
    quadra = str(quadra_id) if quadra_id else (quadra_nome or "")

    return {
        "data_hora": data_hora,
        "quadra": quadra,
        "quadra_id": quadra_id,
        "quadra_nome": quadra_nome,
        "ginasio": ginasio,
    }


def gerar_partidas_avanco_competicao(nome_competicao):
    """Cria/atualiza somente partidas reais do Avanço com origens resolvidas.

    A configuração do chaveamento pode existir desde o início, mas partida real
    só nasce quando as duas origens já estão fechadas. Isso evita quartas/semi
    aparecerem no apontador antes da hora e remove duplicações prematuras.
    """
    avanco = buscar_avanco_config_competicao(nome_competicao)
    jogos = avanco.get("jogos") or []
    criadas = 0
    atualizadas = 0
    aguardando = 0
    removidas_prematuras = 0
    duplicadas_removidas = 0
    fechamento = _mapa_fechamento_classificatoria_avanco(nome_competicao)
    classificatoria_fechada = bool(fechamento.get("geral"))

    with conectar() as conn:
        with conn.cursor() as cur:
            colunas = _buscar_colunas_tabela("partidas")
            comp_regra = buscar_competicao_por_nome(nome_competicao) or {}
            duplicadas_removidas += _remover_duplicadas_avanco_cur(cur, nome_competicao)

            for idx, jogo in enumerate(jogos, start=1):
                if not isinstance(jogo, dict):
                    continue

                jid = str(jogo.get("id") or f"J{idx}").strip()
                if not jid:
                    continue

                serie = str(jogo.get("serie") or "ouro").strip().lower()
                fase = _normalizar_fase_avanco_para_partida(jogo.get("fase"))
                origem_tag = f"avanco:{serie}:{jid}"

                cur.execute("""
                    SELECT id, origem, status, status_jogo, pontos_a, pontos_b, sets_a, sets_b,
                           pre_jogo_iniciado_em, pre_jogo_finalizado, data_fim
                    FROM partidas
                    WHERE competicao = %s AND origem = %s
                    ORDER BY id
                """, (nome_competicao, origem_tag))
                existentes = list(cur.fetchall() or [])

                # Não bloqueia mais o avanço inteiro por causa de pendências em outro grupo.
                # Cada origem decide se já pode ser resolvida:
                # - Grupo posição: libera quando o grupo dela fechou;
                # - Geral/melhor 3º/4º: libera quando a classificatória geral fechou;
                # - Vencedor/Perdedor de jogo: libera quando o jogo anterior finalizou.
                ok_a, equipe_a, motivo_a = resolver_origem_avanco_competicao_se_fechada(
                    nome_competicao, jogo.get("origem_a") or {}, serie=serie
                )
                ok_b, equipe_b, motivo_b = resolver_origem_avanco_competicao_se_fechada(
                    nome_competicao, jogo.get("origem_b") or {}, serie=serie
                )

                # BYE é estrutura de avanço, não jogo jogável no apontador.
                tem_bye = str(equipe_a or "").strip().upper() == "BYE" or str(equipe_b or "").strip().upper() == "BYE"
                pronto_para_jogo = bool(ok_a and ok_b and equipe_a and equipe_b and not tem_bye)

                if not pronto_para_jogo:
                    aguardando += 1
                    for existente in existentes:
                        if not partida_ja_iniciou_ou_finalizou(existente):
                            cur.execute("DELETE FROM partidas WHERE id = %s", (existente["id"],))
                            removidas_prematuras += 1
                    continue

                # Mantém no máximo uma partida real por origem. Se existirem várias,
                # preserva a iniciada/finalizada se houver; senão preserva a mais antiga.
                existente_principal = None
                for partida in existentes:
                    if partida_ja_iniciou_ou_finalizou(partida):
                        existente_principal = partida
                        break
                if existente_principal is None and existentes:
                    existente_principal = existentes[0]

                for partida in existentes:
                    if existente_principal and partida["id"] == existente_principal["id"]:
                        continue
                    if not partida_ja_iniciou_ou_finalizou(partida):
                        cur.execute("DELETE FROM partidas WHERE id = %s", (partida["id"],))
                        duplicadas_removidas += 1

                regra_efetiva = _regra_efetiva_jogo_avanco(avanco, jogo)
                sets_max_avanco, sets_para_vencer_avanco = _sets_avanco_por_regra(regra_efetiva, comp_regra)

                # Regra operacional que fica gravada na própria partida.
                # Sem estes campos, jogos do Avanço criados/atualizados caíam no
                # padrão do banco/apontador e finais configuradas como Melhor de 5
                # podiam nascer como Melhor de 3 ou Simples.
                sets_tipo_avanco = str(
                    regra_efetiva.get("sets_tipo")
                    if regra_efetiva.get("sets_tipo") != "padrao"
                    else (comp_regra.get("sets_tipo") or "melhor_de_3")
                ).strip().lower() or "melhor_de_3"
                if sets_tipo_avanco not in {"set_unico", "melhor_de_3", "melhor_de_5"}:
                    sets_tipo_avanco = "melhor_de_3"

                def _int_regra_avanco(valor, padrao):
                    try:
                        if valor in (None, "", "padrao"):
                            return int(padrao)
                        numero = int(valor)
                        return numero if numero > 0 else int(padrao)
                    except Exception:
                        return int(padrao)

                pontos_set_avanco = _int_regra_avanco(
                    regra_efetiva.get("pontos_set"),
                    comp_regra.get("pontos_set") or 25,
                )
                pontos_tiebreak_avanco = _int_regra_avanco(
                    regra_efetiva.get("pontos_tiebreak"),
                    comp_regra.get("pontos_tiebreak") or 15,
                )
                diferenca_minima_avanco = _int_regra_avanco(
                    regra_efetiva.get("diferenca_minima"),
                    comp_regra.get("diferenca_minima") or 2,
                )
                modo_operacao_avanco = str(
                    regra_efetiva.get("modo_operacao")
                    if regra_efetiva.get("modo_operacao") != "padrao"
                    else (comp_regra.get("modo_operacao") or "simples")
                ).strip().lower() or "simples"
                if modo_operacao_avanco not in {"simples", "avancado"}:
                    modo_operacao_avanco = "simples"

                agenda_avanco = _resolver_agenda_jogo_avanco(nome_competicao, jogo)

                if existente_principal:
                    if partida_ja_iniciou_ou_finalizou(existente_principal):
                        continue

                    update_campos = [
                        "fase = %s",
                        "grupo = NULL",
                        "equipe_a = %s",
                        "equipe_b = %s",
                        "ordem = %s",
                        "status = COALESCE(NULLIF(status, ''), 'aguardando')",
                    ]
                    update_valores = [fase, equipe_a, equipe_b, idx]
                    if "data_hora" in colunas:
                        update_campos.append("data_hora = %s")
                        update_valores.append(agenda_avanco.get("data_hora"))
                    if "quadra" in colunas:
                        update_campos.append("quadra = %s")
                        update_valores.append(agenda_avanco.get("quadra") or "")
                    if "quadra_id" in colunas:
                        update_campos.append("quadra_id = %s")
                        update_valores.append(agenda_avanco.get("quadra_id"))
                    if "quadra_nome" in colunas:
                        update_campos.append("quadra_nome = %s")
                        update_valores.append(agenda_avanco.get("quadra_nome") or "")
                    update_valores.append(existente_principal["id"])
                    cur.execute(
                        f"UPDATE partidas SET {', '.join(update_campos)} WHERE id = %s",
                        tuple(update_valores),
                    )

                    sets_update = []
                    sets_valores = []
                    if "sets_max" in colunas:
                        sets_update.append("sets_max = %s")
                        sets_valores.append(sets_max_avanco)
                    if "sets_para_vencer" in colunas:
                        sets_update.append("sets_para_vencer = %s")
                        sets_valores.append(sets_para_vencer_avanco)
                    if "sets_tipo" in colunas:
                        sets_update.append("sets_tipo = %s")
                        sets_valores.append(sets_tipo_avanco)
                    if "pontos_set" in colunas:
                        sets_update.append("pontos_set = %s")
                        sets_valores.append(pontos_set_avanco)
                    if "pontos_tiebreak" in colunas:
                        sets_update.append("pontos_tiebreak = %s")
                        sets_valores.append(pontos_tiebreak_avanco)
                    if "diferenca_minima" in colunas:
                        sets_update.append("diferenca_minima = %s")
                        sets_valores.append(diferenca_minima_avanco)
                    if "modo_operacao" in colunas:
                        sets_update.append("modo_operacao = %s")
                        sets_valores.append(modo_operacao_avanco)
                    if sets_update:
                        sets_valores.append(existente_principal["id"])
                        cur.execute(f"UPDATE partidas SET {', '.join(sets_update)} WHERE id = %s", tuple(sets_valores))
                    atualizadas += 1
                else:
                    campos = ["competicao", "grupo", "equipe_a", "equipe_b", "fase", "ordem", "origem", "status"]
                    valores = [nome_competicao, None, equipe_a, equipe_b, fase, idx, origem_tag, "aguardando"]
                    if "data_hora" in colunas:
                        campos.append("data_hora")
                        valores.append(agenda_avanco.get("data_hora"))
                    if "quadra" in colunas:
                        campos.append("quadra")
                        valores.append(agenda_avanco.get("quadra") or "")
                    if "quadra_id" in colunas:
                        campos.append("quadra_id")
                        valores.append(agenda_avanco.get("quadra_id"))
                    if "quadra_nome" in colunas:
                        campos.append("quadra_nome")
                        valores.append(agenda_avanco.get("quadra_nome") or "")
                    if "status_jogo" in colunas:
                        campos.append("status_jogo")
                        valores.append("aguardando")
                    if "fase_partida" in colunas:
                        campos.append("fase_partida")
                        valores.append("aguardando")
                    if "sets_max" in colunas:
                        campos.append("sets_max")
                        valores.append(sets_max_avanco)
                    if "sets_para_vencer" in colunas:
                        campos.append("sets_para_vencer")
                        valores.append(sets_para_vencer_avanco)
                    if "sets_tipo" in colunas:
                        campos.append("sets_tipo")
                        valores.append(sets_tipo_avanco)
                    if "pontos_set" in colunas:
                        campos.append("pontos_set")
                        valores.append(pontos_set_avanco)
                    if "pontos_tiebreak" in colunas:
                        campos.append("pontos_tiebreak")
                        valores.append(pontos_tiebreak_avanco)
                    if "diferenca_minima" in colunas:
                        campos.append("diferenca_minima")
                        valores.append(diferenca_minima_avanco)
                    if "modo_operacao" in colunas:
                        campos.append("modo_operacao")
                        valores.append(modo_operacao_avanco)
                    placeholders = ", ".join(["%s"] * len(valores))
                    cur.execute(f"INSERT INTO partidas ({', '.join(campos)}) VALUES ({placeholders})", tuple(valores))
                    criadas += 1
        conn.commit()

    try:
        # Marca como gerado quando a rotina automática/manual já materializou algo
        # ou quando a classificatória geral fechou. Não precisa mais depender só do botão.
        _salvar_flag_avanco_gerado_competicao(
            nome_competicao,
            bool(classificatoria_fechada or criadas > 0 or atualizadas > 0)
        )
    except Exception as e:
        print("AVISO gerar_avanco/salvar_flag:", repr(e))

    return {
        "ok": True,
        "criadas": criadas,
        "atualizadas": atualizadas,
        "aguardando": aguardando,
        "removidas_prematuras": removidas_prematuras,
        "duplicadas_removidas": duplicadas_removidas,
        "bloqueada": bool((not classificatoria_fechada) and criadas == 0 and atualizadas == 0),
        "parcial": bool((not classificatoria_fechada) and (criadas > 0 or atualizadas > 0)),
        "pendentes_classificatoria": int(fechamento.get("pendentes") or 0),
        "total_classificatorias": int(fechamento.get("total_classificatorias") or 0),
    }



def sincronizar_avanco_automatico_competicao(nome_competicao):
    """Sincroniza o avanço sem depender do botão Gerar mata-mata.

    Deve ser chamada depois que qualquer partida é finalizada.
    A função é propositalmente segura:
    - só cria partida real quando as duas origens já estão resolvidas;
    - não apaga nem altera jogo iniciado/finalizado;
    - reaproveita a mesma rotina do botão manual para evitar lógica duplicada.
    """
    try:
        avanco = buscar_avanco_config_competicao(nome_competicao) or {}
        if not (avanco.get("jogos") or []):
            return {"ok": True, "ignorado": True, "motivo": "sem_configuracao_avanco"}
        resultado = gerar_partidas_avanco_competicao(nome_competicao) or {}
        resultado["automatico"] = True
        return resultado
    except Exception as e:
        print("AVISO sincronizar_avanco_automatico_competicao:", repr(e), flush=True)
        return {"ok": False, "erro": str(e), "automatico": True}


# =========================================================
# NUMERAÇÃO DE ATLETAS
# =========================================================
def atualizar_numero_atleta(atleta_id, numero):
    """Atualiza a numeração do atleta e sempre retorna (ok, mensagem).

    Havia uma definição duplicada desta função no fim do arquivo retornando
    apenas True. A rota de conferência espera desempacotar (ok, msg), então
    o retorno booleano causava:
        TypeError: cannot unpack non-iterable bool object
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, equipe, competicao, status
                FROM atletas
                WHERE id = %s
                LIMIT 1
            """, (atleta_id,))
            atleta = cur.fetchone()

            if not atleta or atleta.get("status") != "aprovado":
                return False, "Somente atletas aprovados podem receber numeração."

            if numero not in (None, ""):
                try:
                    numero = int(numero)
                except (TypeError, ValueError):
                    return False, "Número inválido."

                if numero < 1 or numero > 99:
                    return False, "O número do atleta precisa ser entre 1 e 99."

                if not numero_atleta_disponivel(
                    numero,
                    atleta["equipe"],
                    atleta["competicao"],
                    id_atleta=atleta_id,
                ):
                    return False, "Já existe outro atleta com essa numeração nesta equipe."
            else:
                numero = None

            cur.execute("""
                UPDATE atletas
                SET numero = %s
                WHERE id = %s
            """, (numero, atleta_id))

        conn.commit()

    return True, "Numeração atualizada com sucesso."


# =========================================================
# CACHE LEVE EM MEMÓRIA
# =========================================================
from time import time

_CACHE_EQUIPES_COMPETICAO = {}
_CACHE_ATLETAS_EQUIPE = {}

_CACHE_TTL = 15


def _cache_get(cache, chave):
    item = cache.get(chave)

    if not item:
        return None

    if (time() - item["time"]) > _CACHE_TTL:
        cache.pop(chave, None)
        return None

    return item["data"]


def _cache_set(cache, chave, data):
    cache[chave] = {
        "time": time(),
        "data": data
    }


def limpar_cache_equipes():
    _CACHE_EQUIPES_COMPETICAO.clear()
    _CACHE_ATLETAS_EQUIPE.clear()


# =========================================================
# INDICES DE PERFORMANCE
# =========================================================
def criar_indices_performance():

    try:
        with conectar() as conn:
            with conn.cursor() as cur:

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_atletas_competicao_fast
                    ON atletas(competicao)
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_atletas_equipe_fast
                    ON atletas(equipe)
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_equipes_competicao_fast
                    ON equipes(competicao)
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_partidas_competicao_fast
                    ON partidas(competicao)
                """)

            conn.commit()

    except Exception as e:
        print("ERRO INDICES:", repr(e))


# =========================================================
# SUPERADMIN MASTER / CLIENTES (MULTIEMPRESA - ETAPA 1)
# =========================================================
MASTER_SUPERADMIN_LOGIN = "ThalisADM"


def garantir_schema_multiempresa_superadmin():
    """Cria a base para separar SuperADM master e SuperADMs de clientes.

    Mantém compatibilidade: o perfil continua sendo 'superadmin'.
    A hierarquia fica nas colunas superadmin_nivel/cliente_id.
    """
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS clientes_sistema (
                        id SERIAL PRIMARY KEY,
                        nome TEXT NOT NULL,
                        slug TEXT UNIQUE NOT NULL,
                        ativo BOOLEAN DEFAULT TRUE,
                        criado_por TEXT,
                        criado_em TIMESTAMP DEFAULT NOW()
                    )
                """)

                cur.execute("""
                    ALTER TABLE usuarios
                    ADD COLUMN IF NOT EXISTS cliente_id INTEGER,
                    ADD COLUMN IF NOT EXISTS superadmin_nivel TEXT DEFAULT 'cliente',
                    ADD COLUMN IF NOT EXISTS criado_por TEXT
                """)

                cur.execute("""
                    ALTER TABLE competicoes
                    ADD COLUMN IF NOT EXISTS cliente_id INTEGER
                """)

                # Tabelas de cadastro global podem não existir em bancos antigos.
                for tabela in ["equipes", "atletas", "apontadores_acesso", "oficiais"]:
                    try:
                        cur.execute(f"ALTER TABLE {tabela} ADD COLUMN IF NOT EXISTS cliente_id INTEGER")
                    except Exception as e:
                        print(f"AVISO multiempresa/{tabela}:", repr(e), flush=True)

                # O ThalisADM vira o master absoluto. Mantém perfil superadmin.
                cur.execute("""
                    UPDATE usuarios
                    SET perfil = 'superadmin',
                        superadmin_nivel = 'master',
                        cliente_id = NULL,
                        ativo = TRUE
                    WHERE LOWER(login) = LOWER(%s)
                """, (MASTER_SUPERADMIN_LOGIN,))

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_usuarios_cliente_id ON usuarios (cliente_id)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_competicoes_cliente_id ON competicoes (cliente_id)
                """)

            conn.commit()
        try:
            _CACHE_COLUNAS.pop("usuarios", None)
            _CACHE_COLUNAS.pop("competicoes", None)
            _CACHE_COLUNAS.pop("equipes", None)
            _CACHE_COLUNAS.pop("atletas", None)
        except Exception:
            pass
        return True
    except Exception as e:
        print("ERRO garantir_schema_multiempresa_superadmin:", repr(e), flush=True)
        return False


def _slug_cliente(nome):
    base = _normalizar_texto_base(nome or "cliente")
    return base or "cliente"


def _gerar_slug_cliente_unico(cur, nome):
    base = _slug_cliente(nome)
    slug = base
    contador = 1
    while True:
        cur.execute("SELECT id FROM clientes_sistema WHERE slug = %s LIMIT 1", (slug,))
        if not cur.fetchone():
            return slug
        contador += 1
        slug = f"{base}_{contador}"


def obter_contexto_superadmin(login):
    garantir_schema_multiempresa_superadmin()
    login = (login or "").strip()
    if not login:
        return {"eh_master": False, "cliente_id": None, "nivel": "cliente"}

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login, perfil, ativo, cliente_id,
                       COALESCE(superadmin_nivel, 'cliente') AS superadmin_nivel
                FROM usuarios
                WHERE LOWER(login) = LOWER(%s)
                LIMIT 1
            """, (login,))
            usuario = cur.fetchone() or {}

    nivel = (usuario.get("superadmin_nivel") or "cliente").strip().lower()
    eh_master = (login.lower() == MASTER_SUPERADMIN_LOGIN.lower()) or nivel == "master"
    return {
        "login": usuario.get("login") or login,
        "perfil": usuario.get("perfil"),
        "ativo": usuario.get("ativo"),
        "cliente_id": usuario.get("cliente_id"),
        "nivel": "master" if eh_master else "cliente",
        "eh_master": eh_master,
    }


def superadmin_eh_master(login):
    return bool(obter_contexto_superadmin(login).get("eh_master"))


def listar_superadmins_clientes(login_master=None):
    garantir_schema_multiempresa_superadmin()
    if login_master and not superadmin_eh_master(login_master):
        return []

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    u.login,
                    u.nome,
                    u.senha,
                    u.ativo,
                    u.cliente_id,
                    COALESCE(u.superadmin_nivel, 'cliente') AS superadmin_nivel,
                    COALESCE(c.nome, '') AS cliente_nome,
                    COALESCE(c.ativo, TRUE) AS cliente_ativo,
                    c.criado_em
                FROM usuarios u
                LEFT JOIN clientes_sistema c ON c.id = u.cliente_id
                WHERE u.perfil = 'superadmin'
                  AND COALESCE(u.superadmin_nivel, 'cliente') <> 'master'
                ORDER BY COALESCE(c.nome, u.nome), u.nome
            """)
            return cur.fetchall()


def criar_superadmin_cliente(criador_login, nome_cliente, nome_admin, login_admin=None):
    garantir_schema_multiempresa_superadmin()
    if not superadmin_eh_master(criador_login):
        return {"ok": False, "erro": "Apenas o ThalisADM master pode criar SuperADMs de clientes."}

    nome_cliente = (nome_cliente or "").strip()
    nome_admin = (nome_admin or "").strip()
    login_admin = (login_admin or "").strip()

    if not nome_cliente or not nome_admin:
        return {"ok": False, "erro": "Informe o nome do cliente e o nome do SuperADM."}

    if not login_admin:
        login_admin = f"adm_{_normalizar_texto_base(nome_cliente)}"
    login_admin = re.sub(r"\s+", "_", login_admin)
    login_admin = re.sub(r"[^A-Za-z0-9_.@-]", "", login_admin)[:80]
    if not login_admin:
        login_admin = f"adm_{_normalizar_texto_base(nome_admin)}"

    senha = _gerar_senha_aleatoria(8)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT login FROM usuarios WHERE LOWER(login) = LOWER(%s) LIMIT 1", (login_admin,))
            if cur.fetchone():
                return {"ok": False, "erro": "Este login já está em uso."}

            slug = _gerar_slug_cliente_unico(cur, nome_cliente)
            cur.execute("""
                INSERT INTO clientes_sistema (nome, slug, ativo, criado_por)
                VALUES (%s, %s, TRUE, %s)
                RETURNING id
            """, (nome_cliente, slug, criador_login))
            cliente_id = (cur.fetchone() or {}).get("id")

            cur.execute("""
                INSERT INTO usuarios (
                    login, nome, senha, perfil, ativo, equipe, competicao_vinculada,
                    cliente_id, superadmin_nivel, criado_por
                )
                VALUES (%s, %s, %s, 'superadmin', TRUE, NULL, NULL, %s, 'cliente', %s)
            """, (login_admin, nome_admin, senha, cliente_id, criador_login))
        conn.commit()

    return {
        "ok": True,
        "cliente_id": cliente_id,
        "cliente_nome": nome_cliente,
        "login": login_admin,
        "senha": senha,
        "nome": nome_admin,
    }


def excluir_superadmin_cliente(criador_login, login_alvo):
    garantir_schema_multiempresa_superadmin()
    if not superadmin_eh_master(criador_login):
        return {"ok": False, "erro": "Apenas o ThalisADM master pode excluir SuperADMs de clientes."}

    login_alvo = (login_alvo or "").strip()
    if not login_alvo or login_alvo.lower() == MASTER_SUPERADMIN_LOGIN.lower():
        return {"ok": False, "erro": "Não é permitido excluir o SuperADM master."}

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login, cliente_id, COALESCE(superadmin_nivel, 'cliente') AS nivel
                FROM usuarios
                WHERE LOWER(login) = LOWER(%s)
                  AND perfil = 'superadmin'
                LIMIT 1
            """, (login_alvo,))
            alvo = cur.fetchone()
            if not alvo:
                return {"ok": False, "erro": "SuperADM não encontrado."}
            if (alvo.get("nivel") or "").lower() == "master":
                return {"ok": False, "erro": "Não é permitido excluir um SuperADM master."}

            cliente_id = alvo.get("cliente_id")
            cur.execute("DELETE FROM usuarios WHERE login = %s AND perfil = 'superadmin'", (alvo.get("login"),))
            if cliente_id:
                cur.execute("UPDATE clientes_sistema SET ativo = FALSE WHERE id = %s", (cliente_id,))
        conn.commit()

    return {"ok": True}


def listar_competicoes(login_superadmin=None):
    sincronizar_status_competicoes()
    garantir_schema_multiempresa_superadmin()

    campos = _campos_competicao(prefixo="c", incluir_senha_organizador=True)
    where = ""
    params = []

    if login_superadmin:
        contexto = obter_contexto_superadmin(login_superadmin)
        if not contexto.get("eh_master"):
            where = "WHERE c.cliente_id = %s"
            params.append(contexto.get("cliente_id"))

    sql = f"""
        SELECT {", ".join(campos)}
        FROM competicoes c
        LEFT JOIN usuarios u
            ON u.login = c.organizador_login
        {where}
        ORDER BY
            CASE
                WHEN LOWER(COALESCE(c.status, '')) IN ('em andamento', 'em_andamento', 'andamento') THEN 1
                WHEN LOWER(COALESCE(c.status, '')) IN ('em preparação', 'em preparacao', 'preparação', 'preparacao') THEN 2
                WHEN LOWER(COALESCE(c.status, '')) IN ('finalizada', 'finalizado', 'encerrada', 'encerrado') THEN 3
                ELSE 4
            END,
            c.nome
    """

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall()


def contar_competicoes(login_superadmin=None):
    garantir_schema_multiempresa_superadmin()
    where = ""
    params = []
    if login_superadmin:
        contexto = obter_contexto_superadmin(login_superadmin)
        if not contexto.get("eh_master"):
            where = "WHERE cliente_id = %s"
            params.append(contexto.get("cliente_id"))
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM competicoes {where}", tuple(params))
            row = cur.fetchone()
            return row["total"] if row else 0


def contar_equipes(login_superadmin=None):
    garantir_schema_multiempresa_superadmin()
    where = ""
    params = []
    if login_superadmin:
        contexto = obter_contexto_superadmin(login_superadmin)
        if not contexto.get("eh_master"):
            where = "WHERE cliente_id = %s"
            params.append(contexto.get("cliente_id"))
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM equipes {where}", tuple(params))
            row = cur.fetchone()
            return row["total"] if row else 0


def contar_partidas(login_superadmin=None):
    try:
        garantir_schema_multiempresa_superadmin()
        join = ""
        where = ""
        params = []
        if login_superadmin:
            contexto = obter_contexto_superadmin(login_superadmin)
            if not contexto.get("eh_master"):
                join = "LEFT JOIN competicoes c ON c.nome = p.competicao"
                where = "WHERE c.cliente_id = %s"
                params.append(contexto.get("cliente_id"))
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) AS total FROM partidas p {join} {where}", tuple(params))
                row = cur.fetchone()
                return row["total"] if row else 0
    except Exception:
        return 0


# Substitui a versão antiga para vincular a competição ao cliente do SuperADM criador.
def criar_competicao_com_organizador(nome, data, status="Em preparação", modo_operacao="simples", tempos_por_set=2, substituicoes_por_set=6, criador_login=None, data_inicio=None, data_fim=None):
    garantir_schema_multiempresa_superadmin()
    login_organizador = _gerar_login_unico(_normalizar_login_organizador(nome))
    senha_organizador = _gerar_senha_aleatoria(8)

    contexto = obter_contexto_superadmin(criador_login) if criador_login else {"cliente_id": None, "eh_master": True}
    cliente_id = contexto.get("cliente_id")

    # Se o master criar uma competição diretamente, ela fica sem cliente_id e só o master enxerga.
    colunas = _buscar_colunas_tabela("competicoes")
    colunas_usuarios = _buscar_colunas_tabela("usuarios")

    data_base = data or data_inicio or datetime.now().strftime("%Y-%m-%d")

    with conectar() as conn:
        with conn.cursor() as cur:
            campos_usuario = ["login", "nome", "senha", "perfil", "ativo", "equipe", "competicao_vinculada"]
            valores_usuario = [login_organizador, f"Organizador - {nome}", senha_organizador, "organizador", True, None, nome]
            if "cliente_id" in colunas_usuarios:
                campos_usuario.append("cliente_id")
                valores_usuario.append(cliente_id)
            if "criado_por" in colunas_usuarios:
                campos_usuario.append("criado_por")
                valores_usuario.append(criador_login)

            cur.execute(
                f"INSERT INTO usuarios ({', '.join(campos_usuario)}) VALUES ({', '.join(['%s'] * len(valores_usuario))})",
                tuple(valores_usuario),
            )

            campos = ["nome", "data", "status", "organizador_login"]
            valores = [nome, data_base, status or "Em preparação", login_organizador]

            mapa_defaults = {
                "cliente_id": cliente_id,
                "data_inicio": data_inicio or data_base,
                "data_fim": data_fim or data_inicio or data_base,
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
                "exigir_foto_atleta": False,
                "exigir_instagram_atleta": False,
                "aprovacao_automatica_atletas": False,
                "travada": False,
                "motivo_travamento": "",
                "travada_em": None,
            }

            for campo, default in mapa_defaults.items():
                if campo in colunas:
                    campos.append(campo)
                    valores.append(default)

            cur.execute(
                f"INSERT INTO competicoes ({', '.join(campos)}) VALUES ({', '.join(['%s'] * len(valores))})",
                tuple(valores),
            )

        conn.commit()

    try:
        garantir_quadras_competicao(nome, 1)
    except Exception as e:
        print("AVISO: não foi possível criar quadra padrão da competição:", e)

    return {"login": login_organizador, "senha": senha_organizador}


# =========================================================
# MULTIEMPRESA - ISOLAMENTO TOTAL POR CLIENTE/SUPERADM
# Patch: cada SuperADM cliente possui cliente_id próprio. O ThalisADM
# master também possui seu próprio ambiente (cliente_id), e o painel normal
# filtra por esse ambiente. Dados de clientes não são compartilhados.
# =========================================================
MASTER_SUPERADMIN_LOGIN = "ThalisADM"


def _limpar_cache_colunas_multiempresa():
    try:
        for _t in [
            "usuarios", "competicoes", "equipes", "atletas", "atletas_globais",
            "apontadores_acesso", "oficiais", "competicao_oficiais", "equipes_competicoes",
            "partidas", "grupos", "grupos_equipes", "competicao_quadras", "classificacao_cache",
            "competicao_agenda_config", "competicao_avanco_config", "eventos_partida",
            "pins_operacionais", "jogos_avulsos"
        ]:
            _CACHE_COLUNAS.pop(_t, None)
    except Exception:
        pass


def _tabela_existe_cur(cur, tabela):
    cur.execute("SELECT to_regclass(%s) AS tabela", (f"public.{tabela}",))
    return bool((cur.fetchone() or {}).get("tabela"))


def _colunas_cur(cur, tabela):
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
    """, (tabela,))
    return {r["column_name"] for r in (cur.fetchall() or [])}


def _garantir_coluna_cliente_cur(cur, tabela):
    if not _tabela_existe_cur(cur, tabela):
        return False
    try:
        cur.execute(f"ALTER TABLE {tabela} ADD COLUMN IF NOT EXISTS cliente_id INTEGER")
        return True
    except Exception as e:
        print(f"AVISO multiempresa: não foi possível adicionar cliente_id em {tabela}:", repr(e), flush=True)
        return False


def _obter_ou_criar_cliente_thalis_cur(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clientes_sistema (
            id SERIAL PRIMARY KEY,
            nome TEXT NOT NULL,
            slug TEXT UNIQUE NOT NULL,
            ativo BOOLEAN DEFAULT TRUE,
            criado_por TEXT,
            criado_em TIMESTAMP DEFAULT NOW()
        )
    """)
    cur.execute("SELECT id FROM clientes_sistema WHERE slug = 'thalisadm' LIMIT 1")
    row = cur.fetchone()
    if row:
        return row["id"]
    cur.execute("""
        INSERT INTO clientes_sistema (nome, slug, ativo, criado_por)
        VALUES ('ThalisADM', 'thalisadm', TRUE, %s)
        RETURNING id
    """, (MASTER_SUPERADMIN_LOGIN,))
    return (cur.fetchone() or {}).get("id")


def garantir_schema_multiempresa_superadmin():
    """Cria e migra a base multiempresa.

    Regras:
    - ThalisADM é master, mas possui cliente_id próprio para o ambiente dele.
    - SuperADMs criados abaixo possuem outro cliente_id.
    - Registros antigos sem cliente_id viram dados do ambiente ThalisADM.
    - Tabelas operacionais recebem cliente_id para impedir vazamento entre clientes.
    """
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cliente_thalis = _obter_ou_criar_cliente_thalis_cur(cur)

                cur.execute("""
                    ALTER TABLE usuarios
                    ADD COLUMN IF NOT EXISTS cliente_id INTEGER,
                    ADD COLUMN IF NOT EXISTS superadmin_nivel TEXT DEFAULT 'cliente',
                    ADD COLUMN IF NOT EXISTS criado_por TEXT
                """)
                cur.execute("""
                    ALTER TABLE competicoes
                    ADD COLUMN IF NOT EXISTS cliente_id INTEGER,
                    ADD COLUMN IF NOT EXISTS data_inicio TEXT,
                    ADD COLUMN IF NOT EXISTS data_fim TEXT
                """)

                tabelas_cliente = [
                    "equipes", "atletas", "atletas_globais", "apontadores_acesso", "oficiais",
                    "competicao_oficiais", "equipes_competicoes", "partidas", "grupos",
                    "grupos_equipes", "competicao_quadras", "classificacao_cache",
                    "competicao_agenda_config", "competicao_avanco_config", "eventos_partida",
                    "pins_operacionais", "jogos_avulsos"
                ]
                for tabela in tabelas_cliente:
                    _garantir_coluna_cliente_cur(cur, tabela)

                # ThalisADM: master e dono do próprio ambiente.
                cur.execute("""
                    UPDATE usuarios
                    SET perfil = 'superadmin',
                        superadmin_nivel = 'master',
                        cliente_id = %s,
                        ativo = TRUE
                    WHERE LOWER(login) = LOWER(%s)
                """, (cliente_thalis, MASTER_SUPERADMIN_LOGIN))

                # Migração segura: tudo antigo sem cliente_id fica no ambiente ThalisADM.
                cur.execute("UPDATE usuarios SET cliente_id = %s WHERE cliente_id IS NULL", (cliente_thalis,))
                cur.execute("UPDATE competicoes SET cliente_id = %s WHERE cliente_id IS NULL", (cliente_thalis,))

                # Propaga cliente_id por competição quando possível.
                for tabela, campo_comp in [
                    ("equipes", "competicao"),
                    ("atletas", "competicao"),
                    ("competicao_oficiais", "competicao"),
                    ("equipes_competicoes", "competicao"),
                    ("partidas", "competicao"),
                    ("grupos", "competicao"),
                    ("grupos_equipes", "competicao"),
                    ("competicao_quadras", "competicao"),
                    ("classificacao_cache", "competicao"),
                    ("competicao_agenda_config", "competicao"),
                    ("competicao_avanco_config", "competicao"),
                    ("eventos_partida", "competicao"),
                ]:
                    try:
                        if _tabela_existe_cur(cur, tabela):
                            cols = _colunas_cur(cur, tabela)
                            if "cliente_id" in cols and campo_comp in cols:
                                cur.execute(f"""
                                    UPDATE {tabela} t
                                    SET cliente_id = c.cliente_id
                                    FROM competicoes c
                                    WHERE t.cliente_id IS NULL
                                      AND TRIM(LOWER(t.{campo_comp})) = TRIM(LOWER(c.nome))
                                """)
                    except Exception as e:
                        print(f"AVISO multiempresa backfill {tabela}:", repr(e), flush=True)

                # Equipe/usuário por login.
                try:
                    if _tabela_existe_cur(cur, "equipes"):
                        cur.execute("""
                            UPDATE equipes e
                            SET cliente_id = u.cliente_id
                            FROM usuarios u
                            WHERE e.cliente_id IS NULL
                              AND e.login = u.login
                        """)
                except Exception as e:
                    print("AVISO multiempresa equipes/usuarios:", repr(e), flush=True)

                # Oficiais e apontadores antigos ficam no ambiente ThalisADM se não houver vínculo por competição.
                for tabela in ["equipes", "atletas", "atletas_globais", "apontadores_acesso", "oficiais", "competicao_oficiais", "equipes_competicoes", "partidas", "grupos", "grupos_equipes", "competicao_quadras"]:
                    try:
                        if _tabela_existe_cur(cur, tabela) and "cliente_id" in _colunas_cur(cur, tabela):
                            cur.execute(f"UPDATE {tabela} SET cliente_id = %s WHERE cliente_id IS NULL", (cliente_thalis,))
                    except Exception as e:
                        print(f"AVISO multiempresa default {tabela}:", repr(e), flush=True)

                # Índices.
                for tabela in ["usuarios", "competicoes", "equipes", "atletas", "apontadores_acesso", "oficiais", "competicao_oficiais", "equipes_competicoes", "partidas"]:
                    try:
                        if _tabela_existe_cur(cur, tabela) and "cliente_id" in _colunas_cur(cur, tabela):
                            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{tabela}_cliente_id ON {tabela} (cliente_id)")
                    except Exception:
                        pass

                # Permite mesmo CPF em clientes diferentes quando o banco antigo permitir remover UNIQUE global.
                # Se algum ambiente Neon não permitir DROP por nome, apenas ignora e o restante do isolamento continua.
                for tabela, constraint in [("oficiais", "oficiais_cpf_key"), ("apontadores_acesso", "apontadores_acesso_cpf_key")]:
                    try:
                        if _tabela_existe_cur(cur, tabela):
                            cur.execute(f"ALTER TABLE {tabela} DROP CONSTRAINT IF EXISTS {constraint}")
                            cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{tabela}_cpf_cliente ON {tabela} (cpf, cliente_id)")
                    except Exception as e:
                        print(f"AVISO multiempresa unique {tabela}:", repr(e), flush=True)

            conn.commit()
        _limpar_cache_colunas_multiempresa()
        return True
    except Exception as e:
        print("ERRO garantir_schema_multiempresa_superadmin:", repr(e), flush=True)
        return False


def _slug_cliente(nome):
    base = _normalizar_texto_base(nome or "cliente")
    return base or "cliente"


def _gerar_slug_cliente_unico(cur, nome):
    base = _slug_cliente(nome)
    slug = base
    contador = 1
    while True:
        cur.execute("SELECT id FROM clientes_sistema WHERE slug = %s LIMIT 1", (slug,))
        if not cur.fetchone():
            return slug
        contador += 1
        slug = f"{base}_{contador}"


def cliente_id_thalisadm():
    garantir_schema_multiempresa_superadmin()
    with conectar() as conn:
        with conn.cursor() as cur:
            return _obter_ou_criar_cliente_thalis_cur(cur)


def obter_contexto_superadmin(login):
    garantir_schema_multiempresa_superadmin()
    login = (login or "").strip()
    if not login:
        return {"eh_master": False, "cliente_id": None, "nivel": "cliente"}
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login, perfil, ativo, cliente_id,
                       COALESCE(superadmin_nivel, 'cliente') AS superadmin_nivel
                FROM usuarios
                WHERE LOWER(login) = LOWER(%s)
                LIMIT 1
            """, (login,))
            usuario = cur.fetchone() or {}
    nivel = (usuario.get("superadmin_nivel") or "cliente").strip().lower()
    eh_master = (login.lower() == MASTER_SUPERADMIN_LOGIN.lower()) or nivel == "master"
    return {
        "login": usuario.get("login") or login,
        "perfil": usuario.get("perfil"),
        "ativo": usuario.get("ativo"),
        "cliente_id": usuario.get("cliente_id"),
        "nivel": "master" if eh_master else "cliente",
        "eh_master": eh_master,
    }


def superadmin_eh_master(login):
    return bool(obter_contexto_superadmin(login).get("eh_master"))


def cliente_id_por_login(login):
    garantir_schema_multiempresa_superadmin()
    login = (login or "").strip()
    if not login:
        return None
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT cliente_id FROM usuarios WHERE LOWER(login)=LOWER(%s) LIMIT 1", (login,))
            row = cur.fetchone() or {}
            return row.get("cliente_id")


def cliente_id_por_competicao(competicao):
    garantir_schema_multiempresa_superadmin()
    competicao = (competicao or "").strip()
    if not competicao:
        return None
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT cliente_id FROM competicoes WHERE TRIM(LOWER(nome))=TRIM(LOWER(%s)) LIMIT 1", (competicao,))
            row = cur.fetchone() or {}
            return row.get("cliente_id")


def _resolver_cliente_id_contexto(valor=None):
    """Aceita cliente_id direto, nome de competição ou login de usuário."""
    garantir_schema_multiempresa_superadmin()
    if isinstance(valor, int):
        return valor
    texto = (valor or "").strip() if isinstance(valor, str) else ""
    if not texto:
        return None
    # tenta competição
    cid = cliente_id_por_competicao(texto)
    if cid is not None:
        return cid
    # tenta login
    return cliente_id_por_login(texto)


def buscar_usuario_por_login(login, conn=None):
    garantir_schema_multiempresa_superadmin()
    sql = """
        SELECT login, nome, senha, perfil, ativo, equipe, competicao_vinculada,
               cliente_id, COALESCE(superadmin_nivel, 'cliente') AS superadmin_nivel, criado_por
        FROM usuarios
        WHERE LOWER(login) = LOWER(%s)
        LIMIT 1
    """
    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(sql, (login,))
            return cur.fetchone()
    with conectar() as conn:
        return buscar_usuario_por_login(login, conn)


def listar_superadmins_clientes(login_master=None):
    garantir_schema_multiempresa_superadmin()
    if login_master and not superadmin_eh_master(login_master):
        return []
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    u.login, u.nome, u.senha, u.ativo, u.cliente_id,
                    COALESCE(u.superadmin_nivel, 'cliente') AS superadmin_nivel,
                    COALESCE(c.nome, '') AS cliente_nome,
                    COALESCE(c.ativo, TRUE) AS cliente_ativo,
                    c.criado_em
                FROM usuarios u
                LEFT JOIN clientes_sistema c ON c.id = u.cliente_id
                WHERE u.perfil = 'superadmin'
                  AND COALESCE(u.superadmin_nivel, 'cliente') <> 'master'
                ORDER BY COALESCE(c.nome, u.nome), u.nome
            """)
            return cur.fetchall()


def criar_superadmin_cliente(criador_login, nome_cliente, nome_admin, login_admin=None):
    garantir_schema_multiempresa_superadmin()
    if not superadmin_eh_master(criador_login):
        return {"ok": False, "erro": "Apenas o ThalisADM master pode criar SuperADMs de clientes."}
    nome_cliente = (nome_cliente or "").strip()
    nome_admin = (nome_admin or "").strip()
    login_admin = (login_admin or "").strip()
    if not nome_cliente or not nome_admin:
        return {"ok": False, "erro": "Informe o nome do cliente e o nome do SuperADM."}
    if not login_admin:
        login_admin = f"adm_{_normalizar_texto_base(nome_cliente)}"
    login_admin = re.sub(r"\s+", "_", login_admin)
    login_admin = re.sub(r"[^A-Za-z0-9_.@-]", "", login_admin)[:80]
    if not login_admin:
        login_admin = f"adm_{_normalizar_texto_base(nome_admin)}"
    senha = _gerar_senha_aleatoria(8)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT login FROM usuarios WHERE LOWER(login) = LOWER(%s) LIMIT 1", (login_admin,))
            if cur.fetchone():
                return {"ok": False, "erro": "Este login já está em uso."}
            slug = _gerar_slug_cliente_unico(cur, nome_cliente)
            cur.execute("""
                INSERT INTO clientes_sistema (nome, slug, ativo, criado_por)
                VALUES (%s, %s, TRUE, %s)
                RETURNING id
            """, (nome_cliente, slug, criador_login))
            cliente_id = (cur.fetchone() or {}).get("id")
            cur.execute("""
                INSERT INTO usuarios (
                    login, nome, senha, perfil, ativo, equipe, competicao_vinculada,
                    cliente_id, superadmin_nivel, criado_por
                )
                VALUES (%s, %s, %s, 'superadmin', TRUE, NULL, NULL, %s, 'cliente', %s)
            """, (login_admin, nome_admin, senha, cliente_id, criador_login))
        conn.commit()
    return {"ok": True, "cliente_id": cliente_id, "cliente_nome": nome_cliente, "login": login_admin, "senha": senha, "nome": nome_admin}


def excluir_superadmin_cliente(criador_login, login_alvo):
    garantir_schema_multiempresa_superadmin()
    if not superadmin_eh_master(criador_login):
        return {"ok": False, "erro": "Apenas o ThalisADM master pode excluir SuperADMs de clientes."}
    login_alvo = (login_alvo or "").strip()
    if not login_alvo or login_alvo.lower() == MASTER_SUPERADMIN_LOGIN.lower():
        return {"ok": False, "erro": "Não é permitido excluir o SuperADM master."}
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login, cliente_id, COALESCE(superadmin_nivel, 'cliente') AS nivel
                FROM usuarios
                WHERE LOWER(login) = LOWER(%s)
                  AND perfil = 'superadmin'
                LIMIT 1
            """, (login_alvo,))
            alvo = cur.fetchone()
            if not alvo:
                return {"ok": False, "erro": "SuperADM não encontrado."}
            if (alvo.get("nivel") or "").lower() == "master":
                return {"ok": False, "erro": "Não é permitido excluir o SuperADM master."}
            cliente_id = alvo.get("cliente_id")
            # Suspende o cliente inteiro em vez de apagar dados operacionais.
            cur.execute("UPDATE usuarios SET ativo = FALSE WHERE cliente_id = %s", (cliente_id,))
            if cliente_id:
                cur.execute("UPDATE clientes_sistema SET ativo = FALSE WHERE id = %s", (cliente_id,))
        conn.commit()
    return {"ok": True}


def listar_competicoes(login_superadmin=None, incluir_todos=False):
    sincronizar_status_competicoes()
    garantir_schema_multiempresa_superadmin()
    campos = _campos_competicao(prefixo="c", incluir_senha_organizador=True)
    where = ""
    params = []
    if login_superadmin:
        contexto = obter_contexto_superadmin(login_superadmin)
        if not incluir_todos:
            where = "WHERE c.cliente_id = %s"
            params.append(contexto.get("cliente_id"))
    sql = f"""
        SELECT {", ".join(campos)}
        FROM competicoes c
        LEFT JOIN usuarios u ON u.login = c.organizador_login
        {where}
        ORDER BY
            CASE
                WHEN LOWER(COALESCE(c.status, '')) IN ('em andamento', 'em_andamento', 'andamento') THEN 1
                WHEN LOWER(COALESCE(c.status, '')) IN ('em preparação', 'em preparacao', 'preparação', 'preparacao') THEN 2
                WHEN LOWER(COALESCE(c.status, '')) IN ('finalizada', 'finalizado', 'encerrada', 'encerrado') THEN 3
                ELSE 4
            END,
            c.nome
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall()


def contar_competicoes(login_superadmin=None):
    garantir_schema_multiempresa_superadmin()
    where, params = "", []
    if login_superadmin:
        contexto = obter_contexto_superadmin(login_superadmin)
        where = "WHERE cliente_id = %s"
        params.append(contexto.get("cliente_id"))
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM competicoes {where}", tuple(params))
            return (cur.fetchone() or {}).get("total", 0)


def contar_equipes(login_superadmin=None):
    garantir_schema_multiempresa_superadmin()
    where, params = "", []
    if login_superadmin:
        contexto = obter_contexto_superadmin(login_superadmin)
        where = "WHERE cliente_id = %s"
        params.append(contexto.get("cliente_id"))
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM equipes {where}", tuple(params))
            return (cur.fetchone() or {}).get("total", 0)


def contar_partidas(login_superadmin=None):
    garantir_schema_multiempresa_superadmin()
    join, where, params = "", "", []
    if login_superadmin:
        contexto = obter_contexto_superadmin(login_superadmin)
        join = "LEFT JOIN competicoes c ON c.nome = p.competicao"
        where = "WHERE c.cliente_id = %s"
        params.append(contexto.get("cliente_id"))
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM partidas p {join} {where}", tuple(params))
            return (cur.fetchone() or {}).get("total", 0)


def criar_competicao_com_organizador(nome, data=None, status="Em preparação", modo_operacao="simples", tempos_por_set=2, substituicoes_por_set=6, criador_login=None, data_inicio=None, data_fim=None):
    garantir_schema_multiempresa_superadmin()
    login_organizador = _gerar_login_unico(_normalizar_login_organizador(nome))
    senha_organizador = _gerar_senha_aleatoria(8)
    contexto = obter_contexto_superadmin(criador_login) if criador_login else {"cliente_id": cliente_id_thalisadm()}
    cliente_id = contexto.get("cliente_id")
    colunas = _buscar_colunas_tabela("competicoes")
    colunas_usuarios = _buscar_colunas_tabela("usuarios")
    data_base = data or data_inicio or datetime.now().strftime("%Y-%m-%d")
    with conectar() as conn:
        with conn.cursor() as cur:
            campos_usuario = ["login", "nome", "senha", "perfil", "ativo", "equipe", "competicao_vinculada"]
            valores_usuario = [login_organizador, f"Organizador - {nome}", senha_organizador, "organizador", True, None, nome]
            if "cliente_id" in colunas_usuarios:
                campos_usuario.append("cliente_id"); valores_usuario.append(cliente_id)
            if "criado_por" in colunas_usuarios:
                campos_usuario.append("criado_por"); valores_usuario.append(criador_login)
            cur.execute(f"INSERT INTO usuarios ({', '.join(campos_usuario)}) VALUES ({', '.join(['%s'] * len(valores_usuario))})", tuple(valores_usuario))
            campos = ["nome", "data", "status", "organizador_login"]
            valores = [nome, data_base, status or "Em preparação", login_organizador]
            mapa_defaults = {
                "cliente_id": cliente_id, "data_inicio": data_inicio or data_base, "data_fim": data_fim or data_inicio or data_base,
                "cidade": "", "ginasio": "", "categoria": "", "sexo": "", "divisao": "", "qtd_equipes": 0,
                "formato": "grupos", "tem_grupos": False, "qtd_grupos": 0, "qtd_quadras": 1,
                "modo_operacao": modo_operacao or "simples", "tempos_por_set": tempos_por_set, "substituicoes_por_set": substituicoes_por_set,
                "sets_tipo": "melhor_de_3", "pontos_set": 25, "tem_tiebreak": True, "pontos_tiebreak": 15, "diferenca_minima": 2,
                "vitoria_set_unico": 2, "derrota_set_unico": 0, "vitoria_2x0": 3, "vitoria_2x1": 2, "derrota_1x2": 1, "derrota_0x2": 0,
                "vitoria_3x0": 3, "vitoria_3x1": 3, "vitoria_3x2": 2, "derrota_2x3": 1, "derrota_1x3": 0, "derrota_0x3": 0,
                "criterios_desempate": "vitorias,pontos,saldo_sets,sets_pro,sets_contra,saldo_pontos,pontos_pro,pontos_contra,confronto_direto,coef_sets,coef_pontos,fair_play,sorteio",
                "tipo_classificacao": "grupo", "qtd_classificados": 0, "formato_finais": "mata_mata", "possui_bye": False, "qtd_bye": 0,
                "fases_config": json.dumps({}, ensure_ascii=False), "tipo_confronto": "grupo_interno", "cruzamentos_grupos": "",
                "data_limite_inscricao": None, "hora_limite_inscricao": None, "bloquear_apos_inicio": False, "limite_atletas": 0,
                "permitir_edicao_pos_prazo": False, "exigir_foto_atleta": False, "exigir_instagram_atleta": False,
                "aprovacao_automatica_atletas": False, "travada": False, "motivo_travamento": "", "travada_em": None,
            }
            for campo, default in mapa_defaults.items():
                if campo in colunas:
                    campos.append(campo); valores.append(default)
            cur.execute(f"INSERT INTO competicoes ({', '.join(campos)}) VALUES ({', '.join(['%s'] * len(valores))})", tuple(valores))
        conn.commit()
    try:
        garantir_quadras_competicao(nome, 1)
        # garante cliente_id na quadra padrão criada por função antiga
        cid = cliente_id
        with conectar() as conn:
            with conn.cursor() as cur:
                if _tabela_existe_cur(cur, "competicao_quadras") and "cliente_id" in _colunas_cur(cur, "competicao_quadras"):
                    cur.execute("UPDATE competicao_quadras SET cliente_id = %s WHERE competicao = %s AND cliente_id IS NULL", (cid, nome))
            conn.commit()
    except Exception as e:
        print("AVISO: não foi possível criar quadra padrão da competição:", e)
    return {"login": login_organizador, "senha": senha_organizador}


def buscar_equipe_global_por_nome(nome_equipe, conn=None, cliente_id=None, competicao=None):
    garantir_schema_multiempresa_superadmin()
    nome_equipe = (nome_equipe or "").strip()
    if not nome_equipe:
        return None
    cid = cliente_id if cliente_id is not None else cliente_id_por_competicao(competicao)
    sql = """
        SELECT *
        FROM equipes
        WHERE LOWER(TRIM(nome)) = LOWER(TRIM(%s))
          AND (%s::INTEGER IS NULL OR cliente_id = %s)
        ORDER BY id DESC
        LIMIT 1
    """
    params = (nome_equipe, cid, cid)
    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    with conectar() as conn:
        return buscar_equipe_global_por_nome(nome_equipe, conn, cliente_id=cid)


def buscar_equipes_globais_por_nome(termo, limite=20, cliente_id=None, competicao=None):
    garantir_schema_multiempresa_superadmin()
    termo = (termo or "").strip()
    if not termo:
        return []
    cid = cliente_id if cliente_id is not None else cliente_id_por_competicao(competicao)
    criar_campos_quadro_tecnico_equipes(); criar_campos_liberacao_extra_equipes(); criar_campos_perfil_equipe(); criar_campo_escudo_equipes()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nome, login, senha, competicao, treinador, auxiliar_tecnico, preparador_fisico, medico,
                       liberacao_extra_inscricao, liberacao_extra_data, liberacao_extra_hora, cidade, responsavel,
                       telefone, email, instagram,
                       COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo,
                       escudo_blob,
                       COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo_exibicao,
                       COALESCE(perfil_completo, FALSE) AS perfil_completo,
                       cliente_id
                FROM equipes
                WHERE LOWER(TRIM(nome)) LIKE LOWER(TRIM(%s))
                  AND (%s::INTEGER IS NULL OR cliente_id = %s)
                ORDER BY CASE WHEN LOWER(TRIM(nome)) = LOWER(TRIM(%s)) THEN 0 ELSE 1 END, nome ASC, login ASC
                LIMIT %s
            """, (f"%{termo}%", cid, cid, termo, limite))
            return cur.fetchall()


def vincular_equipe_a_competicao(nome_equipe, nome_competicao, conn=None):
    garantir_schema_multiempresa_superadmin(); criar_tabela_equipes_competicoes()
    cid = cliente_id_por_competicao(nome_competicao)
    def _executar(cnx):
        equipe = buscar_equipe_global_por_nome(nome_equipe, cnx, cliente_id=cid)
        if not equipe:
            return None
        with cnx.cursor() as cur:
            cur.execute("""
                SELECT id FROM equipes_competicoes
                WHERE equipe_login = %s AND competicao = %s AND cliente_id = %s
                LIMIT 1
            """, (equipe["login"], nome_competicao, cid))
            existente = cur.fetchone()
            if not existente:
                cur.execute("""
                    INSERT INTO equipes_competicoes (equipe_login, equipe_nome, competicao, status, cliente_id)
                    VALUES (%s, %s, %s, 'ativa', %s)
                    ON CONFLICT (equipe_nome, competicao) DO UPDATE
                    SET equipe_login = EXCLUDED.equipe_login, status = 'ativa', cliente_id = EXCLUDED.cliente_id
                """, (equipe["login"], equipe["nome"], nome_competicao, cid))
            try:
                cur.execute("UPDATE equipes SET cliente_id = %s WHERE login = %s", (cid, equipe["login"]))
                cur.execute("UPDATE usuarios SET cliente_id = %s WHERE login = %s", (cid, equipe["login"]))
            except Exception:
                pass
        equipe = dict(equipe)
        equipe["ja_vinculada"] = bool(existente)
        return equipe
    if conn is not None:
        return _executar(conn)
    with conectar() as conn:
        res = _executar(conn); conn.commit(); return res


def criar_nova_equipe_com_credenciais(nome_equipe, nome_competicao):
    garantir_schema_multiempresa_superadmin()
    criar_campos_quadro_tecnico_equipes(); criar_campos_liberacao_extra_equipes(); criar_campos_perfil_equipe(); criar_tabela_equipes_competicoes()
    nome_equipe = (nome_equipe or "").strip(); nome_competicao = (nome_competicao or "").strip()
    if not nome_equipe or not nome_competicao:
        return None
    cid = cliente_id_por_competicao(nome_competicao)
    with conectar() as conn:
        with conn.cursor() as cur:
            login_equipe = _gerar_login_unico(_normalizar_login_equipe(nome_equipe)); senha_equipe = _gerar_senha_aleatoria(8)
            cur.execute("""
                INSERT INTO equipes (nome, login, senha, competicao, treinador, auxiliar_tecnico, preparador_fisico, medico,
                    liberacao_extra_inscricao, liberacao_extra_data, liberacao_extra_hora, cidade, responsavel, telefone, email, instagram, escudo, perfil_completo, cliente_id)
                VALUES (%s, %s, %s, %s, '', '', '', '', FALSE, NULL, NULL, '', '', '', '', '', '', FALSE, %s)
            """, (nome_equipe, login_equipe, senha_equipe, nome_competicao, cid))
            cur.execute("""
                INSERT INTO equipes_competicoes (equipe_login, equipe_nome, competicao, status, cliente_id)
                VALUES (%s, %s, %s, 'ativa', %s)
                ON CONFLICT (equipe_nome, competicao) DO UPDATE
                SET equipe_login = EXCLUDED.equipe_login, status = 'ativa', cliente_id = EXCLUDED.cliente_id
            """, (login_equipe, nome_equipe, nome_competicao, cid))
            cur.execute("""
                INSERT INTO usuarios (login, nome, senha, perfil, ativo, equipe, competicao_vinculada, cliente_id)
                VALUES (%s, %s, %s, 'equipe', TRUE, %s, %s, %s)
            """, (login_equipe, nome_equipe, senha_equipe, nome_equipe, nome_competicao, cid))
        conn.commit()
    return {"login": login_equipe, "senha": senha_equipe, "nome": nome_equipe, "vinculada": True, "ja_existia": False, "ja_vinculada": False}


def criar_equipe_com_credenciais(nome_equipe, nome_competicao):
    garantir_schema_multiempresa_superadmin()
    cid = cliente_id_por_competicao(nome_competicao)
    with conectar() as conn:
        existente = buscar_equipe_global_por_nome(nome_equipe, conn, cliente_id=cid)
        if existente:
            resultado = vincular_equipe_a_competicao(existente["nome"], nome_competicao, conn)
            conn.commit()
            return {"login": resultado["login"], "senha": resultado["senha"], "nome": resultado["nome"], "vinculada": True, "ja_existia": True, "ja_vinculada": resultado.get("ja_vinculada", False)}
    return criar_nova_equipe_com_credenciais(nome_equipe, nome_competicao)


def buscar_atleta_global_por_cpf(cpf, cliente_id=None, competicao=None):
    """Busca atleta por CPF somente dentro do cliente/competição informados."""
    garantir_schema_multiempresa_superadmin(); criar_tabela_atletas()
    cpf_limpo = somente_digitos(cpf)
    if not cpf_limpo:
        return None
    cid = cliente_id if cliente_id is not None else cliente_id_por_competicao(competicao)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT nome, cpf, data_nascimento, foto_atleta, instagram
                FROM atletas
                WHERE {_cpf_sql_limpo('cpf')} = %s
                  AND (%s::INTEGER IS NULL OR cliente_id = %s)
                ORDER BY CASE WHEN COALESCE(foto_atleta, '') <> '' THEN 0 ELSE 1 END,
                         CASE WHEN COALESCE(instagram, '') <> '' THEN 0 ELSE 1 END,
                         id DESC
                LIMIT 1
            """, (cpf_limpo, cid, cid))
            return cur.fetchone()


def criar_tabelas_oficiais():
    garantir_schema_multiempresa_superadmin()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS oficiais (
                id SERIAL PRIMARY KEY,
                nome TEXT NOT NULL,
                cpf TEXT NOT NULL,
                criado_em TIMESTAMP DEFAULT NOW(),
                cliente_id INTEGER
            )
            """)
            cur.execute("ALTER TABLE oficiais ADD COLUMN IF NOT EXISTS cliente_id INTEGER")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS apontadores_acesso (
                id SERIAL PRIMARY KEY,
                cpf TEXT NOT NULL,
                senha TEXT,
                ativo BOOLEAN DEFAULT TRUE,
                primeiro_acesso BOOLEAN DEFAULT TRUE,
                cliente_id INTEGER
            )
            """)
            cur.execute("ALTER TABLE apontadores_acesso ADD COLUMN IF NOT EXISTS primeiro_acesso BOOLEAN DEFAULT TRUE")
            cur.execute("ALTER TABLE apontadores_acesso ADD COLUMN IF NOT EXISTS cliente_id INTEGER")
            cur.execute("""
            CREATE TABLE IF NOT EXISTS competicao_oficiais (
                id SERIAL PRIMARY KEY,
                competicao TEXT NOT NULL,
                cpf TEXT NOT NULL,
                funcao TEXT NOT NULL,
                criado_em TIMESTAMP DEFAULT NOW(),
                cliente_id INTEGER
            )
            """)
            cur.execute("ALTER TABLE competicao_oficiais ADD COLUMN IF NOT EXISTS cliente_id INTEGER")
            try:
                cur.execute("ALTER TABLE oficiais DROP CONSTRAINT IF EXISTS oficiais_cpf_key")
                cur.execute("ALTER TABLE apontadores_acesso DROP CONSTRAINT IF EXISTS apontadores_acesso_cpf_key")
            except Exception:
                pass
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_oficiais_cpf_cliente ON oficiais (cpf, cliente_id)")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_apontadores_cpf_cliente ON apontadores_acesso (cpf, cliente_id)")
        conn.commit()


def buscar_oficial_por_cpf(cpf, contexto=None, cliente_id=None):
    garantir_schema_multiempresa_superadmin(); criar_tabelas_oficiais()
    cpf_limpo = somente_digitos(cpf)
    cid = cliente_id if cliente_id is not None else _resolver_cliente_id_contexto(contexto)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM oficiais
                WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                  AND (%s::INTEGER IS NULL OR cliente_id = %s)
                LIMIT 1
            """, (cpf_limpo, cid, cid))
            return cur.fetchone()


def cadastrar_oficial(nome, cpf, contexto=None, cliente_id=None):
    garantir_schema_multiempresa_superadmin(); criar_tabelas_oficiais()
    cpf_limpo = somente_digitos(cpf)
    cid = cliente_id if cliente_id is not None else _resolver_cliente_id_contexto(contexto)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO oficiais (nome, cpf, cliente_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (cpf, cliente_id) DO UPDATE
                SET nome = COALESCE(NULLIF(EXCLUDED.nome, ''), oficiais.nome)
            """, (nome, cpf_limpo, cid))
        conn.commit()
    return True


def criar_apontador(cpf, contexto=None, cliente_id=None):
    garantir_schema_multiempresa_superadmin(); criar_tabelas_oficiais()
    cpf_limpo = somente_digitos(cpf)
    cid = cliente_id if cliente_id is not None else _resolver_cliente_id_contexto(contexto)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO apontadores_acesso (cpf, senha, ativo, primeiro_acesso, cliente_id)
                VALUES (%s, NULL, TRUE, TRUE, %s)
                ON CONFLICT (cpf, cliente_id) DO NOTHING
            """, (cpf_limpo, cid))
        conn.commit()
    return True


def buscar_apontador(cpf, contexto=None, cliente_id=None):
    garantir_schema_multiempresa_superadmin(); criar_tabelas_oficiais()
    cpf_limpo = somente_digitos(cpf)
    cid = cliente_id if cliente_id is not None else _resolver_cliente_id_contexto(contexto)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.*, o.nome
                FROM apontadores_acesso a
                LEFT JOIN oficiais o
                  ON REGEXP_REPLACE(COALESCE(o.cpf, ''), '\\D', '', 'g') = REGEXP_REPLACE(COALESCE(a.cpf, ''), '\\D', '', 'g')
                 AND o.cliente_id = a.cliente_id
                WHERE REGEXP_REPLACE(COALESCE(a.cpf, ''), '\\D', '', 'g') = %s
                  AND (%s::INTEGER IS NULL OR a.cliente_id = %s)
                ORDER BY a.id DESC
                LIMIT 1
            """, (cpf_limpo, cid, cid))
            return cur.fetchone()


def definir_senha_apontador(cpf, senha, contexto=None, cliente_id=None):
    garantir_schema_multiempresa_superadmin()
    cpf_limpo = somente_digitos(cpf)
    cid = cliente_id if cliente_id is not None else _resolver_cliente_id_contexto(contexto)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE apontadores_acesso
                SET senha = %s, primeiro_acesso = FALSE
                WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                  AND (%s::INTEGER IS NULL OR cliente_id = %s)
            """, (senha, cpf_limpo, cid, cid))
        conn.commit()
    return True


def autenticar_apontador(cpf, senha):
    garantir_schema_multiempresa_superadmin(); criar_tabelas_oficiais()
    cpf_limpo = somente_digitos(cpf)
    with conectar() as conn:
        with conn.cursor() as cur:
            # Primeiro tenta CPF + senha. Isso permite o mesmo CPF em clientes diferentes.
            cur.execute("""
                SELECT a.*, o.nome
                FROM apontadores_acesso a
                LEFT JOIN oficiais o
                  ON REGEXP_REPLACE(COALESCE(o.cpf, ''), '\\D', '', 'g') = REGEXP_REPLACE(COALESCE(a.cpf, ''), '\\D', '', 'g')
                 AND o.cliente_id = a.cliente_id
                WHERE REGEXP_REPLACE(COALESCE(a.cpf, ''), '\\D', '', 'g') = %s
                  AND COALESCE(a.ativo, TRUE) = TRUE
                  AND a.senha = %s
                ORDER BY a.id DESC
                LIMIT 1
            """, (cpf_limpo, senha))
            row = cur.fetchone()
            if row:
                return row
            # Primeiro acesso: só aceita se houver um único cadastro ativo sem senha para este CPF.
            cur.execute("""
                SELECT a.*, o.nome
                FROM apontadores_acesso a
                LEFT JOIN oficiais o
                  ON REGEXP_REPLACE(COALESCE(o.cpf, ''), '\\D', '', 'g') = REGEXP_REPLACE(COALESCE(a.cpf, ''), '\\D', '', 'g')
                 AND o.cliente_id = a.cliente_id
                WHERE REGEXP_REPLACE(COALESCE(a.cpf, ''), '\\D', '', 'g') = %s
                  AND COALESCE(a.ativo, TRUE) = TRUE
                  AND (a.senha IS NULL OR a.senha = '')
                ORDER BY a.id DESC
                LIMIT 2
            """, (cpf_limpo,))
            rows = cur.fetchall() or []
            if len(rows) == 1:
                return rows[0]
            if len(rows) > 1:
                return False
    return None


def vincular_oficial_competicao(competicao, cpf, funcao):
    garantir_schema_multiempresa_superadmin(); criar_tabelas_oficiais()
    cid = cliente_id_por_competicao(competicao)
    cpf_limpo = somente_digitos(cpf)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM competicao_oficiais
                WHERE TRIM(LOWER(competicao)) = TRIM(LOWER(%s))
                  AND REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                  AND TRIM(LOWER(funcao)) = TRIM(LOWER(%s))
                  AND cliente_id = %s
                LIMIT 1
            """, (competicao, cpf_limpo, funcao, cid))
            if cur.fetchone():
                return True
            cur.execute("""
                INSERT INTO competicao_oficiais (competicao, cpf, funcao, cliente_id)
                VALUES (%s, %s, %s, %s)
            """, (competicao, cpf_limpo, funcao, cid))
        conn.commit()
    return True


def listar_oficiais_competicao(competicao):
    garantir_schema_multiempresa_superadmin(); criar_tabelas_oficiais()
    cid = cliente_id_por_competicao(competicao)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT c.id, c.competicao, c.cpf, c.funcao, c.criado_em,
                       COALESCE(o.nome, c.cpf) AS nome,
                       COALESCE(a.ativo, TRUE) AS apontador_ativo,
                       COALESCE(a.primeiro_acesso, TRUE) AS primeiro_acesso
                FROM competicao_oficiais c
                LEFT JOIN oficiais o
                  ON REGEXP_REPLACE(COALESCE(o.cpf, ''), '\\D', '', 'g') = REGEXP_REPLACE(COALESCE(c.cpf, ''), '\\D', '', 'g')
                 AND o.cliente_id = c.cliente_id
                LEFT JOIN apontadores_acesso a
                  ON REGEXP_REPLACE(COALESCE(a.cpf, ''), '\\D', '', 'g') = REGEXP_REPLACE(COALESCE(c.cpf, ''), '\\D', '', 'g')
                 AND a.cliente_id = c.cliente_id
                WHERE TRIM(LOWER(c.competicao)) = TRIM(LOWER(%s))
                  AND c.cliente_id = %s
                ORDER BY c.funcao, nome
            """, (competicao, cid))
            return cur.fetchall()


def listar_competicoes_apontador(cpf):
    garantir_schema_multiempresa_superadmin(); criar_tabelas_oficiais()
    cpf_limpo = somente_digitos(cpf)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT c.competicao, comp.data, comp.status, c.cliente_id
                FROM competicao_oficiais c
                LEFT JOIN competicoes comp
                  ON TRIM(LOWER(comp.nome)) = TRIM(LOWER(c.competicao))
                 AND comp.cliente_id = c.cliente_id
                WHERE REGEXP_REPLACE(COALESCE(c.cpf, ''), '\\D', '', 'g') = %s
                  AND TRIM(LOWER(c.funcao)) = 'apontador'
                ORDER BY c.competicao
            """, (cpf_limpo,))
            return cur.fetchall()


# =========================================================
# CORREÇÕES FINAIS MULTIEMPRESA - ISOLAMENTO TOTAL
# =========================================================
# As funções abaixo ficam no final do arquivo para sobrescrever versões antigas
# que ainda trabalhavam com cadastros globais. A regra é: todo SuperADM/cliente
# só enxerga registros do próprio cliente_id. O ThalisADM master também possui
# cliente_id próprio para o painel normal dele.

def criar_tabela_equipes_competicoes(force=False):
    chave = "tabela_equipes_competicoes"
    if _schema_ja_pronto(chave, force=force):
        return

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS equipes_competicoes (
                    id SERIAL PRIMARY KEY,
                    equipe_id INTEGER,
                    equipe_login TEXT,
                    equipe_nome TEXT NOT NULL,
                    competicao TEXT NOT NULL,
                    status TEXT DEFAULT 'ativa',
                    grupo TEXT,
                    criado_em TIMESTAMP DEFAULT NOW(),
                    cliente_id INTEGER,
                    UNIQUE (equipe_nome, competicao)
                )
            """)
            cur.execute("ALTER TABLE equipes_competicoes ADD COLUMN IF NOT EXISTS cliente_id INTEGER")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_equipes_competicoes_competicao ON equipes_competicoes (competicao)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_equipes_competicoes_login ON equipes_competicoes (equipe_login)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_equipes_competicoes_cliente_id ON equipes_competicoes (cliente_id)")

            try:
                cur.execute("""
                    INSERT INTO equipes_competicoes (equipe_id, equipe_login, equipe_nome, competicao, status, cliente_id)
                    SELECT NULL, e.login, e.nome, e.competicao, 'ativa', e.cliente_id
                    FROM equipes e
                    WHERE COALESCE(e.competicao, '') <> ''
                    ON CONFLICT (equipe_nome, competicao) DO UPDATE
                    SET equipe_login = EXCLUDED.equipe_login,
                        status = 'ativa',
                        cliente_id = COALESCE(equipes_competicoes.cliente_id, EXCLUDED.cliente_id)
                """)
            except Exception as e:
                print("AVISO criar_tabela_equipes_competicoes/migracao:", repr(e), flush=True)
        conn.commit()
    _marcar_schema_pronto(chave)
    try:
        _CACHE_COLUNAS.pop("equipes_competicoes", None)
    except Exception:
        pass


def vincular_equipe_existente_competicao(login_equipe, nome_competicao, conn=None):
    garantir_schema_multiempresa_superadmin()
    criar_tabela_equipes_competicoes()
    login_equipe = (login_equipe or "").strip()
    nome_competicao = (nome_competicao or "").strip()
    if not login_equipe or not nome_competicao:
        return None

    cid = cliente_id_por_competicao(nome_competicao)

    def _executar(cnx):
        with cnx.cursor() as cur:
            cur.execute("""
                SELECT nome, login, senha, competicao, cidade, responsavel, telefone, email, instagram,
                       COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo,
                       escudo_blob,
                       COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo_exibicao,
                       COALESCE(perfil_completo, FALSE) AS perfil_completo,
                       cliente_id
                FROM equipes
                WHERE login = %s
                  AND (%s::INTEGER IS NULL OR cliente_id = %s)
                LIMIT 1
            """, (login_equipe, cid, cid))
            equipe = cur.fetchone()
            if not equipe:
                return None

            cur.execute("""
                SELECT id
                FROM equipes_competicoes
                WHERE equipe_login = %s
                  AND competicao = %s
                  AND (%s::INTEGER IS NULL OR cliente_id = %s)
                LIMIT 1
            """, (equipe["login"], nome_competicao, cid, cid))
            ja_vinculada = cur.fetchone() is not None

            cur.execute("""
                INSERT INTO equipes_competicoes (equipe_login, equipe_nome, competicao, status, cliente_id)
                VALUES (%s, %s, %s, 'ativa', %s)
                ON CONFLICT (equipe_nome, competicao) DO UPDATE
                SET equipe_login = EXCLUDED.equipe_login,
                    equipe_nome = EXCLUDED.equipe_nome,
                    status = 'ativa',
                    cliente_id = EXCLUDED.cliente_id
            """, (equipe["login"], equipe["nome"], nome_competicao, cid))

            try:
                cur.execute("UPDATE equipes SET cliente_id = %s WHERE login = %s AND cliente_id IS NULL", (cid, equipe["login"]))
                cur.execute("UPDATE usuarios SET cliente_id = %s WHERE login = %s AND cliente_id IS NULL", (cid, equipe["login"]))
            except Exception:
                pass

            equipe = dict(equipe)
            equipe["ja_vinculada"] = ja_vinculada
            return equipe

    if conn is not None:
        return _executar(conn)
    with conectar() as conn:
        resultado = _executar(conn)
        conn.commit()
        return resultado


def buscar_equipe_global_por_nome(nome_equipe, conn=None, cliente_id=None, competicao=None):
    garantir_schema_multiempresa_superadmin()
    nome_equipe = (nome_equipe or "").strip()
    if not nome_equipe:
        return None
    cid = cliente_id if cliente_id is not None else cliente_id_por_competicao(competicao)
    sql = """
        SELECT *
        FROM equipes
        WHERE LOWER(TRIM(nome)) = LOWER(TRIM(%s))
          AND (%s::INTEGER IS NULL OR cliente_id = %s)
        ORDER BY id DESC
        LIMIT 1
    """
    params = (nome_equipe, cid, cid)
    if conn is not None:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()
    with conectar() as conn:
        return buscar_equipe_global_por_nome(nome_equipe, conn=conn, cliente_id=cid)


def buscar_equipes_globais_por_nome(termo, limite=20, cliente_id=None, competicao=None):
    garantir_schema_multiempresa_superadmin()
    termo = (termo or "").strip()
    if not termo:
        return []
    cid = cliente_id if cliente_id is not None else cliente_id_por_competicao(competicao)
    criar_campos_quadro_tecnico_equipes(); criar_campos_liberacao_extra_equipes(); criar_campos_perfil_equipe(); criar_campo_escudo_equipes()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT nome, login, senha, competicao, treinador, auxiliar_tecnico, preparador_fisico, medico,
                       liberacao_extra_inscricao, liberacao_extra_data, liberacao_extra_hora, cidade, responsavel,
                       telefone, email, instagram,
                       COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo,
                       escudo_blob,
                       COALESCE(NULLIF(escudo_blob, ''), NULLIF(escudo, ''), '/static/img/escudo_padrao.svg') AS escudo_exibicao,
                       COALESCE(perfil_completo, FALSE) AS perfil_completo,
                       cliente_id
                FROM equipes
                WHERE LOWER(TRIM(nome)) LIKE LOWER(TRIM(%s))
                  AND (%s::INTEGER IS NULL OR cliente_id = %s)
                ORDER BY CASE WHEN LOWER(TRIM(nome)) = LOWER(TRIM(%s)) THEN 0 ELSE 1 END, nome ASC, login ASC
                LIMIT %s
            """, (f"%{termo}%", cid, cid, termo, limite))
            return cur.fetchall() or []


def criar_nova_equipe_com_credenciais(nome_equipe, nome_competicao):
    garantir_schema_multiempresa_superadmin()
    criar_campos_quadro_tecnico_equipes(); criar_campos_liberacao_extra_equipes(); criar_campos_perfil_equipe(); criar_tabela_equipes_competicoes()
    nome_equipe = (nome_equipe or "").strip(); nome_competicao = (nome_competicao or "").strip()
    if not nome_equipe or not nome_competicao:
        return None
    cid = cliente_id_por_competicao(nome_competicao)
    with conectar() as conn:
        with conn.cursor() as cur:
            login_equipe = _gerar_login_unico(_normalizar_login_equipe(nome_equipe))
            senha_equipe = _gerar_senha_aleatoria(8)
            cur.execute("""
                INSERT INTO equipes (nome, login, senha, competicao, treinador, auxiliar_tecnico, preparador_fisico, medico,
                    liberacao_extra_inscricao, liberacao_extra_data, liberacao_extra_hora, cidade, responsavel, telefone, email, instagram, escudo, perfil_completo, cliente_id)
                VALUES (%s, %s, %s, %s, '', '', '', '', FALSE, NULL, NULL, '', '', '', '', '', '', FALSE, %s)
            """, (nome_equipe, login_equipe, senha_equipe, nome_competicao, cid))
            cur.execute("""
                INSERT INTO equipes_competicoes (equipe_login, equipe_nome, competicao, status, cliente_id)
                VALUES (%s, %s, %s, 'ativa', %s)
                ON CONFLICT (equipe_nome, competicao) DO UPDATE
                SET equipe_login = EXCLUDED.equipe_login,
                    status = 'ativa',
                    cliente_id = EXCLUDED.cliente_id
            """, (login_equipe, nome_equipe, nome_competicao, cid))
            cur.execute("""
                INSERT INTO usuarios (login, nome, senha, perfil, ativo, equipe, competicao_vinculada, cliente_id)
                VALUES (%s, %s, %s, 'equipe', TRUE, %s, %s, %s)
            """, (login_equipe, nome_equipe, senha_equipe, nome_equipe, nome_competicao, cid))
        conn.commit()
    return {"login": login_equipe, "senha": senha_equipe, "nome": nome_equipe, "vinculada": True, "ja_existia": False, "ja_vinculada": False}


def criar_equipe_com_credenciais(nome_equipe, nome_competicao):
    garantir_schema_multiempresa_superadmin()
    cid = cliente_id_por_competicao(nome_competicao)
    with conectar() as conn:
        existente = buscar_equipe_global_por_nome(nome_equipe, conn=conn, cliente_id=cid)
        if existente:
            resultado = vincular_equipe_existente_competicao(existente["login"], nome_competicao, conn=conn)
            conn.commit()
            if not resultado:
                return None
            return {"login": resultado["login"], "senha": resultado["senha"], "nome": resultado["nome"], "vinculada": True, "ja_existia": True, "ja_vinculada": resultado.get("ja_vinculada", False)}
    return criar_nova_equipe_com_credenciais(nome_equipe, nome_competicao)


def buscar_atleta_global_por_cpf(cpf, cliente_id=None, competicao=None):
    garantir_schema_multiempresa_superadmin(); criar_tabela_atletas()
    cpf_limpo = somente_digitos(cpf)
    if not cpf_limpo:
        return None
    cid = cliente_id if cliente_id is not None else cliente_id_por_competicao(competicao)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT nome, cpf, data_nascimento, foto_atleta, instagram, cliente_id
                FROM atletas
                WHERE {_cpf_sql_limpo('cpf')} = %s
                  AND (%s::INTEGER IS NULL OR cliente_id = %s)
                ORDER BY CASE WHEN COALESCE(foto_atleta, '') <> '' THEN 0 ELSE 1 END,
                         CASE WHEN COALESCE(instagram, '') <> '' THEN 0 ELSE 1 END,
                         id DESC
                LIMIT 1
            """, (cpf_limpo, cid, cid))
            return cur.fetchone()


def excluir_apontador_global(cpf, cliente_id=None, contexto=None):
    garantir_schema_multiempresa_superadmin(); criar_tabelas_oficiais()
    cpf_limpo = somente_digitos(cpf)
    cid = cliente_id if cliente_id is not None else _resolver_cliente_id_contexto(contexto)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM apontadores_acesso
                WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                  AND (%s::INTEGER IS NULL OR cliente_id = %s)
            """, (cpf_limpo, cid, cid))
            cur.execute("""
                DELETE FROM competicao_oficiais
                WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                  AND (%s::INTEGER IS NULL OR cliente_id = %s)
            """, (cpf_limpo, cid, cid))
        conn.commit()
    return True


def definir_senha_apontador(cpf, senha, contexto=None, cliente_id=None):
    garantir_schema_multiempresa_superadmin(); criar_tabelas_oficiais()
    cpf_limpo = somente_digitos(cpf)
    cid = cliente_id if cliente_id is not None else _resolver_cliente_id_contexto(contexto)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE apontadores_acesso
                SET senha = %s, primeiro_acesso = FALSE
                WHERE REGEXP_REPLACE(COALESCE(cpf, ''), '\\D', '', 'g') = %s
                  AND (%s::INTEGER IS NULL OR cliente_id = %s)
            """, (senha, cpf_limpo, cid, cid))
        conn.commit()
    return True


# =========================================================
# CORREÇÃO FINAL - MULTIEMPRESA SEM TRAVAR LOGIN
# =========================================================
# As funções acima podem ter sido redefinidas por patches anteriores.
# Estas definições ficam no final do arquivo para prevalecerem no Python.
# Objetivos:
# - ThalisADM é master e também possui cliente próprio.
# - Login não executa migração pesada.
# - Painel usa consultas filtradas por cliente sem chamar ALTER TABLE em todo request.
# - Registros antigos sem cliente_id continuam visíveis para ThalisADM.

_MULTIEMPRESA_LEVE_OK = False
_MULTIEMPRESA_LEVE_LOCK = Lock()


def _coluna_existe_rapido(cur, tabela, coluna):
    try:
        cur.execute("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_name = %s
            LIMIT 1
        """, (tabela, coluna))
        return cur.fetchone() is not None
    except Exception:
        return False


def _tabela_existe_rapido(cur, tabela):
    try:
        cur.execute("SELECT to_regclass(%s) AS tabela", (f"public.{tabela}",))
        return bool((cur.fetchone() or {}).get("tabela"))
    except Exception:
        return False


def _limpar_cache_colunas_multiempresa_rapido(*tabelas):
    try:
        for tabela in (tabelas or []):
            _CACHE_COLUNAS.pop(tabela, None)
    except Exception:
        pass


def garantir_schema_multiempresa_superadmin():
    """Garantia LEVE do schema multiempresa.

    Não faz backfill pesado nem varre tabelas grandes. Pode ser chamada em painel,
    mas não deve atrasar login. A migração completa deve ser feita manualmente se
    necessário; aqui garantimos só o mínimo para o sistema funcionar.
    """
    global _MULTIEMPRESA_LEVE_OK

    if _MULTIEMPRESA_LEVE_OK:
        return True

    with _MULTIEMPRESA_LEVE_LOCK:
        if _MULTIEMPRESA_LEVE_OK:
            return True

        try:
            with conectar() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS clientes_sistema (
                            id SERIAL PRIMARY KEY,
                            nome TEXT NOT NULL,
                            slug TEXT UNIQUE NOT NULL,
                            ativo BOOLEAN DEFAULT TRUE,
                            criado_por TEXT,
                            criado_em TIMESTAMP DEFAULT NOW()
                        )
                    """)

                    # Somente colunas pequenas indispensáveis.
                    cur.execute("""
                        ALTER TABLE usuarios
                        ADD COLUMN IF NOT EXISTS cliente_id INTEGER,
                        ADD COLUMN IF NOT EXISTS superadmin_nivel TEXT DEFAULT 'cliente',
                        ADD COLUMN IF NOT EXISTS criado_por TEXT
                    """)

                    cur.execute("""
                        ALTER TABLE competicoes
                        ADD COLUMN IF NOT EXISTS cliente_id INTEGER,
                        ADD COLUMN IF NOT EXISTS data_inicio TEXT,
                        ADD COLUMN IF NOT EXISTS data_fim TEXT
                    """)

                    # Garante o cliente próprio do ThalisADM.
                    cur.execute("SELECT id FROM clientes_sistema WHERE slug = 'thalisadm' LIMIT 1")
                    row = cur.fetchone()
                    if row:
                        cliente_thalis = row.get("id")
                    else:
                        cur.execute("""
                            INSERT INTO clientes_sistema (nome, slug, ativo, criado_por)
                            VALUES ('ThalisADM', 'thalisadm', TRUE, %s)
                            RETURNING id
                        """, (MASTER_SUPERADMIN_LOGIN,))
                        cliente_thalis = (cur.fetchone() or {}).get("id")

                    cur.execute("""
                        UPDATE usuarios
                        SET perfil = 'superadmin',
                            superadmin_nivel = 'master',
                            cliente_id = %s,
                            ativo = TRUE
                        WHERE LOWER(login) = LOWER(%s)
                    """, (cliente_thalis, MASTER_SUPERADMIN_LOGIN))

                    # Índices leves apenas nas tabelas garantidas acima.
                    try:
                        cur.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_cliente_id ON usuarios (cliente_id)")
                    except Exception:
                        pass
                    try:
                        cur.execute("CREATE INDEX IF NOT EXISTS idx_competicoes_cliente_id ON competicoes (cliente_id)")
                    except Exception:
                        pass

                conn.commit()

            _limpar_cache_colunas_multiempresa_rapido("usuarios", "competicoes")
            _MULTIEMPRESA_LEVE_OK = True
            return True
        except Exception as e:
            print("ERRO garantir_schema_multiempresa_superadmin leve:", repr(e), flush=True)
            return False


def cliente_id_thalisadm():
    garantir_schema_multiempresa_superadmin()
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM clientes_sistema WHERE slug = 'thalisadm' LIMIT 1")
                row = cur.fetchone() or {}
                if row.get("id") is not None:
                    return row.get("id")
                cur.execute("""
                    INSERT INTO clientes_sistema (nome, slug, ativo, criado_por)
                    VALUES ('ThalisADM', 'thalisadm', TRUE, %s)
                    RETURNING id
                """, (MASTER_SUPERADMIN_LOGIN,))
                row = cur.fetchone() or {}
            conn.commit()
            return row.get("id")
    except Exception as e:
        print("AVISO cliente_id_thalisadm:", repr(e), flush=True)
        return None


def buscar_usuario_por_login(login, conn=None):
    """Busca usuário sem rodar migração/DDL no login."""
    login = (login or "").strip()
    if not login:
        return None

    sql_completo = """
        SELECT login, nome, senha, perfil, ativo, equipe, competicao_vinculada,
               cliente_id, COALESCE(superadmin_nivel, 'cliente') AS superadmin_nivel, criado_por
        FROM usuarios
        WHERE LOWER(login) = LOWER(%s)
        LIMIT 1
    """
    sql_basico = """
        SELECT login, nome, senha, perfil, ativo, equipe, competicao_vinculada
        FROM usuarios
        WHERE LOWER(login) = LOWER(%s)
        LIMIT 1
    """

    def _exec(cnx):
        with cnx.cursor() as cur:
            try:
                cur.execute(sql_completo, (login,))
                row = cur.fetchone()
                if row:
                    return row
            except Exception as e:
                # Banco ainda sem colunas multiempresa: mantém login funcionando.
                print("AVISO buscar_usuario_por_login fallback:", repr(e), flush=True)
                try:
                    cnx.rollback()
                except Exception:
                    pass
                with cnx.cursor() as cur2:
                    cur2.execute(sql_basico, (login,))
                    row = cur2.fetchone()
                    if row:
                        row = dict(row)
                        row.setdefault("cliente_id", None)
                        row.setdefault("superadmin_nivel", "master" if login.lower() == MASTER_SUPERADMIN_LOGIN.lower() else "cliente")
                        row.setdefault("criado_por", None)
                    return row
            return None

    if conn is not None:
        return _exec(conn)
    with conectar() as conn:
        return _exec(conn)


def obter_contexto_superadmin(login):
    """Contexto rápido, sem migração pesada em cadeia."""
    login = (login or "").strip()
    if not login:
        return {"eh_master": False, "cliente_id": None, "nivel": "cliente"}

    usuario = buscar_usuario_por_login(login)
    if not usuario:
        if login.lower() == MASTER_SUPERADMIN_LOGIN.lower():
            return {"login": login, "perfil": "superadmin", "ativo": True, "cliente_id": cliente_id_thalisadm(), "nivel": "master", "eh_master": True}
        return {"login": login, "perfil": None, "ativo": None, "cliente_id": None, "nivel": "cliente", "eh_master": False}

    nivel = (usuario.get("superadmin_nivel") or "cliente").strip().lower()
    eh_master = (login.lower() == MASTER_SUPERADMIN_LOGIN.lower()) or nivel == "master"
    cid = usuario.get("cliente_id")

    if eh_master and cid is None:
        cid = cliente_id_thalisadm()

    return {
        "login": usuario.get("login") or login,
        "perfil": usuario.get("perfil"),
        "ativo": usuario.get("ativo"),
        "cliente_id": cid,
        "nivel": "master" if eh_master else "cliente",
        "eh_master": eh_master,
    }


def superadmin_eh_master(login):
    login = (login or "").strip()
    if not login:
        return False
    if login.lower() == MASTER_SUPERADMIN_LOGIN.lower():
        return True
    return bool(obter_contexto_superadmin(login).get("eh_master"))


def cliente_id_por_login(login):
    usuario = buscar_usuario_por_login(login)
    if not usuario:
        return None
    cid = usuario.get("cliente_id")
    if (login or "").strip().lower() == MASTER_SUPERADMIN_LOGIN.lower() and cid is None:
        return cliente_id_thalisadm()
    return cid


def cliente_id_por_competicao(competicao):
    competicao = (competicao or "").strip()
    if not competicao:
        return None
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("SELECT cliente_id FROM competicoes WHERE TRIM(LOWER(nome)) = TRIM(LOWER(%s)) LIMIT 1", (competicao,))
                    row = cur.fetchone() or {}
                    return row.get("cliente_id")
                except Exception:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    return None
    except Exception:
        return None


def _where_cliente_sql(alias, cliente_id, incluir_sem_cliente=False):
    prefixo = f"{alias}." if alias else ""
    if cliente_id is None:
        return "", []
    if incluir_sem_cliente:
        return f"WHERE ({prefixo}cliente_id = %s OR {prefixo}cliente_id IS NULL)", [cliente_id]
    return f"WHERE {prefixo}cliente_id = %s", [cliente_id]


def listar_competicoes(login_superadmin=None, incluir_todos=False):
    """Lista competições sem rodar migração pesada nem sincronização global no login."""
    try:
        campos = _campos_competicao(prefixo="c", incluir_senha_organizador=True)
    except Exception:
        campos = ["c.nome", "c.data", "c.status", "c.organizador_login", "u.senha AS organizador_senha"]

    where = ""
    params = []
    incluir_sem_cliente = False

    if login_superadmin and not incluir_todos:
        contexto = obter_contexto_superadmin(login_superadmin)
        cid = contexto.get("cliente_id")
        incluir_sem_cliente = bool(contexto.get("eh_master"))
        where, params = _where_cliente_sql("c", cid, incluir_sem_cliente=incluir_sem_cliente)

    sql = f"""
        SELECT {", ".join(campos)}
        FROM competicoes c
        LEFT JOIN usuarios u ON u.login = c.organizador_login
        {where}
        ORDER BY
            CASE
                WHEN LOWER(COALESCE(c.status, '')) IN ('em andamento', 'em_andamento', 'andamento') THEN 1
                WHEN LOWER(COALESCE(c.status, '')) IN ('em preparação', 'em preparacao', 'preparação', 'preparacao') THEN 2
                WHEN LOWER(COALESCE(c.status, '')) IN ('finalizada', 'finalizado', 'encerrada', 'encerrado') THEN 3
                ELSE 4
            END,
            c.nome
    """

    with conectar() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(sql, tuple(params))
                return cur.fetchall()
            except Exception as e:
                print("AVISO listar_competicoes fallback:", repr(e), flush=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.execute("""
                    SELECT c.nome, c.data, c.status, c.organizador_login, u.senha AS organizador_senha
                    FROM competicoes c
                    LEFT JOIN usuarios u ON u.login = c.organizador_login
                    ORDER BY c.nome
                """)
                return cur.fetchall()


def contar_competicoes(login_superadmin=None):
    where = ""
    params = []
    if login_superadmin:
        contexto = obter_contexto_superadmin(login_superadmin)
        where, params = _where_cliente_sql("", contexto.get("cliente_id"), incluir_sem_cliente=bool(contexto.get("eh_master")))
    with conectar() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"SELECT COUNT(*) AS total FROM competicoes {where}", tuple(params))
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.execute("SELECT COUNT(*) AS total FROM competicoes")
            return (cur.fetchone() or {}).get("total", 0)


def contar_equipes(login_superadmin=None):
    where = ""
    params = []
    if login_superadmin:
        contexto = obter_contexto_superadmin(login_superadmin)
        where, params = _where_cliente_sql("", contexto.get("cliente_id"), incluir_sem_cliente=bool(contexto.get("eh_master")))
    with conectar() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"SELECT COUNT(*) AS total FROM equipes {where}", tuple(params))
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.execute("SELECT COUNT(*) AS total FROM equipes")
            return (cur.fetchone() or {}).get("total", 0)


def contar_partidas(login_superadmin=None):
    where = ""
    params = []
    if login_superadmin:
        contexto = obter_contexto_superadmin(login_superadmin)
        where, params = _where_cliente_sql("", contexto.get("cliente_id"), incluir_sem_cliente=bool(contexto.get("eh_master")))
    with conectar() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"SELECT COUNT(*) AS total FROM partidas {where}", tuple(params))
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                cur.execute("SELECT COUNT(*) AS total FROM partidas")
            return (cur.fetchone() or {}).get("total", 0)


def listar_superadmins_clientes(login_master=None):
    if login_master and not superadmin_eh_master(login_master):
        return []
    garantir_schema_multiempresa_superadmin()
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    u.login, u.nome, u.senha, u.ativo, u.cliente_id,
                    COALESCE(u.superadmin_nivel, 'cliente') AS superadmin_nivel,
                    COALESCE(c.nome, '') AS cliente_nome,
                    COALESCE(c.ativo, TRUE) AS cliente_ativo,
                    c.criado_em
                FROM usuarios u
                LEFT JOIN clientes_sistema c ON c.id = u.cliente_id
                WHERE u.perfil = 'superadmin'
                  AND LOWER(u.login) <> LOWER(%s)
                  AND COALESCE(u.superadmin_nivel, 'cliente') <> 'master'
                ORDER BY COALESCE(c.nome, u.nome), u.nome
            """, (MASTER_SUPERADMIN_LOGIN,))
            return cur.fetchall()


def criar_superadmin_cliente(criador_login, nome_cliente, nome_admin, login_admin=None):
    garantir_schema_multiempresa_superadmin()
    if not superadmin_eh_master(criador_login):
        return {"ok": False, "erro": "Apenas o ThalisADM master pode criar SuperADMs de clientes."}

    nome_cliente = (nome_cliente or "").strip()
    nome_admin = (nome_admin or "").strip()
    login_admin = (login_admin or "").strip()
    if not nome_cliente or not nome_admin:
        return {"ok": False, "erro": "Informe o nome do cliente e o nome do SuperADM."}

    if not login_admin:
        login_admin = f"adm_{_normalizar_texto_base(nome_cliente)}"
    login_admin = re.sub(r"\s+", "_", login_admin)
    login_admin = re.sub(r"[^A-Za-z0-9_.@-]", "", login_admin)[:80]
    if not login_admin:
        login_admin = f"adm_{_normalizar_texto_base(nome_admin)}"

    senha = _gerar_senha_aleatoria(8)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT login FROM usuarios WHERE LOWER(login) = LOWER(%s) LIMIT 1", (login_admin,))
            if cur.fetchone():
                return {"ok": False, "erro": "Este login já está em uso."}

            slug = _gerar_slug_cliente_unico(cur, nome_cliente)
            cur.execute("""
                INSERT INTO clientes_sistema (nome, slug, ativo, criado_por)
                VALUES (%s, %s, TRUE, %s)
                RETURNING id
            """, (nome_cliente, slug, criador_login))
            cliente_id = (cur.fetchone() or {}).get("id")

            cur.execute("""
                INSERT INTO usuarios (
                    login, nome, senha, perfil, ativo, equipe, competicao_vinculada,
                    cliente_id, superadmin_nivel, criado_por
                )
                VALUES (%s, %s, %s, 'superadmin', TRUE, NULL, NULL, %s, 'cliente', %s)
            """, (login_admin, nome_admin, senha, cliente_id, criador_login))
        conn.commit()

    return {"ok": True, "cliente_id": cliente_id, "cliente_nome": nome_cliente, "login": login_admin, "senha": senha, "nome": nome_admin}


def excluir_superadmin_cliente(criador_login, login_alvo):
    garantir_schema_multiempresa_superadmin()
    if not superadmin_eh_master(criador_login):
        return {"ok": False, "erro": "Apenas o ThalisADM master pode excluir SuperADMs de clientes."}
    login_alvo = (login_alvo or "").strip()
    if not login_alvo or login_alvo.lower() == MASTER_SUPERADMIN_LOGIN.lower():
        return {"ok": False, "erro": "Não é permitido excluir o SuperADM master."}
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT login, cliente_id, COALESCE(superadmin_nivel, 'cliente') AS nivel
                FROM usuarios
                WHERE LOWER(login) = LOWER(%s)
                  AND perfil = 'superadmin'
                LIMIT 1
            """, (login_alvo,))
            alvo = cur.fetchone()
            if not alvo:
                return {"ok": False, "erro": "SuperADM não encontrado."}
            if (alvo.get("nivel") or "").lower() == "master":
                return {"ok": False, "erro": "Não é permitido excluir um SuperADM master."}
            cliente_id = alvo.get("cliente_id")
            cur.execute("UPDATE usuarios SET ativo = FALSE WHERE cliente_id = %s", (cliente_id,))
            if cliente_id:
                cur.execute("UPDATE clientes_sistema SET ativo = FALSE WHERE id = %s", (cliente_id,))
        conn.commit()
    return {"ok": True}


def criar_competicao_com_organizador(nome, data=None, status="Em preparação", modo_operacao="simples", tempos_por_set=2, substituicoes_por_set=6, criador_login=None, data_inicio=None, data_fim=None):
    """Cria competição no cliente do SuperADM logado.

    Mantém compatibilidade com chamadas antigas e com a nova tela simplificada.
    """
    garantir_schema_multiempresa_superadmin()

    nome = (nome or "").strip()
    data = (data or data_inicio or "").strip()
    data_inicio = (data_inicio or data or "").strip()
    data_fim = (data_fim or data_inicio or data or "").strip()
    status = (status or "Em preparação").strip()

    contexto = obter_contexto_superadmin(criador_login) if criador_login else {"cliente_id": cliente_id_thalisadm()}
    cid = contexto.get("cliente_id")
    if cid is None:
        cid = cliente_id_thalisadm()

    login_organizador = _gerar_login_unico(_normalizar_login_organizador(nome))
    senha_organizador = _gerar_senha_aleatoria(8)

    colunas_comp = _buscar_colunas_tabela("competicoes")
    colunas_usuarios = _buscar_colunas_tabela("usuarios")

    with conectar() as conn:
        with conn.cursor() as cur:
            campos_user = ["login", "nome", "senha", "perfil", "ativo", "equipe", "competicao_vinculada"]
            vals_user = [login_organizador, f"Organizador - {nome}", senha_organizador, "organizador", True, None, nome]
            if "cliente_id" in colunas_usuarios:
                campos_user.append("cliente_id"); vals_user.append(cid)
            if "criado_por" in colunas_usuarios:
                campos_user.append("criado_por"); vals_user.append(criador_login)
            ph_user = ", ".join(["%s"] * len(vals_user))
            cur.execute(f"INSERT INTO usuarios ({', '.join(campos_user)}) VALUES ({ph_user})", tuple(vals_user))

            campos = ["nome", "data", "status", "organizador_login"]
            valores = [nome, data, status, login_organizador]

            mapa_defaults = {
                "cliente_id": cid,
                "data_inicio": data_inicio,
                "data_fim": data_fim,
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
                "aprovacao_automatica_atletas": False,
                "travada": False,
                "motivo_travamento": "",
                "travada_em": None,
            }

            for campo, default in mapa_defaults.items():
                if campo in colunas_comp and campo not in campos:
                    campos.append(campo)
                    valores.append(default)

            placeholders = ", ".join(["%s"] * len(valores))
            cur.execute(f"INSERT INTO competicoes ({', '.join(campos)}) VALUES ({placeholders})", tuple(valores))
        conn.commit()

    try:
        garantir_quadras_competicao(nome, 1)
    except Exception as e:
        print("AVISO: não foi possível criar quadra padrão da competição:", e, flush=True)

    return {"login": login_organizador, "senha": senha_organizador, "cliente_id": cid}


# =========================================================
# SOLICITAÇÕES / NOTIFICAÇÕES DE EQUIPES E ORGANIZAÇÃO
# =========================================================
def garantir_schema_solicitacoes_notificacoes():
    """Cria estruturas genéricas para pendências da equipe e avisos.

    Tipos de solicitação previstos:
    - exclusao_atleta
    - liberacao_inscricao
    - editar_atleta
    - adicionar_atleta
    - outro
    """
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS solicitacoes_equipes (
                        id SERIAL PRIMARY KEY,
                        competicao TEXT NOT NULL,
                        equipe TEXT NOT NULL,
                        equipe_login TEXT,
                        atleta_id INTEGER,
                        atleta_nome TEXT,
                        tipo TEXT NOT NULL,
                        motivo TEXT,
                        status TEXT DEFAULT 'pendente',
                        resposta TEXT,
                        criado_por TEXT,
                        criado_em TIMESTAMP DEFAULT NOW(),
                        respondido_por TEXT,
                        respondido_em TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS notificacoes_sistema (
                        id SERIAL PRIMARY KEY,
                        competicao TEXT,
                        destino_tipo TEXT NOT NULL,
                        destino_login TEXT,
                        equipe TEXT,
                        titulo TEXT NOT NULL,
                        mensagem TEXT,
                        tipo TEXT DEFAULT 'aviso',
                        lida BOOLEAN DEFAULT FALSE,
                        criado_por TEXT,
                        criado_em TIMESTAMP DEFAULT NOW(),
                        link TEXT
                    )
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_solicitacoes_equipes_comp_status ON solicitacoes_equipes (competicao, status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_solicitacoes_equipes_equipe_status ON solicitacoes_equipes (competicao, equipe, status)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_notificacoes_destino ON notificacoes_sistema (destino_tipo, destino_login, competicao, lida)")
            conn.commit()
        return True
    except Exception as e:
        print('AVISO garantir_schema_solicitacoes_notificacoes:', repr(e), flush=True)
        return False


def criar_notificacao_sistema(competicao, destino_tipo, titulo, mensagem='', destino_login=None, equipe=None, tipo='aviso', criado_por=None, link=None):
    garantir_schema_solicitacoes_notificacoes()
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO notificacoes_sistema
                        (competicao, destino_tipo, destino_login, equipe, titulo, mensagem, tipo, criado_por, link)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                """, (competicao, destino_tipo, destino_login, equipe, titulo, mensagem, tipo, criado_por, link))
                row = cur.fetchone() or {}
            conn.commit()
        return row.get('id')
    except Exception as e:
        print('AVISO criar_notificacao_sistema:', repr(e), flush=True)
        return None


def listar_notificacoes_sistema(competicao=None, destino_tipo=None, destino_login=None, equipe=None, somente_nao_lidas=False, limite=20):
    garantir_schema_solicitacoes_notificacoes()
    filtros=[]; params=[]
    if competicao:
        filtros.append('(competicao = %s OR competicao IS NULL)'); params.append(competicao)
    if destino_tipo:
        filtros.append('destino_tipo = %s'); params.append(destino_tipo)
    if destino_login:
        filtros.append('(destino_login = %s OR destino_login IS NULL)'); params.append(destino_login)
    if equipe:
        filtros.append('(equipe = %s OR equipe IS NULL)'); params.append(equipe)
    if somente_nao_lidas:
        filtros.append('COALESCE(lida, FALSE) = FALSE')
    where = ('WHERE ' + ' AND '.join(filtros)) if filtros else ''
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT *
                    FROM notificacoes_sistema
                    {where}
                    ORDER BY criado_em DESC, id DESC
                    LIMIT %s
                """, tuple(params+[int(limite or 20)]))
                return cur.fetchall() or []
    except Exception as e:
        print('AVISO listar_notificacoes_sistema:', repr(e), flush=True)
        return []


def contar_notificacoes_nao_lidas(competicao=None, destino_tipo=None, destino_login=None, equipe=None):
    return len(listar_notificacoes_sistema(competicao, destino_tipo, destino_login, equipe, True, 50))


def criar_solicitacao_equipe(competicao, equipe, tipo, motivo, atleta_id=None, equipe_login=None, criado_por=None):
    garantir_schema_solicitacoes_notificacoes()
    atleta_nome = None
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                if atleta_id:
                    cur.execute("SELECT nome FROM atletas WHERE id=%s AND competicao=%s LIMIT 1", (atleta_id, competicao))
                    row = cur.fetchone() or {}
                    atleta_nome = row.get('nome')
                cur.execute("""
                    INSERT INTO solicitacoes_equipes
                        (competicao, equipe, equipe_login, atleta_id, atleta_nome, tipo, motivo, criado_por)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING id
                """, (competicao, equipe, equipe_login, atleta_id, atleta_nome, tipo, motivo, criado_por))
                row = cur.fetchone() or {}
                sid = row.get('id')
            conn.commit()
        criar_notificacao_sistema(
            competicao, 'organizador',
            'Nova solicitação de equipe',
            f'{equipe} enviou uma solicitação: {tipo.replace("_", " ")}.',
            equipe=equipe, tipo='solicitacao', criado_por=criado_por,
            link='/equipes/solicitacoes'
        )
        return sid
    except Exception as e:
        print('AVISO criar_solicitacao_equipe:', repr(e), flush=True)
        return None


def listar_solicitacoes_equipes(competicao, status=None, equipe=None, limite=100):
    garantir_schema_solicitacoes_notificacoes()
    filtros=['competicao = %s']; params=[competicao]
    if status:
        filtros.append('status = %s'); params.append(status)
    if equipe:
        filtros.append('equipe = %s'); params.append(equipe)
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT * FROM solicitacoes_equipes
                    WHERE {' AND '.join(filtros)}
                    ORDER BY CASE WHEN status='pendente' THEN 0 ELSE 1 END, criado_em DESC, id DESC
                    LIMIT %s
                """, tuple(params+[int(limite or 100)]))
                return cur.fetchall() or []
    except Exception as e:
        print('AVISO listar_solicitacoes_equipes:', repr(e), flush=True)
        return []


def contar_solicitacoes_pendentes(competicao):
    return len(listar_solicitacoes_equipes(competicao, status='pendente', limite=200))


def responder_solicitacao_equipe(solicitacao_id, aprovado, respondido_por=None, resposta=''):
    garantir_schema_solicitacoes_notificacoes()
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM solicitacoes_equipes WHERE id=%s LIMIT 1", (solicitacao_id,))
                sol = cur.fetchone()
                if not sol:
                    return False, 'Solicitação não encontrada.'
                if sol.get('status') != 'pendente':
                    return False, 'Esta solicitação já foi respondida.'

                status = 'aprovada' if aprovado else 'reprovada'
                if aprovado and sol.get('tipo') == 'exclusao_atleta' and sol.get('atleta_id'):
                    # Executa exclusão autorizada pela organização, sem passar pela trava de prazo da equipe.
                    cur.execute("DELETE FROM atletas WHERE id=%s AND competicao=%s", (sol.get('atleta_id'), sol.get('competicao')))

                if aprovado and sol.get('tipo') in {'liberacao_inscricao', 'adicionar_atleta', 'editar_atleta'}:
                    cur.execute("ALTER TABLE equipes ADD COLUMN IF NOT EXISTS liberacao_extra_inscricao BOOLEAN DEFAULT FALSE")
                    cur.execute("ALTER TABLE equipes ADD COLUMN IF NOT EXISTS liberacao_extra_data TEXT")
                    cur.execute("ALTER TABLE equipes ADD COLUMN IF NOT EXISTS liberacao_extra_hora TEXT")
                    cur.execute("""
                        UPDATE equipes
                        SET liberacao_extra_inscricao = TRUE,
                            liberacao_extra_data = NULL,
                            liberacao_extra_hora = NULL
                        WHERE competicao = %s
                          AND LOWER(TRIM(nome)) = LOWER(TRIM(%s))
                    """, (sol.get('competicao'), sol.get('equipe')))

                cur.execute("""
                    UPDATE solicitacoes_equipes
                    SET status=%s, resposta=%s, respondido_por=%s, respondido_em=NOW()
                    WHERE id=%s
                """, (status, resposta, respondido_por, solicitacao_id))
            conn.commit()

        msg = 'Sua solicitação foi aprovada.' if aprovado else 'Sua solicitação foi reprovada.'
        if resposta:
            msg += ' Resposta: ' + resposta
        criar_notificacao_sistema(
            sol.get('competicao'), 'equipe',
            'Solicitação ' + ('aprovada' if aprovado else 'reprovada'),
            msg,
            destino_login=sol.get('equipe_login'), equipe=sol.get('equipe'), tipo='resposta_solicitacao', criado_por=respondido_por,
            link='/painel-equipe/inicio'
        )
        return True, 'Solicitação respondida com sucesso.'
    except Exception as e:
        print('AVISO responder_solicitacao_equipe:', repr(e), flush=True)
        return False, f'Erro ao responder solicitação: {e}'


def marcar_notificacoes_lidas(destino_tipo, destino_login=None, competicao=None):
    garantir_schema_solicitacoes_notificacoes()
    try:
        filtros=['destino_tipo=%s']; params=[destino_tipo]
        if destino_login:
            filtros.append('(destino_login=%s OR destino_login IS NULL)'); params.append(destino_login)
        if competicao:
            filtros.append('(competicao=%s OR competicao IS NULL)'); params.append(competicao)
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE notificacoes_sistema SET lida=TRUE WHERE {' AND '.join(filtros)}", tuple(params))
            conn.commit()
        return True
    except Exception as e:
        print('AVISO marcar_notificacoes_lidas:', repr(e), flush=True)
        return False


# =========================================================
# FLUXO DE CONFIGURAÇÃO INICIAL DA COMPETIÇÃO
# =========================================================
def garantir_schema_fluxo_configuracao_competicoes():
    """Cria flags de etapas da configuração inicial.

    Observação importante: avanço/chaveamento NÃO é obrigatório para liberar
    a operação inicial. O organizador pode configurar o avanço depois, antes de
    gerar o mata-mata.
    """
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    ALTER TABLE competicoes
                    ADD COLUMN IF NOT EXISTS config_dados_salva BOOLEAN DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS config_quadras_salva BOOLEAN DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS config_estrutura_salva BOOLEAN DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS config_regras_salva BOOLEAN DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS config_classificacao_salva BOOLEAN DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS config_avanco_salva BOOLEAN DEFAULT FALSE,
                    ADD COLUMN IF NOT EXISTS configuracao_inicial_concluida BOOLEAN DEFAULT FALSE
                """)

                cur.execute("""
                    UPDATE competicoes
                    SET configuracao_inicial_concluida = COALESCE(config_dados_salva, FALSE)
                        AND COALESCE(config_quadras_salva, FALSE)
                        AND COALESCE(config_estrutura_salva, FALSE)
                        AND COALESCE(config_regras_salva, FALSE)
                        AND COALESCE(config_classificacao_salva, FALSE)
                    WHERE configuracao_inicial_concluida IS DISTINCT FROM (
                        COALESCE(config_dados_salva, FALSE)
                        AND COALESCE(config_quadras_salva, FALSE)
                        AND COALESCE(config_estrutura_salva, FALSE)
                        AND COALESCE(config_regras_salva, FALSE)
                        AND COALESCE(config_classificacao_salva, FALSE)
                    )
                """)
            conn.commit()
        try:
            _CACHE_COLUNAS.pop("competicoes", None)
        except Exception:
            pass
        return True
    except Exception as e:
        print("AVISO garantir_schema_fluxo_configuracao_competicoes:", repr(e), flush=True)
        return False


def _config_inicial_bool(valor):
    return bool(valor) if valor is not None else False


def status_configuracao_inicial_competicao(nome_competicao):
    """Retorna status das etapas.

    Avanço é opcional e não entra na conclusão inicial.
    """
    nome_competicao = (nome_competicao or "").strip()
    status_vazio = {
        "dados": False,
        "quadras": False,
        "estrutura": False,
        "regras": False,
        "classificacao": False,
        "avanco": False,
        "concluida": False,
    }
    if not nome_competicao:
        return status_vazio

    garantir_schema_fluxo_configuracao_competicoes()

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COALESCE(config_dados_salva, FALSE) AS dados,
                        COALESCE(config_quadras_salva, FALSE) AS quadras,
                        COALESCE(config_estrutura_salva, FALSE) AS estrutura,
                        COALESCE(config_regras_salva, FALSE) AS regras,
                        COALESCE(config_classificacao_salva, FALSE) AS classificacao,
                        COALESCE(config_avanco_salva, FALSE) AS avanco,
                        COALESCE(configuracao_inicial_concluida, FALSE) AS concluida
                    FROM competicoes
                    WHERE nome = %s
                    LIMIT 1
                """, (nome_competicao,))
                row = cur.fetchone() or {}

        dados = _config_inicial_bool(row.get("dados"))
        quadras = _config_inicial_bool(row.get("quadras"))
        estrutura = _config_inicial_bool(row.get("estrutura"))
        regras = _config_inicial_bool(row.get("regras"))
        classificacao = _config_inicial_bool(row.get("classificacao"))
        avanco = _config_inicial_bool(row.get("avanco"))
        concluida = dados and quadras and estrutura and regras and classificacao

        # Corrige banco antigo automaticamente.
        if concluida != _config_inicial_bool(row.get("concluida")):
            with conectar() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE competicoes
                        SET configuracao_inicial_concluida = %s
                        WHERE nome = %s
                    """, (concluida, nome_competicao))
                conn.commit()

        return {
            "dados": dados,
            "quadras": quadras,
            "estrutura": estrutura,
            "regras": regras,
            "classificacao": classificacao,
            "avanco": avanco,
            "concluida": concluida,
        }
    except Exception as e:
        print("AVISO status_configuracao_inicial_competicao:", repr(e), flush=True)
        return status_vazio


def marcar_etapa_configuracao_competicao(nome_competicao, etapa, salva=True):
    """Marca uma etapa como salva e atualiza conclusão inicial.

    Etapas obrigatórias: dados, quadras, estrutura, regras e classificação.
    Etapa opcional: avanco.
    """
    nome_competicao = (nome_competicao or "").strip()
    etapa = (etapa or "").strip().lower()
    if not nome_competicao:
        return False

    mapa = {
        "dados": "config_dados_salva",
        "quadras": "config_quadras_salva",
        "estrutura": "config_estrutura_salva",
        "regras": "config_regras_salva",
        "pontuacao": "config_classificacao_salva",
        "classificacao": "config_classificacao_salva",
        "avanco": "config_avanco_salva",
    }
    coluna = mapa.get(etapa)
    if not coluna:
        return False

    garantir_schema_fluxo_configuracao_competicoes()

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE competicoes
                    SET {coluna} = %s
                    WHERE nome = %s
                """, (bool(salva), nome_competicao))

                cur.execute("""
                    UPDATE competicoes
                    SET configuracao_inicial_concluida = COALESCE(config_dados_salva, FALSE)
                        AND COALESCE(config_quadras_salva, FALSE)
                        AND COALESCE(config_estrutura_salva, FALSE)
                        AND COALESCE(config_regras_salva, FALSE)
                        AND COALESCE(config_classificacao_salva, FALSE)
                    WHERE nome = %s
                """, (nome_competicao,))
            conn.commit()
        return True
    except Exception as e:
        print("AVISO marcar_etapa_configuracao_competicao:", repr(e), flush=True)
        return False


# =========================================================
# JOGO AVULSO / JOGO RÁPIDO - SESSÃO PERSISTENTE DO DIA
# =========================================================
_JOGO_AVULSO_SCHEMA_OK = False


def _json_dumps_jogo_avulso(payload):
    return json.dumps(payload or {}, ensure_ascii=False, default=str)


def _json_loads_jogo_avulso(valor, padrao=None):
    if padrao is None:
        padrao = {}
    if valor is None:
        return padrao
    if isinstance(valor, (dict, list)):
        return valor
    try:
        return json.loads(valor)
    except Exception:
        return padrao


def garantir_schema_jogo_avulso():
    """Cria tabelas leves para o Jogo Rápido funcionar no Render.

    O estado continua em cache/socket durante o jogo, mas uma cópia persistente
    fica no banco. Assim o jogo não expira quando o Render reinicia ou quando o
    celular abre outra sessão.
    """
    global _JOGO_AVULSO_SCHEMA_OK
    if _JOGO_AVULSO_SCHEMA_OK:
        return True

    with _SCHEMA_LOCK:
        if _JOGO_AVULSO_SCHEMA_OK:
            return True
        try:
            with conectar() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS jogo_avulso_sessoes (
                            id SERIAL PRIMARY KEY,
                            cpf_apontador TEXT NOT NULL,
                            dia DATE NOT NULL,
                            pin TEXT NOT NULL,
                            status TEXT DEFAULT 'ativa',
                            memoria_json JSONB DEFAULT '{}'::jsonb,
                            jogo_atual TEXT DEFAULT '',
                            criado_em TIMESTAMP DEFAULT NOW(),
                            atualizado_em TIMESTAMP DEFAULT NOW(),
                            encerrado_em TIMESTAMP
                        )
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_jogo_avulso_sessoes_cpf_dia
                        ON jogo_avulso_sessoes (cpf_apontador, dia, status)
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_jogo_avulso_sessoes_pin
                        ON jogo_avulso_sessoes (pin, status)
                    """)
                    cur.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS ux_jogo_avulso_sessao_ativa_dia
                        ON jogo_avulso_sessoes (cpf_apontador, dia)
                        WHERE status = 'ativa'
                    """)

                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS jogo_avulso_partidas (
                            codigo TEXT PRIMARY KEY,
                            sessao_id INTEGER REFERENCES jogo_avulso_sessoes(id) ON DELETE SET NULL,
                            cpf_apontador TEXT NOT NULL,
                            pin TEXT NOT NULL,
                            estado_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                            status TEXT DEFAULT 'papeleta',
                            equipe_a TEXT DEFAULT '',
                            equipe_b TEXT DEFAULT '',
                            criado_em TIMESTAMP DEFAULT NOW(),
                            atualizado_em TIMESTAMP DEFAULT NOW(),
                            finalizada_em TIMESTAMP
                        )
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_jogo_avulso_partidas_pin_status
                        ON jogo_avulso_partidas (pin, status, atualizado_em DESC)
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_jogo_avulso_partidas_cpf_status
                        ON jogo_avulso_partidas (cpf_apontador, status, atualizado_em DESC)
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_jogo_avulso_partidas_sessao
                        ON jogo_avulso_partidas (sessao_id, atualizado_em DESC)
                    """)

                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS jogo_avulso_equipes_memoria (
                            sessao_id INTEGER NOT NULL REFERENCES jogo_avulso_sessoes(id) ON DELETE CASCADE,
                            nome_equipe TEXT NOT NULL,
                            numeros_json JSONB DEFAULT '[]'::jsonb,
                            jogos INTEGER DEFAULT 0,
                            atualizado_em TIMESTAMP DEFAULT NOW(),
                            PRIMARY KEY (sessao_id, nome_equipe)
                        )
                    """)
                conn.commit()
            _JOGO_AVULSO_SCHEMA_OK = True
            return True
        except Exception as e:
            print("AVISO garantir_schema_jogo_avulso:", repr(e), flush=True)
            return False


def _jogo_avulso_pin_unico(cur):
    for _ in range(160):
        pin = str(random.randint(1000, 9999))
        cur.execute("""
            SELECT 1
            FROM jogo_avulso_sessoes
            WHERE pin = %s
              AND status = 'ativa'
            LIMIT 1
        """, (pin,))
        if not cur.fetchone():
            return pin
    return str(random.randint(1000, 9999))


def _linha_sessao_jogo_avulso_para_dict(row):
    if not row:
        return None
    memoria = _json_loads_jogo_avulso(row.get("memoria_json"), {})
    if not isinstance(memoria, dict):
        memoria = {}
    memoria.setdefault("equipes", {})
    return {
        "id": row.get("id"),
        "dia": str(row.get("dia") or ""),
        "cpf": row.get("cpf_apontador") or "",
        "pin": str(row.get("pin") or ""),
        "status": row.get("status") or "ativa",
        "equipes": memoria.get("equipes") or {},
        "jogos": memoria.get("jogos") or [],
        "jogo_atual": row.get("jogo_atual") or memoria.get("jogo_atual") or "",
        "criado_em": str(row.get("criado_em") or ""),
        "atualizado_em": str(row.get("atualizado_em") or ""),
    }


def obter_sessao_jogo_avulso_dia(cpf_apontador, criar=True):
    cpf_apontador = str(cpf_apontador or "").strip()
    if not cpf_apontador:
        return None
    garantir_schema_jogo_avulso()
    hoje_sql = datetime.now().date()

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT *
                    FROM jogo_avulso_sessoes
                    WHERE cpf_apontador = %s
                      AND dia = %s
                      AND status = 'ativa'
                    ORDER BY id DESC
                    LIMIT 1
                """, (cpf_apontador, hoje_sql))
                row = cur.fetchone()

                if not row and criar:
                    pin = _jogo_avulso_pin_unico(cur)
                    memoria = {"equipes": {}, "jogos": [], "jogo_atual": ""}
                    cur.execute("""
                        INSERT INTO jogo_avulso_sessoes
                            (cpf_apontador, dia, pin, status, memoria_json, jogo_atual, criado_em, atualizado_em)
                        VALUES (%s, %s, %s, 'ativa', %s::jsonb, '', NOW(), NOW())
                        RETURNING *
                    """, (cpf_apontador, hoje_sql, pin, _json_dumps_jogo_avulso(memoria)))
                    row = cur.fetchone()
                    conn.commit()

                sess = _linha_sessao_jogo_avulso_para_dict(row)
                if sess:
                    # Completa jogos a partir da tabela de partidas para não depender só do JSON.
                    try:
                        cur.execute("""
                            SELECT codigo, equipe_a, equipe_b, status, atualizado_em
                            FROM jogo_avulso_partidas
                            WHERE sessao_id = %s
                            ORDER BY atualizado_em DESC
                            LIMIT 120
                        """, (sess.get("id"),))
                        jogos = []
                        for j in cur.fetchall() or []:
                            jogos.append({
                                "codigo": j.get("codigo"),
                                "equipe_a": j.get("equipe_a") or "Equipe A",
                                "equipe_b": j.get("equipe_b") or "Equipe B",
                                "status": j.get("status") or "",
                                "atualizado_em": str(j.get("atualizado_em") or ""),
                            })
                        if jogos:
                            sess["jogos"] = jogos
                    except Exception:
                        pass
                return sess
    except Exception as e:
        print("AVISO obter_sessao_jogo_avulso_dia:", repr(e), flush=True)
        return None


def salvar_sessao_jogo_avulso_dia(cpf_apontador, sessao):
    cpf_apontador = str(cpf_apontador or "").strip()
    if not cpf_apontador or not isinstance(sessao, dict):
        return False
    garantir_schema_jogo_avulso()
    try:
        atual = obter_sessao_jogo_avulso_dia(cpf_apontador, criar=True) or {}
        sessao_id = sessao.get("id") or atual.get("id")
        pin = str(sessao.get("pin") or atual.get("pin") or "").strip()
        memoria = {
            "equipes": sessao.get("equipes") or {},
            "jogos": sessao.get("jogos") or [],
            "jogo_atual": sessao.get("jogo_atual") or "",
        }
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE jogo_avulso_sessoes
                    SET pin = COALESCE(NULLIF(%s, ''), pin),
                        memoria_json = %s::jsonb,
                        jogo_atual = %s,
                        atualizado_em = NOW()
                    WHERE id = %s
                """, (pin, _json_dumps_jogo_avulso(memoria), memoria.get("jogo_atual") or "", sessao_id))
            conn.commit()
        return True
    except Exception as e:
        print("AVISO salvar_sessao_jogo_avulso_dia:", repr(e), flush=True)
        return False


def _status_jogo_avulso_finalizado(status):
    return str(status or "").strip().lower() in {"finalizada", "finalizado", "encerrada", "encerrado"}


def salvar_partida_jogo_avulso(codigo, cpf_apontador, estado):
    codigo = str(codigo or "").strip().upper()
    cpf_apontador = str(cpf_apontador or "").strip()
    estado = estado or {}
    if not codigo or not cpf_apontador:
        return False
    garantir_schema_jogo_avulso()
    try:
        sess = obter_sessao_jogo_avulso_dia(cpf_apontador, criar=True) or {}
        sessao_id = sess.get("id")
        pin = str(estado.get("pin_arbitragem") or sess.get("pin") or "").strip()
        status = str(estado.get("status_jogo") or estado.get("fase_partida") or "papeleta").strip() or "papeleta"
        equipe_a = str(estado.get("equipe_a") or "Equipe A")[:80]
        equipe_b = str(estado.get("equipe_b") or "Equipe B")[:80]
        finalizada = _status_jogo_avulso_finalizado(status)
        payload = _json_dumps_jogo_avulso(estado)

        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO jogo_avulso_partidas
                        (codigo, sessao_id, cpf_apontador, pin, estado_json, status, equipe_a, equipe_b, criado_em, atualizado_em, finalizada_em)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, NOW(), NOW(), CASE WHEN %s THEN NOW() ELSE NULL END)
                    ON CONFLICT (codigo)
                    DO UPDATE SET
                        sessao_id = EXCLUDED.sessao_id,
                        cpf_apontador = EXCLUDED.cpf_apontador,
                        pin = EXCLUDED.pin,
                        estado_json = EXCLUDED.estado_json,
                        status = EXCLUDED.status,
                        equipe_a = EXCLUDED.equipe_a,
                        equipe_b = EXCLUDED.equipe_b,
                        atualizado_em = NOW(),
                        finalizada_em = CASE WHEN %s THEN COALESCE(jogo_avulso_partidas.finalizada_em, NOW()) ELSE jogo_avulso_partidas.finalizada_em END
                """, (codigo, sessao_id, cpf_apontador, pin, payload, status, equipe_a, equipe_b, finalizada, finalizada))
                if sessao_id:
                    cur.execute("""
                        UPDATE jogo_avulso_sessoes
                        SET jogo_atual = %s,
                            atualizado_em = NOW()
                        WHERE id = %s
                    """, (codigo, sessao_id))
            conn.commit()
        return True
    except Exception as e:
        print("AVISO salvar_partida_jogo_avulso:", repr(e), flush=True)
        return False


def buscar_partida_jogo_avulso(codigo):
    codigo = str(codigo or "").strip().upper()
    if not codigo:
        return None
    garantir_schema_jogo_avulso()
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT p.*, s.dia, s.status AS status_sessao
                    FROM jogo_avulso_partidas p
                    LEFT JOIN jogo_avulso_sessoes s ON s.id = p.sessao_id
                    WHERE p.codigo = %s
                    LIMIT 1
                """, (codigo,))
                row = cur.fetchone()
                if not row:
                    return None
                estado = _json_loads_jogo_avulso(row.get("estado_json"), {})
                if not isinstance(estado, dict):
                    estado = {}
                return {
                    "codigo": row.get("codigo") or codigo,
                    "estado": estado,
                    "criado_em": row.get("criado_em"),
                    "apontador": row.get("cpf_apontador") or "",
                    "pin": row.get("pin") or "",
                    "status": row.get("status") or "",
                }
    except Exception as e:
        print("AVISO buscar_partida_jogo_avulso:", repr(e), flush=True)
        return None


def buscar_jogo_avulso_ativo_apontador(cpf_apontador):
    cpf_apontador = str(cpf_apontador or "").strip()
    if not cpf_apontador:
        return None
    garantir_schema_jogo_avulso()
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT *
                    FROM jogo_avulso_partidas
                    WHERE cpf_apontador = %s
                      AND LOWER(COALESCE(status, '')) NOT IN ('finalizada', 'finalizado', 'encerrada', 'encerrado')
                    ORDER BY atualizado_em DESC
                    LIMIT 1
                """, (cpf_apontador,))
                row = cur.fetchone()
                if not row:
                    return None
                estado = _json_loads_jogo_avulso(row.get("estado_json"), {})
                return {"codigo": row.get("codigo"), "estado": estado}
    except Exception as e:
        print("AVISO buscar_jogo_avulso_ativo_apontador:", repr(e), flush=True)
        return None


def buscar_partida_jogo_avulso_por_pin(pin):
    pin = "".join(ch for ch in str(pin or "") if ch.isdigit())
    if len(pin) != 4:
        return None
    garantir_schema_jogo_avulso()
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                # Primeiro tenta jogo não finalizado do PIN.
                cur.execute("""
                    SELECT *
                    FROM jogo_avulso_partidas
                    WHERE pin = %s
                      AND LOWER(COALESCE(status, '')) NOT IN ('finalizada', 'finalizado', 'encerrada', 'encerrado')
                    ORDER BY atualizado_em DESC
                    LIMIT 1
                """, (pin,))
                row = cur.fetchone()

                # Se não houver jogo ativo, pega o último jogo da sessão ativa para manter o PIN válido.
                if not row:
                    cur.execute("""
                        SELECT p.*
                        FROM jogo_avulso_sessoes s
                        LEFT JOIN jogo_avulso_partidas p ON p.sessao_id = s.id
                        WHERE s.pin = %s
                          AND s.status = 'ativa'
                        ORDER BY p.atualizado_em DESC NULLS LAST
                        LIMIT 1
                    """, (pin,))
                    row = cur.fetchone()

                if not row or not row.get("codigo"):
                    # Sessão existe, mas ainda não tem jogo.
                    cur.execute("""
                        SELECT *
                        FROM jogo_avulso_sessoes
                        WHERE pin = %s
                          AND status = 'ativa'
                        ORDER BY atualizado_em DESC
                        LIMIT 1
                    """, (pin,))
                    sess = cur.fetchone()
                    if sess:
                        return {
                            "codigo": "",
                            "pin": pin,
                            "tipo": "avulso",
                            "status_jogo": "aguardando",
                            "equipe_a": "Equipe A",
                            "equipe_b": "Equipe B",
                        }
                    return None

                estado = _json_loads_jogo_avulso(row.get("estado_json"), {})
                return {
                    "codigo": row.get("codigo"),
                    "pin": pin,
                    "tipo": "avulso",
                    "equipe_a": estado.get("equipe_a") or row.get("equipe_a") or "Equipe A",
                    "equipe_b": estado.get("equipe_b") or row.get("equipe_b") or "Equipe B",
                    "status_jogo": estado.get("status_jogo") or row.get("status") or "",
                    "estado": estado,
                }
    except Exception as e:
        print("AVISO buscar_partida_jogo_avulso_por_pin:", repr(e), flush=True)
        return None


def registrar_memoria_jogo_avulso_estado(estado):
    estado = estado or {}
    cpf = str(estado.get("apontador") or "").strip()
    if not cpf:
        return False
    sess = obter_sessao_jogo_avulso_dia(cpf, criar=True)
    if not sess:
        return False

    def _normalizar_nome(nome):
        return " ".join(str(nome or "").strip().split())[:60]

    def _nums_lado(lado):
        nums = []
        for chave in (f"rotacao_{lado.lower()}", f"banco_{lado.lower()}", f"numeros_{lado.lower()}"):
            valor = estado.get(chave) or []
            if isinstance(valor, dict):
                valor = list(valor.values())
            for item in valor:
                if isinstance(item, dict):
                    item = item.get("numero") or item.get("camisa") or item.get("numero_camisa") or ""
                txt = str(item or "").strip()
                if txt.isdigit() and txt not in nums:
                    nums.append(txt)
        return sorted(nums, key=lambda x: int(x))

    equipes = sess.get("equipes") or {}
    for lado in ("A", "B"):
        nome = _normalizar_nome(estado.get("equipe_a") if lado == "A" else estado.get("equipe_b"))
        if not nome or nome.lower() in {"equipe a", "equipe b"}:
            continue
        item = equipes.get(nome) or {"nome": nome, "numeros": [], "jogos": 0}
        nums = [str(n) for n in item.get("numeros", []) if str(n).isdigit()] + _nums_lado(lado)
        item["numeros"] = sorted(set(nums), key=lambda x: int(x))
        item["atualizado_em"] = datetime.now().isoformat()
        equipes[nome] = item

    codigo = str(estado.get("codigo") or "").strip().upper()
    jogos = list(sess.get("jogos") or [])
    if codigo:
        sess["jogo_atual"] = codigo
        encontrado = False
        for j in jogos:
            if str(j.get("codigo") or "").upper() == codigo:
                j.update({
                    "equipe_a": estado.get("equipe_a") or j.get("equipe_a") or "Equipe A",
                    "equipe_b": estado.get("equipe_b") or j.get("equipe_b") or "Equipe B",
                    "status": estado.get("status_jogo") or estado.get("fase_partida") or j.get("status") or "",
                    "atualizado_em": datetime.now().isoformat(),
                })
                encontrado = True
                break
        if not encontrado:
            jogos.insert(0, {
                "codigo": codigo,
                "equipe_a": estado.get("equipe_a") or "Equipe A",
                "equipe_b": estado.get("equipe_b") or "Equipe B",
                "status": estado.get("status_jogo") or estado.get("fase_partida") or "",
                "criado_em": datetime.now().isoformat(),
            })
        sess["jogos"] = jogos[:120]

    sess["equipes"] = equipes
    return salvar_sessao_jogo_avulso_dia(cpf, sess)


def encerrar_sessao_jogo_avulso(cpf_apontador):
    cpf_apontador = str(cpf_apontador or "").strip()
    if not cpf_apontador:
        return False
    garantir_schema_jogo_avulso()
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE jogo_avulso_sessoes
                    SET status = 'encerrada',
                        encerrado_em = NOW(),
                        atualizado_em = NOW()
                    WHERE cpf_apontador = %s
                      AND dia = %s
                      AND status = 'ativa'
                """, (cpf_apontador, datetime.now().date()))
            conn.commit()
        return True
    except Exception as e:
        print("AVISO encerrar_sessao_jogo_avulso:", repr(e), flush=True)
        return False
