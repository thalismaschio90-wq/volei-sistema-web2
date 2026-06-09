from flask import Blueprint, jsonify, request
from routes.utils import exigir_perfil
from banco import conectar
import time

_CONFIG_CACHE = {}
_CONFIG_CACHE_TTL = 30

offline_config_bp = Blueprint("offline_config", __name__)


def criar_tabela_configuracoes_sistema():
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS configuracoes_sistema (
                    chave TEXT PRIMARY KEY,
                    valor TEXT NOT NULL DEFAULT '',
                    atualizado_em TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        conn.commit()


def obter_configuracao_sistema(chave, padrao=""):
    # Não cria tabela em leitura normal. Se a tabela não existir, usa padrão.
    item = _CONFIG_CACHE.get(chave)
    agora = time.time()
    if item and (agora - item[0]) < _CONFIG_CACHE_TTL:
        return item[1]

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT valor
                      FROM configuracoes_sistema
                     WHERE chave = %s
                     LIMIT 1
                """, (chave,))
                row = cur.fetchone()
    except Exception as e:
        print("AVISO obter_configuracao_sistema:", repr(e))
        return padrao

    if not row:
        valor = padrao
    elif isinstance(row, dict):
        valor = row.get("valor", padrao)
    else:
        valor = row[0] if row else padrao

    _CONFIG_CACHE[chave] = (agora, valor)
    return valor


def salvar_configuracao_sistema(chave, valor):
    criar_tabela_configuracoes_sistema()
    _CONFIG_CACHE.pop(chave, None)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO configuracoes_sistema (chave, valor, atualizado_em)
                VALUES (%s, %s, NOW())
                ON CONFLICT (chave)
                DO UPDATE SET
                    valor = EXCLUDED.valor,
                    atualizado_em = NOW()
            """, (chave, str(valor)))
        conn.commit()


def offline_global_habilitado():
    return str(
        obter_configuracao_sistema("offline_habilitado", "0")
    ).strip().lower() in {"1", "true", "sim", "s", "on"}


@offline_config_bp.route("/superadmin/config/offline", methods=["GET"])
@exigir_perfil("superadmin")
def superadmin_config_offline_get():
    return jsonify({
        "ok": True,
        "offline_habilitado": offline_global_habilitado()
    })


@offline_config_bp.route("/superadmin/config/offline", methods=["POST"])
@exigir_perfil("superadmin")
def superadmin_config_offline_post():
    dados = request.get_json(silent=True) or {}
    ativo = bool(dados.get("offline_habilitado"))

    salvar_configuracao_sistema(
        "offline_habilitado",
        "1" if ativo else "0"
    )

    return jsonify({
        "ok": True,
        "offline_habilitado": ativo
    })
