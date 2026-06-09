from flask import Blueprint, jsonify, session
import os
import time

from banco import (
    buscar_competicao_por_organizador,
    listar_equipes_da_competicao,
    listar_partidas,
    listar_grupos,
    conectar,
    criar_tabela_atletas,
)
from routes.utils import exigir_perfil

bootstrap_bp = Blueprint("bootstrap", __name__)

_BOOTSTRAP_TTL = int(os.environ.get("BOOTSTRAP_ORGANIZADOR_TTL", "20") or 20)
_BOOTSTRAP_CACHE = {}


def _agora():
    return time.time()


def _cache_get(chave):
    item = _BOOTSTRAP_CACHE.get(chave)
    if not item:
        return None
    criado, valor = item
    if (_agora() - criado) > _BOOTSTRAP_TTL:
        _BOOTSTRAP_CACHE.pop(chave, None)
        return None
    return valor


def _cache_set(chave, valor):
    if len(_BOOTSTRAP_CACHE) > 80:
        _BOOTSTRAP_CACHE.clear()
    _BOOTSTRAP_CACHE[chave] = (_agora(), valor)
    return valor


def _listar_atletas_competicao_leve(nome_competicao):
    criar_tabela_atletas()
    """Uma consulta só para os atletas que o painel do organizador usa.

    Não traz eventos ponto-a-ponto nem dados pesados; é só o essencial para
    numeracão, conferência e telas de equipe.
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    nome,
                    cpf,
                    data_nascimento,
                    numero,
                    equipe,
                    competicao,
                    status,
                    equipe_login,
                    equipe_id
                FROM atletas
                WHERE competicao = %s
                ORDER BY equipe,
                         CASE WHEN COALESCE(numero::TEXT, '') ~ '^[0-9]+$' THEN numero ELSE 999999 END,
                         nome
            """, (nome_competicao,))
            return cur.fetchall() or []


def _listar_quadras_competicao_leve(nome_competicao):
    """Lista quadras sem criar/alterar estrutura durante o request."""
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, competicao, nome, local, ordem, ativa, pin_arbitragem
                    FROM competicao_quadras
                    WHERE competicao = %s
                      AND COALESCE(ativa, TRUE) = TRUE
                    ORDER BY COALESCE(ordem, 9999), id
                """, (nome_competicao,))
                return cur.fetchall() or []
    except Exception:
        return []


@bootstrap_bp.route("/api/bootstrap/organizador")
@exigir_perfil("organizador")
def bootstrap_organizador():
    """Pacote inicial leve do painel do organizador.

    A ideia é o navegador pedir uma vez ao entrar no painel. O servidor mantém
    cache curto por competição para evitar várias consultas repetidas quando o
    organizador troca de aba. Socket continua sendo responsável pelo ao vivo.
    """
    usuario = (session.get("usuario") or "").strip()
    competicao = buscar_competicao_por_organizador(usuario)

    if not competicao:
        return jsonify({"ok": False, "erro": "Competição não encontrada."}), 404

    nome_competicao = (competicao.get("nome") or "").strip()
    chave = (usuario, nome_competicao)
    cached = _cache_get(chave)
    if cached is not None:
        return jsonify(cached)

    payload = {
        "ok": True,
        "cache_ttl": _BOOTSTRAP_TTL,
        "competicao": competicao,
        "equipes": listar_equipes_da_competicao(nome_competicao) or [],
        "atletas": _listar_atletas_competicao_leve(nome_competicao),
        "partidas": listar_partidas(nome_competicao) or [],
        "grupos": listar_grupos(nome_competicao) or [],
        "quadras": _listar_quadras_competicao_leve(nome_competicao),
    }

    return jsonify(_cache_set(chave, payload))
