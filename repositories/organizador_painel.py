"""Consultas consolidadas do painel inicial do organizador.

Não executa DDL durante a navegação. A estrutura de notificações, solicitações e
campos de configuração deve ser preparada no bootstrap da aplicação.
"""
from __future__ import annotations

from typing import Any, Callable


def _dict(row: Any) -> dict:
    return dict(row or {})


def _nome_competicao(item: Any) -> str:
    if isinstance(item, dict):
        valor = item.get("competicao") or item.get("nome") or item.get("nome_competicao") or item.get("titulo")
    else:
        valor = item
    return str(valor or "").strip()


def buscar_painel_organizador(
    login: str,
    competicao_preferida: str = "",
    *,
    conectar_fn: Callable,
) -> dict:
    """Monta o contexto do painel usando uma única conexão PostgreSQL."""
    login = str(login or "").strip()
    preferida = str(competicao_preferida or "").strip()
    vazio = {
        "competicoes": [],
        "competicao_atual": "",
        "nomes_competicoes": [],
        "status_config": {},
        "solicitacoes_pendentes": 0,
        "ultimas_solicitacoes": [],
        "notificacoes_organizador": [],
    }
    if not login:
        return vazio

    with conectar_fn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT nome
                FROM competicoes
                WHERE organizador_login = %s
                ORDER BY nome
                """,
                (login,),
            )
            competicoes = [dict(item) for item in (cur.fetchall() or [])]
            nomes = [_nome_competicao(item) for item in competicoes]
            nomes = [nome for nome in nomes if nome]
            atual = preferida if preferida in nomes else (nomes[0] if nomes else "")

            status_config = {}
            solicitacoes_pendentes = 0
            ultimas_solicitacoes = []
            notificacoes = []

            if atual:
                cur.execute(
                    """
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
                    """,
                    (atual,),
                )
                status_config = _dict(cur.fetchone())
                obrigatorias = ("dados", "quadras", "estrutura", "regras", "classificacao")
                status_config["concluida"] = all(bool(status_config.get(campo)) for campo in obrigatorias)

                cur.execute(
                    """
                    SELECT
                        id,
                        equipe,
                        tipo,
                        atleta_nome,
                        criado_em,
                        COUNT(*) OVER() AS total_pendentes
                    FROM solicitacoes_equipes
                    WHERE competicao = %s AND status = 'pendente'
                    ORDER BY criado_em DESC, id DESC
                    LIMIT 5
                    """,
                    (atual,),
                )
                ultimas_solicitacoes = [dict(item) for item in (cur.fetchall() or [])]
                if ultimas_solicitacoes:
                    solicitacoes_pendentes = int(ultimas_solicitacoes[0].pop("total_pendentes", 0) or 0)

                # O painel inicial não exibe a lista de notificações do sistema.
                # Ela deve ser carregada apenas na tela/endpoint que efetivamente
                # a utiliza, evitando uma consulta extra em todo acesso ao painel.
                notificacoes = []

    return {
        "competicoes": competicoes,
        "competicao_atual": atual,
        "nomes_competicoes": nomes,
        "status_config": status_config,
        "solicitacoes_pendentes": solicitacoes_pendentes,
        "ultimas_solicitacoes": ultimas_solicitacoes,
        "notificacoes_organizador": notificacoes,
    }
