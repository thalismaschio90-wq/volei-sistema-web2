"""Persistência de criação e vínculo de equipes às competições.

As operações deste módulo são transacionais e preservam os retornos públicos do
banco.py. Dependências legadas são resolvidas tardiamente para evitar importação
circular durante a migração gradual.
"""
from __future__ import annotations

from core.security import gerar_hash_senha

from typing import Any

from rules.equipes import preparar_equipe_competicao, validar_equipe_competicao
from repositories.conexao import conectar
from repositories.equipes_contexto import (
    buscar_equipe_global_por_nome,
    cliente_id_por_competicao,
    gerar_login_unico,
    gerar_senha_aleatoria,
    normalizar_login_equipe,
)


def _buscar_equipe_por_login(cur, login_equipe: str, cliente_id: int | None):
    cur.execute(
        """
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
        """,
        (login_equipe, cliente_id, cliente_id),
    )
    return cur.fetchone()


def _vincular_em_conexao(cnx, login_equipe: str, nome_competicao: str, cliente_id: int | None):
    with cnx.cursor() as cur:
        equipe = _buscar_equipe_por_login(cur, login_equipe, cliente_id)
        if not equipe:
            return None

        cur.execute(
            """
            SELECT id
            FROM equipes_competicoes
            WHERE equipe_login = %s
              AND competicao = %s
              AND (%s::INTEGER IS NULL OR cliente_id = %s)
            LIMIT 1
            """,
            (equipe["login"], nome_competicao, cliente_id, cliente_id),
        )
        ja_vinculada = cur.fetchone() is not None

        cur.execute(
            """
            INSERT INTO equipes_competicoes
                (equipe_login, equipe_nome, competicao, status, cliente_id)
            VALUES (%s, %s, %s, 'ativa', %s)
            ON CONFLICT (equipe_nome, competicao) DO UPDATE
            SET equipe_login = EXCLUDED.equipe_login,
                equipe_nome = EXCLUDED.equipe_nome,
                status = 'ativa',
                cliente_id = EXCLUDED.cliente_id
            """,
            (equipe["login"], equipe["nome"], nome_competicao, cliente_id),
        )

        # Corrige registros históricos ainda sem cliente_id, sem transferir
        # equipes de outro cliente.
        cur.execute(
            "UPDATE equipes SET cliente_id = %s WHERE login = %s AND cliente_id IS NULL",
            (cliente_id, equipe["login"]),
        )
        cur.execute(
            "UPDATE usuarios SET cliente_id = %s WHERE login = %s AND cliente_id IS NULL",
            (cliente_id, equipe["login"]),
        )

        resultado = dict(equipe)
        resultado["ja_vinculada"] = ja_vinculada
        return resultado


def vincular_equipe_existente_competicao_persistencia(
    login_equipe: str,
    nome_competicao: str,
    conn=None,
):
    login_equipe = str(login_equipe or "").strip()
    nome_competicao = " ".join(str(nome_competicao or "").strip().split())
    if not login_equipe or not nome_competicao:
        return None

    cliente_id = cliente_id_por_competicao(nome_competicao, conn=conn)
    if conn is not None:
        return _vincular_em_conexao(conn, login_equipe, nome_competicao, cliente_id)

    with conectar() as cnx:
        resultado = _vincular_em_conexao(cnx, login_equipe, nome_competicao, cliente_id)
        cnx.commit()
        return resultado


def vincular_equipe_a_competicao_persistencia(
    nome_equipe: str,
    nome_competicao: str,
    conn=None,
):
    dados = preparar_equipe_competicao(nome_equipe, nome_competicao)
    valido, _ = validar_equipe_competicao(dados)
    if not valido:
        return None

    cliente_id = cliente_id_por_competicao(dados.nome_competicao, conn=conn)

    def _executar(cnx):
        equipe = buscar_equipe_global_por_nome(
            dados.nome_equipe,
            conn=cnx,
            cliente_id=cliente_id,
            competicao=dados.nome_competicao,
        )
        if not equipe:
            return None
        return _vincular_em_conexao(
            cnx,
            equipe["login"],
            dados.nome_competicao,
            cliente_id,
        )

    if conn is not None:
        return _executar(conn)
    with conectar() as cnx:
        resultado = _executar(cnx)
        cnx.commit()
        return resultado


def criar_nova_equipe_com_credenciais_persistencia(
    nome_equipe: str,
    nome_competicao: str,
):
    dados = preparar_equipe_competicao(nome_equipe, nome_competicao)
    valido, _ = validar_equipe_competicao(dados)
    if not valido:
        return None


    with conectar() as conn:
        cliente_id = cliente_id_por_competicao(dados.nome_competicao, conn=conn)
        with conn.cursor() as cur:
            login_equipe = gerar_login_unico(normalizar_login_equipe(dados.nome_equipe), conn=conn)
            senha_equipe = gerar_senha_aleatoria(8)
            senha_hash = gerar_hash_senha(senha_equipe)

            cur.execute(
                """
                INSERT INTO equipes (
                    nome, login, senha, competicao, treinador, auxiliar_tecnico,
                    preparador_fisico, medico, liberacao_extra_inscricao,
                    liberacao_extra_data, liberacao_extra_hora, cidade,
                    responsavel, telefone, email, instagram, escudo,
                    perfil_completo, cliente_id
                )
                VALUES (
                    %s, %s, %s, %s, '', '', '', '', FALSE, NULL, NULL,
                    '', '', '', '', '', '', FALSE, %s
                )
                """,
                (
                    dados.nome_equipe,
                    login_equipe,
                    senha_hash,
                    dados.nome_competicao,
                    cliente_id,
                ),
            )
            cur.execute(
                """
                INSERT INTO equipes_competicoes
                    (equipe_login, equipe_nome, competicao, status, cliente_id)
                VALUES (%s, %s, %s, 'ativa', %s)
                ON CONFLICT (equipe_nome, competicao) DO UPDATE
                SET equipe_login = EXCLUDED.equipe_login,
                    status = 'ativa',
                    cliente_id = EXCLUDED.cliente_id
                """,
                (
                    login_equipe,
                    dados.nome_equipe,
                    dados.nome_competicao,
                    cliente_id,
                ),
            )
            cur.execute(
                """
                INSERT INTO usuarios
                    (login, nome, senha, perfil, ativo, equipe,
                     competicao_vinculada, cliente_id)
                VALUES (%s, %s, %s, 'equipe', TRUE, %s, %s, %s)
                """,
                (
                    login_equipe,
                    dados.nome_equipe,
                    senha_hash,
                    dados.nome_equipe,
                    dados.nome_competicao,
                    cliente_id,
                ),
            )
        conn.commit()

    return {
        "login": login_equipe,
        "senha": senha_equipe,
        "nome": dados.nome_equipe,
        "vinculada": True,
        "ja_existia": False,
        "ja_vinculada": False,
    }


def criar_equipe_com_credenciais_persistencia(
    nome_equipe: str,
    nome_competicao: str,
):
    dados = preparar_equipe_competicao(nome_equipe, nome_competicao)
    valido, _ = validar_equipe_competicao(dados)
    if not valido:
        return None

    with conectar() as conn:
        cliente_id = cliente_id_por_competicao(dados.nome_competicao, conn=conn)
        existente = buscar_equipe_global_por_nome(
            dados.nome_equipe,
            conn=conn,
            cliente_id=cliente_id,
            competicao=dados.nome_competicao,
        )
        if existente:
            resultado = _vincular_em_conexao(
                conn,
                existente["login"],
                dados.nome_competicao,
                cliente_id,
            )
            conn.commit()
            if not resultado:
                return None
            return {
                "login": resultado["login"],
                "senha": resultado["senha"],
                "nome": resultado["nome"],
                "vinculada": True,
                "ja_existia": True,
                "ja_vinculada": resultado.get("ja_vinculada", False),
            }

    return criar_nova_equipe_com_credenciais_persistencia(
        dados.nome_equipe,
        dados.nome_competicao,
    )
