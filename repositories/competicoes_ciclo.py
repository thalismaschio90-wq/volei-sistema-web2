"""Persistência do ciclo de vida das competições.

Este módulo concentra listagem, busca, criação e travamento. Helpers legados
são acessados sob demanda para manter a migração gradual sem importação circular.
"""
from __future__ import annotations

from core.security import gerar_hash_senha

import json
import random
import re
import string

from core.schema_inspection import buscar_colunas_tabela
from repositories.conexao import conectar
from repositories.competicoes_campos import campos_competicao
from repositories.quadras import garantir_quadras_competicao

from rules.competicoes_ciclo import normalizar_motivo_travamento, normalizar_nome_competicao



def _normalizar_texto_base(texto: object) -> str:
    valor = str(texto or "").lower().strip()
    valor = re.sub(r"[^\w\s]", "", valor)
    valor = re.sub(r"\s+", "_", valor)[:24].strip("_")
    return valor or "cadastro"


def _normalizar_login_organizador(nome_competicao: object) -> str:
    return f"org_{_normalizar_texto_base(nome_competicao)}"


def _usuario_existe(login: str) -> bool:
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM usuarios WHERE login = %s LIMIT 1", (login,))
            return cur.fetchone() is not None


def _gerar_login_unico(base: str) -> str:
    login = base
    contador = 1
    while _usuario_existe(login):
        contador += 1
        login = f"{base}_{contador}"
    return login


def _gerar_senha_aleatoria(tamanho: int = 8) -> str:
    caracteres = string.ascii_uppercase + string.digits
    return "".join(random.choice(caracteres) for _ in range(max(1, int(tamanho or 8))))

_STATUS_PARTIDA_FINALIZADA = (
    "'finalizada','finalizado','encerrada','encerrado'"
)
_STATUS_PARTIDA_INICIADA = (
    "'pre_jogo','sorteio','papeleta','papeleta_pronta','ao_vivo',"
    "'em_andamento','andamento','jogo','entre_sets','intervalo_set',"
    "'tiebreak_sorteio','pausada','pausado'"
)


def _sql_status_competicao(alias_competicao: str = "c", alias_resumo: str = "rs") -> str:
    """Status calculado na leitura, sem UPDATE global da tabela competicoes."""
    return f"""
        CASE
            WHEN COALESCE({alias_resumo}.total_partidas, 0) > 0
             AND COALESCE({alias_resumo}.finalizadas, 0) = COALESCE({alias_resumo}.total_partidas, 0)
                THEN 'Finalizada'
            WHEN COALESCE({alias_resumo}.iniciadas, 0) > 0
                THEN 'Em andamento'
            ELSE 'Em preparação'
        END
    """.strip()


def _sql_join_resumo_partidas(alias_competicao: str = "c", alias_resumo: str = "rs") -> str:
    return f"""
        LEFT JOIN LATERAL (
            SELECT
                COUNT(p.id) AS total_partidas,
                COUNT(*) FILTER (
                    WHERE LOWER(COALESCE(p.status_jogo, p.status_operacao, p.status, ''))
                          IN ({_STATUS_PARTIDA_FINALIZADA})
                ) AS finalizadas,
                COUNT(*) FILTER (
                    WHERE LOWER(COALESCE(
                        p.status_jogo, p.status_operacao, p.status, p.fase_partida, ''
                    )) IN ({_STATUS_PARTIDA_INICIADA})
                ) AS iniciadas
            FROM partidas p
            WHERE p.competicao = {alias_competicao}.nome
        ) {alias_resumo} ON TRUE
    """.strip()


def _campos_com_status_calculado(campos: list[str], *, alias_competicao: str = "c", alias_resumo: str = "rs") -> list[str]:
    alvo = f"{alias_competicao}.status"
    status_sql = f"{_sql_status_competicao(alias_competicao, alias_resumo)} AS status"
    return [status_sql if campo.strip() == alvo else campo for campo in campos]


def sincronizar_status_competicoes_persistencia(nome_competicao=None):
    filtro = ""
    params = []
    if nome_competicao:
        filtro = "WHERE c.nome = %s"
        params.append(nome_competicao)
    sql = f"""
        WITH resumo AS (
            SELECT c.nome, COUNT(p.id) AS total_partidas,
                SUM(CASE WHEN LOWER(COALESCE(p.status_jogo,p.status_operacao,p.status,''))
                    IN ('finalizada','finalizado','encerrada','encerrado') THEN 1 ELSE 0 END) AS finalizadas,
                SUM(CASE WHEN LOWER(COALESCE(p.status_jogo,p.status_operacao,p.status,p.fase_partida,''))
                    IN ('pre_jogo','sorteio','papeleta','papeleta_pronta','ao_vivo','em_andamento','andamento','jogo','entre_sets','intervalo_set','tiebreak_sorteio','pausada','pausado')
                    THEN 1 ELSE 0 END) AS iniciadas
            FROM competicoes c LEFT JOIN partidas p ON p.competicao = c.nome
            {filtro} GROUP BY c.nome
        ), calculado AS (
            SELECT nome, CASE
                WHEN total_partidas > 0 AND finalizadas = total_partidas THEN 'Finalizada'
                WHEN iniciadas > 0 THEN 'Em andamento'
                ELSE 'Em preparação' END AS novo_status
            FROM resumo
        )
        UPDATE competicoes c SET status = calculado.novo_status
        FROM calculado WHERE c.nome = calculado.nome
          AND COALESCE(c.status,'') <> calculado.novo_status
    """
    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
            conn.commit()
        return True
    except Exception as exc:
        print("AVISO sincronizar_status_competicoes:", repr(exc), flush=True)
        return False


def listar_competicoes_persistencia():
    """Lista competições sem executar sincronização escrita global.

    O status é derivado das partidas na mesma consulta. Abrir uma tela deixa de
    executar UPDATE em todas as competições e não disputa locks com jogos ativos.
    """
    campos = campos_competicao(prefixo="c", incluir_senha_organizador=True)
    campos = _campos_com_status_calculado(campos)
    status_sql = _sql_status_competicao()
    sql = f"""
        SELECT {', '.join(campos)}
        FROM competicoes c
        LEFT JOIN usuarios u ON u.login = c.organizador_login
        {_sql_join_resumo_partidas()}
        ORDER BY CASE
            WHEN ({status_sql}) = 'Em andamento' THEN 1
            WHEN ({status_sql}) = 'Em preparação' THEN 2
            WHEN ({status_sql}) = 'Finalizada' THEN 3
            ELSE 4 END, c.nome
    """
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()


def listar_competicoes_do_organizador_persistencia(login_organizador):
    campos = campos_competicao(prefixo="c")
    campos = _campos_com_status_calculado(campos)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {', '.join(campos)}
                FROM competicoes c
                {_sql_join_resumo_partidas()}
                WHERE c.organizador_login = %s
                ORDER BY c.nome
                """,
                (login_organizador,),
            )
            return cur.fetchall()


def buscar_competicao_por_organizador_persistencia(login_organizador):
    campos = campos_competicao(prefixo="c")
    campos = _campos_com_status_calculado(campos)
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {', '.join(campos)}
                FROM competicoes c
                {_sql_join_resumo_partidas()}
                WHERE c.organizador_login = %s
                ORDER BY c.nome
                LIMIT 1
                """,
                (login_organizador,),
            )
            return cur.fetchone()


def competicao_existe_persistencia(nome):
    nome = normalizar_nome_competicao(nome)
    if not nome:
        return False
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM competicoes WHERE nome=%s LIMIT 1", (nome,))
            return cur.fetchone() is not None


def criar_competicao_com_organizador_persistencia(nome, data, status, modo_operacao="simples", tempos_por_set=2, substituicoes_por_set=6):
    nome = normalizar_nome_competicao(nome)
    if not nome:
        raise ValueError("Nome da competição é obrigatório")
    login = _gerar_login_unico(_normalizar_login_organizador(nome))
    senha = _gerar_senha_aleatoria(8)
    senha_hash = gerar_hash_senha(senha)
    colunas = buscar_colunas_tabela("competicoes")
    defaults = {
        "cidade":"", "ginasio":"", "categoria":"", "sexo":"", "divisao":"", "qtd_equipes":0,
        "formato":"grupos", "tem_grupos":False, "qtd_grupos":0, "qtd_quadras":1,
        "modo_operacao":modo_operacao or "simples", "tempos_por_set":tempos_por_set,
        "substituicoes_por_set":substituicoes_por_set, "sets_tipo":"melhor_de_3", "pontos_set":25,
        "tem_tiebreak":True, "pontos_tiebreak":15, "diferenca_minima":2, "vitoria_set_unico":2,
        "derrota_set_unico":0, "vitoria_2x0":3, "vitoria_2x1":2, "derrota_1x2":1, "derrota_0x2":0,
        "vitoria_3x0":3, "vitoria_3x1":3, "vitoria_3x2":2, "derrota_2x3":1, "derrota_1x3":0,
        "derrota_0x3":0, "criterios_desempate":"vitorias,pontos,saldo_sets,sets_pro,sets_contra,saldo_pontos,pontos_pro,pontos_contra,confronto_direto,coef_sets,coef_pontos,fair_play,sorteio",
        "tipo_classificacao":"grupo", "qtd_classificados":0, "formato_finais":"mata_mata", "possui_bye":False,
        "qtd_bye":0, "fases_config":json.dumps({}, ensure_ascii=False), "tipo_confronto":"grupo_interno",
        "cruzamentos_grupos":"", "data_limite_inscricao":None, "hora_limite_inscricao":None,
        "bloquear_apos_inicio":False, "limite_atletas":0, "permitir_edicao_pos_prazo":False,
        "aprovacao_automatica_atletas":False, "travada":False, "motivo_travamento":"", "travada_em":None,
    }
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO usuarios (login,nome,senha,perfil,ativo,equipe,competicao_vinculada) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (login, f"Organizador - {nome}", senha_hash, "organizador", True, None, nome))
            campos = ["nome","data","status","organizador_login"]
            valores = [nome,data,status,login]
            for campo, valor in defaults.items():
                if campo in colunas:
                    campos.append(campo); valores.append(valor)
            placeholders = ", ".join(["%s"] * len(valores))
            cur.execute(f"INSERT INTO competicoes ({', '.join(campos)}) VALUES ({placeholders})", tuple(valores))
        conn.commit()
    try:
        garantir_quadras_competicao(nome, 1)
    except Exception as exc:
        print("AVISO: não foi possível criar quadra padrão da competição:", exc, flush=True)
    return {"login": login, "senha": senha}


def competicao_esta_travada_persistencia(nome_competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(travada,FALSE) AS travada FROM competicoes WHERE nome=%s LIMIT 1",
                (nome_competicao,),
            )
            row = cur.fetchone()
            return bool(row and row.get("travada"))


def validar_competicao_editavel_persistencia(nome_competicao, escopo="alteração"):
    if competicao_esta_travada_persistencia(nome_competicao):
        return False, f"A competição está travada. Não é permitido realizar esta {escopo}."
    return True, ""


def travar_competicao_persistencia(nome_competicao, motivo="primeiro_ponto"):
    """Trava a competição sem consultar information_schema ou executar DDL."""
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE competicoes
                SET travada = TRUE,
                    motivo_travamento = %s,
                    travada_em = NOW()
                WHERE nome = %s
                  AND COALESCE(travada, FALSE) = FALSE
                """,
                (normalizar_motivo_travamento(motivo), nome_competicao),
            )
            alteradas = cur.rowcount
        conn.commit()
    return alteradas > 0


def destravar_competicao_persistencia(nome_competicao):
    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE competicoes
                SET travada = FALSE,
                    motivo_travamento = '',
                    travada_em = NULL
                WHERE nome = %s
                """,
                (nome_competicao,),
            )
        conn.commit()
    return True

