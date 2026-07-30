"""Persistência de escrita para atletas.

Este repositório concentra INSERT/UPDATE/DELETE e consultas necessárias às
operações de escrita. Regras puras continuam em ``rules.atletas`` e a fachada
legada de ``banco.py`` preserva as assinaturas públicas durante a migração.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Mapping

from rules.atletas import (
    mensagem_pendencias_obrigatorias,
    pendencias_obrigatorias,
    validar_campos_basicos_cadastro,
    validar_campos_basicos_edicao,
)
from services.atletas.dados import preparar_dados_atleta


def _dependencias(deps: Mapping[str, Any]) -> dict[str, Any]:
    obrigatorias = (
        "conectar", "somente_digitos", "formatar_cpf", "cpf_valido",
        "criar_tabela_atletas", "criar_campos_controle_inscricao_competicoes",
        "criar_campos_liberacao_extra_equipes", "criar_campos_conferencia_atletas",
        "cpf_sql_limpo", "salvar_atleta_global",
    )
    faltantes = [nome for nome in obrigatorias if nome not in deps]
    if faltantes:
        raise RuntimeError("Dependências ausentes no repositório de atletas: " + ", ".join(faltantes))
    return dict(deps)

def cadastrar_atleta_persistencia(nome, cpf, data_nascimento, numero, equipe, competicao, foto_atleta=None, instagram=None, *, deps: Mapping[str, Any]):
    _deps = _dependencias(deps)
    conectar = _deps["conectar"]
    somente_digitos = _deps["somente_digitos"]
    formatar_cpf = _deps["formatar_cpf"]
    cpf_valido = _deps["cpf_valido"]
    criar_tabela_atletas = _deps["criar_tabela_atletas"]
    criar_campos_controle_inscricao_competicoes = _deps["criar_campos_controle_inscricao_competicoes"]
    criar_campos_liberacao_extra_equipes = _deps["criar_campos_liberacao_extra_equipes"]
    criar_campos_conferencia_atletas = _deps["criar_campos_conferencia_atletas"]
    _cpf_sql_limpo = _deps["cpf_sql_limpo"]
    _salvar_atleta_global = _deps["salvar_atleta_global"]
    ok_dados, dados, mensagem_dados = preparar_dados_atleta(
        nome=nome,
        cpf=cpf,
        data_nascimento=data_nascimento,
        numero=numero,
        equipe=equipe,
        competicao=competicao,
        foto_atleta=foto_atleta,
        instagram=instagram,
    )
    if not ok_dados or dados is None:
        return False, mensagem_dados

    nome = dados.nome
    cpf_limpo = somente_digitos(dados.cpf_informado)
    cpf = formatar_cpf(cpf_limpo)
    data_nascimento = dados.data_nascimento
    equipe = dados.equipe
    competicao = dados.competicao
    foto_atleta = dados.foto_atleta
    instagram = dados.instagram
    numero_final = dados.numero

    ok_basico, mensagem_basica = validar_campos_basicos_cadastro(nome, cpf_limpo, data_nascimento)
    if not ok_basico:
        return False, mensagem_basica

    if not cpf_valido(cpf_limpo):
        return False, "CPF inválido. Informe um CPF real no formato 000.000.000-00."

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

            pendencias = pendencias_obrigatorias(
                exigir_foto=bool(controle.get("exigir_foto_atleta")),
                exigir_instagram=bool(controle.get("exigir_instagram_atleta")),
                foto_atleta=foto_atleta,
                instagram=instagram,
            )
            if pendencias:
                return False, mensagem_pendencias_obrigatorias(pendencias, acao="cadastro")

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

def atualizar_atleta_equipe_persistencia(id_atleta, equipe, competicao, nome, cpf, data_nascimento, foto_atleta=None, instagram=None, *, deps: Mapping[str, Any]):
    _deps = _dependencias(deps)
    conectar = _deps["conectar"]
    somente_digitos = _deps["somente_digitos"]
    formatar_cpf = _deps["formatar_cpf"]
    cpf_valido = _deps["cpf_valido"]
    criar_tabela_atletas = _deps["criar_tabela_atletas"]
    criar_campos_controle_inscricao_competicoes = _deps["criar_campos_controle_inscricao_competicoes"]
    criar_campos_liberacao_extra_equipes = _deps["criar_campos_liberacao_extra_equipes"]
    criar_campos_conferencia_atletas = _deps["criar_campos_conferencia_atletas"]
    _cpf_sql_limpo = _deps["cpf_sql_limpo"]
    _salvar_atleta_global = _deps["salvar_atleta_global"]
    """
    Atualiza dados básicos do atleta pela própria equipe.
    Regras:
    - Só permite editar atleta da própria equipe/competição.
    - Atleta reprovado não pode ser editado pela equipe; só excluído.
    - CPF não pode duplicar dentro da mesma competição em outro atleta.
    - Respeita o travamento da competição quando a equipe já iniciou jogos.
    """
    ok_dados, dados, mensagem_dados = preparar_dados_atleta(
        nome=nome,
        cpf=cpf,
        data_nascimento=data_nascimento,
        equipe=equipe,
        competicao=competicao,
        foto_atleta=foto_atleta,
        instagram=instagram,
    )
    if not ok_dados or dados is None:
        return False, mensagem_dados

    nome = dados.nome
    cpf = dados.cpf_informado
    data_nascimento = dados.data_nascimento
    foto_atleta = dados.foto_atleta
    instagram = dados.instagram
    equipe = dados.equipe
    competicao = dados.competicao
    cpf_limpo = somente_digitos(cpf)

    ok_basico, mensagem_basica = validar_campos_basicos_edicao(nome, cpf, data_nascimento)
    if not ok_basico:
        return False, mensagem_basica

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

                pendencias = pendencias_obrigatorias(
                    exigir_foto=bool(comp and comp.get("exigir_foto_atleta")),
                    exigir_instagram=bool(comp and comp.get("exigir_instagram_atleta")),
                    foto_atleta=foto_final,
                    instagram=instagram_final,
                )
                if pendencias:
                    return False, mensagem_pendencias_obrigatorias(pendencias, acao="edicao")

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

def excluir_atleta_persistencia(id_atleta, *, deps: Mapping[str, Any]):
    _deps = _dependencias(deps)
    conectar = _deps["conectar"]
    somente_digitos = _deps["somente_digitos"]
    formatar_cpf = _deps["formatar_cpf"]
    cpf_valido = _deps["cpf_valido"]
    criar_tabela_atletas = _deps["criar_tabela_atletas"]
    criar_campos_controle_inscricao_competicoes = _deps["criar_campos_controle_inscricao_competicoes"]
    criar_campos_liberacao_extra_equipes = _deps["criar_campos_liberacao_extra_equipes"]
    criar_campos_conferencia_atletas = _deps["criar_campos_conferencia_atletas"]
    _cpf_sql_limpo = _deps["cpf_sql_limpo"]
    _salvar_atleta_global = _deps["salvar_atleta_global"]
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

