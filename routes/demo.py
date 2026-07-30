from core.security import gerar_hash_senha
from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from urllib.parse import quote
from datetime import timedelta
import random
import string

from banco import conectar, cpf_valido, formatar_cpf, somente_digitos


demo_bp = Blueprint("demo", __name__)

DEMO_PREFIXO = "DEMO-VTP-"
LINK_SISTEMA = "https://volleytablepro.com.br/login"


def _gerar_codigo_demo():
    numero = "".join(random.choice(string.digits) for _ in range(6))
    return f"{DEMO_PREFIXO}{numero}"


def _gerar_senha_demo():
    numero = "".join(random.choice(string.digits) for _ in range(4))
    return f"VTPro-{numero}"


def _gerar_login_demo(codigo):
    return codigo.lower().replace("-", "_")


def _normalizar_whatsapp(valor):
    numero = somente_digitos(valor)
    if numero.startswith("55"):
        return numero
    return f"55{numero}"


def _buscar_tabelas(cur):
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)
    return {row["table_name"] for row in cur.fetchall()}


def _buscar_colunas(cur, tabela):
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
    """, (tabela,))
    return {row["column_name"] for row in cur.fetchall()}


def criar_tabela_demos():
    """Compatibilidade: o schema é garantido no startup da aplicação."""
    from repositories.runtime_schema import garantir_schema_runtime
    garantir_schema_runtime()


def _cpf_ou_whatsapp_ja_usou_demo(cpf, whatsapp):
    criar_tabela_demos()

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
            """, (cpf, whatsapp))

            return cur.fetchone()


def _criar_usuario_e_competicao_demo(nome, cpf, whatsapp):
    criar_tabela_demos()

    codigo = _gerar_codigo_demo()
    competicao = codigo
    login = _gerar_login_demo(codigo)
    senha = _gerar_senha_demo()
    senha_hash = gerar_hash_senha(senha)

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO usuarios (
                    login,
                    nome,
                    senha,
                    perfil,
                    ativo,
                    equipe,
                    competicao_vinculada
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                login,
                f"Demo - {nome}",
                senha_hash,
                "organizador",
                True,
                None,
                competicao,
            ))

            colunas_comp = _buscar_colunas(cur, "competicoes")

            campos = ["nome", "data", "status", "organizador_login"]
            valores = [competicao, None, "Demonstração", login]

            defaults = {
                "cidade": "Demonstração",
                "ginasio": "Ambiente de teste",
                "categoria": "Demo",
                "sexo": "Livre",
                "divisao": "Demonstração",
                "qtd_equipes": 0,
                "formato": "grupos",
                "tem_grupos": False,
                "qtd_grupos": 0,
                "qtd_quadras": 1,
                "modo_operacao": "simples",
                "sets_tipo": "melhor_de_3",
                "pontos_set": 25,
                "tem_tiebreak": True,
                "pontos_tiebreak": 15,
                "diferenca_minima": 2,
                "tempos_por_set": 2,
                "substituicoes_por_set": 6,
                "limite_atletas": 0,
                "permitir_edicao_pos_prazo": True,
                "travada": False,
                "motivo_travamento": "",
            }

            for campo, valor in defaults.items():
                if campo in colunas_comp:
                    campos.append(campo)
                    valores.append(valor)

            placeholders = ", ".join(["%s"] * len(valores))

            cur.execute(
                f"""
                INSERT INTO competicoes ({", ".join(campos)})
                VALUES ({placeholders})
                """,
                tuple(valores)
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
                cpf,
                whatsapp,
                competicao,
                login,
                senha,
            ))

            demo = cur.fetchone()

        conn.commit()

    return demo


def _montar_mensagem_whatsapp(demo):
    expira = demo.get("expira_em")
    expira_txt = expira.strftime("%d/%m/%Y às %H:%M") if expira else "em 4 horas"

    mensagem = f"""Olá, {demo.get('nome')}! Sua demonstração do VolleyTable Pro foi liberada com sucesso. 🏐

🔐 Login: {demo.get('login')}
🔑 Senha: {demo.get('senha')}

⏰ Validade:
Até {expira_txt}

🌐 Acesse:
{LINK_SISTEMA}

Após o período de demonstração, o ambiente de teste poderá ser encerrado automaticamente.

Equipe VolleyTable Pro"""

    return mensagem


def _link_whatsapp(demo):
    whatsapp = _normalizar_whatsapp(demo.get("whatsapp"))
    mensagem = quote(_montar_mensagem_whatsapp(demo))
    return f"https://wa.me/{whatsapp}?text={mensagem}"


def _deletar_por_competicao_se_existir(cur, tabelas, tabela, competicao):
    if tabela not in tabelas:
        return

    colunas = _buscar_colunas(cur, tabela)
    if "competicao" not in colunas:
        return

    cur.execute(f"DELETE FROM {tabela} WHERE competicao = %s", (competicao,))


def limpar_demo_por_competicao(competicao):
    competicao = (competicao or "").strip()

    if not competicao.startswith(DEMO_PREFIXO):
        print("⚠️ Limpeza bloqueada. Competição sem prefixo seguro:", competicao)
        return False

    tabelas_por_competicao = [
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
        "equipes",
        "grupos",
        "partidas",
    ]

    with conectar() as conn:
        with conn.cursor() as cur:
            tabelas = _buscar_tabelas(cur)

            cpfs_demo = []

            if "competicao_oficiais" in tabelas:
                colunas = _buscar_colunas(cur, "competicao_oficiais")
                if "cpf" in colunas and "competicao" in colunas:
                    cur.execute("""
                        SELECT DISTINCT cpf
                        FROM competicao_oficiais
                        WHERE competicao = %s
                    """, (competicao,))
                    cpfs_demo = [row["cpf"] for row in cur.fetchall() if row.get("cpf")]

            if "usuarios" in tabelas:
                colunas = _buscar_colunas(cur, "usuarios")
                if "competicao_vinculada" in colunas:
                    cur.execute("""
                        DELETE FROM usuarios
                        WHERE competicao_vinculada = %s
                          AND perfil <> 'superadmin'
                    """, (competicao,))

            for tabela in tabelas_por_competicao:
                _deletar_por_competicao_se_existir(cur, tabelas, tabela, competicao)

            if cpfs_demo and "apontadores" in tabelas:
                colunas = _buscar_colunas(cur, "apontadores")
                if "cpf" in colunas:
                    for cpf in cpfs_demo:
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
                        WHEN motivo_encerramento IS NULL OR motivo_encerramento = ''
                        THEN 'expirada'
                        ELSE motivo_encerramento
                    END
                WHERE competicao = %s
            """, (competicao,))

        conn.commit()

    return True


def limpar_demos_expiradas():
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

    for demo in demos:
        limpar_demo_por_competicao(demo["competicao"])


def listar_demos_admin():
    criar_tabela_demos()
    limpar_demos_expiradas()

    with conectar() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *,
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

    return limpar_demo_por_competicao(demo["competicao"])


def liberar_novo_teste(demo_id):
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


@demo_bp.route("/demo", methods=["GET", "POST"])
def demo():
    criar_tabela_demos()

    if request.method == "POST":
        nome = (request.form.get("nome") or "").strip()
        cpf = somente_digitos(request.form.get("cpf") or "")
        whatsapp = somente_digitos(request.form.get("whatsapp") or "")

        if not nome:
            flash("Informe seu nome completo.", "erro")
            return render_template("demo_solicitar.html")

        if not cpf_valido(cpf):
            flash("Informe um CPF válido.", "erro")
            return render_template("demo_solicitar.html")

        if len(whatsapp) < 10:
            flash("Informe um WhatsApp válido com DDD.", "erro")
            return render_template("demo_solicitar.html")

        ja_usou = _cpf_ou_whatsapp_ja_usou_demo(cpf, whatsapp)

        if ja_usou:
            flash("Este CPF ou WhatsApp já utilizou a demonstração gratuita. Fale conosco para liberar um novo teste.", "erro")
            return render_template(
                "demo_solicitar.html",
                ja_usou=True,
                whatsapp_suporte="5554999698513"
            )

        demo_criada = _criar_usuario_e_competicao_demo(
            nome=nome,
            cpf=formatar_cpf(cpf),
            whatsapp=whatsapp,
        )

        link = _link_whatsapp(demo_criada)

        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE demos_temporarias
                    SET whatsapp_enviado = TRUE
                    WHERE id = %s
                """, (demo_criada["id"],))
            conn.commit()

        return render_template(
            "demo_whatsapp.html",
            demo=demo_criada,
            link_whatsapp=link,
            link_sistema=LINK_SISTEMA,
        )

    return render_template("demo_solicitar.html")


@demo_bp.route("/demos")
def demos_admin():
    if session.get("perfil") != "superadmin":
        flash("Acesso restrito ao superadmin.", "erro")
        return redirect(url_for("auth.login"))

    demos = listar_demos_admin()

    return render_template(
        "demos_admin.html",
        demos=demos
    )


@demo_bp.route("/demos/<int:demo_id>/estender/<int:horas>", methods=["POST"])
def demos_estender(demo_id, horas):
    if session.get("perfil") != "superadmin":
        flash("Acesso restrito ao superadmin.", "erro")
        return redirect(url_for("auth.login"))

    estender_demo(demo_id, horas)

    flash(f"Demonstração estendida por mais {horas} hora(s).", "sucesso")
    return redirect(url_for("demo.demos_admin"))


@demo_bp.route("/demos/<int:demo_id>/encerrar", methods=["POST"])
def demos_encerrar(demo_id):
    if session.get("perfil") != "superadmin":
        flash("Acesso restrito ao superadmin.", "erro")
        return redirect(url_for("auth.login"))

    encerrar_demo(demo_id)

    flash("Demonstração encerrada com sucesso.", "sucesso")
    return redirect(url_for("demo.demos_admin"))


@demo_bp.route("/demos/<int:demo_id>/liberar-novo-teste", methods=["POST"])
def demos_liberar_novo_teste(demo_id):
    if session.get("perfil") != "superadmin":
        flash("Acesso restrito ao superadmin.", "erro")
        return redirect(url_for("auth.login"))

    liberar_novo_teste(demo_id)

    flash("Novo teste liberado para esse CPF/WhatsApp.", "sucesso")
    return redirect(url_for("demo.demos_admin"))