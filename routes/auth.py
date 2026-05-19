from flask import Blueprint, render_template, request, redirect, session, url_for, flash
from banco import (
    conectar,
    buscar_usuario_por_login,
    autenticar_apontador,
    definir_senha_apontador,
)

try:
    from routes.demo import criar_tabela_demos, limpar_demo_por_competicao
except Exception:
    criar_tabela_demos = None
    limpar_demo_por_competicao = None


auth_bp = Blueprint("auth", __name__)


def _demo_expirada_para_login(login):
    if not login or criar_tabela_demos is None:
        return False

    try:
        criar_tabela_demos()
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
                    if limpar_demo_por_competicao is not None:
                        limpar_demo_por_competicao(demo["competicao"])
                    return True

                return False

    except Exception as e:
        print("ERRO AO VERIFICAR DEMO EXPIRADA:", repr(e))
        return False


def _is_mobile_ou_app():
    """
    Define qual tela de login usar.

    - Navegador desktop normal: login.html
      Mantém botão "Baixar aplicativo Windows".

    - Celular, PWA ou app desktop Electron: app_login.html
      Usa tela premium escura, sem botão Windows.
    """
    ua = (request.headers.get("User-Agent") or "").lower()

    if request.args.get("app") == "1":
        return True

    if "electron" in ua:
        return True

    if "iphone" in ua:
        return True

    if "ipad" in ua:
        return True

    if "android" in ua:
        return True

    if "mobile" in ua:
        return True

    return False


def _template_login():
    if _is_mobile_ou_app():
        return "app_login.html"

    return "login.html"


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if "usuario" in session:
        if _demo_expirada_para_login(session.get("usuario")):
            session.clear()
            flash("Sua demonstração expirou. Gere uma nova demonstração.", "erro")
            return redirect(url_for("demo.demo"))

        if session.get("perfil") == "apontador":
            return redirect(url_for("apontadores.painel_apontador"))

        return redirect(url_for("painel.inicio"))

    if request.method == "POST":
        login_digitado = request.form.get("login", "").strip()
        senha_digitada = request.form.get("senha", "").strip()

        if not login_digitado or not senha_digitada:
            flash("Informe login e senha.", "erro")
            return render_template(_template_login())

        try:
            with conectar() as conn:
                usuario = buscar_usuario_por_login(login_digitado, conn)
        except Exception as e:
            print("ERRO LOGIN BANCO:", repr(e))
            flash("Erro temporário ao conectar no banco. Tente novamente.", "erro")
            return render_template(_template_login())

        if usuario:
            if _demo_expirada_para_login(usuario["login"]):
                flash("Essa demonstração expirou. Gere uma nova demonstração.", "erro")
                return redirect(url_for("demo.demo"))

            if not usuario.get("ativo", True):
                flash("Usuário inativo.", "erro")
                return render_template(_template_login())

            if usuario["senha"] != senha_digitada:
                flash("Senha incorreta.", "erro")
                return render_template(_template_login())

            session["usuario"] = usuario["login"]
            session["nome"] = usuario.get("nome") or usuario["login"]
            session["perfil"] = usuario.get("perfil") or ""
            session["equipe"] = usuario.get("equipe")
            session["competicao_vinculada"] = usuario.get("competicao_vinculada")

            if session.get("perfil") == "apontador":
                return redirect(url_for("apontadores.painel_apontador"))

            return redirect(url_for("painel.inicio"))

        try:
            apontador = autenticar_apontador(login_digitado, senha_digitada)
        except Exception as e:
            print("ERRO LOGIN APONTADOR:", repr(e))
            flash("Erro temporário ao autenticar apontador.", "erro")
            return render_template(_template_login())

        if apontador is False:
            flash("Senha incorreta.", "erro")
            return render_template(_template_login())

        if apontador:
            session["usuario"] = apontador["cpf"]
            session["nome"] = apontador.get("nome") or apontador["cpf"]
            session["perfil"] = "apontador"
            session["equipe"] = None
            session["competicao_vinculada"] = None

            if apontador.get("primeiro_acesso", True) or not apontador.get("senha"):
                return redirect(url_for("auth.criar_senha_apontador"))

            return redirect(url_for("apontadores.painel_apontador"))

        flash("Usuário não encontrado.", "erro")

    return render_template(_template_login())


@auth_bp.route("/criar-senha-apontador", methods=["GET", "POST"])
def criar_senha_apontador():
    if session.get("perfil") != "apontador":
        return redirect(url_for("auth.login"))

    if request.method == "POST":
        senha = request.form.get("senha", "").strip()
        confirmar = request.form.get("confirmar_senha", "").strip()

        if not senha:
            flash("Informe a senha.", "erro")
            return render_template("criar_senha_apontador.html")

        if senha != confirmar:
            flash("As senhas não coincidem.", "erro")
            return render_template("criar_senha_apontador.html")

        try:
            definir_senha_apontador(session.get("usuario"), senha)
        except Exception as e:
            print("ERRO DEFINIR SENHA APONTADOR:", repr(e))
            flash("Erro ao salvar senha. Tente novamente.", "erro")
            return render_template("criar_senha_apontador.html")

        flash("Senha criada com sucesso.", "sucesso")
        return redirect(url_for("apontadores.painel_apontador"))

    return render_template("criar_senha_apontador.html")


@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))
