from flask import Blueprint, render_template, request, redirect, session, url_for, flash
import os
from core.security import gerar_hash_senha, verificar_senha
from banco import (
    conectar,
    buscar_usuario_por_login,
    autenticar_apontador,
    definir_senha_apontador,
    perfil_equipe_incompleto_por_login,
)

try:
    from routes.demo import criar_tabela_demos, limpar_demo_por_competicao
except Exception:
    criar_tabela_demos = None
    limpar_demo_por_competicao = None


auth_bp = Blueprint("auth", __name__)


def _demo_expirada_para_login(login):
    """Verifica demo expirada sem rodar CREATE/ALTER TABLE durante o login.

    Antes esta função chamava criar_tabela_demos() em todo login/redirect. Em
    produção isso fazia schema check/DDL no caminho crítico e ajudava a lotar o
    pool do Neon. Agora ela só consulta a tabela se ela já existir; se não
    existir, ignora silenciosamente.
    """
    if not login:
        return False

    # Permite desligar essa checagem no Render se o torneio não usa demos.
    if str(os.environ.get("DEMO_CHECK_ON_LOGIN", "1")).strip().lower() in {"0", "false", "no", "off", "nao", "não"}:
        return False

    try:
        with conectar() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT to_regclass('public.demos_temporarias') AS tabela
                """)
                existe = cur.fetchone() or {}
                if not existe.get("tabela"):
                    return False

                cur.execute("""
                    SELECT competicao, expira_em, NOW() AS agora
                    FROM demos_temporarias
                    WHERE login = %s
                      AND encerrada = FALSE
                    LIMIT 1
                """, (login,))
                demo = cur.fetchone()

                if not demo:
                    return False

                if demo["expira_em"] <= demo["agora"]:
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

        if session.get("perfil") == "equipe":
            try:
                if perfil_equipe_incompleto_por_login(session.get("usuario")):
                    return redirect(url_for("equipes.perfil_equipe_view"))
            except Exception as e:
                print("AVISO PERFIL EQUIPE INCOMPLETO:", repr(e))

            return redirect(url_for("equipes.painel_equipe_inicio_view"))

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

            senha_ok, precisa_migrar = verificar_senha(senha_digitada, usuario.get("senha"))
            if not senha_ok:
                flash("Senha incorreta.", "erro")
                return render_template(_template_login())

            if precisa_migrar:
                try:
                    from banco import atualizar_senha_usuario
                    atualizar_senha_usuario(usuario["login"], senha_digitada)
                except Exception as exc:
                    print("AVISO MIGRAÇÃO HASH SENHA:", repr(exc), flush=True)

            session["usuario"] = usuario["login"]
            session["nome"] = usuario.get("nome") or usuario["login"]
            session["perfil"] = usuario.get("perfil") or ""
            session["equipe"] = usuario.get("equipe")
            session["competicao_vinculada"] = usuario.get("competicao_vinculada")
            session["cliente_id"] = usuario.get("cliente_id")
            session["superadmin_nivel"] = usuario.get("superadmin_nivel")

            if session.get("perfil") == "equipe":
                try:
                    if perfil_equipe_incompleto_por_login(usuario["login"]):
                        return redirect(url_for("equipes.perfil_equipe_view"))
                except Exception as e:
                    print("AVISO PERFIL EQUIPE INCOMPLETO:", repr(e))

                session.pop("competicao_equipe_atual", None)
                return redirect(url_for("equipes.painel_equipe_inicio_view"))

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
            session["cliente_id"] = apontador.get("cliente_id")
            session["superadmin_nivel"] = None

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
            definir_senha_apontador(session.get("usuario"), senha, cliente_id=session.get("cliente_id"))
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
