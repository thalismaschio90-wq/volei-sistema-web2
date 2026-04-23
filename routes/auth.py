from flask import Blueprint, render_template, request, redirect, session, url_for, flash
from banco import buscar_usuario_por_login, autenticar_apontador, definir_senha_apontador

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/")
def raiz():
    if "usuario" in session:
        if session.get("perfil") == "apontador":
            return redirect(url_for("apontadores.painel_apontador"))
        return redirect(url_for("painel.inicio"))
    return redirect(url_for("auth.login"))


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if "usuario" in session:
        if session.get("perfil") == "apontador":
            return redirect(url_for("apontadores.painel_apontador"))
        return redirect(url_for("painel.inicio"))

    if request.method == "POST":
        login_digitado = request.form.get("login", "").strip()
        senha_digitada = request.form.get("senha", "").strip()

        # =====================================================
        # 1) TENTA LOGIN DE USUÁRIO NORMAL
        # =====================================================
        usuario = buscar_usuario_por_login(login_digitado)

        if usuario:
            if not usuario.get("ativo", True):
                flash("Usuário inativo.", "erro")
                return render_template("login.html")

            if usuario["senha"] != senha_digitada:
                flash("Senha incorreta.", "erro")
                return render_template("login.html")

            session["usuario"] = usuario["login"]
            session["nome"] = usuario.get("nome") or usuario["login"]
            session["perfil"] = usuario.get("perfil") or ""
            session["equipe"] = usuario.get("equipe")
            session["competicao_vinculada"] = usuario.get("competicao_vinculada")

            if session.get("perfil") == "apontador":
                return redirect(url_for("apontadores.painel_apontador"))

            return redirect(url_for("painel.inicio"))

        # =====================================================
        # 2) SE NÃO FOR USUÁRIO NORMAL, TENTA APONTADOR POR CPF
        # =====================================================
        apontador = autenticar_apontador(login_digitado, senha_digitada)

        if apontador is False:
            flash("Senha incorreta.", "erro")
            return render_template("login.html")

        if apontador:
            session["usuario"] = apontador["cpf"]
            session["nome"] = apontador.get("nome") or apontador["cpf"]
            session["perfil"] = "apontador"
            session["equipe"] = None
            session["competicao_vinculada"] = None

            if apontador.get("primeiro_acesso", True) or not apontador.get("senha"):
                return redirect(url_for("auth.criar_senha_apontador"))

            return redirect(url_for("apontadores.painel_apontador"))

        # =====================================================
        # 3) NÃO ENCONTROU NINGUÉM
        # =====================================================
        flash("Usuário não encontrado.", "erro")

    return render_template("login.html")


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

        definir_senha_apontador(session.get("usuario"), senha)
        flash("Senha criada com sucesso.", "sucesso")
        return redirect(url_for("apontadores.painel_apontador"))

    return render_template("criar_senha_apontador.html")


@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))