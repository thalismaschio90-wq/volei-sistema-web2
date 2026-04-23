from flask import Blueprint, render_template, request, redirect, session, url_for
from banco import obter_dados, salvar_dados
from utils.geradores import gerar_senha_automatica

usuarios_bp = Blueprint("usuarios", __name__, url_prefix="/usuarios")


def gerar_login_mesario(nome, usuarios_existentes):
    base = nome.strip().lower()
    caracteres_invalidos = ".,;:/\\|!?@#$%¨&*()[]{}=+´`~^'\""

    for c in caracteres_invalidos:
        base = base.replace(c, "")

    base = base.replace("-", " ")
    base = "_".join(base.split())

    if not base:
        base = "mesario"

    login = f"mesa_{base}"
    contador = 1

    while login in usuarios_existentes:
        login = f"mesa_{base}_{contador}"
        contador += 1

    return login


@usuarios_bp.route("/")
def listar():
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    if session.get("perfil") != "organizador":
        return redirect(url_for("painel.index"))

    dados = obter_dados()
    usuarios = dados.get("usuarios", {})
    login_logado = session.get("usuario")
    comp = dados.get("usuarios", {}).get(login_logado, {}).get("competicao_vinculada", "")

    usuarios_filtrados = {
        login: u
        for login, u in usuarios.items()
        if u.get("competicao_vinculada", "") == comp and u.get("perfil") == "mesario"
    }

    return render_template("usuarios.html", usuarios=usuarios_filtrados)


@usuarios_bp.route("/novo", methods=["POST"])
def novo():
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    if session.get("perfil") != "organizador":
        return redirect(url_for("painel.index"))

    dados = obter_dados()
    dados.setdefault("usuarios", {})

    nome = request.form.get("nome", "").strip()
    login_logado = session.get("usuario")
    competicao_vinculada = dados.get("usuarios", {}).get(login_logado, {}).get("competicao_vinculada", "")

    if not nome:
        return redirect(url_for("usuarios.listar"))

    login = gerar_login_mesario(nome, dados["usuarios"])
    senha = gerar_senha_automatica()

    dados["usuarios"][login] = {
        "nome": nome,
        "senha": senha,
        "perfil": "mesario",
        "ativo": True,
        "equipe": None,
        "competicao_vinculada": competicao_vinculada
    }

    salvar_dados(dados)

    return render_template(
        "mesario_criado.html",
        nome=nome,
        login=login,
        senha=senha
    )


@usuarios_bp.route("/excluir/<login>")
def excluir(login):
    if "usuario" not in session:
        return redirect(url_for("auth.login"))

    if session.get("perfil") != "organizador":
        return redirect(url_for("painel.index"))

    dados = obter_dados()
    usuarios = dados.get("usuarios", {})
    login_logado = session.get("usuario")

    if login not in usuarios:
        return redirect(url_for("usuarios.listar"))

    usuario_alvo = usuarios[login]
    comp_logado = usuarios.get(login_logado, {}).get("competicao_vinculada", "")
    comp_alvo = usuario_alvo.get("competicao_vinculada", "")

    if usuario_alvo.get("perfil") != "mesario":
        return redirect(url_for("usuarios.listar"))

    if comp_logado != comp_alvo:
        return redirect(url_for("usuarios.listar"))

    del usuarios[login]
    salvar_dados(dados)

    return redirect(url_for("usuarios.listar"))