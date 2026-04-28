from functools import wraps
from flask import session, redirect, url_for, flash


def usuario_logado():
    return "usuario" in session


def perfil_atual():
    return session.get("perfil", "").lower().strip()


def exigir_login():
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not usuario_logado():
                return redirect(url_for("auth.login"))
            return func(*args, **kwargs)
        return wrapper
    return decorator


def exigir_perfil(*perfis):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not usuario_logado():
                return redirect(url_for("auth.login"))

            if perfil_atual() not in perfis:
                flash("Você não tem permissão para acessar esta área.", "erro")
                return redirect(url_for("painel.inicio"))

            return func(*args, **kwargs)

        return wrapper
    return decorator


# =========================================================
# COMPATIBILIDADE COM ROTAS ANTIGAS
# =========================================================
def login_obrigatorio(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not usuario_logado():
            return redirect(url_for("auth.login"))
        return func(*args, **kwargs)
    return wrapper