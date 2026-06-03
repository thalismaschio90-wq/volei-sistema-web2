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

# =========================================================
# PLACAR PRINCIPAL DE EXIBIÇÃO
# =========================================================
def normalizar_sets_tipo(valor):
    texto = str(valor or "").strip().lower()
    texto = texto.replace("-", "_").replace(" ", "_")
    if texto in {"set_unico", "único", "unico", "1_set", "melhor_de_1", "md1"}:
        return "set_unico"
    if texto in {"melhor_de_5", "md5", "5"}:
        return "melhor_de_5"
    return "melhor_de_3"


def competicao_eh_set_unico(partida=None, competicao=None):
    partida = partida or {}
    competicao = competicao or {}
    sets_tipo = (
        partida.get("sets_tipo")
        or partida.get("tipo_sets")
        or partida.get("formato_sets")
        or partida.get("melhor_de")
        or competicao.get("sets_tipo")
        or competicao.get("tipo_sets")
        or competicao.get("formato_sets")
        or competicao.get("melhor_de")
        or "melhor_de_3"
    )
    return normalizar_sets_tipo(sets_tipo) == "set_unico"


def _int_placar(valor, padrao=0):
    try:
        if valor is None or valor == "":
            return padrao
        return int(valor)
    except Exception:
        return padrao


def aplicar_placar_exibicao_partida(partida, competicao=None):
    """
    Preenche campos padronizados para todos os cards/telas:
    - set único: placar principal = pontos do set/resultado (ex.: 21 x 17)
    - melhor de 3/5: placar principal = sets (ex.: 2 x 1)
    """
    if not partida:
        return partida

    comp = competicao or {}
    set_unico = competicao_eh_set_unico(partida, comp)

    if set_unico:
        a = _int_placar(
            partida.get("set1_a") if partida.get("set1_a") is not None else
            partida.get("pontos_a") if partida.get("pontos_a") is not None else
            partida.get("placar_a"),
            0,
        )
        b = _int_placar(
            partida.get("set1_b") if partida.get("set1_b") is not None else
            partida.get("pontos_b") if partida.get("pontos_b") is not None else
            partida.get("placar_b"),
            0,
        )
        tipo = "pontos"
    else:
        a = _int_placar(partida.get("sets_a"), 0)
        b = _int_placar(partida.get("sets_b"), 0)
        tipo = "sets"

    partida["set_unico"] = set_unico
    partida["placar_exibicao_a"] = a
    partida["placar_exibicao_b"] = b
    partida["placar_exibicao_tipo"] = tipo
    partida["placar_exibicao"] = f"{a} x {b}"
    return partida


def aplicar_placar_exibicao_lista(partidas, competicao=None):
    return [aplicar_placar_exibicao_partida(dict(p or {}), competicao) for p in (partidas or [])]
