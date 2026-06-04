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


def _primeiro_valor_placar(dados, chaves, padrao=0):
    dados = dados or {}
    for chave in chaves:
        if chave in dados and dados.get(chave) not in (None, ""):
            return dados.get(chave)
    return padrao


def aplicar_placar_exibicao_partida(partida, competicao=None):
    """
    Preenche campos padronizados para todos os cards/telas.

    Regras:
    - set único: placar principal = pontos do set/resultado da partida (25 x 20)
    - melhor de 3/5: placar principal = sets vencidos (2 x 1)

    Observação: em partidas finalizadas de set único, alguns fluxos salvam o resultado
    em set1_a/set1_b; durante o jogo ao vivo, normalmente vem em pontos_a/pontos_b.
    Por isso a ordem abaixo prioriza set1 quando existe, depois pontos/placar.
    """
    if not partida:
        return partida

    comp = competicao or {}
    set_unico = competicao_eh_set_unico(partida, comp)

    if set_unico:
        a = _int_placar(_primeiro_valor_placar(
            partida,
            ["set1_a", "pontos_a", "placar_a", "pontos_equipe_a", "resultado_a"],
            0,
        ), 0)
        b = _int_placar(_primeiro_valor_placar(
            partida,
            ["set1_b", "pontos_b", "placar_b", "pontos_equipe_b", "resultado_b"],
            0,
        ), 0)
        tipo = "pontos"
        rotulo = "PONTOS"
    else:
        a = _int_placar(_primeiro_valor_placar(partida, ["sets_a", "sets_equipe_a"], 0), 0)
        b = _int_placar(_primeiro_valor_placar(partida, ["sets_b", "sets_equipe_b"], 0), 0)
        tipo = "sets"
        rotulo = "SETS"

    partida["set_unico"] = bool(set_unico)
    partida["placar_exibicao_a"] = a
    partida["placar_exibicao_b"] = b
    partida["placar_exibicao_tipo"] = tipo
    partida["placar_exibicao_rotulo"] = rotulo
    partida["placar_exibicao"] = f"{a} x {b}"
    return partida


def aplicar_placar_exibicao_lista(partidas, competicao=None):
    return [aplicar_placar_exibicao_partida(dict(p or {}), competicao) for p in (partidas or [])]
