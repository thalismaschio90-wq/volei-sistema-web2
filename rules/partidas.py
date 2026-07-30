"""Regras puras do cadastro e manutenção de partidas."""

FASES_GRUPOS = {"classificatorias", "classificatória", "classificatorias", "grupo"}
FASES_SEMIFINAL = {"semifinais", "semi", "semis"}
FASES_FINAL = {"finais", "finalíssima", "finalissima"}
STATUS_BLOQUEADOS = {
    "em_andamento", "em andamento", "andamento", "entre_sets", "tiebreak_sorteio",
    "finalizada", "finalizado", "encerrada", "encerrado", "iniciada", "iniciado",
    "ao_vivo", "ao vivo",
}


def texto(valor):
    return str(valor or "").strip()


def normalizar_fase(fase):
    valor = texto(fase or "grupos").lower()
    if valor in FASES_GRUPOS:
        return "grupos"
    if valor in FASES_SEMIFINAL:
        return "semifinal"
    if valor in FASES_FINAL:
        return "final"
    return valor or "grupos"


def status_bloqueado(status, status_jogo=None):
    status = texto(status).lower().replace("-", "_")
    status_jogo = texto(status_jogo).lower().replace("-", "_")
    return status in STATUS_BLOQUEADOS or status_jogo in STATUS_BLOQUEADOS


def partida_iniciada_ou_finalizada(partida):
    if not partida:
        return False
    try:
        pontos_a = int(partida.get("pontos_a") or 0)
        pontos_b = int(partida.get("pontos_b") or 0)
        sets_a = int(partida.get("sets_a") or 0)
        sets_b = int(partida.get("sets_b") or 0)
    except (TypeError, ValueError, AttributeError):
        pontos_a = pontos_b = sets_a = sets_b = 0
    return (
        pontos_a > 0 or pontos_b > 0 or sets_a > 0 or sets_b > 0
        or bool(partida.get("pre_jogo_iniciado_em"))
        or bool(partida.get("pre_jogo_finalizado"))
        or status_bloqueado(partida.get("status"), partida.get("status_jogo"))
    )


def normalizar_limite(limite, padrao=50, minimo=1, maximo=200):
    try:
        valor = int(limite or padrao)
    except (TypeError, ValueError):
        valor = padrao
    return max(minimo, min(valor, maximo))


def grupo_para_fase(grupo, fase, grupo_atual=None):
    fase = normalizar_fase(fase)
    if fase != "grupos":
        return None
    return texto(grupo) or grupo_atual
