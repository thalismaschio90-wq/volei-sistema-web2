"""Regras puras do domínio de quadras."""
import re


def normalizar_nome_competicao(valor):
    return " ".join(str(valor or "").strip().split())


def normalizar_pin_arbitragem(pin):
    pin = re.sub(r"\D", "", str(pin or ""))
    return pin if len(pin) == 4 else ""


def formatar_quadra_exibicao(quadra):
    if not quadra:
        return ""
    nome = str(quadra.get("nome") or "").strip()
    local = str(quadra.get("local") or "").strip()
    if not nome:
        ordem = quadra.get("ordem") or ""
        nome = f"Quadra {ordem}".strip()
    if local and local.lower() not in nome.lower():
        return f"{nome} — {local}"
    return nome


def normalizar_texto_quadra(valor):
    texto = str(valor or "").strip().lower()
    texto = texto.replace("—", "-").replace("–", "-")
    texto = re.sub(r"\s+", " ", texto)
    texto = re.sub(r"[^a-z0-9áàâãéèêíïóôõöúçñ _.-]", "", texto)
    return texto.strip()


def quadra_matches_texto(quadra, texto):
    texto = normalizar_texto_quadra(texto)
    if not texto:
        return False
    nome = normalizar_texto_quadra(quadra.get("nome"))
    local = normalizar_texto_quadra(quadra.get("local"))
    exibicao = normalizar_texto_quadra(formatar_quadra_exibicao(quadra))
    ordem = str(quadra.get("ordem") or "").strip()
    qid = str(quadra.get("id") or "").strip()
    candidatos = {nome, local, exibicao, qid}
    if ordem:
        candidatos.update({ordem, f"quadra {ordem}", f"q{ordem}"})
    if "-" in texto:
        candidatos.update(p.strip() for p in texto.split("-") if p.strip())
    return texto in {c for c in candidatos if c}


def normalizar_quantidade_quadras(valor):
    try:
        valor = int(valor or 1)
    except (TypeError, ValueError):
        valor = 1
    return max(1, valor)


def normalizar_lista_quadras(quadras):
    resultado = []
    for idx, item in enumerate(quadras or [], start=1):
        item = item or {}
        nome = " ".join(str(item.get("nome") or f"Quadra {idx}").strip().split())
        local = " ".join(str(item.get("local") or "").strip().split())
        try:
            ordem = max(1, int(item.get("ordem") or idx))
        except (TypeError, ValueError):
            ordem = idx
        try:
            qid = int(item.get("id")) if item.get("id") else None
        except (TypeError, ValueError):
            qid = None
        resultado.append({"id": qid, "nome": nome, "local": local, "ordem": ordem, "ativa": bool(item.get("ativa", True))})
    if not resultado:
        resultado = [{"id": None, "nome": "Quadra 1", "local": "", "ordem": 1, "ativa": True}]
    if not any(q["ativa"] for q in resultado):
        resultado[0]["ativa"] = True
    return resultado
