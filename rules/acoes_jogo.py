"""Regras puras para tempos, retardamentos, sanções e cartão verde."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping


EQUIPES_VALIDAS = {"A", "B"}
TIPOS_SANCAO_VALIDOS = {"advertencia", "penalidade", "expulsao", "desqualificacao"}
TIPOS_PESSOA_VALIDOS = {"atleta", "tecnico", "auxiliar", "membro", "equipe"}


class ErroAcaoJogo(ValueError):
    """Erro de validação de uma ação extra da partida."""


def normalizar_equipe(equipe: Any) -> str:
    valor = str(equipe or "").strip().upper()
    if valor not in EQUIPES_VALIDAS:
        raise ErroAcaoJogo("Equipe inválida.")
    return valor


def normalizar_tipo_sancao(tipo_sancao: Any) -> str:
    valor = str(tipo_sancao or "").strip().lower()
    if valor not in TIPOS_SANCAO_VALIDOS:
        raise ErroAcaoJogo("Tipo de sanção inválido.")
    return valor


def normalizar_alvo(payload: Mapping[str, Any] | None) -> dict[str, str]:
    dados = dict(payload or {})
    tipo_pessoa = str(dados.get("tipo_pessoa") or "").strip().lower()
    aliases_tipo_pessoa = {
        "jogador": "atleta",
        "jogadora": "atleta",
        "comissao": "membro",
        "comissão": "membro",
    }
    tipo_pessoa = aliases_tipo_pessoa.get(tipo_pessoa, tipo_pessoa)
    numero = str(dados.get("numero") or dados.get("alvo") or "").strip()
    nome = str(dados.get("nome") or dados.get("alvo") or "").strip()

    if not tipo_pessoa:
        raise ErroAcaoJogo("Tipo de pessoa não informado.")
    if tipo_pessoa == "atleta":
        if not numero:
            raise ErroAcaoJogo("Número do atleta não informado.")
        nome = str(dados.get("nome") or "").strip()
    elif not nome:
        raise ErroAcaoJogo("Nome do alvo não informado.")

    return {"tipo_pessoa": tipo_pessoa, "numero": numero, "nome": nome}


def validar_tempo(equipe: Any, estado: Mapping[str, Any] | None) -> tuple[str, int, int]:
    lado = normalizar_equipe(equipe)
    dados = dict(estado or {})
    usados = _inteiro(dados.get("tempos_a") if lado == "A" else dados.get("tempos_b"), 0)
    limite = max(0, _inteiro(dados.get("limite_tempos"), 2))
    if usados >= limite:
        raise ErroAcaoJogo(f"Equipe {lado} não possui mais pedidos de tempo neste set.")
    return lado, usados, limite


def validar_retardamento(equipe: Any) -> str:
    return normalizar_equipe(equipe)


def validar_cartao_verde(equipe: Any, payload: Mapping[str, Any] | None) -> tuple[str, dict[str, str]]:
    lado = normalizar_equipe(equipe)
    return lado, normalizar_alvo(payload)


def validar_sancao(equipe: Any, payload: Mapping[str, Any] | None) -> tuple[str, dict[str, str]]:
    lado = normalizar_equipe(equipe)
    dados = dict(payload or {})
    alvo = normalizar_alvo(dados)
    alvo["tipo_sancao"] = normalizar_tipo_sancao(dados.get("tipo_sancao") or dados.get("sancao"))
    alvo["observacao"] = str(dados.get("observacao") or "").strip()
    return lado, alvo


def descricao_acao(tipo: str, equipe: Any = "", payload: Mapping[str, Any] | None = None) -> str:
    dados = dict(payload or {})
    lado = str(equipe or "").strip().upper()
    equipe_txt = f"Equipe {lado}" if lado else "Equipe"
    tipo = str(tipo or "").strip().lower()

    if tipo == "tempo":
        return f"Tempo solicitado - {equipe_txt}"
    if tipo == "substituicao":
        return f"{equipe_txt} • substituição • {dados.get('numero_sai', '')}>{dados.get('numero_entra', '')}"
    if tipo == "substituicao_excepcional":
        return f"{equipe_txt} • substituição excepcional • {dados.get('numero_sai', '')}>{dados.get('numero_entra', '')}"
    if tipo == "retardamento":
        return f"{equipe_txt} • retardamento"
    if tipo == "sancao":
        return f"{equipe_txt} • sanção • {dados.get('tipo_sancao') or dados.get('sancao') or ''}"
    if tipo == "cartao_verde":
        return f"{equipe_txt} • cartão verde"
    return str(dados.get("descricao") or "Ação registrada")


def aplicar_acao_local(
    estado: Mapping[str, Any] | None,
    tipo: str,
    equipe: Any,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Aplica somente a alteração visual imediata de ações extras.

    A persistência definitiva continua no fluxo de sincronização/fim do set.
    """
    novo = deepcopy(dict(estado or {}))
    dados = dict(payload or {})
    lado = normalizar_equipe(equipe)
    tipo = str(tipo or "").strip().lower()

    if tipo == "tempo":
        _, usados, _ = validar_tempo(lado, novo)
        novo["tempos_a" if lado == "A" else "tempos_b"] = usados + 1
        novo["tempo_ativo"] = {
            "equipe": lado,
            "duracao": max(1, _inteiro(dados.get("duracao"), 30)),
        }
        return novo

    if tipo == "retardamento":
        validar_retardamento(lado)
        campo = "retardamentos_a" if lado == "A" else "retardamentos_b"
        itens = list(novo.get(campo) or [])
        itens.append({
            "equipe": lado,
            "set": _inteiro(novo.get("set_atual"), 1),
            "observacao": str(dados.get("observacao") or "").strip(),
        })
        novo[campo] = itens
        return novo

    if tipo == "cartao_verde":
        _, alvo = validar_cartao_verde(lado, dados)
        campo = "cartoes_verdes_a" if lado == "A" else "cartoes_verdes_b"
        itens = list(novo.get(campo) or [])
        itens.append(alvo)
        novo[campo] = itens
        return novo

    if tipo == "sancao":
        _, sancao = validar_sancao(lado, dados)
        campo = "sancoes_a" if lado == "A" else "sancoes_b"
        itens = list(novo.get(campo) or [])
        itens.append(sancao)
        novo[campo] = itens
        return novo

    return novo


def _inteiro(valor: Any, padrao: int = 0) -> int:
    try:
        if valor in (None, ""):
            return padrao
        return int(valor)
    except (TypeError, ValueError):
        return padrao
