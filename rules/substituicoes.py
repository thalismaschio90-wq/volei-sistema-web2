"""Regras puras do motor de substituições.

Este módulo não acessa Flask, banco de dados, cache ou Socket.IO. Ele recebe o
estado já carregado e devolve uma transição completa, pronta para persistência.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping


class ErroSubstituicao(ValueError):
    """Erro de regra que pode ser exibido ao operador."""


def _texto(valor: Any) -> str:
    return str(valor or "").strip()


def _inteiro(valor: Any, padrao: int = 0) -> int:
    try:
        return int(valor)
    except (TypeError, ValueError):
        return padrao


def normalizar_equipe(equipe: Any) -> str:
    equipe = _texto(equipe).upper()
    if equipe not in {"A", "B"}:
        raise ErroSubstituicao("Equipe inválida.")
    return equipe


def validar_numeros(numero_sai: Any, numero_entra: Any) -> tuple[str, str]:
    sai = _texto(numero_sai)
    entra = _texto(numero_entra)
    if not sai or not entra:
        raise ErroSubstituicao("Informe corretamente quem sai e quem entra.")
    if sai == entra:
        raise ErroSubstituicao("O atleta que entra deve ser diferente do atleta que sai.")
    return sai, entra


def _rotacao(estado: Mapping[str, Any], equipe: str) -> list[str]:
    valores = estado.get("rotacao_a" if equipe == "A" else "rotacao_b") or []
    resultado = [_texto(v) for v in list(valores)[:6]]
    while len(resultado) < 6:
        resultado.append("")
    return resultado


def _status(estado: Mapping[str, Any], equipe: str) -> dict[str, dict[str, Any]]:
    chave = "status_jogadores_a" if equipe == "A" else "status_jogadores_b"
    bruto = estado.get(chave) or {}
    if not isinstance(bruto, Mapping):
        return {}
    return {str(k): deepcopy(v) if isinstance(v, Mapping) else {} for k, v in bruto.items()}


def _titulares(estado: Mapping[str, Any], equipe: str) -> set[str]:
    chave = "titulares_iniciais_a" if equipe == "A" else "titulares_iniciais_b"
    return {_texto(v) for v in (estado.get(chave) or []) if _texto(v)}


def _vinculos(estado: Mapping[str, Any], equipe: str) -> tuple[dict[str, str], dict[str, str]]:
    sufixo = "a" if equipe == "A" else "b"
    tit_res = {
        _texto(k): _texto(v)
        for k, v in dict(estado.get(f"vinculos_titular_reserva_{sufixo}") or {}).items()
        if _texto(k) and _texto(v)
    }
    res_tit = {
        _texto(k): _texto(v)
        for k, v in dict(estado.get(f"vinculos_reserva_titular_{sufixo}") or {}).items()
        if _texto(k) and _texto(v)
    }
    return tit_res, res_tit


def _validar_presenca(rotacao: list[str], sai: str, entra: str) -> int:
    if sai not in rotacao:
        raise ErroSubstituicao("O atleta que sai não está em quadra.")
    if entra in rotacao:
        raise ErroSubstituicao("O atleta que entra já está em quadra.")
    return rotacao.index(sai)


def aplicar_substituicao_normal(
    estado: Mapping[str, Any],
    equipe: Any,
    numero_sai: Any,
    numero_entra: Any,
    *,
    atletas_validos: Mapping[str, Mapping[str, Any]],
    limite: int | None = None,
) -> dict[str, Any]:
    """Aplica uma substituição normal sem persistir nada.

    A dupla titular/reserva é fechada: o reserva somente pode sair para o retorno
    do titular original e, após o retorno, o ciclo fica encerrado no set.
    """
    equipe = normalizar_equipe(equipe)
    sai, entra = validar_numeros(numero_sai, numero_entra)
    atletas_validos = {str(k).strip(): dict(v or {}) for k, v in atletas_validos.items()}

    if sai not in atletas_validos:
        raise ErroSubstituicao("O atleta que sai não pertence à equipe ou não possui número válido.")
    if entra not in atletas_validos:
        raise ErroSubstituicao("O atleta que entra não pertence à equipe ou não possui número válido.")
    if bool(atletas_validos[sai].get("libero")) or bool(atletas_validos[entra].get("libero")):
        raise ErroSubstituicao("Líbero não participa de substituição normal. Use a regra própria do líbero.")

    limite_real = max(0, _inteiro(limite if limite is not None else estado.get("limite_substituicoes"), 6))
    chave_subs = "subs_a" if equipe == "A" else "subs_b"
    usadas = max(0, _inteiro(estado.get(chave_subs), 0))
    if usadas >= limite_real:
        raise ErroSubstituicao("Limite de substituições atingido neste set.")

    rotacao = _rotacao(estado, equipe)
    posicao = _validar_presenca(rotacao, sai, entra)
    status = _status(estado, equipe)
    status_sai = dict(status.get(sai) or {})
    status_entra = dict(status.get(entra) or {})
    titulares = _titulares(estado, equipe)
    tit_res, res_tit = _vinculos(estado, equipe)

    sai_titular = sai in titulares
    entra_titular = entra in titulares

    if sai_titular and not entra_titular:
        if status_sai.get("tipo") in {"titular_retorno", "vinculo_encerrado", "encerrado"} or status_sai.get("substituicao_encerrada"):
            raise ErroSubstituicao(f"O titular #{sai} já retornou e não pode sair novamente neste set.")
        if status_entra.get("tipo") in {"encerrado", "vinculo_encerrado"} or status_entra.get("substituicao_encerrada"):
            raise ErroSubstituicao(f"O atleta #{entra} já teve o vínculo de substituição encerrado neste set.")
        reserva_vinculada = _texto(tit_res.get(sai))
        titular_do_reserva = _texto(res_tit.get(entra))
        if reserva_vinculada and reserva_vinculada != entra:
            raise ErroSubstituicao(f"O titular #{sai} só pode se relacionar com o reserva #{reserva_vinculada}.")
        if titular_do_reserva and titular_do_reserva != sai:
            raise ErroSubstituicao(f"O reserva #{entra} já está vinculado ao titular #{titular_do_reserva}.")
        tit_res[sai] = entra
        res_tit[entra] = sai
        status_entra.update({"em_quadra": True, "tipo": "substituto", "vinculo": sai, "substituicao_encerrada": False})
        status_sai.update({"em_quadra": False, "tipo": "titular_substituido", "vinculo": entra})
        movimento = "saida_titular"
    elif not sai_titular and entra_titular:
        titular_esperado = _texto(res_tit.get(sai) or status_sai.get("vinculo"))
        reserva_esperado = _texto(tit_res.get(entra) or status_entra.get("vinculo"))
        if titular_esperado != entra or (reserva_esperado and reserva_esperado != sai):
            raise ErroSubstituicao(f"O atleta #{sai} só pode sair para o retorno do titular ao qual está vinculado.")
        res_tit.pop(sai, None)
        tit_res.pop(entra, None)
        status_sai.update({"em_quadra": False, "tipo": "vinculo_encerrado", "vinculo": entra, "substituicao_encerrada": True})
        status_entra.update({"em_quadra": True, "tipo": "titular_retorno", "vinculo": sai, "substituicao_encerrada": True})
        movimento = "retorno_titular"
    else:
        raise ErroSubstituicao("Substituição normal deve ser titular por reserva ou retorno do titular. Para exceções, use substituição excepcional.")

    rotacao[posicao] = entra
    status[sai] = status_sai
    status[entra] = status_entra

    resultado = deepcopy(dict(estado))
    sufixo = "a" if equipe == "A" else "b"
    resultado[f"rotacao_{sufixo}"] = rotacao
    resultado[f"status_jogadores_{sufixo}"] = status
    resultado[f"vinculos_titular_reserva_{sufixo}"] = tit_res
    resultado[f"vinculos_reserva_titular_{sufixo}"] = res_tit
    resultado[chave_subs] = usadas + 1
    resultado["limite_substituicoes"] = limite_real
    resultado["substituicao_aplicada"] = {
        "equipe": equipe,
        "numero_sai": sai,
        "numero_entra": entra,
        "tipo": "normal",
        "movimento": movimento,
        "posicao_indice": posicao,
    }
    return resultado


def aplicar_substituicao_excepcional(
    estado: Mapping[str, Any],
    equipe: Any,
    numero_sai: Any,
    numero_entra: Any,
    *,
    atletas_validos: Mapping[str, Mapping[str, Any]],
    motivo: Any = "",
    observacao: Any = "",
) -> dict[str, Any]:
    equipe = normalizar_equipe(equipe)
    sai, entra = validar_numeros(numero_sai, numero_entra)
    atletas_validos = {str(k).strip(): dict(v or {}) for k, v in atletas_validos.items()}
    if entra not in atletas_validos:
        raise ErroSubstituicao("O atleta que entra não pertence ao elenco aprovado da equipe.")

    rotacao = _rotacao(estado, equipe)
    posicao = _validar_presenca(rotacao, sai, entra)
    rotacao[posicao] = entra
    status = _status(estado, equipe)
    motivo_txt = _texto(motivo).lower() or "excepcional"
    observacao_txt = _texto(observacao)
    status[entra] = {"tipo": "substituto", "vinculo": sai, "excepcional": True, "em_quadra": True}
    status[sai] = {"tipo": "bloqueado_excepcional", "motivo": motivo_txt, "em_quadra": False}

    resultado = deepcopy(dict(estado))
    sufixo = "a" if equipe == "A" else "b"
    resultado[f"rotacao_{sufixo}"] = rotacao
    resultado[f"status_jogadores_{sufixo}"] = status
    excepcionais = list(resultado.get("subs_excepcionais") or [])
    excepcionais.append({
        "equipe": equipe,
        "numero_sai": sai,
        "numero_entra": entra,
        "motivo": motivo_txt,
        "observacao": observacao_txt,
        "set": _inteiro(resultado.get("set_atual"), 1),
    })
    resultado["subs_excepcionais"] = excepcionais
    resultado["substituicao_aplicada"] = {
        "equipe": equipe,
        "numero_sai": sai,
        "numero_entra": entra,
        "tipo": "excepcional",
        "posicao_indice": posicao,
    }
    return resultado
