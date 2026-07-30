"""Serviço do apontador para transições locais de substituição."""
from __future__ import annotations

from typing import Any, Mapping

from rules.substituicoes import (
    ErroSubstituicao,
    aplicar_substituicao_excepcional,
    aplicar_substituicao_normal,
    normalizar_equipe,
    validar_numeros,
)


def atletas_por_numero(atletas: list[Mapping[str, Any]] | None) -> dict[str, dict[str, Any]]:
    resultado: dict[str, dict[str, Any]] = {}
    for atleta in atletas or []:
        numero = str(atleta.get("numero") or "").strip()
        if numero:
            resultado[numero] = dict(atleta)
    return resultado


def validar_comando(equipe: Any, numero_sai: Any, numero_entra: Any) -> tuple[str, str, str]:
    equipe_ok = normalizar_equipe(equipe)
    sai, entra = validar_numeros(numero_sai, numero_entra)
    return equipe_ok, sai, entra


def aplicar_local(
    estado: Mapping[str, Any],
    *,
    equipe: Any,
    numero_sai: Any,
    numero_entra: Any,
    excepcional: bool = False,
    atletas: list[Mapping[str, Any]] | None = None,
    motivo: Any = "",
    observacao: Any = "",
    validar_elenco: bool = False,
) -> dict[str, Any]:
    """Aplica a troca no estado vivo.

    No caminho otimista do navegador, ``validar_elenco`` pode permanecer falso,
    porque o banco ainda executará a validação autoritativa. Mesmo assim a troca
    é atômica e não reordena nenhuma outra posição.
    """
    equipe_ok, sai, entra = validar_comando(equipe, numero_sai, numero_entra)
    mapa = atletas_por_numero(atletas)
    if not mapa and not validar_elenco:
        mapa = {sai: {}, entra: {}}
        # Para o caminho otimista, considere os atletas atuais como titulares
        # somente quando o estado ainda não tem essa informação.
        chave = "titulares_iniciais_a" if equipe_ok == "A" else "titulares_iniciais_b"
        titulares = {str(v or "").strip() for v in (estado.get(chave) or [])}
        if not titulares:
            estado = dict(estado)
            estado[chave] = [
                str(v or "").strip()
                for v in estado.get("rotacao_a" if equipe_ok == "A" else "rotacao_b", [])
            ]

    if excepcional:
        return aplicar_substituicao_excepcional(
            estado, equipe_ok, sai, entra,
            atletas_validos=mapa,
            motivo=motivo,
            observacao=observacao,
        )
    return aplicar_substituicao_normal(
        estado, equipe_ok, sai, entra,
        atletas_validos=mapa,
    )



def aplicar_estado_visual(
    estado: Mapping[str, Any],
    *,
    equipe: Any,
    numero_sai: Any,
    numero_entra: Any,
    excepcional: bool = False,
    motivo: Any = "",
    observacao: Any = "",
) -> dict[str, Any]:
    """Atualização otimista compatível com o fluxo local atual.

    Não decide a legalidade desportiva; essa validação continua no motor
    autoritativo antes da persistência. A função garante apenas que contador,
    rotação e status sejam alterados juntos, sob o lock da partida.
    """
    equipe_ok, sai, entra = validar_comando(equipe, numero_sai, numero_entra)
    resultado = dict(estado)
    sufixo = "a" if equipe_ok == "A" else "b"
    chave_rotacao = f"rotacao_{sufixo}"
    rotacao = [str(v or "").strip() for v in list(resultado.get(chave_rotacao) or [])[:6]]
    while len(rotacao) < 6:
        rotacao.append("")
    if sai not in rotacao:
        raise ErroSubstituicao("O atleta que sai não está em quadra.")
    if entra in rotacao:
        raise ErroSubstituicao("O atleta que entra já está em quadra.")
    rotacao[rotacao.index(sai)] = entra
    resultado[chave_rotacao] = rotacao

    chave_status = f"status_jogadores_{sufixo}"
    status_atual = resultado.get(chave_status)
    status = dict(status_atual) if isinstance(status_atual, Mapping) else {}
    status[sai] = {"tipo": "bloqueado_excepcional" if excepcional else "substituido", "numero_entra": entra, "em_quadra": False}
    status[entra] = {"tipo": "substituto", "numero_sai": sai, "em_quadra": True, "excepcional": bool(excepcional)}
    resultado[chave_status] = status

    if excepcional:
        lista = list(resultado.get("subs_excepcionais") or [])
        lista.append({"equipe": equipe_ok, "numero_sai": sai, "numero_entra": entra, "motivo": str(motivo or "").strip(), "observacao": str(observacao or "").strip(), "set": int(resultado.get("set_atual") or 1)})
        resultado["subs_excepcionais"] = lista
    else:
        chave_subs = f"subs_{sufixo}"
        resultado[chave_subs] = int(resultado.get(chave_subs) or 0) + 1

    resultado["substituicao_aplicada"] = {"equipe": equipe_ok, "numero_sai": sai, "numero_entra": entra, "tipo": "excepcional" if excepcional else "normal"}
    return resultado

def montar_descricao(equipe: str, sai: str, entra: str, *, excepcional: bool = False) -> str:
    nome = "Substituição excepcional" if excepcional else "Substituição"
    return f"{nome} {equipe}: #{sai} → #{entra}"


__all__ = [
    "ErroSubstituicao",
    "aplicar_local",
    "aplicar_estado_visual",
    "atletas_por_numero",
    "montar_descricao",
    "validar_comando",
]
