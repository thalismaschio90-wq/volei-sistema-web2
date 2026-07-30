"""Coordenação leve do registro e da publicação de pontos."""
from __future__ import annotations

from typing import Any, Callable, Mapping

from rules.pontos_jogo import detalhes_evento_ponto, normalizar_comando_ponto


CHAVES_RESPOSTA_PONTO = (
    "pontos_a", "pontos_b", "placar_a", "placar_b",
    "sets_a", "sets_b", "set_atual",
    "saque_atual", "sacador_nome", "sacador_numero",
    "rotacao_a", "rotacao_b",
    "tempos_a", "tempos_b", "limite_tempos",
    "subs_a", "subs_b", "limite_substituicoes",
    "sets_tipo", "sets_para_vencer", "sets_max",
    "pontos_set", "ponto_alvo_set", "pontos_para_vencer_set",
    "pontos_tiebreak", "diferenca_minima",
    "fim_set", "set_finalizado", "fim_jogo",
    "partida_finalizada", "encerrado",
    "status_jogo", "fase_partida",
    "vencedor_set", "vencedor_partida",
    "ultima_acao", "lados_invertidos_apontador",
    "ultima_inversao_automatica_set",
)


def preparar_registro_ponto(dados: Mapping[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    comando = normalizar_comando_ponto(dados)
    comando_id = str(dados.get("comando_id") or "").strip()[:100]
    if comando_id:
        comando["comando_id"] = comando_id
    detalhes = detalhes_evento_ponto(comando)
    if comando_id:
        detalhes["comando_id"] = comando_id
    return comando, detalhes


def descricao_ponto(comando: Mapping[str, Any]) -> str:
    atleta_label = str(comando.get("atleta_label") or "").strip()
    if atleta_label:
        return f"Ponto {comando.get('equipe_pontuadora')} • {atleta_label}"
    return "Ponto registrado"


def completar_estado_registrado(
    retorno: Any,
    *,
    competicao: str,
    partida_id: int,
    comando: Mapping[str, Any],
) -> dict[str, Any]:
    estado = dict(retorno) if isinstance(retorno, dict) else {}
    estado["competicao"] = competicao
    estado["partida_id"] = partida_id
    if not estado.get("historico") or not estado.get("ultima_acao"):
        descricao = descricao_ponto(comando)
        estado["historico"] = [{"descricao": descricao}]
        estado["ultima_acao"] = descricao
    estado["_forcar_rebuild_eventos"] = False
    return estado


def montar_payload_socket_ponto(
    *,
    estado: Mapping[str, Any],
    cache_atual: Mapping[str, Any] | None,
    partida: Mapping[str, Any] | None,
    competicao: str,
    partida_id: int,
) -> dict[str, Any]:
    cache_atual = cache_atual or {}
    partida = partida or {}
    payload = dict(cache_atual)
    payload.update(estado)
    payload.update({
        "ok": True,
        "competicao": competicao,
        "partida_id": partida_id,
        "origem": "PONTO_OFICIAL",
    })
    payload["equipe_a_cadastro"] = partida.get("equipe_a") or payload.get("equipe_a_cadastro") or payload.get("equipe_a") or ""
    payload["equipe_b_cadastro"] = partida.get("equipe_b") or payload.get("equipe_b_cadastro") or payload.get("equipe_b") or ""
    payload["equipe_a_operacional"] = partida.get("equipe_a_operacional") or payload.get("equipe_a_operacional") or payload.get("equipe_a") or payload["equipe_a_cadastro"]
    payload["equipe_b_operacional"] = partida.get("equipe_b_operacional") or payload.get("equipe_b_operacional") or payload.get("equipe_b") or payload["equipe_b_cadastro"]
    payload["ultima_acao"] = estado.get("ultima_acao") or "Ponto registrado"
    payload["historico"] = estado.get("historico") or cache_atual.get("historico") or []
    return payload


def publicar_ponto(
    *,
    estado: Mapping[str, Any],
    partida: Mapping[str, Any] | None,
    competicao: str,
    partida_id: int,
    obter_cache: Callable[[int], Mapping[str, Any] | None],
    atualizar_cache: Callable[[int, Mapping[str, Any]], Any],
    emitir_estado: Callable[[int, Mapping[str, Any]], Any],
    login_apontador: str | None = None,
    emitir_placar: Callable[[str, int, Mapping[str, Any]], Any] | None = None,
) -> dict[str, Any]:
    payload = montar_payload_socket_ponto(
        estado=estado,
        cache_atual=obter_cache(partida_id) or {},
        partida=partida,
        competicao=competicao,
        partida_id=partida_id,
    )
    if login_apontador:
        # emitir_estado_partida já atualiza a sala privada do apontador quando
        # o login está no payload. Evita uma segunda emissão do mesmo placar.
        payload["apontador_login"] = login_apontador
    salvo = atualizar_cache(partida_id, payload)
    if isinstance(salvo, Mapping):
        payload = dict(salvo)
    emitir_estado(partida_id, payload)
    return payload


def jogo_finalizado(estado: Mapping[str, Any]) -> bool:
    status = str(estado.get("status_jogo") or "").strip().lower()
    return bool(
        estado.get("fim_jogo")
        or estado.get("partida_finalizada")
        or estado.get("encerrado")
        or status in {"finalizada", "encerrado"}
    )


def set_ou_jogo_finalizado(estado: Mapping[str, Any]) -> bool:
    return bool(
        estado.get("fim_set")
        or estado.get("set_finalizado")
        or jogo_finalizado(estado)
    )


def montar_resposta_ponto(
    estado: Mapping[str, Any],
    *,
    competicao: str,
    partida_id: int,
    url_observacoes: str | None = None,
) -> dict[str, Any]:
    resposta: dict[str, Any] = {
        "ok": True,
        "mensagem": "Ponto registrado com sucesso.",
        "competicao": competicao,
        "partida_id": partida_id,
    }
    for chave in CHAVES_RESPOSTA_PONTO:
        if chave in estado:
            resposta[chave] = estado.get(chave)
    resposta["historico"] = [{"descricao": estado.get("ultima_acao") or "Ponto registrado"}]

    if jogo_finalizado(resposta):
        resposta.update({
            "fim_jogo": True,
            "partida_finalizada": True,
            "encerrado": True,
            "abrir_observacoes": True,
        })
        if url_observacoes:
            resposta["url_observacoes"] = url_observacoes
    return resposta
