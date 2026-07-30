"""Preparação reutilizável de partidas para tabela, painéis e visualizador."""
from __future__ import annotations

from typing import Any, Callable, Iterable, Mapping

from rules.partidas_exibicao import (
    buscar_escudo_mapa,
    fase_partida_normalizada,
    formatar_data_hora,
    int_seguro,
    montar_parciais,
    normalizar_url_escudo,
    partida_conta_como_iniciada,
    partida_esta_ao_vivo,
    partida_esta_finalizada,
    quadra_label,
    status_exibicao,
    status_normalizado,
    to_int_or_none,
)


def preparar_partidas(
    partidas: Iterable[Mapping[str, Any]],
    mapa_escudos: Mapping[str, str] | None = None,
    competicao: Mapping[str, Any] | None = None,
    *,
    aplicar_placar_exibicao: Callable[[dict[str, Any], Mapping[str, Any]], Any],
) -> list[dict[str, Any]]:
    preparadas: list[dict[str, Any]] = []
    for registro in partidas or []:
        partida = dict(registro)
        partida["fase_normalizada"] = fase_partida_normalizada(partida)
        partida["status_normalizado"] = status_normalizado(partida)
        partida["status_exibicao"] = status_exibicao(partida)
        partida["ao_vivo"] = partida_esta_ao_vivo(partida)
        partida["finalizada"] = partida_esta_finalizada(partida)
        partida["parciais_formatadas"] = montar_parciais(partida)
        partida["pode_excluir"] = not partida_conta_como_iniciada(partida)
        partida["placar_ao_vivo_a"] = int_seguro(partida.get("pontos_a") or partida.get("placar_a"))
        partida["placar_ao_vivo_b"] = int_seguro(partida.get("pontos_b") or partida.get("placar_b"))

        aplicar_placar_exibicao(partida, competicao or {})
        if partida["ao_vivo"] and not partida["finalizada"]:
            partida["placar_ao_vivo_a"] = int_seguro(partida.get("pontos_a") or partida.get("placar_a"))
            partida["placar_ao_vivo_b"] = int_seguro(partida.get("pontos_b") or partida.get("placar_b"))
            partida["placar_ao_vivo"] = f'{partida["placar_ao_vivo_a"]} x {partida["placar_ao_vivo_b"]}'

        partida["quadra_label"] = quadra_label(partida)
        partida["quadra_id_normalizado"] = to_int_or_none(partida.get("quadra_id"))
        valor, entrada, label = formatar_data_hora(partida.get("data_hora"))
        partida["data_hora_valor"] = valor
        partida["data_hora_input"] = entrada
        partida["data_hora_label"] = label

        partida["escudo_a"] = (
            normalizar_url_escudo(partida.get("escudo_a"))
            if partida.get("escudo_a")
            else buscar_escudo_mapa(mapa_escudos, partida.get("equipe_a"))
        )
        partida["escudo_b"] = (
            normalizar_url_escudo(partida.get("escudo_b"))
            if partida.get("escudo_b")
            else buscar_escudo_mapa(mapa_escudos, partida.get("equipe_b"))
        )
        partida["equipe_a_escudo"] = partida["escudo_a"]
        partida["equipe_b_escudo"] = partida["escudo_b"]
        preparadas.append(partida)

    return sorted(
        preparadas,
        key=lambda p: (
            p.get("data_hora_valor") or "9999-12-31 23:59",
            p.get("quadra_label") or "",
            p.get("ordem") or 0,
        ),
    )
