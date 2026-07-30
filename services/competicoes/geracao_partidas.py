"""Coordenação da geração automática e persistência de partidas."""
from repositories.partidas import inserir_partidas_em_lote as _inserir_lote
from services.competicoes.grupos import listar_equipes_por_grupo
from services.competicoes.quadras import garantir_quadras_competicao
from rules.agenda_partidas import (
    gerar_rodadas_round_robin,
    gerar_slots_pool_multiquadra,
    gerar_slots_pool_quadra_unica,
    ids_quadras_ativas,
    normalizar_lista_ids,
)


def _inteiro_ou_none(valor):
    try:
        return int(valor) if valor not in (None, "") else None
    except (TypeError, ValueError):
        return None


def quadras_permitidas_para_grupo(grupos, grupo_nome, quadras_ativas, config):
    """Define as quadras que um grupo pode utilizar sem invadir quadras fixas."""
    grupo_nome = str(grupo_nome or "").strip().upper()
    quadras_ativas = [qid for qid in (quadras_ativas or []) if qid is not None]
    if not quadras_ativas:
        return []

    fixas_por_grupo = {}
    quadras_reservadas = set()
    for grupo in grupos or []:
        nome = str(grupo.get("nome") or "").strip().upper()
        qid = _inteiro_ou_none(grupo.get("quadra_id"))
        if nome and qid and qid in quadras_ativas:
            fixas_por_grupo[nome] = qid
            quadras_reservadas.add(qid)

    if grupo_nome in fixas_por_grupo:
        return [fixas_por_grupo[grupo_nome]]

    quadras_livres = [qid for qid in quadras_ativas if qid not in quadras_reservadas]
    if not quadras_livres:
        quadras_livres = list(quadras_ativas)

    config = config or {}
    compartilhados = config.get("grupos_compartilhados") or {}
    ids = normalizar_lista_ids(compartilhados.get(grupo_nome) or compartilhados.get(grupo_nome.lower()))
    ids = [qid for qid in ids if qid in quadras_livres]
    if ids:
        return ids

    ids = normalizar_lista_ids(config.get("quadras_compartilhadas"))
    ids = [qid for qid in ids if qid in quadras_livres]
    return ids or list(quadras_livres)


def montar_pools_classificatorios(grupos, quadras_ativas, config):
    pools = {}
    for grupo in grupos or []:
        nome = str(grupo.get("nome") or "").strip().upper()
        if not nome:
            continue
        permitidas = tuple(
            qid for qid in quadras_permitidas_para_grupo(grupos, nome, quadras_ativas, config)
            if qid in quadras_ativas
        )
        if permitidas:
            pools.setdefault(permitidas, []).append(nome)
    return dict(sorted(pools.items(), key=lambda item: (-len(item[0]), item[0])))


def gerar_agenda_classificatoria(nome_competicao, grupos, config):
    """Monta a agenda em memória; não grava no banco."""
    quadras = garantir_quadras_competicao(nome_competicao, 1)
    quadras_ativas = ids_quadras_ativas(quadras) or [None]

    rodadas_por_grupo = {}
    for grupo in grupos or []:
        equipes = listar_equipes_por_grupo(grupo["id"])
        nomes = [e.get("equipe") for e in equipes if e.get("equipe")]
        if len(nomes) >= 2:
            nome_grupo = str(grupo.get("nome") or "").strip().upper()
            rodadas_por_grupo[nome_grupo] = gerar_rodadas_round_robin(nomes)

    if not rodadas_por_grupo:
        return {"ok": False, "mensagem": "Não há grupos com equipes suficientes para gerar jogos."}

    pools = montar_pools_classificatorios(grupos, quadras_ativas, config)
    if not pools:
        return {"ok": False, "mensagem": "Não foi possível definir as quadras permitidas dos grupos."}

    slots_por_pool = []
    for quadras_pool, grupos_pool in pools.items():
        rodadas_pool = {
            grupo: [dict(r) if isinstance(r, dict) else list(r) for r in (rodadas_por_grupo.get(grupo) or [])]
            for grupo in grupos_pool
        }
        if len(quadras_pool) >= 2:
            slots = gerar_slots_pool_multiquadra(rodadas_pool, grupos_pool, list(quadras_pool))
        else:
            slots = gerar_slots_pool_quadra_unica(rodadas_pool, grupos_pool, quadras_pool[0])
        slots_por_pool.append(slots)

    total_slots = max((len(slots) for slots in slots_por_pool), default=0)
    agenda = []
    for indice_slot in range(total_slots):
        ordem_no_slot = 1
        for slots_pool in slots_por_pool:
            if indice_slot >= len(slots_pool):
                continue
            for jogo in slots_pool[indice_slot]:
                item = dict(jogo)
                item["slot"] = indice_slot + 1
                item["ordem_no_slot"] = ordem_no_slot
                item["rodada_grupo"] = item.get("rodada_grupo") or indice_slot + 1
                agenda.append(item)
                ordem_no_slot += 1

    if not agenda:
        return {"ok": False, "mensagem": "Não foi possível montar a agenda dos jogos."}
    return {"ok": True, "agenda": agenda, "slots": total_slots, "quadras": len(quadras_ativas)}


def inserir_partidas_em_lote(partidas, *, buscar_colunas_tabela=None):
    return _inserir_lote(partidas, buscar_colunas_tabela=buscar_colunas_tabela)
