"""Coordenação da geração e persistência de partidas de mata-mata."""
from repositories.partidas import inserir_partidas_em_lote
from rules.mata_mata import montar_confrontos_mata_mata


def gerar_e_persistir_mata_mata(
    *,
    fase,
    nome_competicao,
    serie,
    classificacao,
    quartas,
    semifinais,
    resolver_vencedor,
    remover_pendentes,
    ordem_inicial,
    quadra_id,
    quadra_nome,
    buscar_data_hora,
    buscar_colunas_tabela=None,
):
    resultado = montar_confrontos_mata_mata(
        fase,
        classificacao=classificacao,
        quartas=quartas,
        semifinais=semifinais,
        resolver_vencedor=resolver_vencedor,
    )
    if not resultado.get("ok"):
        return resultado

    removidas = remover_pendentes()
    partidas = []
    for indice, (equipe_a, equipe_b) in enumerate(resultado.get("confrontos") or [], start=1):
        origem = f"avanco:{serie}:auto_{fase}_{indice}" if serie else "automatica"
        partidas.append({
            "competicao": nome_competicao,
            "grupo": None,
            "equipe_a": equipe_a,
            "equipe_b": equipe_b,
            "fase": fase,
            "ordem": ordem_inicial + indice - 1,
            "quadra_id": quadra_id,
            "quadra_nome": quadra_nome,
            "origem": origem,
            "rodada": indice,
            "data_hora": buscar_data_hora(indice),
        })

    inseridas = inserir_partidas_em_lote(partidas, buscar_colunas_tabela=buscar_colunas_tabela)
    return {
        "ok": True,
        "confrontos": resultado.get("confrontos") or [],
        "partidas": partidas,
        "inseridas": inseridas,
        "removidas": removidas,
        "mensagem": "",
    }
