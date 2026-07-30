"""Serviço do cadastro básico de partidas."""

from repositories import partidas as repo
from rules.partidas import (
    grupo_para_fase,
    normalizar_fase,
    partida_iniciada_ou_finalizada,
)


def criar_tabela_partidas(*, force=False):
    return repo.criar_tabela_partidas(force=force)


def listar_partidas(competicao, *, formatar_quadra=None):
    return repo.listar_partidas(
        competicao,
        formatar_quadra=formatar_quadra,
    )


def listar_partidas_leve(
    competicao,
    *,
    limite=500,
    offset=0,
    formatar_quadra=None,
    incluir_escudos=True,
):
    return repo.listar_partidas_leve(
        competicao,
        limite=limite,
        offset=offset,
        formatar_quadra=formatar_quadra,
        incluir_escudos=incluir_escudos,
    )


def listar_estados_resumidos_partidas(competicao):
    """
    Retorna somente os campos necessários para identificar partidas ao vivo.

    Usa a consulta resumida do repositório quando disponível. O fallback
    mantém compatibilidade com versões anteriores de repositories/partidas.py
    sem impedir a inicialização da aplicação.
    """
    funcao_resumida = getattr(
        repo,
        "listar_estados_resumidos_partidas",
        None,
    )

    if callable(funcao_resumida):
        return funcao_resumida(competicao) or []

    partidas = repo.listar_partidas_leve(
        competicao,
        limite=2000,
        offset=0,
        formatar_quadra=None,
        incluir_escudos=False,
    ) or []

    campos_permitidos = (
        "id",
        "competicao",
        "status",
        "status_jogo",
        "status_operacao",
        "fase_partida",
        "sets_a",
        "sets_b",
        "pontos_a",
        "pontos_b",
        "pre_jogo_iniciado_em",
        "pre_jogo_finalizado",
    )

    return [
        {
            campo: partida.get(campo)
            for campo in campos_permitidos
        }
        for partida in partidas
    ]


def proxima_ordem_partida(competicao):
    return repo.proxima_ordem_partida(competicao)


def listar_partidas_da_equipe(
    competicao,
    equipe,
    limite=50,
    *,
    formatar_quadra=None,
):
    return repo.listar_partidas_da_equipe(
        competicao,
        equipe,
        limite,
        formatar_quadra=formatar_quadra,
    )


def buscar_partida_por_id(
    partida_id,
    competicao,
    *,
    formatar_quadra=None,
):
    return repo.buscar_partida_por_id(
        partida_id,
        competicao,
        formatar_quadra=formatar_quadra,
    )


def competicao_tem_partida_iniciada_por_fase(
    nome_competicao,
    fase=None,
):
    fase_normalizada = normalizar_fase(fase) if fase else None

    return repo.competicao_tem_partida_iniciada_por_fase(
        nome_competicao,
        fase_normalizada,
    )


def fase_pode_ser_alterada(nome_competicao, fase):
    fase_normalizada = normalizar_fase(fase)

    return not competicao_tem_partida_iniciada_por_fase(
        nome_competicao,
        fase_normalizada,
    )


def _resolver_quadra(
    competicao,
    quadra,
    quadra_id,
    quadra_nome,
    *,
    buscar_por_id=None,
    buscar_por_texto=None,
    formatar_quadra=None,
):
    quadra_encontrada = None

    if quadra_id and buscar_por_id:
        quadra_encontrada = buscar_por_id(
            competicao,
            quadra_id,
        )
    elif (quadra_nome or quadra) and buscar_por_texto:
        quadra_encontrada = buscar_por_texto(
            competicao,
            quadra_nome or quadra,
        )

    if quadra_encontrada:
        quadra_id = int(quadra_encontrada["id"])

        if formatar_quadra:
            quadra_nome = formatar_quadra(quadra_encontrada)
        else:
            quadra_nome = quadra_encontrada.get("nome") or ""

        quadra = str(quadra_id)

    return quadra, quadra_id, quadra_nome


def criar_partida(
    competicao,
    grupo,
    equipe_a,
    equipe_b,
    ordem,
    quadra=None,
    fase="grupos",
    data_hora=None,
    rodada=None,
    origem="manual",
    quadra_id=None,
    quadra_nome=None,
    *,
    buscar_colunas,
    buscar_quadra_por_id=None,
    buscar_quadra_por_texto=None,
    formatar_quadra=None,
):
    fase = normalizar_fase(fase)
    grupo = grupo_para_fase(grupo, fase)

    quadra, quadra_id, quadra_nome = _resolver_quadra(
        competicao,
        quadra,
        quadra_id,
        quadra_nome,
        buscar_por_id=buscar_quadra_por_id,
        buscar_por_texto=buscar_quadra_por_texto,
        formatar_quadra=formatar_quadra,
    )

    if not fase_pode_ser_alterada(competicao, fase):
        return False

    dados = {
        "competicao": competicao,
        "grupo": grupo,
        "equipe_a": equipe_a,
        "equipe_b": equipe_b,
        "fase": fase,
        "ordem": ordem,
        "quadra": quadra,
        "quadra_id": quadra_id,
        "quadra_nome": quadra_nome or quadra or "",
        "data_hora": data_hora,
        "rodada": rodada,
        "origem": origem,
        "status": "aguardando",
    }

    return repo.inserir_partida(
        dados,
        buscar_colunas=buscar_colunas,
    )


def atualizar_partida(
    partida_id,
    competicao,
    grupo,
    fase,
    equipe_a,
    equipe_b,
    quadra=None,
    data_hora=None,
    status="aguardando",
    rodada=None,
    quadra_id=None,
    quadra_nome=None,
    *,
    buscar_quadra_por_id=None,
    buscar_quadra_por_texto=None,
    formatar_quadra=None,
):
    fase = normalizar_fase(fase)

    atual = buscar_partida_por_id(
        partida_id,
        competicao,
        formatar_quadra=formatar_quadra,
    )

    if not atual:
        return False

    if partida_iniciada_ou_finalizada(atual):
        return False

    if not fase_pode_ser_alterada(competicao, fase):
        return False

    grupo = grupo_para_fase(
        grupo,
        fase,
        atual.get("grupo"),
    )

    quadra, quadra_id, quadra_nome = _resolver_quadra(
        competicao,
        quadra,
        quadra_id,
        quadra_nome,
        buscar_por_id=buscar_quadra_por_id,
        buscar_por_texto=buscar_quadra_por_texto,
        formatar_quadra=formatar_quadra,
    )

    dados = {
        "grupo": grupo,
        "fase": fase,
        "equipe_a": equipe_a,
        "equipe_b": equipe_b,
        "quadra": quadra,
        "quadra_id": quadra_id,
        "quadra_nome": quadra_nome,
        "data_hora": data_hora,
        "status": status,
        "rodada": rodada,
    }

    return repo.atualizar_partida(
        partida_id,
        competicao,
        dados,
    )


def excluir_partida(
    partida_id,
    competicao,
    *,
    formatar_quadra=None,
):
    partida = buscar_partida_por_id(
        partida_id,
        competicao,
        formatar_quadra=formatar_quadra,
    )

    if not partida:
        return False, "Partida não encontrada."

    if partida_iniciada_ou_finalizada(partida):
        return (
            False,
            "Não é possível excluir uma partida que já iniciou, "
            "teve pré-jogo aberto ou foi finalizada.",
        )

    fase = normalizar_fase(partida.get("fase"))

    if not fase_pode_ser_alterada(competicao, fase):
        return (
            False,
            "Esta fase já iniciou. Não é possível excluir partidas dela.",
        )

    excluiu = repo.excluir_partida(
        partida_id,
        competicao,
    )

    if excluiu:
        return True, "Partida excluída com sucesso."

    return False, "Partida não encontrada."


def limpar_partidas(competicao):
    if competicao_tem_partida_iniciada_por_fase(competicao):
        return False

    return repo.limpar_partidas(competicao)


def limpar_partidas_por_fase(competicao, fase):
    fase = normalizar_fase(fase)

    if not fase_pode_ser_alterada(competicao, fase):
        return False

    return repo.limpar_partidas_por_fase(
        competicao,
        fase,
    )