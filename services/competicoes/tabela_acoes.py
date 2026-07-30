"""Ações administrativas da tabela e das partidas.

Este serviço concentra validações e coordenação que antes estavam diretamente
nas rotas HTTP. Ele não usa Flask e retorna resultados estruturados para a rota
transformar em flash/redirecionamento.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from rules.partidas import normalizar_fase
from services.competicoes import grupos as grupos_service
from services.competicoes import partidas as partidas_service
from services.competicoes import quadras as quadras_service


@dataclass(frozen=True)
class ResultadoAcao:
    ok: bool
    mensagem: str
    categoria: str = "sucesso"
    dados: dict[str, Any] | None = None

    def como_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "mensagem": self.mensagem,
            "categoria": self.categoria,
            "dados": self.dados or {},
        }


def _erro(mensagem: str, **dados: Any) -> ResultadoAcao:
    return ResultadoAcao(False, mensagem, "erro", dados)


def _sucesso(mensagem: str, **dados: Any) -> ResultadoAcao:
    return ResultadoAcao(True, mensagem, "sucesso", dados)


def _texto(valor: Any) -> str:
    return str(valor or "").strip()


def _inteiro_ou_none(valor: Any) -> int | None:
    try:
        return int(valor) if valor not in (None, "") else None
    except (TypeError, ValueError):
        return None


def vincular_grupo_quadra(
    competicao: Mapping[str, Any],
    grupo_nome: Any,
    quadra_id: Any,
    *,
    fase_grupos_travada: Callable[[str], bool],
) -> ResultadoAcao:
    nome_competicao = _texto(competicao.get("nome"))
    grupo = _texto(grupo_nome).upper()
    quadra = _inteiro_ou_none(quadra_id)
    if not grupo:
        return _erro("Grupo inválido.")
    if fase_grupos_travada(nome_competicao):
        return _erro("A fase classificatória já iniciou. Não é possível trocar a quadra padrão do grupo.")
    if not quadra:
        return _erro("Selecione uma quadra válida.")
    if quadras_service.vincular_grupo_a_quadra(nome_competicao, grupo, quadra):
        return _sucesso(f"Grupo {grupo} vinculado à quadra.")
    return _erro("Não foi possível vincular a quadra ao grupo.")


def adicionar_equipe_grupo(
    competicao: Mapping[str, Any],
    grupo_id: Any,
    equipe: Any,
    *,
    fase_grupos_travada: Callable[[str], bool],
) -> ResultadoAcao:
    nome_competicao = _texto(competicao.get("nome"))
    equipe_nome = _texto(equipe)
    if not grupo_id or not equipe_nome:
        return _erro("Preencha todos os campos.")
    if fase_grupos_travada(nome_competicao):
        return _erro("A fase classificatória já iniciou. Não é possível alterar grupos.")
    ok = grupos_service.adicionar_equipe_no_grupo(grupo_id, equipe_nome, nome_competicao)
    return _sucesso("Equipe adicionada ao grupo.") if ok is not False else _erro("Não foi possível adicionar a equipe ao grupo.")


def remover_equipe_grupo(
    competicao: Mapping[str, Any],
    grupo_id: Any,
    equipe: Any,
    *,
    fase_grupos_travada: Callable[[str], bool],
) -> ResultadoAcao:
    nome_competicao = _texto(competicao.get("nome"))
    equipe_nome = _texto(equipe)
    if not grupo_id or not equipe_nome:
        return _erro("Dados inválidos para remover equipe do grupo.")
    if fase_grupos_travada(nome_competicao):
        return _erro("A fase classificatória já iniciou. Não é possível alterar grupos.")
    ok = grupos_service.remover_equipe_do_grupo(grupo_id, equipe_nome, nome_competicao)
    return _sucesso("Equipe removida do grupo.") if ok is not False else _erro("Não foi possível remover a equipe do grupo.")


def excluir_grupo(
    competicao: Mapping[str, Any],
    grupo_id: int,
    *,
    fase_grupos_travada: Callable[[str], bool],
) -> ResultadoAcao:
    nome_competicao = _texto(competicao.get("nome"))
    if fase_grupos_travada(nome_competicao):
        return _erro("A fase classificatória já iniciou. Não é possível excluir grupos.")
    ok = grupos_service.excluir_grupo(grupo_id, nome_competicao)
    return _sucesso("Grupo excluído com sucesso.") if ok is not False else _erro("Não foi possível excluir o grupo.")


def limpar_tabela(
    competicao: Mapping[str, Any],
    *,
    fase_grupos_travada: Callable[[str], bool],
) -> ResultadoAcao:
    nome_competicao = _texto(competicao.get("nome"))
    if fase_grupos_travada(nome_competicao):
        return _erro("A fase classificatória já iniciou. Não é possível limpar toda a tabela.")
    ok = partidas_service.limpar_partidas(nome_competicao)
    if ok is False:
        return _erro("Não foi possível limpar a tabela porque já existe partida iniciada.")
    return _sucesso("Tabela limpa com sucesso.")


def limpar_fase(
    competicao: Mapping[str, Any],
    fase: Any,
    *,
    fase_pode_ser_alterada: Callable[[str, str], bool],
) -> ResultadoAcao:
    nome_competicao = _texto(competicao.get("nome"))
    fase_banco = normalizar_fase(fase)
    if not fase_pode_ser_alterada(nome_competicao, fase_banco):
        return _erro("Esta fase já iniciou. Não é possível limpar as partidas dela.", fase=fase_banco)
    ok = partidas_service.limpar_partidas_por_fase(nome_competicao, fase_banco)
    if ok is False:
        return _erro("Não foi possível limpar esta fase porque já existe partida iniciada.", fase=fase_banco)
    return _sucesso("Partidas da fase removidas com sucesso.", fase=fase_banco)


def criar_partida_manual(
    competicao: Mapping[str, Any],
    dados: Mapping[str, Any],
    *,
    fase_pode_ser_alterada: Callable[[str, str], bool],
    estrutura_grupo_unico: Callable[[Mapping[str, Any]], bool],
    sincronizar_grupo_unico: Callable[[Mapping[str, Any]], Any],
    listar_grupos: Callable[[str], list[dict]],
    quadra_padrao_grupo: Callable[[list[dict], str], int | None],
    listar_partidas: Callable[[str], list[dict]],
    criar_partida: Callable[..., bool],
    obter_proxima_ordem: Callable[[str], int] | None = None,
) -> ResultadoAcao:
    nome_competicao = _texto(competicao.get("nome"))
    fase_banco = normalizar_fase(dados.get("fase"))
    fase_subaba = _texto(dados.get("fase_subaba") or dados.get("fase") or "classificatorias").lower()
    grupo = _texto(dados.get("grupo")).upper() if fase_banco == "grupos" else None
    equipe_a = _texto(dados.get("equipe_a"))
    equipe_b = _texto(dados.get("equipe_b"))
    quadra_id = _inteiro_ou_none(dados.get("quadra_id"))

    if fase_banco == "grupos" and estrutura_grupo_unico(competicao):
        sincronizar_grupo_unico(competicao)
        grupo = "A"
    if fase_banco == "grupos" and not quadra_id:
        quadra_id = quadra_padrao_grupo(listar_grupos(nome_competicao), grupo or "")
    if fase_banco == "grupos" and not grupo:
        return _erro("Informe o grupo para jogo classificatório.", fase_subaba=fase_subaba)
    if not fase_pode_ser_alterada(nome_competicao, fase_banco):
        return _erro("Esta fase já iniciou. Não é possível criar novas partidas nela.", fase_subaba=fase_subaba)

    if fase_banco == "grupos":
        if not equipe_a or not equipe_b:
            return _erro("Selecione as duas equipes.", fase_subaba=fase_subaba)
        if equipe_a == equipe_b:
            return _erro("A partida precisa ter duas equipes diferentes.", fase_subaba=fase_subaba)
    else:
        if equipe_a and equipe_b and equipe_a == equipe_b:
            return _erro("A partida precisa ter duas equipes diferentes.", fase_subaba=fase_subaba)
        equipe_a = equipe_a or "A definir"
        equipe_b = equipe_b or "A definir"

    if obter_proxima_ordem is not None:
        ordem = int(obter_proxima_ordem(nome_competicao) or 1)
    else:
        ordens: list[int] = []
        for partida in listar_partidas(nome_competicao):
            try:
                ordens.append(int(partida.get("ordem") or 0))
            except (TypeError, ValueError, AttributeError):
                continue
        ordem = (max(ordens) + 1) if ordens else 1
    ok = criar_partida(
        nome_competicao,
        grupo,
        equipe_a,
        equipe_b,
        ordem,
        fase_banco,
        origem="manual",
        quadra_id=quadra_id,
    )
    if not ok:
        return _erro("Não foi possível criar a partida. Verifique se esta fase já iniciou.", fase_subaba=fase_subaba)
    return _sucesso("Partida criada com sucesso.", fase_subaba=fase_subaba)


def atualizar_partida_manual(
    competicao: Mapping[str, Any],
    partida_id: int,
    dados: Mapping[str, Any],
    *,
    buscar_partida: Callable[[int, str], dict | None],
    fase_pode_ser_alterada: Callable[[str, str], bool],
    dados_quadra: Callable[[str, int | None], tuple[int | None, str | None]],
    atualizar_partida: Callable[..., bool],
) -> ResultadoAcao:
    nome_competicao = _texto(competicao.get("nome"))
    fase_banco = normalizar_fase(dados.get("fase"))
    fase_subaba = _texto(dados.get("fase_subaba") or dados.get("fase") or "classificatorias").lower()
    atual = buscar_partida(partida_id, nome_competicao) or {}
    if not atual:
        return _erro("Partida não encontrada.", fase_subaba=fase_subaba)

    equipe_a = _texto(dados.get("equipe_a") or atual.get("equipe_a"))
    equipe_b = _texto(dados.get("equipe_b") or atual.get("equipe_b"))
    if equipe_a and equipe_b and equipe_a == equipe_b:
        return _erro("A partida precisa ter duas equipes diferentes.", fase_subaba=fase_subaba)
    equipe_a = equipe_a or "A definir"
    equipe_b = equipe_b or "A definir"
    if not fase_pode_ser_alterada(nome_competicao, fase_banco):
        return _erro("Esta fase já iniciou. Não é possível alterar partidas dela.", fase_subaba=fase_subaba)

    rodada = _inteiro_ou_none(dados.get("rodada"))
    if rodada is None:
        rodada = _inteiro_ou_none(atual.get("rodada"))
    quadra_id, quadra_nome = dados_quadra(nome_competicao, _inteiro_ou_none(dados.get("quadra_id")))
    ok = atualizar_partida(
        partida_id,
        nome_competicao,
        atual.get("grupo"),
        fase_banco,
        equipe_a,
        equipe_b,
        quadra=str(quadra_id) if quadra_id else None,
        quadra_id=quadra_id,
        quadra_nome=quadra_nome,
        data_hora=_texto(dados.get("data_hora")) or None,
        rodada=rodada,
        status="aguardando",
    )
    if ok is False:
        return _erro("Não foi possível salvar. A partida já iniciou ou está bloqueada.", fase_subaba=fase_subaba)
    return _sucesso("Partida salva com sucesso.", fase_subaba=fase_subaba)


def excluir_partida(
    competicao: Mapping[str, Any],
    partida_id: int,
    *,
    excluir: Callable[[int, str], tuple[bool, str]],
) -> ResultadoAcao:
    ok, mensagem = excluir(partida_id, _texto(competicao.get("nome")))
    return _sucesso(mensagem) if ok else _erro(mensagem)
