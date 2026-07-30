"""Tarefas de relatório executáveis por um worker RQ separado."""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any


@dataclass(frozen=True, slots=True)
class SolicitacaoRelatorio:
    tipo: str
    competicao: str
    perfil: str = ""
    equipe_logada_nome: str = ""
    equipe_filtro: str = ""
    partida_id: str = ""
    quadra_filtro: str = ""

    def serializar(self) -> dict[str, Any]:
        return asdict(self)


def nome_tarefa_geracao() -> str:
    return "relatorios.gerar"


def executar_geracao_relatorio(dados: dict[str, Any]) -> dict[str, Any]:
    """Gera os dados e os coloca no mesmo cache usado pelo preview/PDF."""
    from services.relatorios.geracao import _montar_relatorio
    from services.relatorios.cache import gerar_com_cache

    solicitacao = SolicitacaoRelatorio(**{k: dados.get(k, "") for k in SolicitacaoRelatorio.__dataclass_fields__})
    equipe_logada = {"nome": solicitacao.equipe_logada_nome} if solicitacao.equipe_logada_nome else None
    resultado = gerar_com_cache(
        solicitacao.tipo,
        solicitacao.competicao,
        lambda: _montar_relatorio(
            solicitacao.tipo,
            solicitacao.competicao,
            equipe_logada=equipe_logada,
            equipe_filtro=solicitacao.equipe_filtro or None,
            partida_id=solicitacao.partida_id or None,
            quadra_filtro=solicitacao.quadra_filtro or None,
        ),
        ignorar_cache=True,
        perfil=solicitacao.perfil,
        equipe_logada=solicitacao.equipe_logada_nome,
        equipe_filtro=solicitacao.equipe_filtro,
        partida_id=solicitacao.partida_id,
        quadra_filtro=solicitacao.quadra_filtro,
    )
    return {"ok": True, "tipo": solicitacao.tipo, "titulo": resultado.titulo, "linhas": len(resultado.linhas)}
