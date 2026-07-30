"""Verificação segura do uso dos índices críticos no PostgreSQL.

O módulo executa apenas ``EXPLAIN (FORMAT JSON)`` por padrão. O modo
``ANALYZE`` é explicitamente opt-in e deve ser utilizado apenas em homologação.
Nenhum parâmetro SQL é incluído nos relatórios.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Iterable


@dataclass(frozen=True)
class VerificacaoIndice:
    nome: str
    indice_esperado: str
    sql: str
    params: tuple[Any, ...]
    descricao: str


@dataclass
class ResultadoVerificacao:
    nome: str
    indice_esperado: str
    descricao: str
    indice_usado: bool
    indices_encontrados: list[str]
    tipos_nos: list[str]
    seq_scans: list[str]
    custo_total: float | None
    linhas_estimadas: int | None
    tempo_execucao_ms: float | None
    observacoes: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def construir_verificacoes(*, competicao: str, equipe: str, partida_id: int) -> tuple[VerificacaoIndice, ...]:
    return (
        VerificacaoIndice(
            nome="eventos_por_partida",
            indice_esperado="idx_eventos_competicao_partida",
            sql="SELECT COUNT(*) FROM eventos WHERE competicao = %s AND partida_id = %s",
            params=(competicao, partida_id),
            descricao="Contagem dos eventos de uma partida.",
        ),
        VerificacaoIndice(
            nome="vinculo_equipe_normalizado",
            indice_esperado="idx_equipes_comp_nome_normalizado",
            sql=("SELECT id FROM equipes_competicoes "
                 "WHERE competicao = %s AND LOWER(TRIM(equipe_nome)) = LOWER(TRIM(%s)) LIMIT 1"),
            params=(competicao, equipe),
            descricao="Localização do vínculo da equipe pelo nome normalizado.",
        ),
        VerificacaoIndice(
            nome="partidas_equipe_lado_a",
            indice_esperado="idx_partidas_comp_equipe_a_normalizada",
            sql=("SELECT id FROM partidas WHERE competicao = %s "
                 "AND LOWER(TRIM(equipe_a)) = LOWER(TRIM(%s)) LIMIT 50"),
            params=(competicao, equipe),
            descricao="Busca das partidas da equipe pelo lado A.",
        ),
        VerificacaoIndice(
            nome="partidas_equipe_lado_b",
            indice_esperado="idx_partidas_comp_equipe_b_normalizada",
            sql=("SELECT id FROM partidas WHERE competicao = %s "
                 "AND LOWER(TRIM(equipe_b)) = LOWER(TRIM(%s)) LIMIT 50"),
            params=(competicao, equipe),
            descricao="Busca das partidas da equipe pelo lado B.",
        ),
        VerificacaoIndice(
            nome="ordenacao_partidas_competicao",
            indice_esperado="idx_partidas_comp_rodada_ordem_id",
            sql=("SELECT id, rodada, ordem FROM partidas WHERE competicao = %s "
                 "ORDER BY rodada, ordem, id LIMIT 100"),
            params=(competicao,),
            descricao="Ordenação principal da agenda da competição.",
        ),
    )


def _iterar_nos(plano: Any) -> Iterable[dict[str, Any]]:
    if isinstance(plano, list):
        for item in plano:
            yield from _iterar_nos(item)
        return
    if not isinstance(plano, dict):
        return
    if "Plan" in plano and isinstance(plano["Plan"], dict):
        yield from _iterar_nos(plano["Plan"])
    if "Node Type" in plano:
        yield plano
    for filho in plano.get("Plans", []) or []:
        yield from _iterar_nos(filho)


def analisar_plano(
    verificacao: VerificacaoIndice,
    documento_explain: Any,
) -> ResultadoVerificacao:
    raiz = documento_explain
    if isinstance(raiz, list) and raiz:
        raiz = raiz[0]
    nos = list(_iterar_nos(raiz))
    indices = sorted({str(no.get("Index Name")) for no in nos if no.get("Index Name")})
    tipos = [str(no.get("Node Type")) for no in nos if no.get("Node Type")]
    seq_scans = sorted({str(no.get("Relation Name")) for no in nos
                        if no.get("Node Type") == "Seq Scan" and no.get("Relation Name")})
    plan = raiz.get("Plan", raiz) if isinstance(raiz, dict) else {}
    custo = plan.get("Total Cost") if isinstance(plan, dict) else None
    linhas = plan.get("Plan Rows") if isinstance(plan, dict) else None
    tempo = raiz.get("Execution Time") if isinstance(raiz, dict) else None
    esperado = verificacao.indice_esperado in indices
    observacoes: list[str] = []
    if esperado:
        observacoes.append("Índice esperado encontrado no plano.")
    else:
        observacoes.append("Índice esperado não apareceu no plano.")
    if seq_scans:
        observacoes.append("Há Seq Scan; em tabelas pequenas isso pode ser uma escolha correta do planner.")
    if any(tipo in {"Index Scan", "Index Only Scan", "Bitmap Index Scan"} for tipo in tipos):
        observacoes.append("O plano utiliza ao menos uma estratégia baseada em índice.")
    return ResultadoVerificacao(
        nome=verificacao.nome,
        indice_esperado=verificacao.indice_esperado,
        descricao=verificacao.descricao,
        indice_usado=esperado,
        indices_encontrados=indices,
        tipos_nos=tipos,
        seq_scans=seq_scans,
        custo_total=float(custo) if custo is not None else None,
        linhas_estimadas=int(linhas) if linhas is not None else None,
        tempo_execucao_ms=float(tempo) if tempo is not None else None,
        observacoes=observacoes,
    )


def executar_explain(cur, verificacao: VerificacaoIndice, *, analyze: bool = False) -> ResultadoVerificacao:
    opcoes = "ANALYZE, BUFFERS, FORMAT JSON" if analyze else "FORMAT JSON"
    cur.execute(f"EXPLAIN ({opcoes}) {verificacao.sql}", verificacao.params)
    linha = cur.fetchone()
    documento = linha[0] if not isinstance(linha, dict) else next(iter(linha.values()))
    return analisar_plano(verificacao, documento)
