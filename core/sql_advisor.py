"""Análise estrutural segura de SQL lento.

O módulo não recebe parâmetros e não preserva literais. Ele extrai apenas nomes
estruturais (tabelas e colunas) de consultas já normalizadas para sugerir
investigações e possíveis índices. As sugestões nunca são aplicadas
automaticamente no PostgreSQL.
"""
from __future__ import annotations

import re
from typing import Any

_IDENT = r'(?:(?:"[^"]+")|(?:[a-zA-Z_][\w$]*))'
_QUALIFIED = rf'{_IDENT}(?:\s*\.\s*{_IDENT})?'

_TABLE_RE = re.compile(rf'\b(?:FROM|JOIN|UPDATE|INTO)\s+({_QUALIFIED})', re.IGNORECASE)
_WHERE_ORDER_RE = re.compile(
    r'\b(?:WHERE|AND|OR|ON|ORDER\s+BY|GROUP\s+BY)\s+(.+?)(?=\b(?:WHERE|AND|OR|ON|ORDER\s+BY|GROUP\s+BY|LIMIT|OFFSET|RETURNING|UNION|$))',
    re.IGNORECASE,
)
_COLUMN_RE = re.compile(rf'({_QUALIFIED})\s*(?:=|<>|!=|<=|>=|<|>|IN\s*\(|LIKE\b|ILIKE\b|IS\s+(?:NOT\s+)?NULL)', re.IGNORECASE)
_ORDER_GROUP_COLUMN_RE = re.compile(rf'({_QUALIFIED})(?:\s+(?:ASC|DESC))?', re.IGNORECASE)


def _clean_ident(value: str) -> str:
    value = re.sub(r'\s+', '', str(value or ''))
    return value.replace('"', '')[:120]


def _base_column(value: str) -> str:
    clean = _clean_ident(value)
    return clean.split('.')[-1]


def analisar_estrutura_sql(sql_normalizado: str) -> dict[str, Any]:
    """Extrai somente estrutura não sensível da consulta."""
    texto = str(sql_normalizado or '')[:4000]
    operacao = texto.split(' ', 1)[0].upper() if texto.strip() else 'SQL'
    tabelas: list[str] = []
    for match in _TABLE_RE.finditer(texto):
        tabela = _clean_ident(match.group(1))
        if tabela and tabela not in tabelas:
            tabelas.append(tabela)

    filtros: list[str] = []
    ordenacao: list[str] = []
    agrupamento: list[str] = []

    where_match = re.search(
        r'\bWHERE\s+(.+?)(?=\bORDER\s+BY\b|\bGROUP\s+BY\b|\bLIMIT\b|\bOFFSET\b|\bRETURNING\b|\bUNION\b|$)',
        texto,
        re.IGNORECASE,
    )
    if where_match:
        for col_match in _COLUMN_RE.finditer(where_match.group(1)):
            coluna = _base_column(col_match.group(1))
            if coluna and coluna not in filtros:
                filtros.append(coluna)

    for keyword, destino in ((r'ORDER\s+BY', ordenacao), (r'GROUP\s+BY', agrupamento)):
        match = re.search(
            rf'\b{keyword}\s+(.+?)(?=\bLIMIT\b|\bOFFSET\b|\bRETURNING\b|\bUNION\b|$)',
            texto,
            re.IGNORECASE,
        )
        if not match:
            continue
        for parte in match.group(1).split(','):
            ident_match = re.search(_QUALIFIED, parte.strip())
            if not ident_match:
                continue
            coluna = _base_column(ident_match.group(0))
            if coluna and coluna not in destino:
                destino.append(coluna)

    upper = texto.upper()
    return {
        'operacao': operacao,
        'tabelas': tabelas[:8],
        'filtros': filtros[:12],
        'ordenacao': ordenacao[:8],
        'agrupamento': agrupamento[:8],
        'tem_join': ' JOIN ' in f' {upper} ',
        'tem_select_star': bool(re.search(r'\bSELECT\s+\*', texto, re.IGNORECASE)),
    }


def sugerir_otimizacoes(estrutura: dict[str, Any]) -> list[dict[str, str]]:
    """Gera recomendações conservadoras, sem criar SQL executável automaticamente."""
    operacao = str(estrutura.get('operacao') or 'SQL').upper()
    tabelas = [str(x) for x in estrutura.get('tabelas') or []]
    filtros = [str(x) for x in estrutura.get('filtros') or []]
    ordenacao = [str(x) for x in estrutura.get('ordenacao') or []]
    agrupamento = [str(x) for x in estrutura.get('agrupamento') or []]
    sugestoes: list[dict[str, str]] = []

    if operacao == 'SELECT' and estrutura.get('tem_select_star'):
        sugestoes.append({
            'tipo': 'colunas',
            'prioridade': 'media',
            'titulo': 'Evitar SELECT *',
            'detalhe': 'Selecionar somente as colunas usadas reduz tráfego, memória e custo de serialização.',
        })

    if operacao == 'SELECT' and tabelas and filtros:
        colunas = filtros + [c for c in ordenacao if c not in filtros]
        colunas = colunas[:5]
        sugestoes.append({
            'tipo': 'indice',
            'prioridade': 'alta',
            'titulo': f'Revisar índice em {tabelas[0]}',
            'detalhe': 'Candidato estrutural: (' + ', '.join(colunas) + '). Confirmar com EXPLAIN (ANALYZE, BUFFERS) antes de criar.',
        })
    elif operacao == 'SELECT' and tabelas and ordenacao:
        sugestoes.append({
            'tipo': 'indice',
            'prioridade': 'media',
            'titulo': f'Revisar índice de ordenação em {tabelas[0]}',
            'detalhe': 'A ordenação usa: ' + ', '.join(ordenacao[:5]) + '. Verificar se há filtro seletivo e índice compatível.',
        })

    if estrutura.get('tem_join'):
        sugestoes.append({
            'tipo': 'join',
            'prioridade': 'media',
            'titulo': 'Verificar colunas dos JOINs',
            'detalhe': 'Confirme índices nas chaves usadas nos JOINs e procure loops aninhados caros no plano de execução.',
        })

    if agrupamento:
        sugestoes.append({
            'tipo': 'agregacao',
            'prioridade': 'media',
            'titulo': 'Revisar agregação',
            'detalhe': 'GROUP BY em ' + ', '.join(agrupamento[:5]) + '. Avaliar pré-agregação, cache ou índice compatível com filtros.',
        })

    if operacao in {'UPDATE', 'DELETE'} and filtros:
        sugestoes.append({
            'tipo': 'escrita',
            'prioridade': 'alta',
            'titulo': 'Garantir índice no filtro da escrita',
            'detalhe': 'UPDATE/DELETE filtrado por ' + ', '.join(filtros[:5]) + ' pode bloquear e varrer muitas linhas sem índice.',
        })

    if not sugestoes:
        sugestoes.append({
            'tipo': 'plano',
            'prioridade': 'baixa',
            'titulo': 'Capturar plano de execução',
            'detalhe': 'A estrutura não permite recomendar índice com segurança. Use EXPLAIN (ANALYZE, BUFFERS) em homologação.',
        })

    return sugestoes[:5]
