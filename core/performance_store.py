"""Agregador em memória das métricas de performance da aplicação.

O armazenamento é intencionalmente leve e limitado. Ele não guarda SQL, parâmetros,
URLs com query string, corpos de requisição ou dados de usuários.
"""
from __future__ import annotations

import math
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from core.sql_advisor import sugerir_otimizacoes


def _env_int(nome: str, padrao: int, minimo: int, maximo: int) -> int:
    try:
        valor = int(os.environ.get(nome, padrao))
    except (TypeError, ValueError):
        valor = padrao
    return max(minimo, min(maximo, valor))


def _percentil(valores: list[float], percentual: float) -> float:
    if not valores:
        return 0.0
    ordenados = sorted(valores)
    indice = max(0, min(len(ordenados) - 1, math.ceil((percentual / 100.0) * len(ordenados)) - 1))
    return round(float(ordenados[indice]), 2)


@dataclass
class RouteAggregate:
    metodo: str
    endpoint: str
    rota: str
    amostras: deque[float]
    quantidade: int = 0
    erros: int = 0
    duracao_total_ms: float = 0.0
    duracao_max_ms: float = 0.0
    sql_quantidade_total: int = 0
    sql_duracao_total_ms: float = 0.0
    sql_max_ms: float = 0.0
    ultima_em: float = 0.0
    secoes_total_ms: dict[str, float] = field(default_factory=dict)


@dataclass
class QueryAggregate:
    fingerprint: str
    operacao: str
    quantidade: int = 0
    duracao_total_ms: float = 0.0
    duracao_max_ms: float = 0.0
    ultima_em: float = 0.0
    rotas: set[str] = field(default_factory=set)
    origens: set[str] = field(default_factory=set)
    estrutura: dict[str, Any] = field(default_factory=dict)
    plano: dict[str, Any] = field(default_factory=dict)


@dataclass
class RepeatedQueryAggregate:
    chave: str
    fingerprint: str
    operacao: str
    rota: str
    ocorrencias: int = 0
    requisicoes_afetadas: int = 0
    repeticoes_total: int = 0
    duracao_total_ms: float = 0.0
    duracao_max_ms: float = 0.0
    origem: str = ""
    prioridade: str = "baixa"
    ultima_em: float = 0.0


class PerformanceStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._rotas: dict[str, RouteAggregate] = {}
        self._consultas: dict[str, QueryAggregate] = {}
        self._repetidas: dict[str, RepeatedQueryAggregate] = {}
        self._iniciado_em = time.time()

    @property
    def sample_limit(self) -> int:
        return _env_int("PERFORMANCE_SAMPLE_LIMIT", 200, 20, 2000)

    @property
    def route_limit(self) -> int:
        return _env_int("PERFORMANCE_ROUTE_LIMIT", 300, 20, 2000)

    @property
    def query_limit(self) -> int:
        return _env_int("PERFORMANCE_QUERY_LIMIT", 300, 20, 2000)

    def registrar_requisicao(
        self,
        *,
        metodo: str,
        endpoint: str,
        rota: str,
        status: int,
        duracao_ms: float,
        sql: dict[str, Any],
        secoes: dict[str, float] | None = None,
    ) -> None:
        chave = f"{metodo.upper()}:{endpoint or rota}"
        agora = time.time()
        with self._lock:
            item = self._rotas.get(chave)
            if item is None:
                if len(self._rotas) >= self.route_limit:
                    mais_antiga = min(self._rotas, key=lambda k: self._rotas[k].ultima_em)
                    self._rotas.pop(mais_antiga, None)
                item = RouteAggregate(
                    metodo=metodo.upper(),
                    endpoint=endpoint or "",
                    rota=rota,
                    amostras=deque(maxlen=self.sample_limit),
                )
                self._rotas[chave] = item

            item.quantidade += 1
            item.erros += int(status >= 500)
            item.duracao_total_ms += float(duracao_ms)
            item.duracao_max_ms = max(item.duracao_max_ms, float(duracao_ms))
            item.sql_quantidade_total += int(sql.get("quantidade") or 0)
            item.sql_duracao_total_ms += float(sql.get("duracao_total_ms") or 0.0)
            item.sql_max_ms = max(item.sql_max_ms, float(sql.get("duracao_max_ms") or 0.0))
            item.ultima_em = agora
            item.amostras.append(float(duracao_ms))
            for nome, valor in (secoes or {}).items():
                item.secoes_total_ms[nome] = item.secoes_total_ms.get(nome, 0.0) + float(valor or 0.0)

            for lenta in sql.get("lentas") or []:
                fingerprint = str(lenta.get("fingerprint") or "").strip()
                if not fingerprint:
                    continue
                consulta = self._consultas.get(fingerprint)
                if consulta is None:
                    if len(self._consultas) >= self.query_limit:
                        mais_antiga = min(self._consultas, key=lambda k: self._consultas[k].ultima_em)
                        self._consultas.pop(mais_antiga, None)
                    consulta = QueryAggregate(
                        fingerprint=fingerprint,
                        operacao=str(lenta.get("operacao") or "SQL"),
                    )
                    self._consultas[fingerprint] = consulta
                duracao = float(lenta.get("duracao_ms") or 0.0)
                consulta.quantidade += 1
                consulta.duracao_total_ms += duracao
                consulta.duracao_max_ms = max(consulta.duracao_max_ms, duracao)
                consulta.ultima_em = agora
                if len(consulta.rotas) < 10:
                    consulta.rotas.add(endpoint or rota)
                origem = str(lenta.get("origem") or "").strip()
                if origem and len(consulta.origens) < 10:
                    consulta.origens.add(origem)
                estrutura = lenta.get("estrutura")
                if isinstance(estrutura, dict) and not consulta.estrutura:
                    consulta.estrutura = dict(estrutura)
                plano = lenta.get("plano")
                if isinstance(plano, dict) and plano.get("ok") and not consulta.plano:
                    consulta.plano = dict(plano)

            rota_nome = endpoint or rota
            for repetida in sql.get("repetidas") or []:
                fingerprint = str(repetida.get("fingerprint") or "").strip()
                if not fingerprint:
                    continue
                chave_repetida = f"{rota_nome}:{fingerprint}"
                agregado = self._repetidas.get(chave_repetida)
                if agregado is None:
                    agregado = RepeatedQueryAggregate(
                        chave=chave_repetida,
                        fingerprint=fingerprint,
                        operacao=str(repetida.get("operacao") or "SQL"),
                        rota=rota_nome,
                    )
                    self._repetidas[chave_repetida] = agregado
                agregado.ocorrencias += 1
                agregado.requisicoes_afetadas += 1
                agregado.repeticoes_total += int(repetida.get("quantidade") or 0)
                agregado.duracao_total_ms += float(repetida.get("duracao_total_ms") or 0.0)
                agregado.duracao_max_ms = max(
                    agregado.duracao_max_ms, float(repetida.get("duracao_max_ms") or 0.0)
                )
                if not agregado.origem:
                    agregado.origem = str(repetida.get("origem") or "")
                prioridade = str(repetida.get("prioridade") or "baixa")
                ordem = {"baixa": 0, "media": 1, "alta": 2}
                if ordem.get(prioridade, 0) > ordem.get(agregado.prioridade, 0):
                    agregado.prioridade = prioridade
                agregado.ultima_em = agora

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            rotas = []
            for item in self._rotas.values():
                qtd = max(1, item.quantidade)
                amostras = list(item.amostras)
                rotas.append({
                    "metodo": item.metodo,
                    "endpoint": item.endpoint,
                    "rota": item.rota,
                    "quantidade": item.quantidade,
                    "erros": item.erros,
                    "duracao_media_ms": round(item.duracao_total_ms / qtd, 2),
                    "duracao_p95_ms": _percentil(amostras, 95),
                    "duracao_max_ms": round(item.duracao_max_ms, 2),
                    "sql_media_consultas": round(item.sql_quantidade_total / qtd, 2),
                    "sql_media_ms": round(item.sql_duracao_total_ms / qtd, 2),
                    "sql_max_ms": round(item.sql_max_ms, 2),
                    "ultima_em": item.ultima_em,
                    "secoes_media_ms": {k: round(v / qtd, 2) for k, v in item.secoes_total_ms.items()},
                })

            consultas = []
            for item in self._consultas.values():
                qtd = max(1, item.quantidade)
                consultas.append({
                    "fingerprint": item.fingerprint,
                    "operacao": item.operacao,
                    "quantidade": item.quantidade,
                    "duracao_media_ms": round(item.duracao_total_ms / qtd, 2),
                    "duracao_max_ms": round(item.duracao_max_ms, 2),
                    "rotas": sorted(item.rotas),
                    "origens": sorted(item.origens),
                    "estrutura": dict(item.estrutura),
                    "sugestoes": sugerir_otimizacoes(item.estrutura),
                    "plano": dict(item.plano),
                    "ultima_em": item.ultima_em,
                })

            rotas.sort(key=lambda x: (x["duracao_p95_ms"], x["sql_media_ms"]), reverse=True)
            repetidas = []
            for item in self._repetidas.values():
                qtd_req = max(1, item.requisicoes_afetadas)
                repetidas.append({
                    "fingerprint": item.fingerprint,
                    "operacao": item.operacao,
                    "rota": item.rota,
                    "requisicoes_afetadas": item.requisicoes_afetadas,
                    "repeticoes_total": item.repeticoes_total,
                    "repeticoes_media_por_requisicao": round(item.repeticoes_total / qtd_req, 2),
                    "duracao_total_ms": round(item.duracao_total_ms, 2),
                    "duracao_media_por_requisicao_ms": round(item.duracao_total_ms / qtd_req, 2),
                    "duracao_max_ms": round(item.duracao_max_ms, 2),
                    "origem": item.origem,
                    "prioridade": item.prioridade,
                    "ultima_em": item.ultima_em,
                })

            ordem_prioridade = {"alta": 0, "media": 1, "baixa": 2}
            repetidas.sort(key=lambda x: (ordem_prioridade.get(x["prioridade"], 9), -x["duracao_total_ms"], -x["repeticoes_total"]))
            consultas.sort(key=lambda x: (x["duracao_max_ms"], x["quantidade"]), reverse=True)
            return {
                "ok": True,
                "backend": "local",
                "iniciado_em": self._iniciado_em,
                "gerado_em": time.time(),
                "rotas": rotas,
                "consultas_lentas": consultas,
                "possiveis_n_plus_one": repetidas,
                "limites": {
                    "amostras_por_rota": self.sample_limit,
                    "rotas": self.route_limit,
                    "consultas": self.query_limit,
                },
            }

    def limpar(self) -> None:
        with self._lock:
            self._rotas.clear()
            self._consultas.clear()
            self._repetidas.clear()
            self._iniciado_em = time.time()


performance_store = PerformanceStore()
