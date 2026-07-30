"""Métricas de homologação do transporte incremental Socket.IO.

O módulo mantém apenas contadores agregados e tipos de cliente. Nenhum estado da
partida, nome, CPF ou conteúdo de payload é armazenado.
"""
from __future__ import annotations

import os
import threading
import time
from collections import defaultdict, deque
from copy import deepcopy
from typing import Any


def _env_int(nome: str, padrao: int) -> int:
    try:
        return max(0, int(os.environ.get(nome, str(padrao)) or padrao))
    except (TypeError, ValueError):
        return padrao


def _env_float(nome: str, padrao: float) -> float:
    try:
        return max(0.0, float(os.environ.get(nome, str(padrao)) or padrao))
    except (TypeError, ValueError):
        return padrao


def _tipos_requeridos() -> set[str]:
    bruto = os.environ.get(
        "SOCKET_DELTA_HEALTH_REQUIRED_CLIENTS",
        "apontador,arbitro,placar_profissional,visualizador_publico",
    )
    return {item.strip().lower() for item in bruto.split(",") if item.strip()}


def _grupo_cliente(tipo: object) -> str:
    texto = str(tipo or "desconhecido").strip().lower() or "desconhecido"
    if texto.startswith("arbitro_") or texto in {"primeiro_arbitro", "segundo_arbitro", "arbitro_unico"}:
        return "arbitro"
    return texto


class DeltaMetricsStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._iniciado_em = time.time()
        self._server = defaultdict(int)
        self._clients: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._renders: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self._origens: dict[str, deque[float]] = defaultdict(deque)

    def limpar(self) -> None:
        with self._lock:
            self._iniciado_em = time.time()
            self._server.clear()
            self._clients.clear()
            self._renders.clear()
            self._origens.clear()

    def registrar_delta_servidor(
        self,
        *,
        emitido: bool,
        bytes_delta: int = 0,
        bytes_estado: int = 0,
        economia_percentual: float = 0.0,
    ) -> None:
        with self._lock:
            self._server["deltas_avaliados"] += 1
            self._server["bytes_estado_equivalente"] += max(0, int(bytes_estado or 0))
            if emitido:
                self._server["deltas_emitidos"] += 1
                self._server["bytes_delta_emitidos"] += max(0, int(bytes_delta or 0))
                self._server["bytes_economizados"] += max(0, int(bytes_estado or 0) - int(bytes_delta or 0))
                self._server["economia_percentual_soma_milesimal"] += int(max(0.0, economia_percentual) * 1000)
            else:
                self._server["deltas_descartados"] += 1

    def registrar_publicacao(self, tipo: str) -> None:
        chave = {
            "legacy": "lotes_legados_emitidos",
            "snapshot": "snapshots_completos_emitidos",
            "placar": "placares_rapidos_emitidos",
        }.get(str(tipo or "").strip().lower())
        if not chave:
            return
        with self._lock:
            self._server[chave] += 1

    def permitir_origem(self, origem: object) -> bool:
        """Limite simples para impedir telemetria pública excessiva."""
        chave = str(origem or "desconhecida")[:120]
        agora = time.time()
        limite = _env_int("SOCKET_DELTA_TELEMETRY_MAX_PER_MINUTE", 120)
        if limite <= 0:
            return False
        with self._lock:
            fila = self._origens[chave]
            while fila and agora - fila[0] > 60:
                fila.popleft()
            if len(fila) >= limite:
                return False
            fila.append(agora)
            return True

    def registrar_cliente(self, tipo_cliente: object, evento: object, quantidade: object = 1) -> bool:
        evento_norm = str(evento or "").strip().lower()
        permitidos = {
            "delta_aplicado",
            "delta_antigo",
            "delta_invalido",
            "lacuna_versao",
            "snapshot_solicitado",
            "snapshot_aceito",
            "outra_partida",
        }
        if evento_norm not in permitidos:
            return False
        try:
            qtd = min(100, max(1, int(quantidade or 1)))
        except (TypeError, ValueError):
            qtd = 1
        tipo = _grupo_cliente(tipo_cliente)
        with self._lock:
            self._clients[tipo][evento_norm] += qtd
        return True


    def registrar_render_cliente(
        self,
        tipo_cliente: object,
        duracao_ms: object,
        quantidade_agregada: object = 1,
    ) -> bool:
        """Registra somente métricas agregadas da renderização no navegador."""
        try:
            duracao = float(duracao_ms)
            quantidade = max(1, min(100, int(quantidade_agregada or 1)))
        except (TypeError, ValueError):
            return False
        if duracao < 0 or duracao > 60_000:
            return False
        tipo = _grupo_cliente(tipo_cliente)
        with self._lock:
            dados = self._renders[tipo]
            dados["renderizacoes"] += 1
            dados["atualizacoes_agregadas"] += quantidade
            dados["duracao_total_ms"] += duracao
            dados["duracao_max_ms"] = max(dados.get("duracao_max_ms", 0.0), duracao)
        return True

    def _status_saude_locked(self) -> dict[str, Any]:
        total_aplicados = sum(dados.get("delta_aplicado", 0) for dados in self._clients.values())
        total_lacunas = sum(dados.get("lacuna_versao", 0) for dados in self._clients.values())
        total_processados = total_aplicados + total_lacunas
        taxa_lacunas = (100.0 * total_lacunas / total_processados) if total_processados else 0.0
        vistos = {tipo for tipo, dados in self._clients.items() if sum(dados.values()) > 0}
        requeridos = _tipos_requeridos()
        faltantes = sorted(requeridos - vistos)
        minimo = _env_int("SOCKET_DELTA_HEALTH_MIN_APPLIED", 50)
        max_lacunas = _env_float("SOCKET_DELTA_HEALTH_MAX_GAP_PERCENT", 1.0)
        homologado = total_aplicados >= minimo and taxa_lacunas <= max_lacunas and not faltantes
        return {
            "homologado": homologado,
            "deltas_aplicados": total_aplicados,
            "lacunas_versao": total_lacunas,
            "taxa_lacunas_percentual": round(taxa_lacunas, 3),
            "minimo_deltas_aplicados": minimo,
            "maximo_lacunas_percentual": max_lacunas,
            "tipos_clientes_vistos": sorted(vistos),
            "tipos_clientes_requeridos": sorted(requeridos),
            "tipos_clientes_faltantes": faltantes,
        }

    def esta_homologado(self) -> bool:
        with self._lock:
            return bool(self._status_saude_locked()["homologado"])

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            server = dict(self._server)
            emitidos = server.get("deltas_emitidos", 0)
            economia_media = (
                server.get("economia_percentual_soma_milesimal", 0) / 1000.0 / emitidos
                if emitidos else 0.0
            )
            server.pop("economia_percentual_soma_milesimal", None)
            server["economia_media_percentual"] = round(economia_media, 2)
            base = server.get("bytes_estado_equivalente", 0)
            server["economia_total_percentual"] = round(
                (100.0 * server.get("bytes_economizados", 0) / base) if base else 0.0,
                2,
            )
            renders = {}
            for tipo, dados in self._renders.items():
                renderizacoes = int(dados.get("renderizacoes", 0) or 0)
                total_ms = float(dados.get("duracao_total_ms", 0.0) or 0.0)
                renders[tipo] = {
                    "renderizacoes": renderizacoes,
                    "atualizacoes_agregadas": int(dados.get("atualizacoes_agregadas", 0) or 0),
                    "duracao_media_ms": round(total_ms / renderizacoes, 3) if renderizacoes else 0.0,
                    "duracao_max_ms": round(float(dados.get("duracao_max_ms", 0.0) or 0.0), 3),
                }
            return {
                "iniciado_em_epoch": self._iniciado_em,
                "duracao_segundos": round(max(0.0, time.time() - self._iniciado_em), 1),
                "servidor": server,
                "clientes": deepcopy({tipo: dict(dados) for tipo, dados in self._clients.items()}),
                "renderizacao": renders,
                "saude": self._status_saude_locked(),
            }


delta_metrics_store = DeltaMetricsStore()
