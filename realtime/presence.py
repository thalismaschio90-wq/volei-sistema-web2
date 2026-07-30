"""Presença operacional das telas conectadas ao tempo real.

Mantém heartbeat de aplicação por SID e partida. Usa Redis quando disponível,
permitindo que o dashboard enxergue clientes conectados em múltiplos workers.
"""
from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class PresencaCliente:
    sid: str
    partida_id: str
    perfil: str
    competicao: str
    cliente_id: str
    conectado_em: float
    ultimo_heartbeat: float
    latencia_ms: float
    estado_versao: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "sid": self.sid,
            "partida_id": self.partida_id,
            "perfil": self.perfil,
            "competicao": self.competicao,
            "cliente_id": self.cliente_id,
            "conectado_em": self.conectado_em,
            "ultimo_heartbeat": self.ultimo_heartbeat,
            "latencia_ms": self.latencia_ms,
            "estado_versao": self.estado_versao,
        }


def _numero(valor: Any, padrao: float = 0.0) -> float:
    try:
        return float(valor)
    except (TypeError, ValueError):
        return padrao


class PresenceStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._local: dict[str, dict[str, Any]] = {}
        self._ttl = max(30, int(os.getenv("REALTIME_HEARTBEAT_TTL", "45") or 45))
        self._redis = None
        redis_url = str(os.getenv("REDIS_URL") or os.getenv("REALTIME_REDIS_URL") or "").strip()
        if redis_url:
            try:
                import redis
                cliente = redis.Redis.from_url(
                    redis_url,
                    decode_responses=True,
                    socket_connect_timeout=2,
                    socket_timeout=2,
                    health_check_interval=30,
                )
                cliente.ping()
                self._redis = cliente
            except Exception:
                self._redis = None

    @property
    def backend(self) -> str:
        return "redis" if self._redis is not None else "local"

    def _key(self, sid: str) -> str:
        return f"vtp:presence:{sid}"

    def registrar(self, sid: str, dados: dict[str, Any] | None = None) -> dict[str, Any]:
        dados = dict(dados or {})
        agora = time.time()
        sid = str(sid or "").strip()
        if not sid:
            return {}
        enviado_em = _numero(dados.get("cliente_enviado_em_ms"), 0.0)
        latencia = max(0.0, agora * 1000.0 - enviado_em) if enviado_em else 0.0
        anterior = self.obter_sid(sid) or {}
        item = {
            "sid": sid,
            "partida_id": str(dados.get("partida_id") or anterior.get("partida_id") or "").strip(),
            "perfil": str(dados.get("perfil") or anterior.get("perfil") or "desconhecido").strip(),
            "competicao": str(dados.get("competicao") or anterior.get("competicao") or "").strip(),
            "cliente_id": str(dados.get("cliente_id") or dados.get("dispositivo_id") or anterior.get("cliente_id") or "").strip(),
            "conectado_em": _numero(anterior.get("conectado_em"), agora) or agora,
            "ultimo_heartbeat": agora,
            "latencia_ms": round(latencia, 2),
            "estado_versao": int(_numero(dados.get("estado_versao"), _numero(anterior.get("estado_versao"), 0))),
        }
        if self._redis is not None:
            self._redis.set(self._key(sid), json.dumps(item, separators=(",", ":"), ensure_ascii=False), ex=self._ttl)
        else:
            with self._lock:
                self._local[sid] = item
                self._limpar_local(agora)
        return dict(item)

    def obter_sid(self, sid: str) -> dict[str, Any] | None:
        sid = str(sid or "").strip()
        if not sid:
            return None
        if self._redis is not None:
            bruto = self._redis.get(self._key(sid))
            if not bruto:
                return None
            try:
                return dict(json.loads(bruto))
            except Exception:
                return None
        with self._lock:
            item = self._local.get(sid)
            return dict(item) if item else None

    def remover(self, sid: str) -> None:
        sid = str(sid or "").strip()
        if not sid:
            return
        if self._redis is not None:
            self._redis.delete(self._key(sid))
        else:
            with self._lock:
                self._local.pop(sid, None)

    def _limpar_local(self, agora: float | None = None) -> None:
        agora = agora or time.time()
        limite = agora - self._ttl
        for sid, item in list(self._local.items()):
            if _numero(item.get("ultimo_heartbeat"), 0) < limite:
                self._local.pop(sid, None)

    def snapshot(self) -> dict[str, Any]:
        agora = time.time()
        itens: list[dict[str, Any]] = []
        if self._redis is not None:
            for chave in self._redis.scan_iter(match="vtp:presence:*", count=200):
                bruto = self._redis.get(chave)
                if not bruto:
                    continue
                try:
                    itens.append(dict(json.loads(bruto)))
                except Exception:
                    continue
        else:
            with self._lock:
                self._limpar_local(agora)
                itens = [dict(v) for v in self._local.values()]

        por_perfil: dict[str, int] = {}
        por_partida: dict[str, dict[str, Any]] = {}
        for item in itens:
            perfil = str(item.get("perfil") or "desconhecido")
            por_perfil[perfil] = por_perfil.get(perfil, 0) + 1
            partida = str(item.get("partida_id") or "sem_partida")
            grupo = por_partida.setdefault(partida, {"partida_id": partida, "total": 0, "perfis": {}, "latencia_media_ms": 0.0, "clientes": []})
            grupo["total"] += 1
            grupo["perfis"][perfil] = grupo["perfis"].get(perfil, 0) + 1
            grupo["clientes"].append(item)
        for grupo in por_partida.values():
            latencias = [_numero(i.get("latencia_ms"), 0) for i in grupo["clientes"]]
            grupo["latencia_media_ms"] = round(sum(latencias) / len(latencias), 2) if latencias else 0.0

        return {
            "backend": self.backend,
            "ttl_segundos": self._ttl,
            "total_clientes": len(itens),
            "por_perfil": por_perfil,
            "partidas": sorted(por_partida.values(), key=lambda x: (-x["total"], x["partida_id"])),
            "gerado_em": agora,
        }


presence_store = PresenceStore()
