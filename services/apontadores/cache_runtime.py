"""Caches curtos usados pelas telas do apontador.

Centraliza TTL, limites e invalidação para que a rota HTTP não mantenha vários
dicionários globais independentes.
"""
from __future__ import annotations

import os
import time
from threading import RLock
from typing import Any, Callable


class CacheTTL:
    def __init__(self, *, ttl: int, limite: int) -> None:
        self.ttl = max(1, int(ttl or 1))
        self.limite = max(10, int(limite or 10))
        self._lock = RLock()
        self._dados: dict[Any, tuple[float, Any]] = {}

    def obter(self, chave: Any, *, ttl: int | None = None):
        agora = time.time()
        validade = self.ttl if ttl is None else max(1, int(ttl or 1))
        with self._lock:
            item = self._dados.get(chave)
            if not item:
                return None
            criado, valor = item
            if agora - criado > validade:
                self._dados.pop(chave, None)
                return None
            return valor

    def salvar(self, chave: Any, valor: Any):
        with self._lock:
            if len(self._dados) >= self.limite and chave not in self._dados:
                mais_antiga = min(self._dados, key=lambda item: self._dados[item][0])
                self._dados.pop(mais_antiga, None)
            self._dados[chave] = (time.time(), valor)
        return valor

    def remover_se(self, predicado: Callable[[Any], bool]) -> None:
        with self._lock:
            for chave in list(self._dados):
                if predicado(chave):
                    self._dados.pop(chave, None)

    def limpar(self) -> None:
        with self._lock:
            self._dados.clear()


class CachesApontador:
    def __init__(self) -> None:
        self.painel = CacheTTL(
            ttl=int(os.environ.get("APONTADOR_PAINEL_CACHE_TTL", "12") or 12),
            limite=80,
        )
        self.home = CacheTTL(
            ttl=int(os.environ.get("APONTADOR_AUX_CACHE_TTL", "30") or 30),
            limite=200,
        )
        self.pin = CacheTTL(ttl=60, limite=200)

    def limpar_painel_competicao(self, competicao: object = None) -> None:
        if not competicao:
            self.painel.limpar()
            return
        prefixo = ("painel_competicao", str(competicao or "").strip())
        self.painel.remover_se(
            lambda chave: isinstance(chave, tuple) and chave[:2] == prefixo
        )

    def limpar_operacao(self, *, competicao: object = None, cpf: object = None) -> None:
        self.limpar_painel_competicao(competicao)
        if cpf:
            prefixo = ("home", str(cpf or "").strip())
            self.home.remover_se(
                lambda chave: isinstance(chave, tuple) and chave[:2] == prefixo
            )
        elif competicao:
            self.home.limpar()

        if competicao:
            nome = str(competicao or "").strip()
            self.pin.remover_se(
                lambda chave: isinstance(chave, tuple)
                and len(chave) >= 2
                and chave[1] == nome
            )

    def montar_home(
        self,
        *,
        cpf: object,
        cliente_id: object,
        pode_jogo_avulso: Callable[[str], bool],
        buscar_oficial: Callable[..., Any],
        listar_competicoes: Callable[[str], Any],
        offline_habilitado: Callable[[], bool],
    ) -> dict[str, Any]:
        cpf_texto = str(cpf or "").strip()
        chave = ("home", cpf_texto, cliente_id, "v3_jogo_rapido_global")
        cached = self.home.obter(chave, ttl=30)
        if cached is not None:
            return cached

        payload = {
            "cpf": cpf_texto,
            "oficial": None,
            "competicoes": [],
            "pode_jogo_avulso": False,
            "offline_habilitado": False,
        }
        if cpf_texto:
            payload["pode_jogo_avulso"] = bool(pode_jogo_avulso(cpf_texto))
            payload["oficial"] = buscar_oficial(cpf_texto, cliente_id=cliente_id)
            if payload["oficial"]:
                payload["competicoes"] = listar_competicoes(cpf_texto) or []
        try:
            payload["offline_habilitado"] = bool(offline_habilitado())
        except Exception:
            payload["offline_habilitado"] = False
        return self.home.salvar(chave, payload)

    def garantir_pin(
        self,
        *,
        competicao: object,
        login: object,
        gerar_pin: Callable[[str, str], Any],
    ):
        competicao_texto = str(competicao or "").strip()
        login_texto = str(login or "").strip()
        chave = ("pin", competicao_texto, login_texto)
        cached = self.pin.obter(chave, ttl=60)
        if cached is not None:
            return cached
        return self.pin.salvar(chave, gerar_pin(competicao_texto, login_texto))


caches_apontador = CachesApontador()
