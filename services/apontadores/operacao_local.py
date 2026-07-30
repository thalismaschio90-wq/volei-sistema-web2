"""Snapshot auxiliar da operação local do apontador.

O estado vivo (placar, saque, rotação e set) pertence à camada ``realtime``.
Este store mantém somente dados estáticos ou auxiliares usados entre pré-jogo,
papeleta e jogo.
"""
from __future__ import annotations

import time
from threading import RLock
from typing import Any


class OperacaoLocalStore:
    def __init__(self, *, limite: int = 200) -> None:
        self._limite = max(20, int(limite or 200))
        self._lock = RLock()
        self._dados: dict[tuple[str, int], dict[str, Any]] = {}

    @staticmethod
    def chave(partida_id: object, competicao: object) -> tuple[str, int]:
        return str(competicao or "").strip(), int(partida_id)

    def salvar(self, partida_id: object, competicao: object, partida=None, **extras):
        chave = self.chave(partida_id, competicao)
        with self._lock:
            if len(self._dados) >= self._limite and chave not in self._dados:
                mais_antiga = min(
                    self._dados,
                    key=lambda item: float(self._dados[item].get("atualizado_em") or 0),
                )
                self._dados.pop(mais_antiga, None)

            atual = dict(self._dados.get(chave) or {})
            if partida:
                atual["partida"] = dict(partida)
            atual.update({
                nome: valor
                for nome, valor in extras.items()
                if valor is not None and nome != "estado"
            })
            atual["atualizado_em"] = time.time()
            self._dados[chave] = atual
            return dict(atual)

    def obter(self, partida_id: object, competicao: object) -> dict[str, Any]:
        chave = self.chave(partida_id, competicao)
        with self._lock:
            return dict(self._dados.get(chave) or {})

    def partida(self, partida_id: object, competicao: object) -> dict[str, Any]:
        return dict(self.obter(partida_id, competicao).get("partida") or {})

    def remover(self, partida_id: object, competicao: object) -> None:
        with self._lock:
            self._dados.pop(self.chave(partida_id, competicao), None)

    def limpar(self) -> None:
        with self._lock:
            self._dados.clear()


operacao_local_store = OperacaoLocalStore()
