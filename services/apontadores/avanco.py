"""Coordenação protegida da atualização do chaveamento após finalizações."""
from __future__ import annotations

import threading
from typing import Any, Callable

_EXECUCAO: set[str] = set()
_LOCK = threading.Lock()


def _reservar(competicao: str) -> bool:
    with _LOCK:
        if competicao in _EXECUCAO:
            return False
        _EXECUCAO.add(competicao)
        return True


def _liberar(competicao: str) -> None:
    with _LOCK:
        _EXECUCAO.discard(competicao)


def atualizar(
    competicao: object,
    *,
    gerar: Callable[[str], Any],
) -> dict[str, Any]:
    nome = str(competicao or "").strip()
    if not nome:
        return {}
    if not _reservar(nome):
        return {"em_execucao": True}
    try:
        return gerar(nome) or {}
    except Exception as erro:
        print("AVISO apontador/atualizar_avanco_apos_finalizacao:", repr(erro), flush=True)
        return {}
    finally:
        _liberar(nome)


def atualizar_async(
    competicao: object,
    *,
    gerar: Callable[[str], Any],
    ao_concluir: Callable[[str], Any] | None = None,
) -> dict[str, Any]:
    nome = str(competicao or "").strip()
    if not nome:
        return {"agendado": False}
    if not _reservar(nome):
        return {"agendado": False, "em_execucao": True}

    def worker() -> None:
        try:
            gerar(nome)
            if ao_concluir:
                ao_concluir(nome)
        except Exception as erro:
            print("AVISO apontador/atualizar_avanco_async:", repr(erro), flush=True)
        finally:
            _liberar(nome)

    threading.Thread(target=worker, daemon=True).start()
    return {"agendado": True}
